package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/agenteval"
	"mirrorcore/internal/database"
)

type planRow struct {
	SchemaVersion           int                       `json:"schema_version"`
	DatasetKind             string                    `json:"dataset_kind"`
	ExperimentID            string                    `json:"experiment_id"`
	CommonConfigFingerprint string                    `json:"common_config_fingerprint"`
	RunID                   string                    `json:"run_id,omitempty"`
	FixedRAGRunID           string                    `json:"fixed_rag_run_id,omitempty"`
	AgentRunID              string                    `json:"agent_run_id,omitempty"`
	CaseID                  string                    `json:"case_id"`
	VideoKey                string                    `json:"video_key"`
	VideoID                 string                    `json:"video_id,omitempty"`
	TargetResultID          string                    `json:"target_result_id,omitempty"`
	QuestionText            string                    `json:"question_text,omitempty"`
	Split                   string                    `json:"split"`
	Category                string                    `json:"category"`
	ExpectedStatus          string                    `json:"expected_status"`
	RequiredPointIDs        []string                  `json:"required_point_ids"`
	RequiredPoints          []agenteval.RequiredPoint `json:"required_points,omitempty"`
	RequiredEvidence        []agenteval.EvidenceGroup `json:"required_evidence_groups"`
	FixedRAGQuestionID      string                    `json:"fixed_rag_question_id"`
	AgentQuestionID         string                    `json:"agent_question_id"`
}

type questionRecord struct {
	ID                uuid.UUID
	VideoID           uuid.UUID
	ResultID          uuid.UUID
	TranscriptID      uuid.UUID
	QuestionText      string
	ResultType        string
	Status            string
	GenerationProfile []byte
	RetrievalProfile  []byte
	Answer            []byte
	Error             []byte
	CreatedAt         time.Time
	CompletedAt       *time.Time
}

type stepRecord struct {
	Kind             string
	PromptTokens     *int64
	CompletionTokens *int64
	UsageTokens      *int64
	Metadata         []byte
	CompletedAt      *time.Time
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(2)
	}
}

func run() error {
	planPath := flag.String("plan", "", "JSONL annotation plan with paired question IDs")
	outputPath := flag.String("output", "", "annotation draft path (default: stdout)")
	flag.Parse()
	if *planPath == "" || flag.NArg() != 0 {
		return errors.New("usage: qa-eval-export -plan annotation-plan.jsonl [-output annotation-draft.jsonl]")
	}
	plans, err := loadPlan(*planPath)
	if err != nil {
		return err
	}

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	drafts := make([]agenteval.Sample, 0, len(plans))
	seenQuestions := make(map[uuid.UUID]string, len(plans)*2)
	for _, plan := range plans {
		fixedID, _ := uuid.Parse(plan.FixedRAGQuestionID)
		agentID, _ := uuid.Parse(plan.AgentQuestionID)
		for _, id := range []uuid.UUID{fixedID, agentID} {
			if prior, exists := seenQuestions[id]; exists {
				return fmt.Errorf("question_id %s is reused by cases %q and %q", id, prior, plan.CaseID)
			}
			seenQuestions[id] = plan.CaseID
		}
		fixed, err := loadQuestion(ctx, pool, fixedID)
		if err != nil {
			return fmt.Errorf("case %q fixed_rag question: %w", plan.CaseID, err)
		}
		agent, err := loadQuestion(ctx, pool, agentID)
		if err != nil {
			return fmt.Errorf("case %q agent question: %w", plan.CaseID, err)
		}
		if fixed.ID == agent.ID || fixed.VideoID != agent.VideoID || fixed.ResultID != agent.ResultID {
			return fmt.Errorf("case %q question pair does not share a video and pinned result", plan.CaseID)
		}
		if plan.VideoID != "" {
			videoID, _ := uuid.Parse(plan.VideoID)
			if fixed.VideoID != videoID {
				return fmt.Errorf("case %q persisted question video does not match video_id in the plan", plan.CaseID)
			}
		}
		if plan.TargetResultID != "" {
			resultID, _ := uuid.Parse(plan.TargetResultID)
			if fixed.ResultID != resultID {
				return fmt.Errorf("case %q persisted result does not match target_result_id in the plan", plan.CaseID)
			}
		}
		if plan.QuestionText != "" && (fixed.QuestionText != plan.QuestionText || agent.QuestionText != plan.QuestionText) {
			return fmt.Errorf("case %q persisted question text does not match the frozen plan", plan.CaseID)
		}
		if !bytes.Equal(canonicalJSON(fixed.RetrievalProfile), canonicalJSON(agent.RetrievalProfile)) {
			return fmt.Errorf("case %q pair has different retrieval profiles", plan.CaseID)
		}
		fixedRun, fixedInitial, fixedProvided, err := buildRun(ctx, pool, fixed)
		if err != nil {
			return fmt.Errorf("case %q fixed_rag trace: %w", plan.CaseID, err)
		}
		agentRun, agentInitial, agentProvided, err := buildRun(ctx, pool, agent)
		if err != nil {
			return fmt.Errorf("case %q agent trace: %w", plan.CaseID, err)
		}
		if fixedRun.Strategy != "fixed_rag" || agentRun.Strategy != "agent" {
			return fmt.Errorf("case %q question IDs do not match their persisted strategies", plan.CaseID)
		}
		if !sameSeqs(fixedInitial, agentInitial) || !sameSeqs(fixedProvided, agentProvided) {
			return fmt.Errorf("case %q pair does not share both the initial ranking and provided context", plan.CaseID)
		}
		evidenceSentences, err := loadEvidenceSentences(ctx, pool, fixed.TranscriptID, plan.RequiredEvidence)
		if err != nil {
			return fmt.Errorf("case %q required evidence text: %w", plan.CaseID, err)
		}
		drafts = append(drafts, agenteval.Sample{
			SchemaVersion: agenteval.SchemaVersion, DatasetKind: plan.DatasetKind,
			ExperimentID: plan.ExperimentID, CommonConfigFingerprint: plan.CommonConfigFingerprint,
			CaseID: plan.CaseID, VideoKey: plan.VideoKey, QuestionText: fixed.QuestionText,
			Split: plan.Split, Category: plan.Category, ExpectedStatus: plan.ExpectedStatus,
			RequiredPointIDs: plan.RequiredPointIDs, RequiredPoints: plan.RequiredPoints,
			RequiredEvidence: plan.RequiredEvidence, EvidenceSentences: evidenceSentences,
			Runs: []agenteval.Run{fixedRun, agentRun},
		})
	}
	if err := agenteval.ValidateAnnotationDraft(drafts); err != nil {
		return fmt.Errorf("validate annotation draft: %w", err)
	}

	var output io.Writer = os.Stdout
	if *outputPath != "" {
		file, err := os.OpenFile(*outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("create annotation draft without overwriting an existing file: %w", err)
		}
		defer file.Close()
		output = file
	}
	encoder := json.NewEncoder(output)
	for _, draft := range drafts {
		if err := encoder.Encode(draft); err != nil {
			return fmt.Errorf("write annotation draft: %w", err)
		}
	}
	return nil
}

func loadPlan(path string) ([]planRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open annotation plan: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	plans := make([]planRow, 0, 64)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		var plan planRow
		if err := decoder.Decode(&plan); err != nil {
			return nil, fmt.Errorf("decode annotation plan line %d: %w", lineNo, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return nil, fmt.Errorf("annotation plan line %d must contain one JSON value", lineNo)
		}
		if plan.SchemaVersion != 1 {
			return nil, fmt.Errorf("annotation plan line %d schema_version must be 1", lineNo)
		}
		if plan.VideoID != "" {
			if _, err := uuid.Parse(plan.VideoID); err != nil {
				return nil, fmt.Errorf("annotation plan line %d video_id is invalid", lineNo)
			}
		}
		if plan.TargetResultID != "" {
			if _, err := uuid.Parse(plan.TargetResultID); err != nil {
				return nil, fmt.Errorf("annotation plan line %d target_result_id is invalid", lineNo)
			}
		}
		if _, err := uuid.Parse(plan.FixedRAGQuestionID); err != nil {
			return nil, fmt.Errorf("annotation plan line %d fixed_rag_question_id is invalid", lineNo)
		}
		if _, err := uuid.Parse(plan.AgentQuestionID); err != nil {
			return nil, fmt.Errorf("annotation plan line %d agent_question_id is invalid", lineNo)
		}
		if plan.FixedRAGQuestionID == plan.AgentQuestionID {
			return nil, fmt.Errorf("annotation plan line %d requires two distinct question IDs", lineNo)
		}
		plans = append(plans, plan)
		if len(plans) > 1000 {
			return nil, errors.New("annotation plan exceeds 1000 cases")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read annotation plan: %w", err)
	}
	if len(plans) == 0 {
		return nil, errors.New("annotation plan is empty")
	}
	return plans, nil
}

func loadQuestion(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (questionRecord, error) {
	var item questionRecord
	if err := pool.QueryRow(ctx, `SELECT q.id,q.video_id,q.result_id,COALESCE(pr.transcript_id,'00000000-0000-0000-0000-000000000000'::uuid),
		q.question_text,COALESCE(pr.result_type,''),q.status,q.generation_profile,
		q.retrieval_profile,q.answer_json,q.error,q.created_at,q.completed_at
		FROM questions q LEFT JOIN parse_results pr ON pr.id=q.result_id AND pr.video_id=q.video_id
		WHERE q.id=$1 AND q.purged_at IS NULL`, id).Scan(&item.ID, &item.VideoID, &item.ResultID,
		&item.TranscriptID, &item.QuestionText, &item.ResultType, &item.Status, &item.GenerationProfile, &item.RetrievalProfile,
		&item.Answer, &item.Error, &item.CreatedAt, &item.CompletedAt); errors.Is(err, pgx.ErrNoRows) {
		return questionRecord{}, errors.New("question not found or purged")
	} else if err != nil {
		return questionRecord{}, fmt.Errorf("load persisted question: %w", err)
	}
	if item.Status != "SUCCEEDED" && item.Status != "FAILED" && item.Status != "INTERRUPTED" {
		return questionRecord{}, fmt.Errorf("question is not terminal (status %s)", item.Status)
	}
	if item.CompletedAt == nil || item.CompletedAt.Before(item.CreatedAt) {
		return questionRecord{}, errors.New("terminal question lacks a valid completed_at")
	}
	if len(item.GenerationProfile) == 0 || !json.Valid(item.GenerationProfile) || len(item.RetrievalProfile) == 0 || !json.Valid(item.RetrievalProfile) {
		return questionRecord{}, errors.New("question profile is missing or invalid")
	}
	if item.ResultType != "TEXT" && item.ResultType != "NO_SPEECH" {
		return questionRecord{}, errors.New("question result type is missing or unsupported")
	}
	return item, nil
}

func loadEvidenceSentences(ctx context.Context, pool *pgxpool.Pool, transcriptID uuid.UUID,
	groups []agenteval.EvidenceGroup) ([]agenteval.EvidenceQuote, error) {
	seqSet := make(map[int]struct{})
	for _, group := range groups {
		for _, seq := range group.SentenceSeqs {
			seqSet[seq] = struct{}{}
		}
	}
	seqs := make([]int, 0, len(seqSet))
	for seq := range seqSet {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	if len(seqs) == 0 {
		return []agenteval.EvidenceQuote{}, nil
	}
	if transcriptID == uuid.Nil {
		return nil, errors.New("required evidence refers to a question without a transcript")
	}
	rows, err := pool.Query(ctx, `SELECT seq,text,start_ms,end_ms FROM transcript_segments
		WHERE transcript_id=$1 AND seq=ANY($2::integer[]) ORDER BY seq`, transcriptID, seqs)
	if err != nil {
		return nil, fmt.Errorf("query required transcript sentences: %w", err)
	}
	defer rows.Close()
	quotes := make([]agenteval.EvidenceQuote, 0, len(seqs))
	for rows.Next() {
		var quote agenteval.EvidenceQuote
		if err := rows.Scan(&quote.SentenceSeq, &quote.Text, &quote.StartMS, &quote.EndMS); err != nil {
			return nil, fmt.Errorf("scan required transcript sentence: %w", err)
		}
		quotes = append(quotes, quote)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate required transcript sentences: %w", err)
	}
	if len(quotes) != len(seqs) {
		return nil, fmt.Errorf("required evidence references %d transcript sentences, but only %d exist", len(seqs), len(quotes))
	}
	return quotes, nil
}

func buildRun(ctx context.Context, pool *pgxpool.Pool, question questionRecord) (agenteval.Run, []int, []int, error) {
	var profile struct {
		Strategy string `json:"strategy"`
	}
	if err := json.Unmarshal(question.GenerationProfile, &profile); err != nil || (profile.Strategy != "agent" && profile.Strategy != "fixed_rag") {
		return agenteval.Run{}, nil, nil, errors.New("persisted generation profile has no supported strategy")
	}
	configIdentity, err := json.Marshal(struct {
		Generation json.RawMessage `json:"generation_profile"`
		Retrieval  json.RawMessage `json:"retrieval_profile"`
	}{canonicalJSON(question.GenerationProfile), canonicalJSON(question.RetrievalProfile)})
	if err != nil {
		return agenteval.Run{}, nil, nil, fmt.Errorf("encode strategy profile: %w", err)
	}
	configHash := sha256.Sum256(configIdentity)
	result := agenteval.Run{Strategy: profile.Strategy, StrategyConfigID: "sha256:" + hex.EncodeToString(configHash[:]),
		QuestionID: question.ID.String(), ResultID: question.ResultID.String(), HumanReviewed: false,
		InitialRankedSentenceSeqs: []int{}, FinalRankedSentenceSeqs: []int{},
		InitialProvidedSeqs: []int{}, FinalProvidedSeqs: []int{}}
	if question.CompletedAt != nil {
		result.LatencyMS = question.CompletedAt.Sub(question.CreatedAt).Milliseconds()
	}
	switch question.Status {
	case "SUCCEEDED":
		result.ExecutionStatus = "succeeded"
		if len(question.Answer) == 0 || !json.Valid(question.Answer) {
			return agenteval.Run{}, nil, nil, errors.New("successful question has no valid answer_json")
		}
		var answer struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(question.Answer, &answer); err != nil {
			return agenteval.Run{}, nil, nil, fmt.Errorf("decode answer status: %w", err)
		}
		result.PredictedStatus = answer.Status
		result.RawAnswer = append(json.RawMessage(nil), question.Answer...)
	case "FAILED":
		result.ExecutionStatus = "failed"
	case "INTERRUPTED":
		result.ExecutionStatus = "interrupted"
	}
	if len(question.Error) > 0 && json.Valid(question.Error) {
		var failure struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(question.Error, &failure) == nil {
			result.BudgetExhausted = strings.Contains(failure.Code, "BUDGET_EXHAUSTED")
		}
	}

	rows, err := pool.Query(ctx, `SELECT kind,prompt_tokens,completion_tokens,usage_tokens,metadata,completed_at
		FROM question_steps WHERE question_id=$1 ORDER BY seq`, question.ID)
	if err != nil {
		return agenteval.Run{}, nil, nil, fmt.Errorf("read persisted question steps: %w", err)
	}
	defer rows.Close()
	var gotRetrieval bool
	var initialRanked, initialProvided []int
	seenRanked := make(map[int]struct{})
	seenProvided := make(map[int]struct{})
	toolEvidence := make([][]int, 0)
	initialTokenInput, outputTokens := int64(0), int64(0)
	hasInputTokens, hasOutputTokens := false, false
	for rows.Next() {
		var step stepRecord
		if err := rows.Scan(&step.Kind, &step.PromptTokens, &step.CompletionTokens, &step.UsageTokens,
			&step.Metadata, &step.CompletedAt); err != nil {
			return agenteval.Run{}, nil, nil, fmt.Errorf("scan question step: %w", err)
		}
		if step.CompletedAt == nil {
			return agenteval.Run{}, nil, nil, fmt.Errorf("question has an incomplete %s step; trace cannot be exported faithfully", step.Kind)
		}
		metadata, err := decodeMetadata(step.Metadata)
		if err != nil {
			return agenteval.Run{}, nil, nil, fmt.Errorf("decode %s step metadata: %w", step.Kind, err)
		}
		switch step.Kind {
		case "RETRIEVAL":
			if gotRetrieval {
				return agenteval.Run{}, nil, nil, errors.New("question has more than one initial retrieval step")
			}
			gotRetrieval = true
			initialRanked, err = metadataSeqs(metadata, "initial_ranked_sentence_seqs")
			if err != nil {
				return agenteval.Run{}, nil, nil, err
			}
			initialProvided, err = metadataSeqs(metadata, "initial_provided_sentence_seqs")
			if err != nil {
				return agenteval.Run{}, nil, nil, err
			}
			result.HTTPAttempts += metadataInt(metadata, "query_embedding_attempts")
			if value, ok := metadataInt64(metadata, "embedding_usage_tokens"); ok {
				initialTokenInput += value
				hasInputTokens = true
			} else if step.UsageTokens != nil {
				initialTokenInput += *step.UsageTokens
				hasInputTokens = true
			}
		case "GENERATION":
			result.HTTPAttempts += metadataInt(metadata, "http_attempts")
			if step.PromptTokens != nil {
				initialTokenInput += *step.PromptTokens
				hasInputTokens = true
			}
			if step.CompletionTokens != nil {
				outputTokens += *step.CompletionTokens
				hasOutputTokens = true
			}
		case "TOOL":
			result.ToolCalls++
			result.HTTPAttempts += metadataInt(metadata, "query_embedding_attempts")
			if count, ok := metadataInt64(metadata, "new_evidence_count"); !ok || count == 0 {
				result.NoProgressToolCalls++
			}
			if value, ok := metadataInt64(metadata, "usage_tokens"); ok {
				initialTokenInput += value
				hasInputTokens = true
			}
			added, err := metadataSeqs(metadata, "new_evidence_sentence_seqs")
			if err != nil {
				return agenteval.Run{}, nil, nil, err
			}
			toolEvidence = append(toolEvidence, added)
		}
	}
	if err := rows.Err(); err != nil {
		return agenteval.Run{}, nil, nil, fmt.Errorf("iterate question steps: %w", err)
	}
	if gotRetrieval {
		result.InitialRankedSentenceSeqs = append([]int{}, initialRanked...)
		result.InitialProvidedSeqs = append([]int{}, initialProvided...)
		result.FinalRankedSentenceSeqs = append([]int{}, initialRanked...)
		result.FinalProvidedSeqs = append([]int{}, initialProvided...)
		for _, seq := range initialRanked {
			seenRanked[seq] = struct{}{}
		}
		for _, seq := range initialProvided {
			seenProvided[seq] = struct{}{}
		}
		for _, added := range toolEvidence {
			for _, seq := range added {
				if _, exists := seenRanked[seq]; !exists {
					seenRanked[seq] = struct{}{}
					result.FinalRankedSentenceSeqs = append(result.FinalRankedSentenceSeqs, seq)
				}
				if _, exists := seenProvided[seq]; !exists {
					seenProvided[seq] = struct{}{}
					result.FinalProvidedSeqs = append(result.FinalProvidedSeqs, seq)
				}
			}
		}
	} else if len(toolEvidence) != 0 {
		return agenteval.Run{}, nil, nil, errors.New("question has tool evidence without an initial retrieval checkpoint")
	}
	if question.ResultType != "NO_SPEECH" && question.Status == "SUCCEEDED" && !gotRetrieval {
		return agenteval.Run{}, nil, nil, errors.New("successful speech question has no retrieval checkpoint")
	}
	if hasInputTokens {
		result.InputTokens = &initialTokenInput
	}
	if hasOutputTokens {
		result.OutputTokens = &outputTokens
	}
	// Cost is intentionally left unknown: the persisted local trace has no authoritative billing record.
	return result, initialRanked, initialProvided, nil
}

func decodeMetadata(raw []byte) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, errors.New("metadata is empty or invalid JSON")
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(raw, &metadata); err != nil || metadata == nil {
		return nil, errors.New("metadata must be a JSON object")
	}
	return metadata, nil
}

func metadataSeqs(metadata map[string]json.RawMessage, key string) ([]int, error) {
	raw, exists := metadata[key]
	if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return []int{}, nil
	}
	var seqs []int
	if err := json.Unmarshal(raw, &seqs); err != nil {
		return nil, fmt.Errorf("metadata field %q is not an integer array: %w", key, err)
	}
	return seqs, nil
}

func metadataInt(metadata map[string]json.RawMessage, key string) int {
	value, _ := metadataInt64(metadata, key)
	if value < 0 || value > int64(^uint(0)>>1) {
		return 0
	}
	return int(value)
}

func metadataInt64(metadata map[string]json.RawMessage, key string) (int64, bool) {
	raw, exists := metadata[key]
	if !exists {
		return 0, false
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
		return 0, false
	}
	return value, true
}

func canonicalJSON(raw []byte) []byte {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return canonical
}

func sameSeqs(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
