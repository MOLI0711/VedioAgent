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
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/agenteval"
	"mirrorcore/internal/config"
	"mirrorcore/internal/database"
	"mirrorcore/internal/localdemo"
	"mirrorcore/internal/providerquota"
	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/providers/qwen"
	"mirrorcore/internal/qa"
	"mirrorcore/internal/repository/postgres"
)

type planRow struct {
	SchemaVersion           int                       `json:"schema_version"`
	DatasetKind             string                    `json:"dataset_kind"`
	ExperimentID            string                    `json:"experiment_id"`
	CommonConfigFingerprint string                    `json:"common_config_fingerprint"`
	RunID                   string                    `json:"run_id"`
	FixedRAGRunID           string                    `json:"fixed_rag_run_id,omitempty"`
	AgentRunID              string                    `json:"agent_run_id,omitempty"`
	CaseID                  string                    `json:"case_id"`
	VideoKey                string                    `json:"video_key"`
	VideoID                 string                    `json:"video_id"`
	TargetResultID          string                    `json:"target_result_id"`
	QuestionText            string                    `json:"question_text"`
	Split                   string                    `json:"split"`
	Category                string                    `json:"category"`
	ExpectedStatus          string                    `json:"expected_status"`
	RequiredPointIDs        []string                  `json:"required_point_ids"`
	RequiredPoints          []agenteval.RequiredPoint `json:"required_points,omitempty"`
	RequiredEvidence        []agenteval.EvidenceGroup `json:"required_evidence_groups"`
	FixedRAGQuestionID      string                    `json:"fixed_rag_question_id,omitempty"`
	AgentQuestionID         string                    `json:"agent_question_id,omitempty"`
}

type runService struct {
	strategy string
	service  *qa.Service
}

type initialTrace struct {
	Ranked   []int
	Provided []int
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(2)
	}
}

func run() error {
	planPath := flag.String("plan", "", "frozen JSONL annotation plan with video/result/question")
	outputPath := flag.String("output", "", "paired run plan output (default: stdout; existing file is never overwritten)")
	allowProviderCalls := flag.Bool("allow-provider-calls", false, "explicitly allow billable Bailian Embedding/Qwen requests")
	maxCases := flag.Int("max-cases", 1, "maximum cases to execute (1..50); raise explicitly for larger runs")
	flag.Parse()
	if *planPath == "" || flag.NArg() != 0 {
		return errors.New("usage: qa-eval-run -plan frozen-plan.jsonl [-output run-plan.jsonl] [-allow-provider-calls]")
	}
	plans, err := loadPlan(*planPath)
	if err != nil {
		return err
	}
	if *maxCases < 1 || *maxCases > 50 || len(plans) > *maxCases {
		return fmt.Errorf("plan has %d cases; set -max-cases from 1 to 50 to cover it", len(plans))
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.LocalDemoMode {
		if *allowProviderCalls {
			return errors.New("LOCAL_DEMO_MODE cannot be combined with -allow-provider-calls")
		}
		for _, plan := range plans {
			if plan.DatasetKind != agenteval.DatasetSynthetic {
				return errors.New("LOCAL_DEMO_MODE can only produce dataset_kind=synthetic_fixture")
			}
		}
	} else {
		if !*allowProviderCalls {
			return errors.New("refusing real-provider execution; pass -allow-provider-calls to explicitly permit potentially billable requests")
		}
		if !cfg.Bailian.EmbeddingEnabled() {
			return errors.New("Bailian workspace and API key are required for real evaluation runs")
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	services, err := buildServices(ctx, cfg, pool)
	if err != nil {
		return err
	}
	output, closeOutput, err := openOutput(*outputPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	writer := bufio.NewWriter(output)

	for index, plan := range plans {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("evaluation stopped after %d/%d cases: %w", index, len(plans), err)
		}
		videoID, _ := uuid.Parse(plan.VideoID)
		resultID, _ := uuid.Parse(plan.TargetResultID)
		var ownerID uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT owner_id FROM videos WHERE id=$1 AND lifecycle='ACTIVE'`, videoID).Scan(&ownerID); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("case %q video_id is missing or not active", plan.CaseID)
		} else if err != nil {
			return fmt.Errorf("case %q load video owner: %w", plan.CaseID, err)
		}

		fixedQuestion, err := executeRun(ctx, pool, ownerID, videoID, resultID, plan, services[0])
		if err != nil {
			return fmt.Errorf("case %q fixed_rag: %w", plan.CaseID, err)
		}
		agentQuestion, err := executeRun(ctx, pool, ownerID, videoID, resultID, plan, services[1])
		if err != nil {
			return fmt.Errorf("case %q agent: %w", plan.CaseID, err)
		}
		if fixedQuestion.ID == agentQuestion.ID || fixedQuestion.ResultID == nil || agentQuestion.ResultID == nil ||
			*fixedQuestion.ResultID != resultID || *agentQuestion.ResultID != resultID {
			return fmt.Errorf("case %q pair did not preserve distinct questions pinned to target_result_id", plan.CaseID)
		}
		if plan.FixedRAGQuestionID != "" && (plan.FixedRAGQuestionID != fixedQuestion.ID.String() ||
			plan.AgentQuestionID != agentQuestion.ID.String()) {
			return fmt.Errorf("case %q persisted question IDs differ from the IDs already recorded in the plan", plan.CaseID)
		}
		fixedTrace, err := readInitialTrace(ctx, pool, fixedQuestion.ID)
		if err != nil {
			return fmt.Errorf("case %q fixed_rag initial trace: %w", plan.CaseID, err)
		}
		agentTrace, err := readInitialTrace(ctx, pool, agentQuestion.ID)
		if err != nil {
			return fmt.Errorf("case %q agent initial trace: %w", plan.CaseID, err)
		}
		if !sameSeqs(fixedTrace.Ranked, agentTrace.Ranked) || !sameSeqs(fixedTrace.Provided, agentTrace.Provided) {
			return fmt.Errorf("case %q pair has different initial ranking or provided context; refusing to emit an unpaired run", plan.CaseID)
		}
		plan.FixedRAGQuestionID = fixedQuestion.ID.String()
		plan.AgentQuestionID = agentQuestion.ID.String()
		if plan.FixedRAGRunID == "" {
			plan.FixedRAGRunID = plan.RunID
		}
		if plan.AgentRunID == "" {
			plan.AgentRunID = plan.RunID
		}
		encoded, err := json.Marshal(plan)
		if err != nil {
			return fmt.Errorf("case %q encode run plan: %w", plan.CaseID, err)
		}
		if _, err := writer.Write(append(encoded, '\n')); err != nil {
			return fmt.Errorf("write case %q run plan: %w", plan.CaseID, err)
		}
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("flush case %q run plan: %w", plan.CaseID, err)
		}
		log.Printf("completed evaluation case %d/%d (%s); plan row written", index+1, len(plans), plan.CaseID)
	}
	return nil
}

func buildServices(ctx context.Context, cfg config.Config, pool *pgxpool.Pool) ([]runService, error) {
	repository := postgres.NewQuestionRepository(pool)
	if cfg.LocalDemoMode {
		embedder := localdemo.Embedder{}
		generator := localdemo.Generator{}
		agentService, err := qa.NewLocalDemoService(repository, embedder, generator, "qa-eval-run-"+uuid.NewString())
		if err != nil {
			return nil, err
		}
		fixedService, err := qa.NewLocalDemoFixedRAGEvaluationService(repository, embedder, generator, "qa-eval-run-fixed-"+uuid.NewString())
		if err != nil {
			return nil, err
		}
		return []runService{{strategy: qa.StrategyFixedRAG, service: fixedService},
			{strategy: qa.StrategyAgent, service: agentService}}, nil
	}

	embeddingLimiter, err := providerquota.New(ctx, pool, cfg.Bailian.WorkspaceID, embedding.Model,
		cfg.Bailian.EmbeddingRPM, cfg.Bailian.EmbeddingTPM)
	if err != nil {
		return nil, fmt.Errorf("initialize Embedding quota gate: %w", err)
	}
	embeddingClient, err := embedding.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
	if err != nil {
		return nil, err
	}
	queryEmbedder, err := providerquota.NewEmbeddingAdapter(embeddingClient, embeddingLimiter)
	if err != nil {
		return nil, err
	}
	qwenLimiter, err := providerquota.New(ctx, pool, cfg.Bailian.WorkspaceID, qwen.Model,
		cfg.Bailian.QwenRPM, cfg.Bailian.QwenTPM)
	if err != nil {
		return nil, fmt.Errorf("initialize Qwen quota gate: %w", err)
	}
	qwenClient, err := qwen.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
	if err != nil {
		return nil, err
	}
	answerGenerator, err := providerquota.NewQwenAdapter(qwenClient, qwenLimiter)
	if err != nil {
		return nil, err
	}
	agentService, err := qa.NewService(repository, queryEmbedder, answerGenerator, cfg.Bailian.WorkspaceID, "qa-eval-run-"+uuid.NewString())
	if err != nil {
		return nil, err
	}
	fixedService, err := qa.NewFixedRAGEvaluationService(repository, queryEmbedder, answerGenerator,
		cfg.Bailian.WorkspaceID, "qa-eval-run-fixed-"+uuid.NewString())
	if err != nil {
		return nil, err
	}
	return []runService{{strategy: qa.StrategyFixedRAG, service: fixedService},
		{strategy: qa.StrategyAgent, service: agentService}}, nil
}

func executeRun(ctx context.Context, pool *pgxpool.Pool, ownerID, videoID, resultID uuid.UUID,
	plan planRow, strategy runService) (postgres.Question, error) {
	requestKey := requestKey(plan, strategy.strategy)
	question, created, err := strategy.service.Admit(ctx, qa.CreateInput{OwnerID: ownerID, VideoID: videoID,
		RequiredResultID: &resultID, RequestKey: requestKey, Question: plan.QuestionText})
	if err != nil {
		return postgres.Question{}, fmt.Errorf("admit question: %w", err)
	}
	if question.ResultID == nil || *question.ResultID != resultID || question.VideoID != videoID || question.QuestionText != plan.QuestionText {
		return postgres.Question{}, errors.New("idempotent question does not match the frozen video/result/question inputs")
	}
	if err := validatePersistedStrategy(question.GenerationProfile, strategy.strategy); err != nil {
		return postgres.Question{}, err
	}
	if !created {
		switch question.Status {
		case "SUCCEEDED":
			return question, nil
		case "FAILED", "INTERRUPTED":
			return postgres.Question{}, fmt.Errorf("stable run key already has terminal status %s; assign a new strategy-specific run ID to retry only this side", question.Status)
		default:
			return postgres.Question{}, fmt.Errorf("stable run key already has nonterminal status %s; wait for its owner to finish before retrying", question.Status)
		}
	}
	log.Printf("starting evaluation run case=%s strategy=%s", plan.CaseID, strategy.strategy)
	runCtx, cancel := context.WithDeadline(ctx, question.DeadlineAt)
	defer cancel()
	leaseDone := make(chan struct{})
	go renewQuestionLease(runCtx, cancel, strategy.service, question, leaseDone)
	answer, runErr := strategy.service.Run(runCtx, question, func(stage string) error {
		log.Printf("evaluation progress case=%s strategy=%s stage=%s", plan.CaseID, strategy.strategy, stage)
		return runCtx.Err()
	})
	runContextErr := runCtx.Err()
	cancel()
	<-leaseDone
	if runErr != nil {
		if errors.Is(runErr, postgres.ErrQuestionExecutionLost) {
			return postgres.Question{}, fmt.Errorf("run question lost its execution lease: %w", runErr)
		}
		status, code, message := "FAILED", "EVALUATION_RUN_FAILED", "evaluation run failed"
		var typed *qa.RunError
		if errors.As(runErr, &typed) && typed.Code != "" {
			code = typed.Code
		}
		if errors.Is(runContextErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			status, code, message = "INTERRUPTED", "EVALUATION_RUN_CANCELLED", "evaluation run interrupted"
		}
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
		finishErr := strategy.service.Finish(finishCtx, question, status, code, message)
		finishCancel()
		if finishErr != nil {
			return postgres.Question{}, errors.Join(fmt.Errorf("run question: %w", runErr), fmt.Errorf("persist run terminal state: %w", finishErr))
		}
		return postgres.Question{}, fmt.Errorf("run question (%s)", code)
	}
	if answer.Status != "answered" && answer.Status != "partial" && answer.Status != "insufficient" {
		return postgres.Question{}, errors.New("question service returned an invalid terminal answer status")
	}
	question.Status = "SUCCEEDED"
	return question, nil
}

func renewQuestionLease(ctx context.Context, cancel context.CancelFunc, service *qa.Service,
	question postgres.Question, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(context.Background(), 2*time.Second)
			renewed, err := service.RenewLease(renewCtx, question)
			renewCancel()
			if err != nil || !renewed {
				cancel()
				return
			}
		}
	}
}

func requestKey(plan planRow, strategy string) string {
	runID := plan.RunID
	if strategy == qa.StrategyFixedRAG && plan.FixedRAGRunID != "" {
		runID = plan.FixedRAGRunID
	}
	if strategy == qa.StrategyAgent && plan.AgentRunID != "" {
		runID = plan.AgentRunID
	}
	identity, _ := json.Marshal(struct {
		Version                 string                    `json:"version"`
		RunID                   string                    `json:"run_id"`
		ExperimentID            string                    `json:"experiment_id"`
		CommonConfigFingerprint string                    `json:"common_config_fingerprint"`
		CaseID                  string                    `json:"case_id"`
		VideoKey                string                    `json:"video_key"`
		VideoID                 string                    `json:"video_id"`
		TargetResultID          string                    `json:"target_result_id"`
		QuestionText            string                    `json:"question_text"`
		Split                   string                    `json:"split"`
		Category                string                    `json:"category"`
		ExpectedStatus          string                    `json:"expected_status"`
		RequiredPointIDs        []string                  `json:"required_point_ids"`
		RequiredPoints          []agenteval.RequiredPoint `json:"required_points,omitempty"`
		RequiredEvidence        []agenteval.EvidenceGroup `json:"required_evidence_groups"`
		Strategy                string                    `json:"strategy"`
	}{"mirrorcore-qa-eval-v1", runID, plan.ExperimentID, plan.CommonConfigFingerprint, plan.CaseID,
		plan.VideoKey, plan.VideoID, plan.TargetResultID, plan.QuestionText, plan.Split, plan.Category,
		plan.ExpectedStatus, plan.RequiredPointIDs, plan.RequiredPoints, plan.RequiredEvidence, strategy})
	hash := sha256.Sum256(identity)
	return "qa-eval-" + hex.EncodeToString(hash[:])
}

func validatePersistedStrategy(raw json.RawMessage, expected string) error {
	var profile struct {
		Strategy string `json:"strategy"`
	}
	if err := json.Unmarshal(raw, &profile); err != nil || profile.Strategy != expected {
		return fmt.Errorf("persisted question profile strategy does not match %s", expected)
	}
	return nil
}

func readInitialTrace(ctx context.Context, pool *pgxpool.Pool, questionID uuid.UUID) (initialTrace, error) {
	var raw []byte
	err := pool.QueryRow(ctx, `SELECT metadata FROM question_steps WHERE question_id=$1 AND kind='RETRIEVAL' ORDER BY seq LIMIT 1`, questionID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return initialTrace{Ranked: []int{}, Provided: []int{}}, nil
	}
	if err != nil {
		return initialTrace{}, err
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return initialTrace{}, fmt.Errorf("decode retrieval metadata: %w", err)
	}
	trace := initialTrace{Ranked: []int{}, Provided: []int{}}
	if value := metadata["initial_ranked_sentence_seqs"]; len(value) > 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		if err := json.Unmarshal(value, &trace.Ranked); err != nil {
			return initialTrace{}, fmt.Errorf("decode initial ranked sentences: %w", err)
		}
	}
	if value := metadata["initial_provided_sentence_seqs"]; len(value) > 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		if err := json.Unmarshal(value, &trace.Provided); err != nil {
			return initialTrace{}, fmt.Errorf("decode initial provided sentences: %w", err)
		}
	}
	return trace, nil
}

func loadPlan(path string) ([]planRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open frozen plan: %w", err)
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
			return nil, fmt.Errorf("decode frozen plan line %d: %w", lineNo, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return nil, fmt.Errorf("frozen plan line %d must contain exactly one JSON value", lineNo)
		}
		if plan.SchemaVersion != agenteval.SchemaVersion {
			return nil, fmt.Errorf("frozen plan line %d schema_version must be %d", lineNo, agenteval.SchemaVersion)
		}
		if plan.RunID == "" || len(plan.RunID) > 128 || strings.TrimSpace(plan.RunID) != plan.RunID {
			return nil, fmt.Errorf("frozen plan line %d requires a trimmed run_id up to 128 bytes", lineNo)
		}
		for label, value := range map[string]string{"fixed_rag_run_id": plan.FixedRAGRunID, "agent_run_id": plan.AgentRunID} {
			if value != "" && (len(value) > 128 || strings.TrimSpace(value) != value) {
				return nil, fmt.Errorf("frozen plan line %d %s must be trimmed and at most 128 bytes", lineNo, label)
			}
		}
		videoID, err := uuid.Parse(plan.VideoID)
		if err != nil || videoID == uuid.Nil {
			return nil, fmt.Errorf("frozen plan line %d video_id is invalid", lineNo)
		}
		resultID, err := uuid.Parse(plan.TargetResultID)
		if err != nil || resultID == uuid.Nil {
			return nil, fmt.Errorf("frozen plan line %d target_result_id is invalid", lineNo)
		}
		questionText := strings.TrimSpace(plan.QuestionText)
		if questionText == "" || len(questionText) > 4096 {
			return nil, fmt.Errorf("frozen plan line %d question_text must contain 1 to 4096 bytes", lineNo)
		}
		plan.QuestionText = questionText
		if plan.DatasetKind == agenteval.DatasetHuman && len(plan.RequiredPointIDs) > 0 && len(plan.RequiredPoints) != len(plan.RequiredPointIDs) {
			return nil, fmt.Errorf("frozen plan line %d human annotations require one required_points description for each required_point_id", lineNo)
		}
		if (plan.FixedRAGQuestionID == "") != (plan.AgentQuestionID == "") {
			return nil, fmt.Errorf("frozen plan line %d must contain both question IDs or neither", lineNo)
		}
		if plan.FixedRAGQuestionID != "" {
			fixedID, fixedErr := uuid.Parse(plan.FixedRAGQuestionID)
			agentID, agentErr := uuid.Parse(plan.AgentQuestionID)
			if fixedErr != nil || agentErr != nil || fixedID == agentID {
				return nil, fmt.Errorf("frozen plan line %d has invalid or duplicate question IDs", lineNo)
			}
		}
		plans = append(plans, plan)
		if len(plans) > 1000 {
			return nil, errors.New("frozen plan exceeds 1000 cases")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read frozen plan: %w", err)
	}
	if len(plans) == 0 {
		return nil, errors.New("frozen plan is empty")
	}
	if err := validatePlanLabels(plans); err != nil {
		return nil, err
	}
	return plans, nil
}

func validatePlanLabels(plans []planRow) error {
	samples := make([]agenteval.Sample, 0, len(plans))
	for _, plan := range plans {
		samples = append(samples, agenteval.Sample{
			SchemaVersion: plan.SchemaVersion, DatasetKind: plan.DatasetKind,
			ExperimentID: plan.ExperimentID, CommonConfigFingerprint: plan.CommonConfigFingerprint,
			CaseID: plan.CaseID, VideoKey: plan.VideoKey, Split: plan.Split, Category: plan.Category,
			QuestionText: plan.QuestionText, ExpectedStatus: plan.ExpectedStatus,
			RequiredPointIDs: plan.RequiredPointIDs, RequiredPoints: plan.RequiredPoints, RequiredEvidence: plan.RequiredEvidence,
			Runs: []agenteval.Run{
				{Strategy: "fixed_rag", StrategyConfigID: "pending-fixed-rag", ExecutionStatus: "succeeded",
					PredictedStatus: plan.ExpectedStatus, InitialRankedSentenceSeqs: []int{}, FinalRankedSentenceSeqs: []int{}},
				{Strategy: "agent", StrategyConfigID: "pending-agent", ExecutionStatus: "succeeded",
					PredictedStatus: plan.ExpectedStatus, InitialRankedSentenceSeqs: []int{}, FinalRankedSentenceSeqs: []int{}},
			},
		})
	}
	return agenteval.ValidateAnnotationDraft(samples)
}

func openOutput(path string) (io.Writer, func(), error) {
	if path == "" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, func() {}, fmt.Errorf("create run plan without overwriting existing data: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
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
