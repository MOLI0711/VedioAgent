package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/config"
	"mirrorcore/internal/database"
	"mirrorcore/internal/localdemo"
	"mirrorcore/internal/qa"
	"mirrorcore/internal/repository/postgres"
)

type strategyRun struct {
	Strategy                  string `json:"strategy"`
	QuestionID                string `json:"question_id"`
	ResultID                  string `json:"result_id"`
	Status                    string `json:"status"`
	AnswerStatus              string `json:"answer_status"`
	InitialRankedSentenceSeqs []int  `json:"initial_ranked_sentence_seqs"`
	FinalRankedSentenceSeqs   []int  `json:"final_ranked_sentence_seqs"`
	ToolCalls                 int    `json:"tool_calls"`
	NoProgressToolCalls       int    `json:"no_progress_tool_calls"`
	ElapsedMS                 int64  `json:"elapsed_ms_local_fixture"`
}

type report struct {
	DatasetKind               string        `json:"dataset_kind"`
	Interpretation            string        `json:"interpretation"`
	ResultPinMismatchRejected bool          `json:"result_pin_mismatch_rejected"`
	SameInitialRetrieval      bool          `json:"same_initial_retrieval"`
	QuestionsRetained         bool          `json:"questions_retained_for_trace_export"`
	Runs                      []strategyRun `json:"runs"`
}

func main() {
	keepQuestions := flag.Bool("keep-questions", false, "retain the two local fixture questions for cmd/qa-eval-export")
	flag.Parse()
	if flag.NArg() != 0 {
		log.Print("unexpected positional arguments")
		os.Exit(2)
	}
	if err := run(*keepQuestions); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(keepQuestions bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.LocalDemoMode {
		return errors.New("refusing to run deterministic QA strategies unless LOCAL_DEMO_MODE=true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	var ownerID, videoID, requiredResultID uuid.UUID
	err = pool.QueryRow(ctx, `SELECT t.owner_id,t.video_id,v.active_result_id
		FROM parse_tasks t JOIN videos v ON v.id=t.video_id
		WHERE t.config_snapshot->>'profile'='local_demo_media_pipeline_v1'
		AND t.status='SUCCEEDED' AND v.lifecycle='ACTIVE' AND v.active_result_id IS NOT NULL
		ORDER BY t.created_at DESC LIMIT 1`).Scan(&ownerID, &videoID, &requiredResultID)
	if err != nil {
		return fmt.Errorf("find a completed active local media fixture (run cmd/seed-media-demo first): %w", err)
	}

	repository := postgres.NewQuestionRepository(pool)
	localEmbedder := localdemo.Embedder{}
	localGenerator := localdemo.Generator{}
	agentService, err := qa.NewLocalDemoService(repository, localEmbedder, localGenerator, "qa-strategy-smoke-agent")
	if err != nil {
		return err
	}
	fixedService, err := qa.NewLocalDemoFixedRAGEvaluationService(repository, localEmbedder, localGenerator, "qa-strategy-smoke-fixed")
	if err != nil {
		return err
	}
	wrongResultID := uuid.New()
	_, _, pinErr := fixedService.Admit(ctx, qa.CreateInput{OwnerID: ownerID, VideoID: videoID,
		RequiredResultID: &wrongResultID, RequestKey: "local-demo-qa-result-pin-" + uuid.NewString(),
		Question: "snapshot precondition smoke"})
	if !errors.Is(pinErr, postgres.ErrQuestionResultChanged) {
		return fmt.Errorf("expected a mismatched result pin to be rejected, got %v", pinErr)
	}
	services := []struct {
		strategy string
		service  *qa.Service
	}{{qa.StrategyFixedRAG, fixedService}, {qa.StrategyAgent, agentService}}
	created := make([]uuid.UUID, 0, len(services))
	defer func() {
		if keepQuestions {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range created {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM questions WHERE id=$1`, id)
		}
	}()

	results := make([]strategyRun, 0, len(services))
	for _, item := range services {
		requestKey := "local-demo-qa-strategy-smoke-" + uuid.NewString()
		question, createdNow, err := item.service.Admit(ctx, qa.CreateInput{OwnerID: ownerID, VideoID: videoID,
			RequiredResultID: &requiredResultID,
			RequestKey:       requestKey, Question: "这个视频中的字幕来源和执行进度如何保证可追溯？"})
		if err != nil {
			return fmt.Errorf("admit %s question: %w", item.strategy, err)
		}
		if !createdNow {
			return fmt.Errorf("admit %s question unexpectedly replayed an existing idempotency key", item.strategy)
		}
		created = append(created, question.ID)
		if question.ResultID == nil || *question.ResultID != requiredResultID {
			return fmt.Errorf("%s question did not pin the requested parse result", item.strategy)
		}
		started := time.Now()
		answer, err := item.service.Run(ctx, question, nil)
		elapsedMS := time.Since(started).Milliseconds()
		if err != nil {
			return fmt.Errorf("run %s question: %w", item.strategy, err)
		}
		run, err := loadRunTrace(ctx, pool, question.ID, *question.ResultID,
			item.strategy, answer.Status, elapsedMS)
		if err != nil {
			return err
		}
		results = append(results, run)
	}
	if len(results) != 2 || results[0].ResultID != results[1].ResultID ||
		!sameSeqs(results[0].InitialRankedSentenceSeqs, results[1].InitialRankedSentenceSeqs) {
		return errors.New("fixed RAG and Agent did not share the same parse result and initial retrieval ranking")
	}
	result := report{DatasetKind: "synthetic_fixture", ResultPinMismatchRejected: true,
		Interpretation:       "Local deterministic wiring smoke only; elapsed_ms_local_fixture is not provider or production latency and does not measure retrieval quality, answer quality, model usage, or cost.",
		SameInitialRetrieval: true, QuestionsRetained: keepQuestions, Runs: results}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func loadRunTrace(ctx context.Context, pool *pgxpool.Pool, questionID, resultID uuid.UUID,
	strategy, answerStatus string, elapsedMS int64) (strategyRun, error) {
	var rawRetrieval []byte
	if err := pool.QueryRow(ctx, `SELECT metadata->'initial_ranked_sentence_seqs' FROM question_steps
		WHERE question_id=$1 AND kind='RETRIEVAL' ORDER BY seq LIMIT 1`, questionID).Scan(&rawRetrieval); err != nil {
		return strategyRun{}, fmt.Errorf("read %s initial retrieval trace: %w", strategy, err)
	}
	var initial []int
	if err := json.Unmarshal(rawRetrieval, &initial); err != nil {
		return strategyRun{}, fmt.Errorf("decode %s initial retrieval trace: %w", strategy, err)
	}
	result := strategyRun{Strategy: strategy, QuestionID: questionID.String(), ResultID: resultID.String(), Status: "SUCCEEDED",
		AnswerStatus: answerStatus, InitialRankedSentenceSeqs: initial,
		FinalRankedSentenceSeqs: append([]int(nil), initial...), ElapsedMS: elapsedMS}
	seen := make(map[int]struct{}, len(initial))
	for _, seq := range initial {
		seen[seq] = struct{}{}
	}
	rows, err := pool.Query(ctx, `SELECT metadata->'new_evidence_sentence_seqs',
		COALESCE((metadata->>'new_evidence_count')::integer,0)
		FROM question_steps WHERE question_id=$1 AND kind='TOOL' ORDER BY seq`, questionID)
	if err != nil {
		return strategyRun{}, fmt.Errorf("read %s tool trace: %w", strategy, err)
	}
	defer rows.Close()
	for rows.Next() {
		var rawSeqs []byte
		var newEvidenceCount int
		if err := rows.Scan(&rawSeqs, &newEvidenceCount); err != nil {
			return strategyRun{}, fmt.Errorf("scan %s tool trace: %w", strategy, err)
		}
		result.ToolCalls++
		if newEvidenceCount == 0 {
			result.NoProgressToolCalls++
		}
		if len(rawSeqs) == 0 || string(rawSeqs) == "null" {
			continue
		}
		var added []int
		if err := json.Unmarshal(rawSeqs, &added); err != nil {
			return strategyRun{}, fmt.Errorf("decode %s tool evidence trace: %w", strategy, err)
		}
		for _, seq := range added {
			if _, exists := seen[seq]; exists {
				continue
			}
			seen[seq] = struct{}{}
			result.FinalRankedSentenceSeqs = append(result.FinalRankedSentenceSeqs, seq)
		}
	}
	if err := rows.Err(); err != nil {
		return strategyRun{}, fmt.Errorf("iterate %s tool trace: %w", strategy, err)
	}
	return result, nil
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
