package agenteval

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const SchemaVersion = 1

const (
	DatasetSynthetic = "synthetic_fixture"
	DatasetHuman     = "human_annotated"
)

type Sample struct {
	SchemaVersion           int             `json:"schema_version"`
	DatasetKind             string          `json:"dataset_kind"`
	ExperimentID            string          `json:"experiment_id"`
	CommonConfigFingerprint string          `json:"common_config_fingerprint"`
	CaseID                  string          `json:"case_id"`
	VideoKey                string          `json:"video_key"`
	QuestionText            string          `json:"question_text,omitempty"`
	Split                   string          `json:"split"`
	Category                string          `json:"category"`
	ExpectedStatus          string          `json:"expected_status"`
	RequiredPointIDs        []string        `json:"required_point_ids"`
	RequiredPoints          []RequiredPoint `json:"required_points,omitempty"`
	RequiredEvidence        []EvidenceGroup `json:"required_evidence_groups"`
	EvidenceSentences       []EvidenceQuote `json:"evidence_sentences,omitempty"`
	Runs                    []Run           `json:"runs"`
}

type RequiredPoint struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

type EvidenceQuote struct {
	SentenceSeq int    `json:"sentence_seq"`
	Text        string `json:"text"`
	StartMS     int64  `json:"start_ms"`
	EndMS       int64  `json:"end_ms"`
}

type EvidenceGroup struct {
	ID           string `json:"id"`
	SentenceSeqs []int  `json:"sentence_seqs"`
}

type Run struct {
	Strategy                  string          `json:"strategy"`
	StrategyConfigID          string          `json:"strategy_config_id"`
	QuestionID                string          `json:"question_id,omitempty"`
	ResultID                  string          `json:"result_id,omitempty"`
	ExecutionStatus           string          `json:"execution_status"`
	PredictedStatus           string          `json:"predicted_status,omitempty"`
	InitialRankedSentenceSeqs []int           `json:"initial_ranked_sentence_seqs"`
	FinalRankedSentenceSeqs   []int           `json:"final_ranked_sentence_seqs"`
	InitialProvidedSeqs       []int           `json:"initial_provided_sentence_seqs"`
	FinalProvidedSeqs         []int           `json:"final_provided_sentence_seqs"`
	RawAnswer                 json.RawMessage `json:"raw_answer_json,omitempty"`
	HumanReviewed             bool            `json:"human_reviewed"`
	HumanReviewerID           string          `json:"human_reviewer_id,omitempty"`
	Claims                    []Claim         `json:"claims,omitempty"`
	ToolCalls                 int             `json:"tool_calls"`
	NoProgressToolCalls       int             `json:"no_progress_tool_calls"`
	HTTPAttempts              int             `json:"http_attempts"`
	BudgetExhausted           bool            `json:"budget_exhausted"`
	InputTokens               *int64          `json:"input_tokens,omitempty"`
	OutputTokens              *int64          `json:"output_tokens,omitempty"`
	CostMicros                *int64          `json:"cost_micros,omitempty"`
	LatencyMS                 int64           `json:"latency_ms"`
}

type Claim struct {
	AnswerBlockID string     `json:"answer_block_id,omitempty"`
	PointID       string     `json:"point_id,omitempty"`
	Correct       bool       `json:"correct"`
	Supported     bool       `json:"supported_by_evidence"`
	Citations     []Citation `json:"citations,omitempty"`
}

type Citation struct {
	SentenceSeq   int  `json:"sentence_seq"`
	SupportsClaim bool `json:"supports_claim"`
}

type Measurement struct {
	Value       *float64 `json:"value"`
	Numerator   float64  `json:"numerator"`
	Denominator int      `json:"denominator"`
}

type PercentileMeasurement struct {
	Value       *float64 `json:"value"`
	SampleCount int      `json:"sample_count"`
	Rank        int      `json:"rank"`
}

type Summary struct {
	Strategy                     string                `json:"strategy"`
	StrategyConfigID             string                `json:"strategy_config_id"`
	Split                        string                `json:"split"`
	Category                     string                `json:"category"`
	VideoCount                   int                   `json:"video_count"`
	Runs                         int                   `json:"runs"`
	Succeeded                    int                   `json:"succeeded"`
	Failed                       int                   `json:"failed"`
	Interrupted                  int                   `json:"interrupted"`
	HumanReviewedRuns            int                   `json:"human_reviewed_runs"`
	CompletionRate               Measurement           `json:"completion_rate"`
	ExpectedStatusAccuracy       Measurement           `json:"expected_status_accuracy"`
	InitialEvidenceGroupCoverage Measurement           `json:"initial_evidence_group_coverage"`
	FinalEvidenceGroupCoverage   Measurement           `json:"final_evidence_group_coverage"`
	FinalEvidenceGroupComplete   Measurement           `json:"final_evidence_group_complete_rate"`
	InitialGroupMRR              Measurement           `json:"initial_evidence_group_mrr"`
	FinalGroupMRR                Measurement           `json:"final_evidence_group_mrr"`
	CorrectPointCoverage         Measurement           `json:"correct_point_coverage"`
	GroundedPointCoverage        Measurement           `json:"grounded_point_coverage"`
	ClaimPrecision               Measurement           `json:"claim_precision"`
	CitationSupportPrecision     Measurement           `json:"citation_support_precision"`
	UnsupportedClaimRate         Measurement           `json:"unsupported_claim_rate"`
	MeanToolCalls                Measurement           `json:"mean_tool_calls"`
	MeanNoProgressToolCalls      Measurement           `json:"mean_no_progress_tool_calls"`
	BudgetExhaustedRate          Measurement           `json:"budget_exhausted_rate"`
	MeanHTTPAttempts             Measurement           `json:"mean_http_attempts"`
	MeanInputTokens              Measurement           `json:"mean_input_tokens"`
	MeanOutputTokens             Measurement           `json:"mean_output_tokens"`
	MeanCostMicros               Measurement           `json:"mean_cost_micros"`
	MeanLatencyMS                Measurement           `json:"mean_latency_ms"`
	P95LatencyMS                 PercentileMeasurement `json:"p95_latency_ms"`
}

type Comparison struct {
	CaseID                          string      `json:"case_id"`
	Split                           string      `json:"split"`
	Category                        string      `json:"category"`
	PairedCases                     int         `json:"paired_cases"`
	FinalEvidenceGroupCoverageDelta Measurement `json:"final_evidence_group_coverage_delta"`
	GroundedPointCoverageDelta      Measurement `json:"grounded_point_coverage_delta"`
	ExpectedStatusAccuracyDelta     Measurement `json:"expected_status_accuracy_delta"`
	LatencyMSDelta                  Measurement `json:"latency_ms_delta"`
	CostMicrosDelta                 Measurement `json:"cost_micros_delta"`
	ToolCallsDelta                  Measurement `json:"tool_calls_delta"`
	NoProgressCallsDelta            Measurement `json:"no_progress_calls_delta"`
}

type Report struct {
	SchemaVersion           int          `json:"schema_version"`
	DatasetKind             string       `json:"dataset_kind"`
	ExperimentID            string       `json:"experiment_id"`
	CommonConfigFingerprint string       `json:"common_config_fingerprint"`
	Interpretation          string       `json:"interpretation"`
	Cases                   int          `json:"cases"`
	VideoCount              int          `json:"video_count"`
	Runs                    int          `json:"runs"`
	Summaries               []Summary    `json:"summaries"`
	PairedComparisons       []Comparison `json:"paired_comparisons"`
}

func LoadJSONL(reader io.Reader) ([]Sample, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	samples := make([]Sample, 0, 64)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var sample Sample
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&sample); err != nil {
			return nil, fmt.Errorf("decode evaluation JSONL line %d: %w", lineNo, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, fmt.Errorf("evaluation JSONL line %d contains more than one JSON value", lineNo)
			}
			return nil, fmt.Errorf("decode trailing evaluation JSONL line %d: %w", lineNo, err)
		}
		if err := validateSample(sample); err != nil {
			return nil, fmt.Errorf("invalid evaluation JSONL line %d: %w", lineNo, err)
		}
		samples = append(samples, sample)
		if len(samples) > 10_000 {
			return nil, errors.New("evaluation dataset exceeds 10000 cases")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read evaluation JSONL: %w", err)
	}
	if len(samples) == 0 {
		return nil, errors.New("evaluation dataset is empty")
	}
	if err := validateDataset(samples); err != nil {
		return nil, err
	}
	return samples, nil
}

func Evaluate(samples []Sample) (Report, error) {
	if len(samples) == 0 {
		return Report{}, errors.New("evaluation dataset is empty")
	}
	for _, sample := range samples {
		if err := validateSample(sample); err != nil {
			return Report{}, fmt.Errorf("invalid evaluation case %q: %w", sample.CaseID, err)
		}
	}
	if err := validateDataset(samples); err != nil {
		return Report{}, err
	}
	datasetKind := samples[0].DatasetKind
	groups := make(map[summaryKey][]runSample)
	pairs := make(map[pairKey]map[string]runSample)
	totalRuns := 0
	videoKeys := make(map[string]struct{}, len(samples))
	for _, sample := range samples {
		videoKeys[sample.VideoKey] = struct{}{}
		pair := pairKey{split: sample.Split, category: sample.Category, caseID: sample.CaseID}
		pairs[pair] = make(map[string]runSample, len(sample.Runs))
		for _, run := range sample.Runs {
			key := summaryKey{strategy: run.Strategy, split: sample.Split, category: sample.Category}
			entry := runSample{sample: sample, run: run}
			groups[key] = append(groups[key], entry)
			pairs[pair][run.Strategy] = entry
			totalRuns++
		}
	}

	keys := make([]summaryKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].less(keys[j]) })
	summaries := make([]Summary, 0, len(keys))
	for _, key := range keys {
		summaries = append(summaries, summarize(key, groups[key]))
	}
	pairKeys := make([]pairKey, 0, len(pairs))
	for key := range pairs {
		pairKeys = append(pairKeys, key)
	}
	sort.Slice(pairKeys, func(i, j int) bool { return pairKeys[i].less(pairKeys[j]) })
	comparisons := make([]Comparison, 0)
	for _, key := range pairKeys {
		comparisons = append(comparisons, comparePair(key, pairs[key]))
	}
	interpretation := "Synthetic fixture only: these metrics validate the evaluator and do not measure ASR, retrieval quality, answer quality, model cost, or Agent benefit."
	if datasetKind == DatasetHuman {
		interpretation = "Human-annotated evaluation data. Report sample counts and holdout results; this report alone does not establish production generalization."
	}
	return Report{SchemaVersion: SchemaVersion, DatasetKind: datasetKind, ExperimentID: samples[0].ExperimentID,
		CommonConfigFingerprint: samples[0].CommonConfigFingerprint, Interpretation: interpretation,
		Cases: len(samples), VideoCount: len(videoKeys), Runs: totalRuns, Summaries: summaries, PairedComparisons: comparisons}, nil
}

// ValidateAnnotationDraft checks an exporter-produced draft without treating
// successful human-annotated answers as reviewed evaluation data.
func ValidateAnnotationDraft(samples []Sample) error {
	if len(samples) == 0 {
		return errors.New("annotation draft is empty")
	}
	for _, sample := range samples {
		if err := validateSampleMode(sample, false); err != nil {
			return fmt.Errorf("invalid annotation draft case %q: %w", sample.CaseID, err)
		}
	}
	return validateDataset(samples)
}

type summaryKey struct{ strategy, split, category string }

func (k summaryKey) less(other summaryKey) bool {
	if k.strategy != other.strategy {
		return k.strategy < other.strategy
	}
	if k.split != other.split {
		return k.split < other.split
	}
	return k.category < other.category
}

type pairKey struct{ split, category, caseID string }

func (k pairKey) less(other pairKey) bool {
	if k.split != other.split {
		return k.split < other.split
	}
	if k.category != other.category {
		return k.category < other.category
	}
	return k.caseID < other.caseID
}

type runSample struct {
	sample Sample
	run    Run
}

type accum struct {
	runs, succeeded, failed, interrupted, reviewed int
	statusCorrect, statusN                         int
	initialCoverage, finalCoverage, complete       []float64
	initialMRR, finalMRR                           []float64
	correctPointCoverage, groundedPointCoverage    []float64
	claimCorrect, claimTotal, citationSupported    int
	citationTotal, unsupportedClaims               int
	toolCalls, noProgressCalls, httpAttempts       []float64
	budgetExhausted                                int
	inputTokens, outputTokens, costs               []float64
	latencies                                      []float64
}

func summarize(key summaryKey, entries []runSample) Summary {
	var a accum
	videoKeys := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		sample, run := entry.sample, entry.run
		videoKeys[sample.VideoKey] = struct{}{}
		a.runs++
		switch run.ExecutionStatus {
		case "succeeded":
			a.succeeded++
		case "failed":
			a.failed++
		case "interrupted":
			a.interrupted++
		}
		if run.PredictedStatus != "" && run.ExecutionStatus == "succeeded" {
			a.statusN++
			if run.PredictedStatus == sample.ExpectedStatus {
				a.statusCorrect++
			}
		}
		if len(sample.RequiredEvidence) > 0 {
			initialCoverage, _, initialMRR := evidenceMetrics(sample.RequiredEvidence, run.InitialRankedSentenceSeqs)
			finalCoverage, finalComplete, finalMRR := evidenceMetrics(sample.RequiredEvidence, run.FinalRankedSentenceSeqs)
			a.initialCoverage = append(a.initialCoverage, initialCoverage)
			a.finalCoverage = append(a.finalCoverage, finalCoverage)
			a.complete = append(a.complete, boolFloat(finalComplete))
			a.initialMRR = append(a.initialMRR, initialMRR)
			a.finalMRR = append(a.finalMRR, finalMRR)
		}
		if run.HumanReviewed && run.ExecutionStatus == "succeeded" {
			a.reviewed++
			if len(sample.RequiredPointIDs) > 0 {
				correct, grounded := pointCoverage(sample.RequiredPointIDs, run.Claims)
				a.correctPointCoverage = append(a.correctPointCoverage, correct)
				a.groundedPointCoverage = append(a.groundedPointCoverage, grounded)
			}
			for _, claim := range run.Claims {
				a.claimTotal++
				if claim.Correct {
					a.claimCorrect++
				}
				if !claim.Supported {
					a.unsupportedClaims++
				}
				for _, citation := range claim.Citations {
					a.citationTotal++
					if citation.SupportsClaim {
						a.citationSupported++
					}
				}
			}
		}
		a.toolCalls = append(a.toolCalls, float64(run.ToolCalls))
		a.noProgressCalls = append(a.noProgressCalls, float64(run.NoProgressToolCalls))
		a.httpAttempts = append(a.httpAttempts, float64(run.HTTPAttempts))
		if run.BudgetExhausted {
			a.budgetExhausted++
		}
		if run.InputTokens != nil {
			a.inputTokens = append(a.inputTokens, float64(*run.InputTokens))
		}
		if run.OutputTokens != nil {
			a.outputTokens = append(a.outputTokens, float64(*run.OutputTokens))
		}
		if run.CostMicros != nil {
			a.costs = append(a.costs, float64(*run.CostMicros))
		}
		a.latencies = append(a.latencies, float64(run.LatencyMS))
	}
	latencies := append([]float64(nil), a.latencies...)
	sort.Float64s(latencies)
	return Summary{
		Strategy: key.strategy, StrategyConfigID: entries[0].run.StrategyConfigID,
		Split: key.split, Category: key.category, VideoCount: len(videoKeys),
		Runs: a.runs, Succeeded: a.succeeded, Failed: a.failed, Interrupted: a.interrupted,
		HumanReviewedRuns:            a.reviewed,
		CompletionRate:               metric(float64(a.succeeded), a.runs),
		ExpectedStatusAccuracy:       metric(float64(a.statusCorrect), a.statusN),
		InitialEvidenceGroupCoverage: mean(a.initialCoverage), FinalEvidenceGroupCoverage: mean(a.finalCoverage),
		FinalEvidenceGroupComplete: mean(a.complete), InitialGroupMRR: mean(a.initialMRR), FinalGroupMRR: mean(a.finalMRR),
		CorrectPointCoverage: mean(a.correctPointCoverage), GroundedPointCoverage: mean(a.groundedPointCoverage),
		ClaimPrecision:           metric(float64(a.claimCorrect), a.claimTotal),
		CitationSupportPrecision: metric(float64(a.citationSupported), a.citationTotal),
		UnsupportedClaimRate:     metric(float64(a.unsupportedClaims), a.claimTotal),
		MeanToolCalls:            mean(a.toolCalls), MeanNoProgressToolCalls: mean(a.noProgressCalls),
		BudgetExhaustedRate: metric(float64(a.budgetExhausted), a.runs), MeanHTTPAttempts: mean(a.httpAttempts),
		MeanInputTokens: mean(a.inputTokens), MeanOutputTokens: mean(a.outputTokens), MeanCostMicros: mean(a.costs),
		MeanLatencyMS: mean(a.latencies), P95LatencyMS: percentile95(latencies),
	}
}

func comparePair(key pairKey, pair map[string]runSample) Comparison {
	fixed, agent := pair["fixed_rag"], pair["agent"]
	c := Comparison{CaseID: key.caseID, Split: key.split, Category: key.category, PairedCases: 1}
	if len(fixed.sample.RequiredEvidence) > 0 {
		fixedCoverage, _, _ := evidenceMetrics(fixed.sample.RequiredEvidence, fixed.run.FinalRankedSentenceSeqs)
		agentCoverage, _, _ := evidenceMetrics(agent.sample.RequiredEvidence, agent.run.FinalRankedSentenceSeqs)
		c.FinalEvidenceGroupCoverageDelta = metric(agentCoverage-fixedCoverage, 1)
	}
	if len(fixed.sample.RequiredPointIDs) > 0 && fixed.run.HumanReviewed && agent.run.HumanReviewed &&
		fixed.run.ExecutionStatus == "succeeded" && agent.run.ExecutionStatus == "succeeded" {
		_, fixedGrounded := pointCoverage(fixed.sample.RequiredPointIDs, fixed.run.Claims)
		_, agentGrounded := pointCoverage(agent.sample.RequiredPointIDs, agent.run.Claims)
		c.GroundedPointCoverageDelta = metric(agentGrounded-fixedGrounded, 1)
	}
	if fixed.run.ExecutionStatus == "succeeded" && agent.run.ExecutionStatus == "succeeded" &&
		fixed.run.PredictedStatus != "" && agent.run.PredictedStatus != "" {
		fixedCorrect := boolFloat(fixed.run.PredictedStatus == fixed.sample.ExpectedStatus)
		agentCorrect := boolFloat(agent.run.PredictedStatus == agent.sample.ExpectedStatus)
		c.ExpectedStatusAccuracyDelta = metric(agentCorrect-fixedCorrect, 1)
	}
	c.LatencyMSDelta = metric(float64(agent.run.LatencyMS-fixed.run.LatencyMS), 1)
	c.ToolCallsDelta = metric(float64(agent.run.ToolCalls-fixed.run.ToolCalls), 1)
	c.NoProgressCallsDelta = metric(float64(agent.run.NoProgressToolCalls-fixed.run.NoProgressToolCalls), 1)
	if fixed.run.CostMicros != nil && agent.run.CostMicros != nil {
		c.CostMicrosDelta = metric(float64(*agent.run.CostMicros-*fixed.run.CostMicros), 1)
	}
	return c
}

func evidenceMetrics(groups []EvidenceGroup, ranked []int) (coverage float64, complete bool, mrr float64) {
	if len(groups) == 0 {
		return 0, false, 0
	}
	ranks := make(map[int]int, len(ranked))
	for i, seq := range ranked {
		ranks[seq] = i + 1
	}
	hits := 0
	reciprocalSum := 0.0
	for _, group := range groups {
		best := 0
		for _, seq := range group.SentenceSeqs {
			if rank := ranks[seq]; rank > 0 && (best == 0 || rank < best) {
				best = rank
			}
		}
		if best > 0 {
			hits++
			reciprocalSum += 1 / float64(best)
		}
	}
	return float64(hits) / float64(len(groups)), hits == len(groups), reciprocalSum / float64(len(groups))
}

func pointCoverage(required []string, claims []Claim) (correctCoverage, groundedCoverage float64) {
	correct := make(map[string]struct{})
	grounded := make(map[string]struct{})
	for _, claim := range claims {
		if claim.PointID == "" || !claim.Correct {
			continue
		}
		correct[claim.PointID] = struct{}{}
		if claim.Supported {
			grounded[claim.PointID] = struct{}{}
		}
	}
	for id := range correct {
		if !contains(required, id) {
			delete(correct, id)
		}
	}
	for id := range grounded {
		if !contains(required, id) {
			delete(grounded, id)
		}
	}
	return float64(len(correct)) / float64(len(required)), float64(len(grounded)) / float64(len(required))
}

func metric(numerator float64, denominator int) Measurement {
	if denominator <= 0 {
		return Measurement{Numerator: numerator}
	}
	value := numerator / float64(denominator)
	return Measurement{Value: &value, Numerator: numerator, Denominator: denominator}
}

func mean(values []float64) Measurement {
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return metric(sum, len(values))
}

func percentile95(sorted []float64) PercentileMeasurement {
	if len(sorted) == 0 {
		return PercentileMeasurement{}
	}
	index := int(math.Ceil(0.95*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	value := sorted[index]
	return PercentileMeasurement{Value: &value, SampleCount: len(sorted), Rank: index + 1}
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func validateDataset(samples []Sample) error {
	if len(samples) == 0 {
		return errors.New("evaluation dataset is empty")
	}
	kind := samples[0].DatasetKind
	experimentID := samples[0].ExperimentID
	commonFingerprint := samples[0].CommonConfigFingerprint
	caseIDs := make(map[string]struct{}, len(samples))
	questionIDs := make(map[string]string)
	videoSplits := make(map[string]string)
	strategyConfigIDs := make(map[string]string)
	for _, sample := range samples {
		if sample.DatasetKind != kind {
			return errors.New("dataset_kind must be consistent across the JSONL file")
		}
		if sample.ExperimentID != experimentID || sample.CommonConfigFingerprint != commonFingerprint {
			return errors.New("experiment_id and common_config_fingerprint must be consistent across the JSONL file")
		}
		if _, exists := caseIDs[sample.CaseID]; exists {
			return fmt.Errorf("duplicate case_id %q", sample.CaseID)
		}
		caseIDs[sample.CaseID] = struct{}{}
		if split, exists := videoSplits[sample.VideoKey]; exists && split != sample.Split {
			return fmt.Errorf("video_key %q appears in both %q and %q; split by source video to prevent leakage",
				sample.VideoKey, split, sample.Split)
		}
		videoSplits[sample.VideoKey] = sample.Split
		for _, run := range sample.Runs {
			if run.QuestionID != "" {
				if prior, exists := questionIDs[run.QuestionID]; exists {
					return fmt.Errorf("question_id %q is reused by cases %q and %q", run.QuestionID, prior, sample.CaseID)
				}
				questionIDs[run.QuestionID] = sample.CaseID
			}
			if configID, exists := strategyConfigIDs[run.Strategy]; exists && configID != run.StrategyConfigID {
				return fmt.Errorf("strategy_config_id for %q must be consistent across the JSONL file", run.Strategy)
			}
			strategyConfigIDs[run.Strategy] = run.StrategyConfigID
		}
	}
	return nil
}

func validateSample(sample Sample) error {
	return validateSampleMode(sample, true)
}

func validateSampleMode(sample Sample, requireHumanReview bool) error {
	if sample.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version must be %d", SchemaVersion)
	}
	if sample.DatasetKind != DatasetSynthetic && sample.DatasetKind != DatasetHuman {
		return errors.New("dataset_kind must be synthetic_fixture or human_annotated")
	}
	if strings.TrimSpace(sample.ExperimentID) == "" || len(sample.ExperimentID) > 128 ||
		strings.TrimSpace(sample.CommonConfigFingerprint) == "" || len(sample.CommonConfigFingerprint) > 128 {
		return errors.New("experiment_id and common_config_fingerprint are required and bounded")
	}
	if strings.TrimSpace(sample.CaseID) == "" || len(sample.CaseID) > 128 || strings.TrimSpace(sample.VideoKey) == "" || len(sample.VideoKey) > 128 {
		return errors.New("case_id and video_key are required and bounded")
	}
	if sample.QuestionText != "" && (strings.TrimSpace(sample.QuestionText) != sample.QuestionText || len(sample.QuestionText) > 4096) {
		return errors.New("question_text must be trimmed and at most 4096 bytes")
	}
	if sample.Split != "dev" && sample.Split != "holdout" {
		return errors.New("split must be dev or holdout")
	}
	if sample.Category != "direct" && sample.Category != "multi_span" && sample.Category != "partial_support" && sample.Category != "insufficient" {
		return errors.New("category must be direct, multi_span, partial_support, or insufficient")
	}
	if !validStatus(sample.ExpectedStatus) {
		return errors.New("expected_status is invalid")
	}
	switch sample.Category {
	case "direct", "multi_span":
		if sample.ExpectedStatus != "answered" || len(sample.RequiredEvidence) == 0 || len(sample.RequiredPointIDs) == 0 {
			return errors.New("direct and multi_span cases require answered status, evidence groups, and required points")
		}
	case "partial_support":
		if sample.ExpectedStatus != "partial" || len(sample.RequiredEvidence) == 0 || len(sample.RequiredPointIDs) == 0 {
			return errors.New("partial_support cases require partial status, evidence groups, and required points")
		}
	}
	points := make(map[string]struct{}, len(sample.RequiredPointIDs))
	for _, id := range sample.RequiredPointIDs {
		if strings.TrimSpace(id) == "" || len(id) > 128 {
			return errors.New("required point IDs must be nonempty and bounded")
		}
		if _, exists := points[id]; exists {
			return fmt.Errorf("duplicate required point ID %q", id)
		}
		points[id] = struct{}{}
	}
	if len(sample.RequiredPoints) > 0 {
		pointDetails := make(map[string]struct{}, len(sample.RequiredPoints))
		for _, point := range sample.RequiredPoints {
			if strings.TrimSpace(point.ID) == "" || len(point.ID) > 128 || strings.TrimSpace(point.Description) == "" ||
				strings.TrimSpace(point.Description) != point.Description || len(point.Description) > 2048 {
				return errors.New("required point descriptions must have trimmed IDs and descriptions up to 2048 bytes")
			}
			if _, exists := points[point.ID]; !exists {
				return fmt.Errorf("required point description %q has no matching required_point_id", point.ID)
			}
			if _, exists := pointDetails[point.ID]; exists {
				return fmt.Errorf("duplicate required point description %q", point.ID)
			}
			pointDetails[point.ID] = struct{}{}
		}
		if len(pointDetails) != len(points) {
			return errors.New("required_points must describe every required_point_id when supplied")
		}
	}
	quoteSeqs := make(map[int]struct{}, len(sample.EvidenceSentences))
	for _, quote := range sample.EvidenceSentences {
		if quote.SentenceSeq <= 0 || strings.TrimSpace(quote.Text) == "" || !utf8.ValidString(quote.Text) || strings.ContainsRune(quote.Text, '\x00') ||
			quote.StartMS < 0 || quote.EndMS <= quote.StartMS || len(quote.Text) > 16384 {
			return errors.New("evidence_sentences contain an invalid sequence, text, or time range")
		}
		if _, exists := quoteSeqs[quote.SentenceSeq]; exists {
			return fmt.Errorf("duplicate evidence sentence %d", quote.SentenceSeq)
		}
		quoteSeqs[quote.SentenceSeq] = struct{}{}
	}
	for _, group := range sample.RequiredEvidence {
		for _, seq := range group.SentenceSeqs {
			if len(quoteSeqs) > 0 {
				if _, exists := quoteSeqs[seq]; !exists {
					return fmt.Errorf("required evidence sentence %d has no matching evidence_sentences quote", seq)
				}
			}
		}
	}
	if sample.Category == "insufficient" && (sample.ExpectedStatus != "insufficient" || len(points) != 0 || len(sample.RequiredEvidence) != 0) {
		return errors.New("insufficient cases require insufficient status and no required points/evidence groups")
	}
	groupIDs := make(map[string]struct{}, len(sample.RequiredEvidence))
	for _, group := range sample.RequiredEvidence {
		if strings.TrimSpace(group.ID) == "" || len(group.SentenceSeqs) == 0 {
			return errors.New("evidence groups require an ID and at least one sentence sequence")
		}
		if _, exists := groupIDs[group.ID]; exists {
			return fmt.Errorf("duplicate evidence group ID %q", group.ID)
		}
		groupIDs[group.ID] = struct{}{}
		if err := validateSeqs(group.SentenceSeqs); err != nil {
			return fmt.Errorf("evidence group %q: %w", group.ID, err)
		}
	}
	if len(sample.Runs) != 2 {
		return errors.New("each case must contain one fixed_rag run and one agent run")
	}
	runs := make(map[string]Run, 2)
	for _, run := range sample.Runs {
		if run.Strategy != "fixed_rag" && run.Strategy != "agent" {
			return errors.New("run strategy must be fixed_rag or agent")
		}
		if strings.TrimSpace(run.StrategyConfigID) == "" || len(run.StrategyConfigID) > 128 {
			return fmt.Errorf("run %q requires a bounded strategy_config_id", run.Strategy)
		}
		if run.QuestionID != "" || run.ResultID != "" {
			if run.QuestionID == "" || run.ResultID == "" {
				return fmt.Errorf("run %q must provide both question_id and result_id", run.Strategy)
			}
			if _, err := uuid.Parse(run.QuestionID); err != nil {
				return fmt.Errorf("run %q question_id is invalid", run.Strategy)
			}
			if _, err := uuid.Parse(run.ResultID); err != nil {
				return fmt.Errorf("run %q result_id is invalid", run.Strategy)
			}
		}
		if _, exists := runs[run.Strategy]; exists {
			return fmt.Errorf("duplicate strategy %q", run.Strategy)
		}
		if run.ExecutionStatus != "succeeded" && run.ExecutionStatus != "failed" && run.ExecutionStatus != "interrupted" {
			return fmt.Errorf("run %q has invalid execution_status", run.Strategy)
		}
		if run.ExecutionStatus == "succeeded" && !validStatus(run.PredictedStatus) {
			return fmt.Errorf("successful run %q requires a valid predicted_status", run.Strategy)
		}
		if run.ExecutionStatus != "succeeded" && run.PredictedStatus != "" {
			return fmt.Errorf("unsuccessful run %q cannot have predicted_status", run.Strategy)
		}
		if run.ExecutionStatus != "succeeded" && run.HumanReviewed {
			return fmt.Errorf("unsuccessful run %q cannot be marked human_reviewed", run.Strategy)
		}
		if run.HumanReviewerID != "" && (!run.HumanReviewed || strings.TrimSpace(run.HumanReviewerID) != run.HumanReviewerID || len(run.HumanReviewerID) > 128) {
			return fmt.Errorf("run %q has an invalid human_reviewer_id", run.Strategy)
		}
		if len(run.RawAnswer) != 0 && (!json.Valid(run.RawAnswer) || run.ExecutionStatus != "succeeded") {
			return fmt.Errorf("run %q has invalid raw_answer_json", run.Strategy)
		}
		if len(run.RawAnswer) > 0 {
			var rawAnswer struct {
				Status       string `json:"status"`
				AnswerBlocks []struct {
					Text string `json:"text"`
				} `json:"answer_blocks"`
			}
			if err := json.Unmarshal(run.RawAnswer, &rawAnswer); err != nil || !validStatus(rawAnswer.Status) || rawAnswer.Status != run.PredictedStatus {
				return fmt.Errorf("run %q raw_answer_json status does not match predicted_status", run.Strategy)
			}
			if run.HumanReviewed {
				for _, claim := range run.Claims {
					if claim.AnswerBlockID == "" {
						continue
					}
					var blockIndex int
					if _, err := fmt.Sscanf(claim.AnswerBlockID, "B%d", &blockIndex); err != nil ||
						blockIndex < 1 || blockIndex > len(rawAnswer.AnswerBlocks) ||
						claim.AnswerBlockID != fmt.Sprintf("B%d", blockIndex) {
						return fmt.Errorf("run %q claim references an invalid answer_block_id", run.Strategy)
					}
				}
			}
		}
		if run.ToolCalls < 0 || run.NoProgressToolCalls < 0 || run.NoProgressToolCalls > run.ToolCalls ||
			run.HTTPAttempts < 0 || run.LatencyMS < 0 {
			return fmt.Errorf("run %q contains a negative or inconsistent runtime count", run.Strategy)
		}
		for label, value := range map[string]*int64{"input_tokens": run.InputTokens, "output_tokens": run.OutputTokens, "cost_micros": run.CostMicros} {
			if value != nil && *value < 0 {
				return fmt.Errorf("run %q has negative %s", run.Strategy, label)
			}
		}
		if err := validateSeqs(run.InitialRankedSentenceSeqs); err != nil {
			return fmt.Errorf("run %q initial retrieval: %w", run.Strategy, err)
		}
		if err := validateSeqs(run.FinalRankedSentenceSeqs); err != nil {
			return fmt.Errorf("run %q final retrieval: %w", run.Strategy, err)
		}
		if run.InitialProvidedSeqs != nil {
			if err := validateSeqs(run.InitialProvidedSeqs); err != nil {
				return fmt.Errorf("run %q initial provided evidence: %w", run.Strategy, err)
			}
			if !isSubsequence(run.InitialProvidedSeqs, run.InitialRankedSentenceSeqs) {
				return fmt.Errorf("run %q initial provided evidence must retain retrieval order", run.Strategy)
			}
		}
		if run.FinalProvidedSeqs != nil {
			if err := validateSeqs(run.FinalProvidedSeqs); err != nil {
				return fmt.Errorf("run %q final provided evidence: %w", run.Strategy, err)
			}
			if !isSubsequence(run.FinalProvidedSeqs, run.FinalRankedSentenceSeqs) {
				return fmt.Errorf("run %q provided evidence must be contained in final retrieval", run.Strategy)
			}
			if run.InitialProvidedSeqs != nil && !isPrefix(run.InitialProvidedSeqs, run.FinalProvidedSeqs) {
				return fmt.Errorf("run %q final provided evidence must retain its initial context", run.Strategy)
			}
		}
		if !isPrefix(run.InitialRankedSentenceSeqs, run.FinalRankedSentenceSeqs) {
			return fmt.Errorf("run %q final evidence must retain its initial ranked retrieval as a prefix", run.Strategy)
		}
		if run.Strategy == "fixed_rag" && (run.ToolCalls != 0 || !sameSeqs(run.InitialRankedSentenceSeqs, run.FinalRankedSentenceSeqs)) {
			return errors.New("fixed_rag must have zero tool calls and preserve its initial retrieval")
		}
		if run.Strategy == "fixed_rag" && run.InitialProvidedSeqs != nil && run.FinalProvidedSeqs != nil &&
			!sameSeqs(run.InitialProvidedSeqs, run.FinalProvidedSeqs) {
			return errors.New("fixed_rag must preserve its initial provided evidence")
		}
		if run.HumanReviewed && run.ExecutionStatus == "succeeded" {
			for _, claim := range run.Claims {
				if claim.Correct && (claim.PointID == "" || !contains(sample.RequiredPointIDs, claim.PointID)) {
					return fmt.Errorf("run %q marks a non-required claim as correct", run.Strategy)
				}
				citationSupport := false
				for _, citation := range claim.Citations {
					if citation.SentenceSeq <= 0 || !containsSeq(citationEvidenceSeqs(run), citation.SentenceSeq) {
						return fmt.Errorf("run %q cites a sentence that was not provided as evidence", run.Strategy)
					}
					citationSupport = citationSupport || citation.SupportsClaim
				}
				if claim.Supported && !citationSupport {
					return fmt.Errorf("run %q marks a claim supported without a supporting citation", run.Strategy)
				}
			}
		} else if len(run.Claims) != 0 {
			return fmt.Errorf("run %q has claim judgments without a successful reviewed answer", run.Strategy)
		}
		if sample.DatasetKind == DatasetSynthetic && run.HumanReviewed {
			return fmt.Errorf("synthetic_fixture run %q cannot claim human review", run.Strategy)
		}
		if requireHumanReview && sample.DatasetKind == DatasetHuman && run.ExecutionStatus == "succeeded" && !run.HumanReviewed {
			return fmt.Errorf("human_annotated dataset requires answer review for successful run %q", run.Strategy)
		}
		runs[run.Strategy] = run
	}
	fixed, hasFixed := runs["fixed_rag"]
	agent, hasAgent := runs["agent"]
	if !hasFixed || !hasAgent {
		return errors.New("each case requires paired fixed_rag and agent runs")
	}
	if !sameSeqs(fixed.InitialRankedSentenceSeqs, agent.InitialRankedSentenceSeqs) {
		return errors.New("paired fixed_rag and agent runs must share the same initial retrieval")
	}
	if fixed.ResultID != "" || agent.ResultID != "" {
		if fixed.ResultID == "" || fixed.ResultID != agent.ResultID {
			return errors.New("paired fixed_rag and agent runs must pin the same result_id")
		}
		if fixed.QuestionID == "" || agent.QuestionID == "" || fixed.QuestionID == agent.QuestionID {
			return errors.New("paired runs with result IDs require distinct question IDs")
		}
	}
	if fixed.InitialProvidedSeqs != nil || agent.InitialProvidedSeqs != nil {
		if fixed.InitialProvidedSeqs == nil || agent.InitialProvidedSeqs == nil || !sameSeqs(fixed.InitialProvidedSeqs, agent.InitialProvidedSeqs) {
			return errors.New("paired fixed_rag and agent runs must share the same initial provided evidence")
		}
	}
	return nil
}

func citationEvidenceSeqs(run Run) []int {
	if run.FinalProvidedSeqs != nil {
		return run.FinalProvidedSeqs
	}
	return run.FinalRankedSentenceSeqs
}

func isSubsequence(subseq, full []int) bool {
	if len(subseq) == 0 {
		return true
	}
	index := 0
	for _, value := range full {
		if value == subseq[index] {
			index++
			if index == len(subseq) {
				return true
			}
		}
	}
	return false
}

func validateSeqs(seqs []int) error {
	seen := make(map[int]struct{}, len(seqs))
	for _, seq := range seqs {
		if seq <= 0 {
			return errors.New("sentence sequences must be positive")
		}
		if _, exists := seen[seq]; exists {
			return fmt.Errorf("duplicate sentence sequence %d", seq)
		}
		seen[seq] = struct{}{}
	}
	return nil
}

func validStatus(status string) bool {
	return status == "answered" || status == "partial" || status == "insufficient"
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

func isPrefix(prefix, full []int) bool {
	if len(prefix) > len(full) {
		return false
	}
	for i := range prefix {
		if prefix[i] != full[i] {
			return false
		}
	}
	return true
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsSeq(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
