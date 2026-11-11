package agenteval

import (
	"bufio"
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const BlindReviewSchemaVersion = 1

type ReviewCandidate struct {
	Label           string              `json:"label"`
	ExecutionStatus string              `json:"execution_status"`
	PredictedStatus string              `json:"predicted_status,omitempty"`
	AnswerBlocks    []ReviewAnswerBlock `json:"answer_blocks,omitempty"`
	Unanswered      []string            `json:"unanswered_points,omitempty"`
}

type ReviewAnswerBlock struct {
	ID           string   `json:"id"`
	Text         string   `json:"text"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type ReviewSource struct {
	Ref         string `json:"ref"`
	SentenceSeq int    `json:"-"`
	Text        string `json:"text"`
	StartMS     int64  `json:"start_ms"`
	EndMS       int64  `json:"end_ms"`
}

type ReviewEvidenceGroup struct {
	ID           string   `json:"id"`
	SentenceRefs []string `json:"sentence_refs"`
}

// ReviewCase is safe to give to an annotator: it deliberately omits strategy,
// question/result IDs, split, tool counts, latency, usage, cost and profiles.
type ReviewCase struct {
	SchemaVersion    int                   `json:"schema_version"`
	ReviewID         string                `json:"review_id"`
	QuestionText     string                `json:"question_text,omitempty"`
	RequiredPoints   []RequiredPoint       `json:"required_points,omitempty"`
	RequiredEvidence []ReviewEvidenceGroup `json:"required_evidence_groups,omitempty"`
	Evidence         []ReviewSource        `json:"evidence,omitempty"`
	Candidates       []ReviewCandidate     `json:"candidates"`
	Notice           string                `json:"notice,omitempty"`
}

type ReviewKey struct {
	SchemaVersion int              `json:"schema_version"`
	DraftSHA256   string           `json:"draft_sha256"`
	PackSHA256    string           `json:"pack_sha256"`
	Entries       []ReviewKeyEntry `json:"entries"`
}

type ReviewKeyEntry struct {
	ReviewID           string            `json:"review_id"`
	CaseID             string            `json:"case_id"`
	Candidates         []ReviewKeyRun    `json:"candidates"`
	EvidenceRefSeq     map[string]int    `json:"evidence_ref_sentence_seqs"`
	RequiredPointIDMap map[string]string `json:"required_point_id_map"`
}

type ReviewKeyRun struct {
	Label      string `json:"label"`
	Strategy   string `json:"strategy"`
	QuestionID string `json:"question_id,omitempty"`
}

type ReviewSubmission struct {
	SchemaVersion int               `json:"schema_version"`
	ReviewID      string            `json:"review_id"`
	ReviewerID    string            `json:"reviewer_id"`
	Candidates    []CandidateReview `json:"candidates"`
}

type CandidateReview struct {
	Label    string          `json:"label"`
	Reviewed bool            `json:"reviewed"`
	Claims   []ReviewedClaim `json:"claims"`
}

type ReviewedClaim struct {
	AnswerBlockID string             `json:"answer_block_id"`
	PointID       string             `json:"point_id,omitempty"`
	Correct       bool               `json:"correct"`
	Supported     bool               `json:"supported_by_evidence"`
	Citations     []ReviewedCitation `json:"citations,omitempty"`
}

type ReviewedCitation struct {
	EvidenceRef   string `json:"evidence_ref"`
	SupportsClaim bool   `json:"supports_claim"`
}

type rawAnswer struct {
	Status           string                     `json:"status"`
	AnswerBlocks     []rawAnswerBlock           `json:"answer_blocks"`
	UnansweredPoints []string                   `json:"unanswered_points"`
	Sources          map[string]rawAnswerSource `json:"sources"`
}

type rawAnswerBlock struct {
	Text         string   `json:"text"`
	EvidenceRefs []string `json:"evidence_refs"`
}

type rawAnswerSource struct {
	SentenceSeq int    `json:"sentence_seq"`
	Text        string `json:"text"`
	StartMS     int64  `json:"start_ms"`
	EndMS       int64  `json:"end_ms"`
}

func LoadAnnotationDraftJSONL(reader io.Reader) ([]Sample, error) {
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
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		var sample Sample
		if err := decoder.Decode(&sample); err != nil {
			return nil, fmt.Errorf("decode annotation draft line %d: %w", lineNo, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("annotation draft line %d must contain one JSON value", lineNo)
		}
		samples = append(samples, sample)
		if len(samples) > 10_000 {
			return nil, errors.New("annotation draft exceeds 10000 cases")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read annotation draft: %w", err)
	}
	if err := ValidateAnnotationDraft(samples); err != nil {
		return nil, err
	}
	return samples, nil
}

func BuildBlindReviewArtifacts(draftBytes []byte) (packBytes, templateBytes, keyBytes []byte, caseCount int, err error) {
	if len(draftBytes) == 0 {
		return nil, nil, nil, 0, errors.New("source annotation draft is empty")
	}
	samples, err := LoadAnnotationDraftJSONL(bytes.NewReader(draftBytes))
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if err := ValidateAnnotationDraft(samples); err != nil {
		return nil, nil, nil, 0, err
	}
	packCases := make([]ReviewCase, 0, len(samples))
	keyEntries := make([]ReviewKeyEntry, 0, len(samples))
	templates := make([]ReviewSubmission, 0, len(samples))
	for _, sample := range samples {
		if sample.DatasetKind == DatasetHuman {
			if strings.TrimSpace(sample.QuestionText) == "" {
				return nil, nil, nil, 0, fmt.Errorf("case %q requires question_text for blind review", sample.CaseID)
			}
			if len(sample.RequiredPoints) != len(sample.RequiredPointIDs) {
				return nil, nil, nil, 0, fmt.Errorf("case %q requires a description for every required point", sample.CaseID)
			}
			if len(sample.RequiredEvidence) > 0 && len(sample.EvidenceSentences) == 0 {
				return nil, nil, nil, 0, fmt.Errorf("case %q requires transcript quotes for blind review evidence groups", sample.CaseID)
			}
			for _, run := range sample.Runs {
				if run.QuestionID == "" {
					return nil, nil, nil, 0, fmt.Errorf("case %q run %q requires question_id for auditable unblinding", sample.CaseID, run.Strategy)
				}
			}
		}
		packCase, keyEntry, err := buildReviewCase(sample)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("case %q: %w", sample.CaseID, err)
		}
		packCases = append(packCases, packCase)
		keyEntries = append(keyEntries, keyEntry)
		template := ReviewSubmission{SchemaVersion: BlindReviewSchemaVersion, ReviewID: packCase.ReviewID,
			Candidates: make([]CandidateReview, 0, len(packCase.Candidates))}
		for _, candidate := range packCase.Candidates {
			template.Candidates = append(template.Candidates, CandidateReview{Label: candidate.Label, Claims: []ReviewedClaim{}})
		}
		templates = append(templates, template)
	}
	if err := secureShuffle(packCases); err != nil {
		return nil, nil, nil, 0, err
	}
	// Align the blank form with the shuffled pack by review_id.
	templateByID := make(map[string]ReviewSubmission, len(templates))
	for _, template := range templates {
		templateByID[template.ReviewID] = template
	}
	shuffledTemplates := make([]ReviewSubmission, 0, len(packCases))
	for _, item := range packCases {
		shuffledTemplates = append(shuffledTemplates, templateByID[item.ReviewID])
	}
	packBytes, err = marshalReviewJSONL(packCases)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	templateBytes, err = marshalReviewSubmissionJSONL(shuffledTemplates)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	draftHash := sha256.Sum256(draftBytes)
	packHash := sha256.Sum256(packBytes)
	key := ReviewKey{SchemaVersion: BlindReviewSchemaVersion,
		DraftSHA256: "sha256:" + hex.EncodeToString(draftHash[:]),
		PackSHA256:  "sha256:" + hex.EncodeToString(packHash[:]), Entries: keyEntries}
	keyBytes, err = json.MarshalIndent(key, "", "  ")
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("encode private review key: %w", err)
	}
	return packBytes, templateBytes, append(keyBytes, '\n'), len(samples), nil
}

func buildReviewCase(sample Sample) (ReviewCase, ReviewKeyEntry, error) {
	bySeq := make(map[int]ReviewSource)
	addSource := func(source ReviewSource) error {
		if source.SentenceSeq <= 0 || strings.TrimSpace(source.Text) == "" || source.StartMS < 0 || source.EndMS <= source.StartMS {
			return errors.New("contains an invalid evidence source")
		}
		if old, exists := bySeq[source.SentenceSeq]; exists {
			if old.Text != source.Text || old.StartMS != source.StartMS || old.EndMS != source.EndMS {
				return fmt.Errorf("sentence %d has conflicting text or time ranges", source.SentenceSeq)
			}
			return nil
		}
		bySeq[source.SentenceSeq] = source
		return nil
	}
	for _, quote := range sample.EvidenceSentences {
		if err := addSource(ReviewSource{SentenceSeq: quote.SentenceSeq, Text: quote.Text,
			StartMS: quote.StartMS, EndMS: quote.EndMS}); err != nil {
			return ReviewCase{}, ReviewKeyEntry{}, err
		}
	}
	runs := make(map[string]Run, 2)
	for _, run := range sample.Runs {
		runs[run.Strategy] = run
		if run.ExecutionStatus != "succeeded" {
			continue
		}
		var answer rawAnswer
		if err := json.Unmarshal(run.RawAnswer, &answer); err != nil || answer.Status != run.PredictedStatus {
			return ReviewCase{}, ReviewKeyEntry{}, fmt.Errorf("run %q has no usable raw answer", run.Strategy)
		}
		for _, source := range answer.Sources {
			if err := addSource(ReviewSource{SentenceSeq: source.SentenceSeq, Text: source.Text,
				StartMS: source.StartMS, EndMS: source.EndMS}); err != nil {
				return ReviewCase{}, ReviewKeyEntry{}, err
			}
		}
	}
	seqs := make([]int, 0, len(bySeq))
	for seq := range bySeq {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	refBySeq := make(map[int]string, len(seqs))
	refSeq := make(map[string]int, len(seqs))
	evidence := make([]ReviewSource, 0, len(seqs))
	for i, seq := range seqs {
		ref := fmt.Sprintf("S%d", i+1)
		refBySeq[seq] = ref
		refSeq[ref] = seq
		source := bySeq[seq]
		source.Ref = ref
		evidence = append(evidence, source)
	}
	groups := make([]ReviewEvidenceGroup, 0, len(sample.RequiredEvidence))
	for i, group := range sample.RequiredEvidence {
		item := ReviewEvidenceGroup{ID: fmt.Sprintf("G%d", i+1), SentenceRefs: make([]string, 0, len(group.SentenceSeqs))}
		for _, seq := range group.SentenceSeqs {
			ref, ok := refBySeq[seq]
			if !ok {
				return ReviewCase{}, ReviewKeyEntry{}, fmt.Errorf("required evidence sentence %d has no review excerpt", seq)
			}
			item.SentenceRefs = append(item.SentenceRefs, ref)
		}
		groups = append(groups, item)
	}
	fixed, fixedOK := runs["fixed_rag"]
	agent, agentOK := runs["agent"]
	if !fixedOK || !agentOK {
		return ReviewCase{}, ReviewKeyEntry{}, errors.New("paired fixed_rag and agent runs are required")
	}
	swap, err := cryptoCoinFlip()
	if err != nil {
		return ReviewCase{}, ReviewKeyEntry{}, err
	}
	ordered := []Run{fixed, agent}
	if swap {
		ordered[0], ordered[1] = ordered[1], ordered[0]
	}
	labels := []string{"A", "B"}
	candidates := make([]ReviewCandidate, 0, 2)
	keyRuns := make([]ReviewKeyRun, 0, 2)
	for i, run := range ordered {
		candidate, err := sanitizeReviewCandidate(labels[i], run, refBySeq)
		if err != nil {
			return ReviewCase{}, ReviewKeyEntry{}, err
		}
		candidates = append(candidates, candidate)
		keyRuns = append(keyRuns, ReviewKeyRun{Label: labels[i], Strategy: run.Strategy, QuestionID: run.QuestionID})
	}
	reviewPoints := make([]RequiredPoint, 0, len(sample.RequiredPoints))
	pointMap := make(map[string]string, len(sample.RequiredPointIDs))
	for i, id := range sample.RequiredPointIDs {
		reviewID := fmt.Sprintf("P%d", i+1)
		pointMap[reviewID] = id
		for _, point := range sample.RequiredPoints {
			if point.ID == id {
				reviewPoints = append(reviewPoints, RequiredPoint{ID: reviewID, Description: point.Description})
				break
			}
		}
	}
	reviewID, err := uuid.NewRandom()
	if err != nil {
		return ReviewCase{}, ReviewKeyEntry{}, fmt.Errorf("generate opaque review ID: %w", err)
	}
	item := ReviewCase{SchemaVersion: BlindReviewSchemaVersion, ReviewID: reviewID.String(),
		QuestionText: sample.QuestionText, RequiredPoints: reviewPoints,
		RequiredEvidence: groups, Evidence: evidence, Candidates: candidates}
	if sample.DatasetKind == DatasetSynthetic {
		item.Notice = "合成夹具：仅验证盲审文件格式，不可作为模型质量或人工评估数据。"
	}
	return item, ReviewKeyEntry{ReviewID: item.ReviewID, CaseID: sample.CaseID,
		Candidates: keyRuns, EvidenceRefSeq: refSeq, RequiredPointIDMap: pointMap}, nil
}

func sanitizeReviewCandidate(label string, run Run, refBySeq map[int]string) (ReviewCandidate, error) {
	candidate := ReviewCandidate{Label: label, ExecutionStatus: run.ExecutionStatus, PredictedStatus: run.PredictedStatus,
		AnswerBlocks: []ReviewAnswerBlock{}, Unanswered: []string{}}
	if run.ExecutionStatus != "succeeded" {
		return candidate, nil
	}
	var answer rawAnswer
	if err := json.Unmarshal(run.RawAnswer, &answer); err != nil {
		return ReviewCandidate{}, fmt.Errorf("decode successful %s answer: %w", run.Strategy, err)
	}
	for i, block := range answer.AnswerBlocks {
		item := ReviewAnswerBlock{ID: fmt.Sprintf("B%d", i+1), Text: block.Text, EvidenceRefs: []string{}}
		seen := make(map[string]struct{})
		for _, evidenceID := range block.EvidenceRefs {
			source, ok := answer.Sources[evidenceID]
			if !ok {
				return ReviewCandidate{}, fmt.Errorf("%s answer cites an unknown source", run.Strategy)
			}
			ref, ok := refBySeq[source.SentenceSeq]
			if !ok {
				return ReviewCandidate{}, fmt.Errorf("%s answer source has no anonymized evidence reference", run.Strategy)
			}
			if _, exists := seen[ref]; !exists {
				item.EvidenceRefs = append(item.EvidenceRefs, ref)
				seen[ref] = struct{}{}
			}
		}
		item.EvidenceRefs = sortedStrings(item.EvidenceRefs)
		candidate.AnswerBlocks = append(candidate.AnswerBlocks, item)
	}
	candidate.Unanswered = append([]string{}, answer.UnansweredPoints...)
	return candidate, nil
}

func ApplyBlindReviewArtifacts(draftBytes, packBytes, keyBytes, submissionBytes []byte) ([]byte, error) {
	samples, err := LoadAnnotationDraftJSONL(bytes.NewReader(draftBytes))
	if err != nil {
		return nil, err
	}
	if samples[0].DatasetKind != DatasetHuman {
		return nil, errors.New("blind review import requires dataset_kind=human_annotated; synthetic fixtures cannot become human evidence")
	}
	var key ReviewKey
	keyDecoder := json.NewDecoder(bytes.NewReader(keyBytes))
	keyDecoder.DisallowUnknownFields()
	if err := keyDecoder.Decode(&key); err != nil {
		return nil, fmt.Errorf("decode private review key: %w", err)
	}
	var trailing any
	if err := keyDecoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("private review key must contain one JSON value")
	}
	if key.SchemaVersion != BlindReviewSchemaVersion {
		return nil, errors.New("private review key schema_version is unsupported")
	}
	if digest(draftBytes) != key.DraftSHA256 || digest(packBytes) != key.PackSHA256 {
		return nil, errors.New("draft or blind review pack does not match the private key")
	}
	packs, err := loadReviewPackJSONL(bytes.NewReader(packBytes))
	if err != nil {
		return nil, err
	}
	submissions, err := loadReviewSubmissionsJSONL(bytes.NewReader(submissionBytes))
	if err != nil {
		return nil, err
	}
	if len(packs) != len(samples) || len(key.Entries) != len(samples) || len(submissions) != len(samples) {
		return nil, errors.New("draft, review pack, private key and submissions must have the same case count")
	}
	keyByReview := make(map[string]ReviewKeyEntry, len(key.Entries))
	keyCaseIDs := make(map[string]struct{}, len(key.Entries))
	for _, entry := range key.Entries {
		if entry.ReviewID == "" || entry.CaseID == "" {
			return nil, errors.New("private review key contains an empty identity")
		}
		if _, exists := keyByReview[entry.ReviewID]; exists {
			return nil, errors.New("private review key contains duplicate review IDs")
		}
		if _, exists := keyCaseIDs[entry.CaseID]; exists {
			return nil, errors.New("private review key contains duplicate case IDs")
		}
		keyByReview[entry.ReviewID] = entry
		keyCaseIDs[entry.CaseID] = struct{}{}
	}
	packByReview := make(map[string]ReviewCase, len(packs))
	for _, item := range packs {
		if item.SchemaVersion != BlindReviewSchemaVersion || item.ReviewID == "" || len(item.Candidates) != 2 {
			return nil, errors.New("blind review pack contains an invalid case")
		}
		if _, exists := packByReview[item.ReviewID]; exists {
			return nil, errors.New("blind review pack contains duplicate review IDs")
		}
		packByReview[item.ReviewID] = item
	}
	submissionByReview := make(map[string]ReviewSubmission, len(submissions))
	reviewerID := ""
	for _, submission := range submissions {
		if submission.SchemaVersion != BlindReviewSchemaVersion || submission.ReviewID == "" ||
			strings.TrimSpace(submission.ReviewerID) == "" || strings.TrimSpace(submission.ReviewerID) != submission.ReviewerID ||
			len(submission.ReviewerID) > 128 {
			return nil, errors.New("review submission has an invalid identity")
		}
		if reviewerID == "" {
			reviewerID = submission.ReviewerID
		} else if submission.ReviewerID != reviewerID {
			return nil, errors.New("one import must contain submissions from exactly one reviewer")
		}
		if _, exists := submissionByReview[submission.ReviewID]; exists {
			return nil, errors.New("review submission contains duplicate review IDs")
		}
		submissionByReview[submission.ReviewID] = submission
	}
	caseByID := make(map[string]int, len(samples))
	for i, sample := range samples {
		caseByID[sample.CaseID] = i
	}
	for reviewID, entry := range keyByReview {
		index, ok := caseByID[entry.CaseID]
		if !ok {
			return nil, errors.New("private review key references a case missing from the source draft")
		}
		item, ok := packByReview[reviewID]
		if !ok {
			return nil, errors.New("blind review pack is missing a keyed review ID")
		}
		submission, ok := submissionByReview[reviewID]
		if !ok {
			return nil, errors.New("review submission is missing a case")
		}
		if err := applyCaseReview(&samples[index], item, entry, submission); err != nil {
			return nil, fmt.Errorf("review %s: %w", reviewID, err)
		}
	}
	for _, sample := range samples {
		for i := range sample.Runs {
			if sample.Runs[i].HumanReviewed && sample.Runs[i].HumanReviewerID != reviewerID {
				return nil, errors.New("reviewer identity mismatch while applying annotations")
			}
		}
	}
	if _, err := Evaluate(samples); err != nil {
		return nil, fmt.Errorf("reviewed dataset failed evaluation validation: %w", err)
	}
	return marshalSampleJSONL(samples)
}

func applyCaseReview(sample *Sample, item ReviewCase, entry ReviewKeyEntry, submission ReviewSubmission) error {
	if item.ReviewID != entry.ReviewID || submission.ReviewID != entry.ReviewID || len(entry.Candidates) != 2 {
		return errors.New("review identity mapping is inconsistent")
	}
	if len(submission.Candidates) > 2 {
		return errors.New("submission contains too many candidates")
	}
	strategyByLabel := make(map[string]string, 2)
	for _, mapped := range entry.Candidates {
		if mapped.Label != "A" && mapped.Label != "B" || (mapped.Strategy != "fixed_rag" && mapped.Strategy != "agent") {
			return errors.New("private key has an invalid candidate mapping")
		}
		if _, exists := strategyByLabel[mapped.Label]; exists {
			return errors.New("private key has duplicate candidate labels")
		}
		strategyByLabel[mapped.Label] = mapped.Strategy
	}
	packCandidates := make(map[string]ReviewCandidate, 2)
	for _, candidate := range item.Candidates {
		if candidate.Label != "A" && candidate.Label != "B" {
			return errors.New("blind pack has an invalid candidate label")
		}
		packCandidates[candidate.Label] = candidate
	}
	if len(packCandidates) != 2 {
		return errors.New("blind pack must contain A and B candidates")
	}
	reviewByLabel := make(map[string]CandidateReview, 2)
	for _, review := range submission.Candidates {
		if _, exists := reviewByLabel[review.Label]; exists {
			return errors.New("submission contains duplicate candidate labels")
		}
		reviewByLabel[review.Label] = review
	}
	if len(reviewByLabel) != 2 {
		return errors.New("submission must explicitly review both candidate slots; mark failed candidates reviewed with no claims")
	}
	for label, strategy := range strategyByLabel {
		candidate, ok := packCandidates[label]
		if !ok {
			return errors.New("private key and blind pack candidate labels differ")
		}
		review, ok := reviewByLabel[label]
		if !ok || !review.Reviewed {
			return fmt.Errorf("candidate %s is not marked reviewed", label)
		}
		if candidate.ExecutionStatus != "succeeded" && len(review.Claims) != 0 {
			return fmt.Errorf("failed candidate %s cannot have answer claims", label)
		}
		for i := range sample.Runs {
			if sample.Runs[i].Strategy != strategy {
				continue
			}
			if sample.Runs[i].ExecutionStatus != candidate.ExecutionStatus || sample.Runs[i].QuestionID != questionIDForLabel(entry, label) {
				return errors.New("source run no longer matches the keyed candidate")
			}
			if candidate.ExecutionStatus == "succeeded" {
				claims, err := convertReviewedClaims(review.Claims, candidate, entry.EvidenceRefSeq,
					entry.RequiredPointIDMap, sample.RequiredPointIDs)
				if err != nil {
					return fmt.Errorf("candidate %s: %w", label, err)
				}
				sample.Runs[i].HumanReviewed = true
				sample.Runs[i].HumanReviewerID = submission.ReviewerID
				sample.Runs[i].Claims = claims
			} else if len(review.Claims) != 0 {
				return errors.New("unsuccessful execution received answer claims")
			}
			break
		}
	}
	return nil
}

func convertReviewedClaims(reviews []ReviewedClaim, candidate ReviewCandidate, refs map[string]int,
	pointMap map[string]string, requiredIDs []string) ([]Claim, error) {
	blocks := make(map[string]struct{}, len(candidate.AnswerBlocks))
	for _, block := range candidate.AnswerBlocks {
		blocks[block.ID] = struct{}{}
	}
	required := make(map[string]struct{}, len(requiredIDs))
	for _, id := range requiredIDs {
		required[id] = struct{}{}
	}
	claims := make([]Claim, 0, len(reviews))
	coveredBlocks := make(map[string]struct{}, len(blocks))
	for _, review := range reviews {
		if _, ok := blocks[review.AnswerBlockID]; !ok {
			return nil, fmt.Errorf("claim references unknown answer block %q", review.AnswerBlockID)
		}
		pointID := ""
		if review.PointID != "" {
			var ok bool
			pointID, ok = pointMap[review.PointID]
			if !ok {
				return nil, fmt.Errorf("claim references unknown blind required point %q", review.PointID)
			}
			if _, ok := required[pointID]; !ok {
				return nil, fmt.Errorf("blind required point maps to unknown original point %q", pointID)
			}
		}
		claim := Claim{AnswerBlockID: review.AnswerBlockID, PointID: pointID,
			Correct: review.Correct, Supported: review.Supported, Citations: []Citation{}}
		coveredBlocks[review.AnswerBlockID] = struct{}{}
		seen := make(map[int]struct{})
		citedSupporting := false
		for _, citation := range review.Citations {
			seq, ok := refs[citation.EvidenceRef]
			if !ok {
				return nil, fmt.Errorf("citation references unknown evidence label %q", citation.EvidenceRef)
			}
			if _, exists := seen[seq]; exists {
				return nil, fmt.Errorf("duplicate citation for sentence %d", seq)
			}
			seen[seq] = struct{}{}
			claim.Citations = append(claim.Citations, Citation{SentenceSeq: seq, SupportsClaim: citation.SupportsClaim})
			citedSupporting = citedSupporting || citation.SupportsClaim
		}
		if claim.Supported != citedSupporting {
			return nil, errors.New("supported_by_evidence must match whether any cited sentence supports the claim")
		}
		claims = append(claims, claim)
	}
	if len(coveredBlocks) != len(blocks) {
		return nil, errors.New("every answer block must have at least one claim-level judgment")
	}
	return claims, nil
}

func questionIDForLabel(entry ReviewKeyEntry, label string) string {
	for _, candidate := range entry.Candidates {
		if candidate.Label == label {
			return candidate.QuestionID
		}
	}
	return ""
}

func loadReviewPackJSONL(reader io.Reader) ([]ReviewCase, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	items := make([]ReviewCase, 0, 64)
	for scanner.Scan() {
		lineNo := len(items) + 1
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		var item ReviewCase
		if err := decoder.Decode(&item); err != nil {
			return nil, fmt.Errorf("decode blind review pack line %d: %w", lineNo, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("blind review pack line %d must contain one JSON value", lineNo)
		}
		items = append(items, item)
		if len(items) > 10_000 {
			return nil, errors.New("blind review pack exceeds 10000 cases")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, errors.New("blind review pack is empty")
	}
	return items, nil
}

func loadReviewSubmissionsJSONL(reader io.Reader) ([]ReviewSubmission, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	items := make([]ReviewSubmission, 0, 64)
	for scanner.Scan() {
		lineNo := len(items) + 1
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		var item ReviewSubmission
		if err := decoder.Decode(&item); err != nil {
			return nil, fmt.Errorf("decode review submission line %d: %w", lineNo, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("review submission line %d must contain one JSON value", lineNo)
		}
		items = append(items, item)
		if len(items) > 10_000 {
			return nil, errors.New("review submission exceeds 10000 cases")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, errors.New("review submission is empty")
	}
	return items, nil
}

func marshalReviewJSONL(items []ReviewCase) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	for _, item := range items {
		if err := encoder.Encode(item); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func marshalSampleJSONL(items []Sample) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	for _, item := range items {
		if err := encoder.Encode(item); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func marshalReviewSubmissionJSONL(items []ReviewSubmission) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	for _, item := range items {
		if err := encoder.Encode(item); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func secureShuffle[T any](values []T) error {
	for i := len(values) - 1; i > 0; i-- {
		j, err := cryptoIndex(i + 1)
		if err != nil {
			return err
		}
		values[i], values[j] = values[j], values[i]
	}
	return nil
}

func cryptoCoinFlip() (bool, error) {
	value, err := cryptoIndex(2)
	return value == 1, err
}

func cryptoIndex(limit int) (int, error) {
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(limit)))
	if err != nil {
		return 0, fmt.Errorf("randomize blind review order: %w", err)
	}
	return int(value.Int64()), nil
}

func sortedStrings(values []string) []string {
	sort.Strings(values)
	return values
}

func digest(value []byte) string {
	hash := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(hash[:])
}
