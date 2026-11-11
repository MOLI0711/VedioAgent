package content

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"mirrorcore/internal/contentprofile"
	"mirrorcore/internal/repository/postgres"
)

type plannedSource struct {
	Range   postgres.ContentSourceRange
	Text    string
	StartMS int64
	EndMS   int64
}

type coverageRange struct {
	Seq   int `json:"seq"`
	Start int `json:"start_offset"`
	End   int `json:"end_offset"`
}

type omittedRange struct {
	FromSeq    int `json:"from_seq"`
	FromOffset int `json:"from_offset"`
	ToSeq      int `json:"to_seq"`
}

type Coverage struct {
	Full          bool            `json:"full"`
	CoveredRanges []coverageRange `json:"covered_ranges"`
	OmittedRanges []omittedRange  `json:"omitted_ranges"`
	Reason        string          `json:"reason,omitempty"`
}

type contentPlan struct {
	Batches  []postgres.ContentBatchPlan
	Sources  map[string]plannedSource
	Coverage Coverage
}

type promptSource struct {
	SourceID string `json:"source_id"`
	Text     string `json:"text"`
}

func makePlan(input postgres.ContentInput) (contentPlan, error) {
	if input.TotalSegments <= 0 || len(input.Segments) == 0 || len(input.Segments) > 1201 || len(input.Segments) > input.TotalSegments {
		return contentPlan{}, errors.New("content planner received an invalid bounded transcript")
	}
	plan := contentPlan{Sources: make(map[string]plannedSource), Coverage: Coverage{
		Full: true, CoveredRanges: make([]coverageRange, 0, 1200), OmittedRanges: []omittedRange{},
	}}
	current := make([]plannedSource, 0, 100)
	currentBytes := 0
	var omitted *omittedRange
	flush := func() bool {
		if len(current) == 0 {
			return true
		}
		if len(plan.Batches) >= contentprofile.MaxBatches {
			return false
		}
		batchNo := len(plan.Batches) + 1
		ranges := make([]postgres.ContentSourceRange, 0, len(current))
		texts := make([]promptSource, 0, len(current))
		for _, source := range current {
			ranges = append(ranges, source.Range)
			texts = append(texts, promptSource{SourceID: source.Range.SourceID, Text: source.Text})
		}
		canonical, err := json.Marshal(struct {
			BatchNo int                           `json:"batch_no"`
			Ranges  []postgres.ContentSourceRange `json:"ranges"`
			Texts   []promptSource                `json:"texts"`
		}{batchNo, ranges, texts})
		if err != nil {
			return false
		}
		hash := sha256.Sum256(canonical)
		plan.Batches = append(plan.Batches, postgres.ContentBatchPlan{BatchNo: batchNo, Ranges: ranges,
			InputHash: hash[:], InputBytes: currentBytes})
		for _, source := range current {
			plan.Sources[source.Range.SourceID] = source
			plan.Coverage.CoveredRanges = append(plan.Coverage.CoveredRanges,
				coverageRange{Seq: source.Range.Seq, Start: source.Range.Start, End: source.Range.End})
		}
		current = make([]plannedSource, 0, 100)
		currentBytes = 0
		return true
	}
	for index, segment := range input.Segments {
		if segment.Seq != index+1 || !utf8.ValidString(segment.Text) || segment.Text == "" ||
			segment.StartMS < 0 || segment.EndMS <= segment.StartMS || len(segment.Text) > 64*1024 {
			return contentPlan{}, fmt.Errorf("transcript segment %d is invalid or exceeds its planning bound", segment.Seq)
		}
		for offset := 0; offset < len(segment.Text); {
			end := offset + 2048
			if end > len(segment.Text) {
				end = len(segment.Text)
			} else {
				for end > offset && !utf8.RuneStart(segment.Text[end]) {
					end--
				}
			}
			if end <= offset {
				return contentPlan{}, errors.New("planner could not split transcript on a UTF-8 boundary")
			}
			piece := segment.Text[offset:end]
			sequence := len(plan.Sources) + len(current) + 1
			sourceID := fmt.Sprintf("S%04d", sequence)
			sum := sha256.Sum256([]byte(piece))
			source := plannedSource{Range: postgres.ContentSourceRange{SourceID: sourceID, Seq: segment.Seq,
				Start: offset, End: end, TextHash: hex.EncodeToString(sum[:])}, Text: piece,
				StartMS: segment.StartMS, EndMS: segment.EndMS}
			candidate := append(append([]plannedSource(nil), current...), source)
			promptSources := make([]promptSource, 0, len(candidate))
			for _, item := range candidate {
				promptSources = append(promptSources, promptSource{SourceID: item.Range.SourceID, Text: item.Text})
			}
			encoded, err := json.Marshal(promptSources)
			if err != nil {
				return contentPlan{}, err
			}
			if len(current) > 0 && (len(candidate) > contentprofile.MaxSourcesPerBatch ||
				currentBytes+len(piece) > contentprofile.MaxBatchBytes || len(encoded) > 24*1024) {
				if !flush() || len(plan.Batches) >= contentprofile.MaxBatches {
					omitted = &omittedRange{FromSeq: segment.Seq, FromOffset: offset, ToSeq: input.TotalSegments}
					break
				}
				sequence = len(plan.Sources) + len(current) + 1
				sourceID = fmt.Sprintf("S%04d", sequence)
				source.Range.SourceID = sourceID
				candidate = []plannedSource{source}
				encoded, err = json.Marshal([]promptSource{{SourceID: sourceID, Text: piece}})
				if err != nil {
					return contentPlan{}, err
				}
			}
			if len(encoded) > 24*1024 || len(piece) > contentprofile.MaxBatchBytes {
				return contentPlan{}, errors.New("single transcript range exceeds the content request bound")
			}
			current = append(current, source)
			currentBytes += len(piece)
			offset = end
		}
		if omitted != nil {
			break
		}
	}
	if omitted == nil && !flush() {
		if len(current) > 0 {
			first := current[0]
			omitted = &omittedRange{FromSeq: first.Range.Seq, FromOffset: first.Range.Start, ToSeq: input.TotalSegments}
		}
	}
	if omitted == nil && len(input.Segments) < input.TotalSegments {
		omitted = &omittedRange{FromSeq: len(input.Segments) + 1, FromOffset: 0, ToSeq: input.TotalSegments}
	}
	if omitted != nil {
		plan.Coverage.Full = false
		plan.Coverage.Reason = "batch_budget_exceeded"
		plan.Coverage.OmittedRanges = append(plan.Coverage.OmittedRanges, *omitted)
	}
	if len(plan.Batches) == 0 || len(plan.Batches) > contentprofile.MaxBatches || len(plan.Sources) == 0 {
		return contentPlan{}, errors.New("content input does not fit the fixed processing budget")
	}
	return plan, nil
}
