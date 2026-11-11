package localdemo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"mirrorcore/internal/providers/qwen"
)

// ContentGenerator returns deterministic, source-copying structures for the
// offline content-worker path. It does not emulate semantic model behavior.
type ContentGenerator struct{}

type contentSource struct {
	SourceID string `json:"source_id"`
	Text     string `json:"text"`
}

type extractionRequest struct {
	BatchNo int             `json:"batch_no"`
	Sources []contentSource `json:"sources"`
}

type extractionResponse struct {
	Summary          string        `json:"summary"`
	SummarySourceIDs []string      `json:"summary_source_ids"`
	KeyPoints        []contentItem `json:"key_points"`
	Chapters         []contentItem `json:"chapters"`
}

type contentItem struct {
	Text      string   `json:"text,omitempty"`
	Title     string   `json:"title,omitempty"`
	Summary   string   `json:"summary,omitempty"`
	SourceIDs []string `json:"source_ids"`
}

type mergeBatch struct {
	BatchNo          int           `json:"batch_no"`
	Summary          string        `json:"summary"`
	SummarySourceIDs []string      `json:"summary_source_ids"`
	KeyPoints        []contentItem `json:"key_points"`
	Chapters         []contentItem `json:"chapters"`
}

type mergeRequest struct {
	Batches []mergeBatch `json:"batches"`
}

type mergeResponse struct {
	Summary          string        `json:"summary"`
	SummarySourceIDs []string      `json:"summary_source_ids"`
	KeyPoints        []contentItem `json:"key_points"`
	Chapters         []contentItem `json:"chapters"`
}

func (ContentGenerator) CompleteStructuredSchema(ctx context.Context, messages []qwen.Message,
	name string, _ map[string]any, _ int) (qwen.Result, error) {
	if err := ctx.Err(); err != nil {
		return qwen.Result{}, err
	}
	if len(messages) == 0 {
		return qwen.Result{}, errors.New("local demo content request has no messages")
	}
	input := []byte(messages[len(messages)-1].Content)
	var output any
	switch name {
	case "mirrorcore_content_extract":
		var request extractionRequest
		if err := json.Unmarshal(input, &request); err != nil {
			return qwen.Result{}, fmt.Errorf("decode local demo extraction request: %w", err)
		}
		if len(request.Sources) == 0 {
			return qwen.Result{}, errors.New("local demo extraction request has no sources")
		}
		source := request.Sources[0]
		ids := []string{source.SourceID}
		output = extractionResponse{Summary: source.Text, SummarySourceIDs: ids,
			KeyPoints: []contentItem{{Text: source.Text, SourceIDs: ids}},
			Chapters:  []contentItem{{Title: "本地演示章节", Summary: source.Text, SourceIDs: ids}}}
	case "mirrorcore_content_overview":
		var request mergeRequest
		if err := json.Unmarshal(input, &request); err != nil {
			return qwen.Result{}, fmt.Errorf("decode local demo merge request: %w", err)
		}
		if len(request.Batches) == 0 {
			return qwen.Result{}, errors.New("local demo merge request has no batches")
		}
		first := request.Batches[0]
		if len(first.SummarySourceIDs) == 0 {
			return qwen.Result{}, errors.New("local demo merge input has no cited source")
		}
		keyPoints := make([]contentItem, 0, len(request.Batches))
		chapters := make([]contentItem, 0, len(request.Batches))
		for _, batch := range request.Batches {
			keyPoints = append(keyPoints, batch.KeyPoints...)
			chapters = append(chapters, batch.Chapters...)
		}
		output = mergeResponse{Summary: first.Summary, SummarySourceIDs: first.SummarySourceIDs,
			KeyPoints: keyPoints, Chapters: chapters}
	default:
		return qwen.Result{}, fmt.Errorf("unsupported local demo content schema %q", name)
	}
	content, err := json.Marshal(output)
	if err != nil {
		return qwen.Result{}, fmt.Errorf("encode local demo content response: %w", err)
	}
	hash := sha256.Sum256(append(append([]byte(name), 0), input...))
	return qwen.Result{RequestID: "local-demo-content-" + hex.EncodeToString(hash[:8]),
		Content: string(content), FinishReason: "stop"}, nil
}
