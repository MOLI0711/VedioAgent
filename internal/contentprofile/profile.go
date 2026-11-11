package contentprofile

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
)

const (
	Model              = "qwen3.7-plus-2026-05-26"
	ProfileVersion     = "content_v1"
	LocalDemoWorkspace = "local-demo"
	LocalDemoModel     = "deterministic-content-v1"
	LocalDemoProfile   = "local_demo_content_v1"
	ExtractionPrompt   = "content_extract_v1"
	MergePrompt        = "content_merge_v1"
	MaxBatches         = 12
	MaxSourcesPerBatch = 100
	MaxBatchBytes      = 16 * 1024
	MaxRequestBytes    = 32 * 1024
	MaxMergeInputBytes = 64 * 1024
	MaxTotalAttempts   = 26
	MaxMergeAttempts   = 2
	MaxChapters        = 24
	MaxOverviewBytes   = 3000
	MaxChapterTitle    = 180
	MaxChapterSummary  = 1200
)

type Snapshot struct {
	Provider             string `json:"provider"`
	Region               string `json:"region"`
	WorkspaceID          string `json:"workspace_id"`
	Model                string `json:"model"`
	EnableThinking       bool   `json:"enable_thinking"`
	Temperature          int    `json:"temperature"`
	Profile              string `json:"profile"`
	ExtractionPrompt     string `json:"extraction_prompt"`
	MergePrompt          string `json:"merge_prompt"`
	MaxBatches           int    `json:"max_batches"`
	MaxSourcesPerBatch   int    `json:"max_sources_per_batch"`
	MaxBatchBytes        int    `json:"max_batch_bytes"`
	MaxRequestBytes      int    `json:"max_request_bytes"`
	MaxMergeInputBytes   int    `json:"max_merge_input_bytes"`
	MaxTotalAttempts     int    `json:"max_total_attempts"`
	MaxMergeAttempts     int    `json:"max_merge_attempts"`
	MaxChapters          int    `json:"max_chapters"`
	MaxOverviewBytes     int    `json:"max_overview_bytes"`
	MaxChapterTitleBytes int    `json:"max_chapter_title_bytes"`
	MaxChapterSummary    int    `json:"max_chapter_summary_bytes"`
}

func NewSnapshot(workspaceID string) (json.RawMessage, []byte, error) {
	if len(workspaceID) > 128 {
		return nil, nil, errors.New("content profile workspace ID is too long")
	}
	return newSnapshot("aliyun-bailian", "cn-beijing", workspaceID, Model, ProfileVersion)
}

// NewSnapshotForWorkspace selects a profile whose persisted provider identity
// matches the worker configured for the workspace.
func NewSnapshotForWorkspace(workspaceID string) (json.RawMessage, []byte, error) {
	if workspaceID == LocalDemoWorkspace {
		return NewLocalDemoSnapshot()
	}
	return NewSnapshot(workspaceID)
}

// NewLocalDemoSnapshot gives deterministic offline content fixtures a separate
// persisted identity so they cannot be mistaken for Bailian model output.
func NewLocalDemoSnapshot() (json.RawMessage, []byte, error) {
	return newSnapshot("local-demo", "local", LocalDemoWorkspace, LocalDemoModel, LocalDemoProfile)
}

func newSnapshot(provider, region, workspaceID, model, profile string) (json.RawMessage, []byte, error) {
	value := Snapshot{Provider: provider, Region: region, WorkspaceID: workspaceID,
		Model: model, EnableThinking: false, Temperature: 0, Profile: profile,
		ExtractionPrompt: ExtractionPrompt, MergePrompt: MergePrompt, MaxBatches: MaxBatches,
		MaxSourcesPerBatch: MaxSourcesPerBatch, MaxBatchBytes: MaxBatchBytes,
		MaxRequestBytes: MaxRequestBytes, MaxMergeInputBytes: MaxMergeInputBytes,
		MaxTotalAttempts: MaxTotalAttempts, MaxMergeAttempts: MaxMergeAttempts,
		MaxChapters: MaxChapters, MaxOverviewBytes: MaxOverviewBytes,
		MaxChapterTitleBytes: MaxChapterTitle, MaxChapterSummary: MaxChapterSummary}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	hash := sha256.Sum256(encoded)
	return encoded, hash[:], nil
}
