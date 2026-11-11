package application

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const defaultProfileID = "voice-v1"

type CreateParseTaskInput struct {
	VideoID       uuid.UUID
	OwnerID       uuid.UUID
	RequestKey    string
	TraceID       string
	ProfileID     string `json:"profile_id"`
	ReuseMode     string `json:"reuse_mode"`
	RecomputeFrom string `json:"recompute_from"`
}

type CreateParseTaskRecord struct {
	Input          CreateParseTaskInput
	RequestHash    []byte
	ConfigSnapshot []byte
}

type ParseTask struct {
	ID            uuid.UUID                         `json:"task_id"`
	VideoID       uuid.UUID                         `json:"video_id"`
	TaskVersion   int32                             `json:"task_version"`
	Status        string                            `json:"status"`
	Stage         string                            `json:"stage"`
	WaitReason    string                            `json:"wait_reason,omitempty"`
	NextRunAt     time.Time                         `json:"next_run_at"`
	CreatedAt     time.Time                         `json:"created_at"`
	CompletedAt   *time.Time                        `json:"completed_at,omitempty"`
	DeadlineAt    *time.Time                        `json:"deadline_at,omitempty"`
	RetryAllowed  bool                              `json:"retry_allowed"`
	LastError     *ParseTaskError                   `json:"last_error,omitempty"`
	StageProgress map[string]ParseTaskStageProgress `json:"stage_progress,omitempty"`
}

type ParseTaskError struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

type ParseTaskStageProgress struct {
	State          string `json:"state"`
	FailureCount   int    `json:"failure_count"`
	CommittedCount int    `json:"committed_count"`
	ExpectedCount  int    `json:"expected_count"`
}

type Video struct {
	ID             uuid.UUID  `json:"video_id"`
	FileState      string     `json:"file_state"`
	Lifecycle      string     `json:"lifecycle"`
	SourceName     string     `json:"source_name"`
	SourceSize     int64      `json:"source_size_bytes"`
	LatestTask     *ParseTask `json:"latest_task,omitempty"`
	ActiveResultID *uuid.UUID `json:"active_result_id,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

type Page struct {
	Videos     []Video `json:"videos"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

type ResourceRepository interface {
	CreateParseTask(context.Context, CreateParseTaskRecord) (ParseTask, bool, error)
	ListVideos(context.Context, uuid.UUID, int, *time.Time, *uuid.UUID) (Page, error)
	GetVideo(context.Context, uuid.UUID, uuid.UUID) (Video, error)
	GetParseTask(context.Context, uuid.UUID, uuid.UUID) (ParseTask, error)
	LookupParseTask(context.Context, uuid.UUID, string) (ParseTask, error)
	RetryParseTask(context.Context, uuid.UUID, uuid.UUID, string) (ParseTask, bool, error)
	CancelParseTask(context.Context, uuid.UUID, uuid.UUID) (ParseTask, bool, error)
	DeleteVideo(context.Context, uuid.UUID, uuid.UUID) (VideoDeletion, error)
}

type VideoDeletion struct {
	DeletionID     uuid.UUID `json:"deletion_id"`
	VideoID        uuid.UUID `json:"video_id"`
	State          string    `json:"state"`
	Phase          string    `json:"phase"`
	BlockingReason string    `json:"blocking_reason,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type ResourceService struct{ repository ResourceRepository }

func NewResourceService(repository ResourceRepository) *ResourceService {
	return &ResourceService{repository: repository}
}

type ServiceError struct {
	Status       int
	Code         string
	Message      string
	Retryable    bool
	RetryAfterMS int64
}

func (e *ServiceError) Error() string { return e.Code + ": " + e.Message }

func (s *ResourceService) CreateParseTask(ctx context.Context, input CreateParseTaskInput) (ParseTask, bool, error) {
	if input.RequestKey == "" || len(input.RequestKey) > 128 {
		return ParseTask{}, false, &ServiceError{Status: 400, Code: "INVALID_IDEMPOTENCY_KEY", Message: "请提供有效的 Idempotency-Key"}
	}
	if input.ProfileID == "" {
		input.ProfileID = defaultProfileID
	}
	if input.ReuseMode == "" {
		input.ReuseMode = "reuse_ready"
	}
	if input.ProfileID != defaultProfileID || (input.ReuseMode != "reuse_ready" && input.ReuseMode != "recompute_from") {
		return ParseTask{}, false, &ServiceError{Status: 422, Code: "PROFILE_NOT_ALLOWED", Message: "解析配置不受支持"}
	}
	if input.ReuseMode == "reuse_ready" && input.RecomputeFrom != "" {
		return ParseTask{}, false, &ServiceError{Status: 422, Code: "INVALID_RECOMPUTE_STAGE", Message: "reuse_ready 不能指定重建起点"}
	}
	if input.ReuseMode == "recompute_from" && input.RecomputeFrom != "AUDIO" && input.RecomputeFrom != "ASR" && input.RecomputeFrom != "CHUNK" && input.RecomputeFrom != "EMBEDDING" {
		return ParseTask{}, false, &ServiceError{Status: 422, Code: "INVALID_RECOMPUTE_STAGE", Message: "重建起点必须是 AUDIO、ASR、CHUNK 或 EMBEDDING"}
	}

	canonicalRequest := struct {
		ProfileID     string `json:"profile_id"`
		ReuseMode     string `json:"reuse_mode"`
		RecomputeFrom string `json:"recompute_from"`
	}{input.ProfileID, input.ReuseMode, input.RecomputeFrom}
	encodedRequest, err := json.Marshal(canonicalRequest)
	if err != nil {
		return ParseTask{}, false, fmt.Errorf("encode idempotency request: %w", err)
	}
	configSnapshot, err := json.Marshal(map[string]any{
		"profile_id": input.ProfileID, "reuse_mode": input.ReuseMode,
		"recompute_from": input.RecomputeFrom, "asr_provider": "aliyun-bailian",
		"asr_model": "fun-asr", "asr_region": "cn-beijing",
		"embedding_model": "text-embedding-v4", "embedding_dimensions": 1024,
		"generation_model": "qwen3.7-plus-2026-05-26", "enable_thinking": false,
	})
	if err != nil {
		return ParseTask{}, false, fmt.Errorf("encode task config snapshot: %w", err)
	}
	requestHash := sha256.Sum256(encodedRequest)
	return s.repository.CreateParseTask(ctx, CreateParseTaskRecord{
		Input: input, RequestHash: requestHash[:], ConfigSnapshot: configSnapshot,
	})
}

func (s *ResourceService) ListVideos(ctx context.Context, ownerID uuid.UUID, limit int, cursorCreatedAt *time.Time, cursorID *uuid.UUID) (Page, error) {
	if limit < 1 || limit > 100 {
		return Page{}, &ServiceError{Status: 400, Code: "INVALID_LIMIT", Message: "limit 必须在1到100之间"}
	}
	return s.repository.ListVideos(ctx, ownerID, limit, cursorCreatedAt, cursorID)
}

func (s *ResourceService) GetVideo(ctx context.Context, ownerID, videoID uuid.UUID) (Video, error) {
	return s.repository.GetVideo(ctx, ownerID, videoID)
}

func (s *ResourceService) GetParseTask(ctx context.Context, ownerID, taskID uuid.UUID) (ParseTask, error) {
	return s.repository.GetParseTask(ctx, ownerID, taskID)
}

func (s *ResourceService) LookupParseTask(ctx context.Context, ownerID uuid.UUID, requestKey string) (ParseTask, error) {
	if requestKey == "" || len(requestKey) > 128 {
		return ParseTask{}, &ServiceError{Status: 400, Code: "INVALID_IDEMPOTENCY_KEY", Message: "请求键无效"}
	}
	return s.repository.LookupParseTask(ctx, ownerID, requestKey)
}

func (s *ResourceService) RetryParseTask(ctx context.Context, ownerID, taskID uuid.UUID, requestKey string) (ParseTask, bool, error) {
	if requestKey == "" || len(requestKey) > 128 {
		return ParseTask{}, false, &ServiceError{Status: 400, Code: "INVALID_IDEMPOTENCY_KEY", Message: "请提供有效的 Idempotency-Key"}
	}
	return s.repository.RetryParseTask(ctx, ownerID, taskID, requestKey)
}

func (s *ResourceService) CancelParseTask(ctx context.Context, ownerID, taskID uuid.UUID) (ParseTask, bool, error) {
	return s.repository.CancelParseTask(ctx, ownerID, taskID)
}

func (s *ResourceService) DeleteVideo(ctx context.Context, ownerID, videoID uuid.UUID) (VideoDeletion, error) {
	return s.repository.DeleteVideo(ctx, ownerID, videoID)
}

func IsNotFound(err error) bool {
	var serviceErr *ServiceError
	return errors.As(err, &serviceErr) && serviceErr.Status == 404
}
