package application

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"mirrorcore/internal/storage/ossstore"
)

const (
	UploadPartSize       int64 = 8 << 20
	UploadMaxBytes       int64 = 2 << 30
	UploadMaxDuration          = 120 * time.Minute
	UploadSessionTTL           = 24 * time.Hour
	UploadOperationLease       = 2 * time.Minute
	UploadPartURLTTL           = 15 * time.Minute
	UploadMaxURLBatch          = 50
)

type UploadSession struct {
	ID                    uuid.UUID  `json:"upload_session_id"`
	VideoID               uuid.UUID  `json:"video_id"`
	OwnerID               uuid.UUID  `json:"-"`
	Status                string     `json:"status"`
	CancellationRequested bool       `json:"cancellation_requested"`
	SourceName            string     `json:"file_name"`
	ExpectedSizeBytes     int64      `json:"expected_size_bytes"`
	PartSizeBytes         int64      `json:"part_size_bytes"`
	DeclaredContentType   string     `json:"declared_content_type"`
	ExpiresAt             time.Time  `json:"expires_at"`
	InitialTaskID         *uuid.UUID `json:"initial_task_id,omitempty"`
	ObjectKey             string     `json:"-"`
	UploadID              string     `json:"-"`
	OperationEpoch        int64      `json:"-"`
	OperationLeaseUntil   *time.Time `json:"-"`
	TerminationReason     string     `json:"-"`
	TerminationAfter      *time.Time `json:"-"`
	RequestHash           []byte     `json:"-"`
}

type UploadInput struct {
	OwnerID     uuid.UUID
	RequestKey  string
	SourceName  string
	SizeBytes   int64
	ContentType string
}

type UploadRecord struct {
	Input       UploadInput
	RequestHash []byte
	SessionID   uuid.UUID
	VideoID     uuid.UUID
	ObjectKey   string
}

type UploadTask struct {
	ID          uuid.UUID `json:"task_id"`
	VideoID     uuid.UUID `json:"video_id"`
	TaskVersion int32     `json:"task_version"`
	Status      string    `json:"status"`
	Stage       string    `json:"stage"`
}

type UploadRepository interface {
	CreateUpload(context.Context, UploadRecord) (UploadSession, bool, error)
	ClaimInitiation(context.Context, uuid.UUID, uuid.UUID) (UploadSession, bool, error)
	MarkInitiated(context.Context, uuid.UUID, int64, string) error
	GetUpload(context.Context, uuid.UUID, uuid.UUID) (UploadSession, error)
	CanIssuePartURLs(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	GetUploadByRequestKey(context.Context, uuid.UUID, string) (UploadSession, error)
	ClaimCompletion(context.Context, uuid.UUID, uuid.UUID) (UploadSession, bool, *UploadTask, error)
	RestoreUploading(context.Context, uuid.UUID, int64) error
	MarkVerifying(context.Context, uuid.UUID, int64) error
	FinalizeUpload(context.Context, uuid.UUID, uuid.UUID, int64, ossstore.ObjectInfo) (UploadTask, error)
	RequestCancel(context.Context, uuid.UUID, uuid.UUID) (UploadSession, error)
}

type UploadService struct {
	repository UploadRepository
	store      ossstore.ObjectStore
}

func NewUploadService(repository UploadRepository, store ossstore.ObjectStore) *UploadService {
	return &UploadService{repository: repository, store: store}
}

type UploadProgress struct {
	Session       UploadSession `json:"session"`
	PartCount     int32         `json:"part_count"`
	UploadedParts []PartStatus  `json:"uploaded_parts"`
	UploadedBytes int64         `json:"uploaded_bytes"`
}

type PartStatus struct {
	PartNumber int32 `json:"part_number"`
	SizeBytes  int64 `json:"size_bytes"`
}

type PartURLInput struct {
	PartNumbers []int32 `json:"part_numbers"`
}

type PartURL struct {
	PartNumber int32                  `json:"part_number"`
	Request    ossstore.SignedRequest `json:"request"`
}

type CompleteUploadResult struct {
	SessionID uuid.UUID   `json:"upload_session_id"`
	VideoID   uuid.UUID   `json:"video_id"`
	Status    string      `json:"status"`
	Task      *UploadTask `json:"initial_task,omitempty"`
}

type CompletePart struct {
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
}

type CompleteUploadInput struct {
	Parts []CompletePart `json:"parts"`
}

func (s *UploadService) Create(ctx context.Context, input UploadInput, requestID string) (UploadSession, bool, error) {
	if s.store == nil {
		return UploadSession{}, false, &ServiceError{Status: 503, Code: "STORAGE_NOT_CONFIGURED", Message: "视频存储尚未配置", Retryable: false}
	}
	if input.RequestKey == "" || len(input.RequestKey) > 128 {
		return UploadSession{}, false, &ServiceError{Status: 400, Code: "INVALID_IDEMPOTENCY_KEY", Message: "请提供有效的 Idempotency-Key"}
	}
	if !utf8.ValidString(input.SourceName) || len(input.SourceName) == 0 || len(input.SourceName) > 255 ||
		filepath.Base(input.SourceName) != input.SourceName || strings.ContainsAny(input.SourceName, "/\\") ||
		!strings.EqualFold(filepath.Ext(input.SourceName), ".mp4") {
		return UploadSession{}, false, &ServiceError{Status: 422, Code: "UNSUPPORTED_FILE_NAME", Message: "首版只接受MP4文件名"}
	}
	for _, character := range input.SourceName {
		if unicode.IsControl(character) {
			return UploadSession{}, false, &ServiceError{Status: 422, Code: "UNSUPPORTED_FILE_NAME", Message: "文件名包含不可用字符"}
		}
	}
	if input.SizeBytes <= 0 || input.SizeBytes > UploadMaxBytes {
		return UploadSession{}, false, &ServiceError{Status: 413, Code: "FILE_SIZE_LIMIT", Message: "视频文件大小须在1字节至2GiB之间"}
	}
	input.ContentType = strings.TrimSpace(strings.ToLower(input.ContentType))
	if input.ContentType == "" {
		input.ContentType = "video/mp4"
	}
	if len(input.ContentType) > 100 || (input.ContentType != "video/mp4" && input.ContentType != "application/octet-stream") {
		return UploadSession{}, false, &ServiceError{Status: 422, Code: "UNSUPPORTED_CONTENT_TYPE", Message: "声明的文件类型不受支持"}
	}
	canonical, err := json.Marshal(struct {
		SourceName  string `json:"file_name"`
		SizeBytes   int64  `json:"size_bytes"`
		ContentType string `json:"content_type"`
	}{input.SourceName, input.SizeBytes, input.ContentType})
	if err != nil {
		return UploadSession{}, false, fmt.Errorf("encode upload request identity: %w", err)
	}
	requestHash := sha256.Sum256(canonical)
	record := UploadRecord{Input: input, RequestHash: requestHash[:], SessionID: uuid.New(), VideoID: uuid.New()}
	record.ObjectKey = fmt.Sprintf("source/%s/%s/%s", record.VideoID, record.SessionID, uuid.New())
	session, created, err := s.repository.CreateUpload(ctx, record)
	if err != nil {
		return UploadSession{}, false, err
	}
	if session.Status != "INITIATING" {
		return session, !created, nil
	}
	claim := created
	if !created {
		session, claim, err = s.repository.ClaimInitiation(ctx, input.OwnerID, session.ID)
		if err != nil {
			return UploadSession{}, false, err
		}
	}
	if !claim {
		return session, !created, nil
	}
	operationCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	uploadID, err := s.store.InitiateMultipart(operationCtx, session.ObjectKey, session.ID.String(), session.VideoID.String())
	if err != nil {
		return session, false, &ServiceError{Status: 503, Code: "STORAGE_OPERATION_UNKNOWN", Message: "上传初始化状态待核实，请查询或重试", Retryable: true, RetryAfterMS: 2000}
	}
	if err := s.repository.MarkInitiated(ctx, session.ID, session.OperationEpoch, uploadID); err != nil {
		return session, false, fmt.Errorf("persist OSS multipart upload identity: %w", err)
	}
	session.Status, session.UploadID = "UPLOADING", uploadID
	session.OperationLeaseUntil = nil
	return session, !created, nil
}

func (s *UploadService) Get(ctx context.Context, ownerID, sessionID uuid.UUID) (UploadProgress, error) {
	session, err := s.repository.GetUpload(ctx, ownerID, sessionID)
	if err != nil {
		return UploadProgress{}, err
	}
	progress := UploadProgress{Session: session, PartCount: partCount(session.ExpectedSizeBytes), UploadedParts: []PartStatus{}}
	if session.Status != "UPLOADING" || session.UploadID == "" {
		return progress, nil
	}
	parts, err := s.store.ListParts(ctx, session.ObjectKey, session.UploadID)
	if err != nil {
		return UploadProgress{}, &ServiceError{Status: 503, Code: "STORAGE_UNAVAILABLE", Message: "暂时无法查询已上传分片", Retryable: true, RetryAfterMS: 1000}
	}
	for _, part := range parts {
		progress.UploadedParts = append(progress.UploadedParts, PartStatus{PartNumber: part.Number, SizeBytes: part.Size})
		progress.UploadedBytes += part.Size
	}
	sort.Slice(progress.UploadedParts, func(i, j int) bool {
		return progress.UploadedParts[i].PartNumber < progress.UploadedParts[j].PartNumber
	})
	return progress, nil
}

func (s *UploadService) Lookup(ctx context.Context, ownerID uuid.UUID, requestKey string) (UploadProgress, error) {
	if requestKey == "" || len(requestKey) > 128 {
		return UploadProgress{}, &ServiceError{Status: 400, Code: "INVALID_IDEMPOTENCY_KEY", Message: "请提供有效的创建键"}
	}
	session, err := s.repository.GetUploadByRequestKey(ctx, ownerID, requestKey)
	if err != nil {
		return UploadProgress{}, err
	}
	return s.Get(ctx, ownerID, session.ID)
}

func (s *UploadService) PartURLs(ctx context.Context, ownerID, sessionID uuid.UUID, input PartURLInput) ([]PartURL, error) {
	session, err := s.repository.GetUpload(ctx, ownerID, sessionID)
	if err != nil {
		return nil, err
	}
	if session.Status != "UPLOADING" || session.UploadID == "" {
		return nil, &ServiceError{Status: 409, Code: "UPLOAD_NOT_ACTIVE", Message: "上传会话当前不可签发分片URL"}
	}
	canSign, err := s.repository.CanIssuePartURLs(ctx, ownerID, sessionID)
	if err != nil {
		return nil, err
	}
	if !canSign {
		return nil, &ServiceError{Status: 409, Code: "UPLOAD_NOT_ACTIVE", Message: "上传会话当前不可签发分片URL"}
	}
	if len(input.PartNumbers) == 0 || len(input.PartNumbers) > UploadMaxURLBatch {
		return nil, &ServiceError{Status: 400, Code: "INVALID_PART_BATCH", Message: "每次须请求1至50个分片URL"}
	}
	maxPart := partCount(session.ExpectedSizeBytes)
	seen := make(map[int32]struct{}, len(input.PartNumbers))
	result := make([]PartURL, 0, len(input.PartNumbers))
	for _, number := range input.PartNumbers {
		if number < 1 || number > maxPart {
			return nil, &ServiceError{Status: 400, Code: "INVALID_PART_NUMBER", Message: "分片编号超出本文件范围"}
		}
		if _, exists := seen[number]; exists {
			return nil, &ServiceError{Status: 400, Code: "DUPLICATE_PART_NUMBER", Message: "分片编号不能重复"}
		}
		seen[number] = struct{}{}
		expectedLength := session.PartSizeBytes
		if number == maxPart {
			expectedLength = session.ExpectedSizeBytes - int64(maxPart-1)*session.PartSizeBytes
		}
		request, err := s.store.PresignPart(ctx, session.ObjectKey, session.UploadID, number, expectedLength, UploadPartURLTTL)
		if err != nil {
			return nil, &ServiceError{Status: 503, Code: "STORAGE_UNAVAILABLE", Message: "暂时无法签发分片URL", Retryable: true, RetryAfterMS: 1000}
		}
		result = append(result, PartURL{PartNumber: number, Request: request})
	}
	return result, nil
}

func (s *UploadService) Complete(ctx context.Context, ownerID, sessionID uuid.UUID, input CompleteUploadInput) (CompleteUploadResult, error) {
	session, claim, task, err := s.repository.ClaimCompletion(ctx, ownerID, sessionID)
	if err != nil {
		return CompleteUploadResult{}, err
	}
	if task != nil {
		return CompleteUploadResult{SessionID: session.ID, VideoID: session.VideoID, Status: "COMPLETE", Task: task}, nil
	}
	if !claim {
		return CompleteUploadResult{SessionID: session.ID, VideoID: session.VideoID, Status: session.Status}, nil
	}

	operationCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	info, exists, err := s.store.HeadObject(operationCtx, session.ObjectKey)
	if err != nil {
		s.markVerifying(session)
		return CompleteUploadResult{}, &ServiceError{Status: 503, Code: "STORAGE_VERIFY_FAILED", Message: "暂时无法核实上传对象，请稍后查询或重试", Retryable: true, RetryAfterMS: 2000}
	}
	if exists {
		if err := verifyUploadedObject(session, info); err != nil {
			s.markVerifying(session)
			return CompleteUploadResult{}, err
		}
		return s.finalize(ctx, ownerID, session, info)
	}

	parts, err := s.store.ListParts(operationCtx, session.ObjectKey, session.UploadID)
	if err != nil {
		s.markVerifying(session)
		return CompleteUploadResult{}, &ServiceError{Status: 503, Code: "STORAGE_VERIFY_FAILED", Message: "暂时无法核实已上传分片", Retryable: true, RetryAfterMS: 2000}
	}
	parts, err = validateUploadedParts(session, parts, input.Parts)
	if err != nil {
		if restoreErr := s.repository.RestoreUploading(ctx, session.ID, session.OperationEpoch); restoreErr != nil {
			return CompleteUploadResult{}, errors.Join(err, restoreErr)
		}
		return CompleteUploadResult{}, err
	}
	if err := s.store.CompleteMultipart(operationCtx, session.ObjectKey, session.UploadID, parts); err != nil {
		s.markVerifying(session)
		return CompleteUploadResult{}, &ServiceError{Status: 503, Code: "STORAGE_VERIFY_FAILED", Message: "合并结果待核实，请稍后重试", Retryable: true, RetryAfterMS: 2000}
	}
	info, exists, err = s.store.HeadObject(operationCtx, session.ObjectKey)
	if err != nil || !exists {
		s.markVerifying(session)
		return CompleteUploadResult{}, &ServiceError{Status: 503, Code: "STORAGE_VERIFY_FAILED", Message: "合并结果待核实，请稍后重试", Retryable: true, RetryAfterMS: 2000}
	}
	if err := verifyUploadedObject(session, info); err != nil {
		s.markVerifying(session)
		return CompleteUploadResult{}, err
	}
	return s.finalize(ctx, ownerID, session, info)
}

// Cancel stops issuing new part URLs immediately. The background maintenance
// loop applies a bounded URL/request grace period before aborting the multipart
// upload and releasing the reserved parse slot.
func (s *UploadService) Cancel(ctx context.Context, ownerID, sessionID uuid.UUID) (UploadSession, error) {
	return s.repository.RequestCancel(ctx, ownerID, sessionID)
}

func (s *UploadService) finalize(ctx context.Context, ownerID uuid.UUID, session UploadSession, info ossstore.ObjectInfo) (CompleteUploadResult, error) {
	task, err := s.repository.FinalizeUpload(ctx, session.ID, ownerID, session.OperationEpoch, info)
	if err != nil {
		return CompleteUploadResult{}, err
	}
	return CompleteUploadResult{SessionID: session.ID, VideoID: session.VideoID, Status: "COMPLETE", Task: &task}, nil
}

func (s *UploadService) markVerifying(session UploadSession) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.repository.MarkVerifying(ctx, session.ID, session.OperationEpoch)
}

func validateUploadedParts(session UploadSession, serverParts []ossstore.UploadedPart, clientParts []CompletePart) ([]ossstore.UploadedPart, error) {
	wantCount := partCount(session.ExpectedSizeBytes)
	if len(serverParts) != int(wantCount) || len(clientParts) != int(wantCount) {
		return nil, &ServiceError{Status: 409, Code: "UPLOAD_INCOMPLETE", Message: "还有分片未上传完成，或客户端未提交全部分片ETag"}
	}
	sort.Slice(serverParts, func(i, j int) bool { return serverParts[i].Number < serverParts[j].Number })
	validated := make([]ossstore.UploadedPart, 0, len(serverParts))
	for i, part := range serverParts {
		partNumber := int32(i + 1)
		if part.Number != partNumber || part.ETag == "" {
			return nil, &ServiceError{Status: 409, Code: "UPLOAD_PARTS_INVALID", Message: "服务端核实的分片列表不完整"}
		}
		clientPart := clientParts[i]
		if clientPart.PartNumber != partNumber || clientPart.ETag == "" || len(clientPart.ETag) > 128 ||
			clientPart.ETag != strings.TrimSpace(clientPart.ETag) || strings.ContainsAny(clientPart.ETag, "\r\n") {
			return nil, &ServiceError{Status: 400, Code: "INVALID_PART_ETAG", Message: "请按顺序提交每个分片上传响应中的ETag"}
		}
		if clientPart.ETag != part.ETag {
			return nil, &ServiceError{Status: 409, Code: "UPLOAD_PART_ETAG_MISMATCH", Message: "分片内容已变化，请重新上传对应分片并记录ETag"}
		}
		wantSize := session.PartSizeBytes
		if partNumber == wantCount {
			wantSize = session.ExpectedSizeBytes - int64(wantCount-1)*session.PartSizeBytes
		}
		if part.Size != wantSize {
			return nil, &ServiceError{Status: 409, Code: "UPLOAD_PART_SIZE_MISMATCH", Message: "分片大小与上传声明不匹配"}
		}
		validated = append(validated, ossstore.UploadedPart{Number: partNumber, Size: part.Size, ETag: clientPart.ETag})
	}
	return validated, nil
}

func verifyUploadedObject(session UploadSession, info ossstore.ObjectInfo) error {
	if info.Size != session.ExpectedSizeBytes || metadataValue(info.Metadata, "upload-session-id") != session.ID.String() || metadataValue(info.Metadata, "video-id") != session.VideoID.String() {
		return &ServiceError{Status: 409, Code: "UPLOAD_OBJECT_MISMATCH", Message: "对象大小或会话标识与上传声明不匹配"}
	}
	return nil
}

func metadataValue(metadata map[string]string, name string) string {
	for key, value := range metadata {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func partCount(sizeBytes int64) int32 {
	return int32(math.Ceil(float64(sizeBytes) / float64(UploadPartSize)))
}
