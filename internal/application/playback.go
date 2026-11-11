package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/storage/ossstore"
)

const PlaybackURLTTL = 2 * time.Hour

type PlaybackSource struct {
	VideoID   uuid.UUID
	Bucket    string
	ObjectKey string
	VersionID string
	StartMS   int64
	EndMS     int64
	Duration  int64
}

type PlaybackRepository interface {
	GetPlaybackSource(context.Context, uuid.UUID, uuid.UUID, *uuid.UUID, string, *uuid.UUID, string) (PlaybackSource, error)
}

type EvidencePlayback struct {
	VideoID         uuid.UUID              `json:"video_id"`
	Question        *uuid.UUID             `json:"question_id,omitempty"`
	Evidence        string                 `json:"evidence_id,omitempty"`
	ContentArtifact *uuid.UUID             `json:"content_artifact_id,omitempty"`
	ContentRef      string                 `json:"content_ref_id,omitempty"`
	StartMS         int64                  `json:"start_ms,omitempty"`
	EndMS           int64                  `json:"end_ms,omitempty"`
	Request         ossstore.SignedRequest `json:"request"`
}

type PlaybackService struct {
	repository PlaybackRepository
	store      ossstore.ObjectStore
	bucket     string
}

func NewPlaybackService(repository PlaybackRepository, store ossstore.ObjectStore, bucket string) *PlaybackService {
	return &PlaybackService{repository: repository, store: store, bucket: bucket}
}

func (s *PlaybackService) ForVideo(ctx context.Context, ownerID, videoID uuid.UUID, questionID *uuid.UUID,
	evidenceID string, contentArtifactID *uuid.UUID, contentRefID string) (EvidencePlayback, error) {
	if s.repository == nil || s.store == nil || s.bucket == "" {
		return EvidencePlayback{}, &ServiceError{Status: 503, Code: "PLAYBACK_UNAVAILABLE", Message: "播放服务暂不可用", Retryable: true, RetryAfterMS: 1000}
	}
	questionReference := questionID != nil || evidenceID != ""
	contentReference := contentArtifactID != nil || contentRefID != ""
	if questionReference && ((questionID == nil) != (evidenceID == "")) {
		return EvidencePlayback{}, &ServiceError{Status: 400, Code: "INVALID_PLAYBACK_REFERENCE", Message: "question_id与evidence_id必须同时提供"}
	}
	if contentReference && ((contentArtifactID == nil) != (contentRefID == "")) {
		return EvidencePlayback{}, &ServiceError{Status: 400, Code: "INVALID_PLAYBACK_REFERENCE", Message: "content_artifact_id与content_ref_id必须同时提供"}
	}
	if questionReference && contentReference {
		return EvidencePlayback{}, &ServiceError{Status: 400, Code: "INVALID_PLAYBACK_REFERENCE", Message: "一次播放请求只能使用一种来源引用"}
	}
	if evidenceID != "" && (len(evidenceID) < 2 || len(evidenceID) > 12) {
		return EvidencePlayback{}, &ServiceError{Status: 404, Code: "NOT_FOUND", Message: "引用不存在"}
	}
	source, err := s.repository.GetPlaybackSource(ctx, ownerID, videoID, questionID, evidenceID, contentArtifactID, contentRefID)
	if err != nil {
		return EvidencePlayback{}, err
	}
	if source.Bucket != s.bucket || source.Duration <= 0 || source.StartMS < 0 || source.EndMS < 0 ||
		(source.EndMS != 0 && (source.EndMS <= source.StartMS || source.EndMS > source.Duration)) {
		return EvidencePlayback{}, errors.New("stored playback source violates media bounds")
	}
	request, err := s.store.PresignGet(ctx, source.ObjectKey, source.VersionID, PlaybackURLTTL)
	if err != nil {
		return EvidencePlayback{}, &ServiceError{Status: 503, Code: "PLAYBACK_SIGNING_FAILED", Message: "暂时无法生成播放链接", Retryable: true, RetryAfterMS: 1000}
	}
	return EvidencePlayback{VideoID: source.VideoID, Question: questionID, Evidence: evidenceID,
		ContentArtifact: contentArtifactID, ContentRef: contentRefID,
		StartMS: source.StartMS, EndMS: source.EndMS, Request: request}, nil
}
