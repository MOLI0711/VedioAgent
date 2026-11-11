package mediaworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/providers/funasr"
	"mirrorcore/internal/repository/postgres"
)

const (
	transcriptProfile   = "video-transcript-normalizer-v1"
	transcriptBatchSize = 200
	maxTranscriptItems  = 1_000_000
	maxTimestampOverrun = int64(1_000)
)

type TranscriptProcessor struct {
	repository *postgres.StageRepository
	store      ArtifactStore
	bucket     string
}

func NewTranscriptProcessor(repository *postgres.StageRepository, store ArtifactStore, bucket string) (*TranscriptProcessor, error) {
	if repository == nil || store == nil || bucket == "" {
		return nil, errors.New("transcript processor requires repository, object store, and bucket")
	}
	return &TranscriptProcessor{repository: repository, store: store, bucket: bucket}, nil
}

func (p *TranscriptProcessor) Process(ctx context.Context, task tasks.Task) error {
	stageTask := postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID,
		Stage: task.Stage, RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}
	input, err := p.repository.LoadTranscriptInput(ctx, stageTask)
	if err != nil {
		return &StageFailure{Code: "TRANSCRIPT_INPUT_NOT_READY", Retryable: false, Err: err}
	}
	profileHash, _, err := postgres.HashJSON(struct {
		Profile            string `json:"profile"`
		BlankText          string `json:"blank_text"`
		Ordering           string `json:"ordering"`
		TimestampPolicy    string `json:"timestamp_policy"`
		MaxBoundaryOverrun int64  `json:"max_boundary_overrun_ms"`
	}{transcriptProfile, "trim-and-skip", "stable-begin-time-then-source-index",
		"audio-origin-to-video-timeline;clip-partial-start-end;drop-fully-outside", maxTimestampOverrun})
	if err != nil {
		return err
	}
	var inputFingerprint []byte
	if input.ASRState == "NO_AUDIO" {
		inputFingerprint, _, err = postgres.HashJSON(struct {
			ASRInputFingerprint string    `json:"asr_input_fingerprint"`
			ASRProfileHash      string    `json:"asr_profile_hash"`
			SourceBlobID        uuid.UUID `json:"source_blob_id"`
		}{hex.EncodeToString(input.ASRInputFingerprint), hex.EncodeToString(input.ASRProfileHash), input.SourceBlobID})
	} else {
		inputFingerprint, _, err = postgres.HashJSON(struct {
			RawResultBlobID uuid.UUID `json:"raw_result_blob_id"`
			RawResultSHA256 string    `json:"raw_result_sha256"`
			ASRProfileHash  string    `json:"asr_profile_hash"`
			AudioOriginMS   int64     `json:"audio_origin_ms"`
		}{input.RawResult.ID, hex.EncodeToString(input.RawResult.SHA256), hex.EncodeToString(input.ASRProfileHash), input.AudioOriginMS})
	}
	if err != nil {
		return err
	}
	if _, err := p.repository.StartStage(ctx, stageTask, input.SourceBlobID, inputFingerprint, profileHash); err != nil {
		return err
	}
	if input.AudioOriginMS < -input.MediaDurationMS || input.AudioOriginMS > input.MediaDurationMS {
		return &StageFailure{Code: "TRANSCRIPT_AUDIO_TIMELINE_INVALID", Retryable: false,
			Err: errors.New("audio origin is outside the source video timeline")}
	}

	var segments []postgres.TranscriptSegment
	var outsideVideoCount int
	if input.ASRState == "READY" {
		body, err := p.loadVerifiedRawResult(ctx, task, input)
		if err != nil {
			return err
		}
		segments, outsideVideoCount, err = normalizeTranscript(body, input.AudioOriginMS, input.MediaDurationMS)
		if err != nil {
			return &StageFailure{Code: "INVALID_TRANSCRIPT", Retryable: false, Err: err}
		}
	} else if input.ASRState != "NO_AUDIO" {
		return &StageFailure{Code: "TRANSCRIPT_ASR_STATE_INVALID", Retryable: false,
			Err: fmt.Errorf("unsupported ASR checkpoint state %q", input.ASRState)}
	}
	if len(segments) > maxTranscriptItems {
		return &StageFailure{Code: "TRANSCRIPT_TOO_LARGE", Retryable: false,
			Err: fmt.Errorf("transcript contains more than %d normalized sentences", maxTranscriptItems)}
	}
	artifact, err := p.repository.PrepareTranscript(ctx, stageTask, input, inputFingerprint, profileHash, len(segments))
	if err != nil {
		return err
	}
	if artifact.LastCommitted > len(segments) {
		return &StageFailure{Code: "TRANSCRIPT_CHECKPOINT_INVALID", Retryable: false,
			Err: errors.New("transcript committed prefix exceeds normalized source")}
	}
	if artifact.State == "BUILDING" {
		if err := p.repository.VerifyTranscriptPrefix(ctx, artifact.ID, segments, artifact.LastCommitted); err != nil {
			markErr := p.repository.MarkTranscriptBroken(ctx, stageTask, artifact.ID)
			return &StageFailure{Code: "TRANSCRIPT_CHECKPOINT_CORRUPT", Retryable: true, Err: errors.Join(err, markErr)}
		}
	} else if !artifact.Complete() {
		return &StageFailure{Code: "TRANSCRIPT_CHECKPOINT_INVALID", Retryable: false,
			Err: errors.New("published transcript is missing its complete continuous segment prefix")}
	}
	if artifact.State != "READY" && artifact.State != "NO_SPEECH" {
		for start := artifact.LastCommitted; start < len(segments); {
			end := min(start+transcriptBatchSize, len(segments))
			if err := p.repository.SaveTranscriptBatch(ctx, stageTask, artifact.ID, start+1, len(segments), segments[start:end]); err != nil {
				return err
			}
			start = end
		}
	}
	state, reason := "READY", ""
	if len(segments) == 0 {
		state = "NO_SPEECH"
		if input.ASRState == "NO_AUDIO" {
			reason = "NO_AUDIO_STREAM"
		} else {
			reason = "NO_RECOGNIZABLE_SPEECH"
		}
	}
	metadata, _ := json.Marshal(map[string]any{"profile": transcriptProfile, "source_asr_blob_id": nullableUUID(input.RawResult.ID),
		"audio_blob_id": nullableUUIDPtr(input.AudioBlobID), "audio_origin_ms": input.AudioOriginMS,
		"segment_count": len(segments), "state": state, "reason": reason,
		"boundary_clipped_count": clippedCount(segments), "outside_video_sentence_count": outsideVideoCount})
	return p.repository.CompleteTranscript(ctx, stageTask, input, artifact.ID, inputFingerprint, profileHash, state, metadata)
}

func (p *TranscriptProcessor) loadVerifiedRawResult(ctx context.Context, task tasks.Task, input postgres.TranscriptInput) ([]byte, error) {
	if input.RawResult.Bucket != p.bucket || input.RawResult.State != "READY" || input.RawResult.SizeBytes <= 0 ||
		input.RawResult.SizeBytes > funasr.MaxResultBytes || len(input.RawResult.SHA256) != sha256.Size ||
		!bytes.Equal(input.RawResult.ArtifactFingerprint, input.ASRInputFingerprint) ||
		!bytes.Equal(input.RawResult.ProfileHash, input.ASRProfileHash) {
		return nil, &StageFailure{Code: "ASR_RAW_BLOB_INVALID", Retryable: false,
			Err: errors.New("raw ASR result database metadata does not match the committed ASR checkpoint")}
	}
	checkCtx, cancelCheck := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, input.RawResult.ObjectKey, input.RawResult.ObjectVersion)
	cancelCheck()
	if err != nil {
		return nil, &StageFailure{Code: "ASR_RAW_HEAD_FAILED", Retryable: true, Err: err}
	}
	if !exists || info.Size != input.RawResult.SizeBytes || !objectVersionMatches(input.RawResult.ObjectVersion, info.VersionID) ||
		!metadataEquals(info.Metadata, "artifact-fingerprint", hex.EncodeToString(input.ASRInputFingerprint)) ||
		!metadataEquals(info.Metadata, "profile-hash", hex.EncodeToString(input.ASRProfileHash)) ||
		!metadataEquals(info.Metadata, "sha256", hex.EncodeToString(input.RawResult.SHA256)) ||
		!metadataEquals(info.Metadata, "video-id", task.VideoID.String()) {
		return nil, &StageFailure{Code: "ASR_RAW_OBJECT_MISMATCH", Retryable: false,
			Err: errors.New("raw ASR object does not match its persisted blob checkpoint")}
	}
	var body bytes.Buffer
	downloadCtx, cancelDownload := context.WithTimeout(ctx, 60*time.Second)
	objectInfo, err := p.store.DownloadObject(downloadCtx, input.RawResult.ObjectKey, input.RawResult.ObjectVersion,
		&body, funasr.MaxResultBytes)
	cancelDownload()
	if err != nil {
		return nil, &StageFailure{Code: "ASR_RAW_DOWNLOAD_FAILED", Retryable: true, Err: err}
	}
	if objectInfo.Size != input.RawResult.SizeBytes || !objectVersionMatches(input.RawResult.ObjectVersion, objectInfo.VersionID) {
		return nil, &StageFailure{Code: "ASR_RAW_DOWNLOAD_MISMATCH", Retryable: false,
			Err: errors.New("downloaded ASR result did not match the committed object version and size")}
	}
	sha := sha256.Sum256(body.Bytes())
	if !bytes.Equal(sha[:], input.RawResult.SHA256) {
		return nil, &StageFailure{Code: "ASR_RAW_CHECKSUM_MISMATCH", Retryable: false,
			Err: errors.New("raw ASR result checksum differs from its database record")}
	}
	if err := funasr.ValidateResult(body.Bytes()); err != nil {
		return nil, &StageFailure{Code: "ASR_RAW_RESULT_INVALID", Retryable: false, Err: err}
	}
	return body.Bytes(), nil
}

type providerTranscript struct {
	Transcripts []struct {
		Sentences []struct {
			BeginTime int64  `json:"begin_time"`
			EndTime   int64  `json:"end_time"`
			Text      string `json:"text"`
		} `json:"sentences"`
	} `json:"transcripts"`
}

func normalizeTranscript(body []byte, audioOriginMS, mediaDurationMS int64) ([]postgres.TranscriptSegment, int, error) {
	if mediaDurationMS <= 0 || audioOriginMS < -mediaDurationMS || audioOriginMS > mediaDurationMS {
		return nil, 0, errors.New("transcript requires a bounded audio origin and known video duration")
	}
	var result providerTranscript
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, 0, fmt.Errorf("decode provider transcript: %w", err)
	}
	if result.Transcripts == nil {
		return nil, 0, errors.New("provider result lacks transcript array")
	}
	segments := make([]postgres.TranscriptSegment, 0)
	outsideVideoCount := 0
	sourceIndex := 0
	for _, transcript := range result.Transcripts {
		for _, sentence := range transcript.Sentences {
			sourceIndex++
			if sourceIndex > maxTranscriptItems {
				return nil, 0, fmt.Errorf("provider result exceeds %d source sentences", maxTranscriptItems)
			}
			if sentence.BeginTime < 0 || sentence.EndTime <= sentence.BeginTime {
				return nil, 0, fmt.Errorf("provider sentence %d has an invalid time interval", sourceIndex)
			}
			text := strings.TrimSpace(sentence.Text)
			if strings.ContainsRune(text, '\x00') {
				return nil, 0, fmt.Errorf("provider sentence %d contains a NUL character", sourceIndex)
			}
			if text == "" {
				continue
			}
			if audioOriginMS > 0 && (sentence.BeginTime > math.MaxInt64-audioOriginMS || sentence.EndTime > math.MaxInt64-audioOriginMS) {
				return nil, 0, fmt.Errorf("provider sentence %d timestamp overflows the video timeline", sourceIndex)
			}
			start := sentence.BeginTime + audioOriginMS
			end := sentence.EndTime + audioOriginMS
			if end <= 0 {
				outsideVideoCount++
				continue
			}
			if start >= mediaDurationMS {
				outsideVideoCount++
				continue
			}
			var startCorrection, endCorrection int64
			if start < 0 {
				underrun := -start
				if underrun > maxTimestampOverrun {
					return nil, 0, fmt.Errorf("provider sentence %d starts %dms before the video", sourceIndex, underrun)
				}
				startCorrection = underrun
				start = 0
			}
			if end > mediaDurationMS {
				overrun := end - mediaDurationMS
				if overrun > maxTimestampOverrun {
					return nil, 0, fmt.Errorf("provider sentence %d ends %dms beyond the video duration", sourceIndex, overrun)
				}
				endCorrection = -overrun
				end = mediaDurationMS
			}
			if start < 0 || end <= start || start >= mediaDurationMS {
				return nil, 0, fmt.Errorf("provider sentence %d is outside the video timeline", sourceIndex)
			}
			segments = append(segments, postgres.TranscriptSegment{SourceSentenceIndex: sourceIndex,
				ProviderStartMS: sentence.BeginTime, ProviderEndMS: sentence.EndTime, StartMS: start, EndMS: end,
				StartCorrectionMS: int32(startCorrection), EndCorrectionMS: int32(endCorrection), Text: text})
		}
	}
	sort.SliceStable(segments, func(i, j int) bool {
		if segments[i].ProviderStartMS == segments[j].ProviderStartMS {
			return segments[i].SourceSentenceIndex < segments[j].SourceSentenceIndex
		}
		return segments[i].ProviderStartMS < segments[j].ProviderStartMS
	})
	for i := range segments {
		segments[i].Seq = i + 1
	}
	return segments, outsideVideoCount, nil
}

func clippedCount(segments []postgres.TranscriptSegment) int {
	count := 0
	for _, segment := range segments {
		if segment.StartCorrectionMS > 0 || segment.EndCorrectionMS < 0 {
			count++
		}
	}
	return count
}

func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id.String()
}

func nullableUUIDPtr(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return id.String()
}
