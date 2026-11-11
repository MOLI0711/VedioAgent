package mediaworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/application"
	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/config"
	"mirrorcore/internal/media"
	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

const (
	maxTaskScratchBytes = int64(3 << 30)
	maxProbeDownload    = 20 * time.Minute
	maxProbeRuntime     = 60 * time.Second
	maxAudioRuntime     = 30 * time.Minute
	maxObjectTransfer   = 30 * time.Minute
)

type StageFailure struct {
	Code       string
	Retryable  bool
	RetryAfter time.Duration
	Err        error
}

func (e *StageFailure) Error() string {
	if e.Err == nil {
		return e.Code
	}
	return e.Err.Error()
}

func (e *StageFailure) Unwrap() error { return e.Err }

type Processor struct {
	repository     *postgres.StageRepository
	store          ArtifactStore
	config         config.MediaTools
	bucket         string
	ffmpegSlot     chan struct{}
	ffmpegVersion  string
	ffprobeVersion string
}

func NewProcessor(repository *postgres.StageRepository, store ArtifactStore, cfg config.MediaTools, bucket string) (*Processor, error) {
	if repository == nil || store == nil || bucket == "" {
		return nil, errors.New("media processor requires a stage repository, object store, and bucket")
	}
	if err := secureTempRoot(cfg.TempDir); err != nil {
		return nil, err
	}
	versionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ffmpegVersion, err := media.ToolVersion(versionCtx, cfg.FFmpegPath)
	if err != nil {
		cancel()
		return nil, err
	}
	ffprobeVersion, err := media.ToolVersion(versionCtx, cfg.FFprobePath)
	cancel()
	if err != nil {
		return nil, err
	}
	return &Processor{repository: repository, store: store, config: cfg, bucket: bucket,
		ffmpegSlot: make(chan struct{}, 1), ffmpegVersion: ffmpegVersion, ffprobeVersion: ffprobeVersion}, nil
}

func (p *Processor) Process(ctx context.Context, task tasks.Task) error {
	stageTask := postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID,
		Stage: task.Stage, RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}
	source, err := p.repository.LoadSourceBlob(ctx, stageTask)
	if err != nil {
		return &StageFailure{Code: "SOURCE_NOT_READY", Retryable: true, Err: err}
	}
	workDir, err := os.MkdirTemp(p.config.TempDir, fmt.Sprintf("%s-%d-", task.ID, task.RunEpoch))
	if err != nil {
		return &StageFailure{Code: "SCRATCH_CREATE_FAILED", Retryable: true, Err: err}
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	switch task.Stage {
	case "PROBE":
		return p.processProbe(ctx, stageTask, source, workDir)
	case "AUDIO":
		return p.processAudio(ctx, stageTask, source, workDir)
	default:
		return &StageFailure{Code: "UNSUPPORTED_WORKER_STAGE", Retryable: false,
			Err: fmt.Errorf("media worker cannot execute stage %q", task.Stage)}
	}
}

func (p *Processor) processProbe(ctx context.Context, task postgres.StageTask, source postgres.SourceBlob, workDir string) error {
	probeConfigHash, _, err := postgres.HashJSON(struct {
		Profile string `json:"profile"`
		Rules   string `json:"rules"`
		FFprobe string `json:"ffprobe_version"`
	}{media.ProbeProfile, "mp4-h264-aac-default-then-decodable-v2", p.ffprobeVersion})
	if err != nil {
		return err
	}
	provisionalInput := source.SHA256
	if len(provisionalInput) != sha256.Size {
		provisionalInput, _, err = postgres.HashJSON(struct {
			BlobID    string `json:"blob_id"`
			ObjectKey string `json:"object_key"`
			VersionID string `json:"version_id"`
			SizeBytes int64  `json:"size_bytes"`
		}{source.ID.String(), source.ObjectKey, source.ObjectVersion, source.SizeBytes})
		if err != nil {
			return err
		}
	}
	if _, err := p.repository.StartStage(ctx, task, source.ID, provisionalInput, probeConfigHash); err != nil {
		return err
	}
	if source.Bucket != p.bucket {
		return &StageFailure{Code: "SOURCE_BUCKET_MISMATCH", Retryable: false, Err: errors.New("source blob belongs to an unexpected bucket")}
	}
	if source.State != "STORED" && source.State != "READY" {
		return &StageFailure{Code: "SOURCE_BLOB_UNAVAILABLE", Retryable: false, Err: fmt.Errorf("source blob is %s", source.State)}
	}
	if err := p.verifySourceObject(ctx, task, source); err != nil {
		return err
	}

	if source.State == "READY" && len(source.SHA256) == sha256.Size && equalBytes(source.ProfileHash, probeConfigHash) {
		var metadata media.ProbeMetadata
		if json.Unmarshal(source.Probe, &metadata) == nil && metadata.Profile == media.ProbeProfile && metadata.DurationMS > 0 {
			return p.repository.CompleteProbe(ctx, task, source, source.SHA256, probeConfigHash, source.SHA256, source.Probe)
		}
	}

	inputPath := filepath.Join(workDir, "source.mp4")
	file, err := os.OpenFile(inputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return &StageFailure{Code: "SCRATCH_CREATE_FAILED", Retryable: true, Err: err}
	}
	downloadCtx, cancelDownload := context.WithTimeout(ctx, maxProbeDownload)
	objectInfo, downloadErr := p.store.DownloadObject(downloadCtx, source.ObjectKey, source.ObjectVersion, file, source.SizeBytes)
	cancelDownload()
	closeErr := file.Close()
	if downloadErr != nil {
		return &StageFailure{Code: "SOURCE_DOWNLOAD_FAILED", Retryable: true, Err: downloadErr}
	}
	if closeErr != nil {
		return &StageFailure{Code: "SOURCE_DOWNLOAD_FAILED", Retryable: true, Err: closeErr}
	}
	if objectInfo.Size != source.SizeBytes || objectInfo.Size > application.UploadMaxBytes ||
		!objectVersionMatches(source.ObjectVersion, objectInfo.VersionID) {
		return p.permanentSourceFailure(ctx, task, source, "SOURCE_OBJECT_MISMATCH", errors.New("downloaded source object did not match its recorded size or version"))
	}
	sourceSHA, downloadedBytes, err := hashFile(inputPath, application.UploadMaxBytes)
	if err != nil {
		return &StageFailure{Code: "SOURCE_HASH_FAILED", Retryable: true, Err: err}
	}
	if downloadedBytes != source.SizeBytes {
		return p.permanentSourceFailure(ctx, task, source, "SOURCE_OBJECT_MISMATCH", errors.New("downloaded source object length changed"))
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, maxProbeRuntime)
	metadata, err := media.Probe(probeCtx, p.config.FFprobePath, inputPath)
	cancelProbe()
	if err != nil {
		if errors.Is(err, media.ErrUnsupportedMedia) {
			return &StageFailure{Code: "UNSUPPORTED_MEDIA", Retryable: false, Err: err}
		}
		return &StageFailure{Code: "PROBE_FAILED", Retryable: true, Err: err}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return p.repository.CompleteProbe(ctx, task, source, sourceSHA, probeConfigHash, sourceSHA, encoded)
}

func (p *Processor) processAudio(ctx context.Context, task postgres.StageTask, source postgres.SourceBlob, workDir string) error {
	var probe media.ProbeMetadata
	probeErr := json.Unmarshal(source.Probe, &probe)
	candidates := usableAudioCandidates(probe)
	audioConfigHash, _, err := postgres.HashJSON(struct {
		Profile    string `json:"profile"`
		SampleRate int    `json:"sample_rate"`
		Channels   int    `json:"channels"`
		Codec      string `json:"codec"`
		FFmpeg     string `json:"ffmpeg_version"`
		FFprobe    string `json:"ffprobe_version"`
	}{media.AudioProfile, 16000, 1, "pcm_s16le", p.ffmpegVersion, p.ffprobeVersion})
	if err != nil {
		return err
	}
	stageInputHash, _, err := postgres.HashJSON(struct {
		SourceSHA256 string                 `json:"source_sha256"`
		Candidates   []media.AudioCandidate `json:"candidates"`
	}{hex.EncodeToString(source.SHA256), candidates})
	if err != nil {
		return err
	}
	if _, err := p.repository.StartStage(ctx, task, source.ID, stageInputHash, audioConfigHash); err != nil {
		return err
	}
	if len(source.SHA256) != sha256.Size || probeErr != nil || probe.Profile != media.ProbeProfile || probe.DurationMS <= 0 {
		return &StageFailure{Code: "PROBE_CHECKPOINT_MISSING", Retryable: true, Err: errors.New("a valid PROBE checkpoint is required before AUDIO")}
	}
	if probe.HasAudio && len(candidates) == 0 {
		return &StageFailure{Code: "INVALID_PROBE_CHECKPOINT", Retryable: false, Err: errors.New("PROBE marked audio present without a usable track candidate")}
	}
	if !probe.HasAudio {
		metadata, _ := json.Marshal(map[string]any{"reason": "NO_AUDIO_STREAM", "profile": media.AudioProfile,
			"source_sha256": hex.EncodeToString(source.SHA256), "audio_origin_ms": probe.AudioOriginMS})
		_, err := p.repository.CompleteAudio(ctx, task, source, stageInputHash, audioConfigHash, postgres.AudioBlob{}, metadata, true)
		return err
	}
	for _, candidate := range candidates {
		inputFingerprint, err := audioInputFingerprint(source.SHA256, candidate)
		if err != nil {
			return err
		}
		reusable, ok, err := p.repository.FindReusableAudio(ctx, task.VideoID, inputFingerprint, audioConfigHash)
		if err != nil {
			return err
		}
		if !ok {
			staged, found, recoverErr := p.recoverStagedAudio(ctx, task, source, workDir,
				candidate, inputFingerprint, audioConfigHash)
			if recoverErr != nil {
				return recoverErr
			}
			if found {
				_, err := p.repository.CompleteAudio(ctx, task, source, inputFingerprint,
					audioConfigHash, staged, staged.Metadata, false)
				return err
			}
			continue
		}
		valid, err := p.verifyReusableAudio(ctx, task.VideoID, reusable, inputFingerprint, audioConfigHash)
		if err != nil {
			return &StageFailure{Code: "AUDIO_ARTIFACT_CHECK_FAILED", Retryable: true, Err: err}
		}
		if valid {
			metadata := reusable.Metadata
			if len(metadata) == 0 {
				metadata = json.RawMessage(`{}`)
			}
			_, err := p.repository.CompleteAudio(ctx, task, source, inputFingerprint, audioConfigHash, reusable, metadata, false)
			return err
		}
		if err := p.repository.MarkAudioBlobBroken(ctx, task, reusable.ID); err != nil {
			return err
		}
	}
	if source.Bucket != p.bucket {
		return &StageFailure{Code: "SOURCE_BUCKET_MISMATCH", Retryable: false, Err: errors.New("source blob belongs to an unexpected bucket")}
	}
	if source.State != "READY" {
		return &StageFailure{Code: "SOURCE_CHECKPOINT_MISSING", Retryable: true, Err: errors.New("verified source object is required to extract a new audio artifact")}
	}
	if err := p.verifySourceObject(ctx, task, source); err != nil {
		return err
	}

	sourcePath := filepath.Join(workDir, "source.mp4")
	if err := p.downloadAndVerifySource(ctx, task, source, sourcePath); err != nil {
		return err
	}
	select {
	case p.ffmpegSlot <- struct{}{}:
	case <-ctx.Done():
		return &StageFailure{Code: "AUDIO_EXTRACTION_CANCELLED", Retryable: true, Err: ctx.Err()}
	}
	audioCtx, cancelAudio := context.WithTimeout(ctx, maxAudioRuntime)
	var selected *media.AudioCandidate
	var audioPath string
	var wav media.WAVMetadata
	var lastExtractErr error
	for i := range candidates {
		candidate := &candidates[i]
		candidatePath := filepath.Join(workDir, fmt.Sprintf("audio-%d.wav", candidate.StreamIndex))
		if _, extractErr := media.ExtractAudio(audioCtx, p.config.FFmpegPath, sourcePath, candidatePath, candidate.StreamIndex); extractErr != nil {
			lastExtractErr = extractErr
			_ = os.Remove(candidatePath)
			if audioCtx.Err() != nil {
				break
			}
			continue
		}
		candidateWAV, validateErr := media.ValidateWAV(audioCtx, p.config.FFprobePath, candidatePath)
		if validateErr != nil {
			lastExtractErr = validateErr
			_ = os.Remove(candidatePath)
			if audioCtx.Err() != nil {
				break
			}
			continue
		}
		selected, audioPath, wav = candidate, candidatePath, candidateWAV
		break
	}
	audioCtxErr := audioCtx.Err()
	cancelAudio()
	<-p.ffmpegSlot
	if selected == nil {
		if ctx.Err() != nil || audioCtxErr != nil || errors.Is(lastExtractErr, context.DeadlineExceeded) {
			return &StageFailure{Code: "AUDIO_EXTRACTION_TIMEOUT", Retryable: true, Err: errors.Join(audioCtxErr, lastExtractErr)}
		}
		return &StageFailure{Code: "AUDIO_EXTRACTION_FAILED", Retryable: false, Err: lastExtractErr}
	}
	audioInputHash, err := audioInputFingerprint(source.SHA256, *selected)
	if err != nil {
		return err
	}
	audioSHA, audioSize, err := hashFile(audioPath, media.MaxAudioBytes)
	if err != nil {
		return &StageFailure{Code: "AUDIO_HASH_FAILED", Retryable: true, Err: err}
	}
	if audioSize != wav.SizeBytes {
		return &StageFailure{Code: "AUDIO_SIZE_MISMATCH", Retryable: false, Err: errors.New("WAV size changed during validation")}
	}
	artifactMetadata := map[string]string{
		"artifact-fingerprint": hex.EncodeToString(audioInputHash),
		"profile-hash":         hex.EncodeToString(audioConfigHash),
		"sha256":               hex.EncodeToString(audioSHA),
		"video-id":             task.VideoID.String(),
	}
	objectKey := audioArtifactObjectKey(task.VideoID, task.ID, audioInputHash, audioConfigHash)
	putCtx, cancelPut := context.WithTimeout(ctx, maxObjectTransfer)
	putInfo, err := p.store.PutObjectFile(putCtx, objectKey, audioPath, "audio/wav", artifactMetadata)
	cancelPut()
	if err != nil {
		return &StageFailure{Code: "AUDIO_UPLOAD_FAILED", Retryable: true, Err: err}
	}
	if putInfo.Size != audioSize {
		return &StageFailure{Code: "AUDIO_UPLOAD_MISMATCH", Retryable: true, Err: errors.New("OSS accepted an audio object with an unexpected size")}
	}
	objectInfo, exists, err := p.store.HeadObjectVersion(ctx, objectKey, putInfo.VersionID)
	if err != nil {
		return &StageFailure{Code: "AUDIO_UPLOAD_VERIFY_FAILED", Retryable: true, Err: err}
	}
	if !exists || !objectVersionMatches(putInfo.VersionID, objectInfo.VersionID) ||
		!validAudioObject(objectInfo, audioSize, audioSHA, audioInputHash, audioConfigHash, task.VideoID) {
		return &StageFailure{Code: "AUDIO_UPLOAD_VERIFY_FAILED", Retryable: true, Err: errors.New("uploaded audio object failed its HEAD integrity checks")}
	}
	metadata, err := json.Marshal(map[string]any{
		"profile": media.AudioProfile, "codec": wav.Codec, "sample_rate": wav.SampleRate,
		"channels": wav.Channels, "duration_ms": wav.DurationMS, "size_bytes": audioSize,
		"sha256": hex.EncodeToString(audioSHA), "source_sha256": hex.EncodeToString(source.SHA256),
		"audio_stream_index": selected.StreamIndex, "audio_origin_ms": selected.OriginMS,
	})
	if err != nil {
		return err
	}
	artifact := postgres.AudioBlob{ID: uuid.New(), Bucket: p.bucket, ObjectKey: objectKey,
		ObjectVersion: putInfo.VersionID, SizeBytes: audioSize, SHA256: audioSHA,
		ArtifactFingerprint: audioInputHash, ProfileHash: audioConfigHash, Metadata: metadata}
	_, err = p.repository.CompleteAudio(ctx, task, source, audioInputHash, audioConfigHash, artifact, metadata, false)
	return err
}

func (p *Processor) verifySourceObject(ctx context.Context, task postgres.StageTask, source postgres.SourceBlob) error {
	if source.SizeBytes <= 0 || source.SizeBytes > application.UploadMaxBytes {
		return p.permanentSourceFailure(ctx, task, source, "SOURCE_SIZE_INVALID", errors.New("source size is outside the supported limit"))
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, source.ObjectKey, source.ObjectVersion)
	cancel()
	if err != nil {
		return &StageFailure{Code: "SOURCE_HEAD_FAILED", Retryable: true, Err: err}
	}
	if !exists || info.Size != source.SizeBytes || !objectVersionMatches(source.ObjectVersion, info.VersionID) ||
		!metadataEquals(info.Metadata, "video-id", task.VideoID.String()) {
		return p.permanentSourceFailure(ctx, task, source, "SOURCE_OBJECT_MISSING_OR_MISMATCHED", errors.New("source object is missing or does not match its database record"))
	}
	return nil
}

func (p *Processor) permanentSourceFailure(ctx context.Context, task postgres.StageTask, source postgres.SourceBlob, code string, cause error) error {
	if err := p.repository.MarkSourceBlobBroken(ctx, task, source.ID); err != nil {
		return &StageFailure{Code: code, Retryable: true, Err: errors.Join(cause, err)}
	}
	return &StageFailure{Code: code, Retryable: false, Err: cause}
}

func (p *Processor) downloadAndVerifySource(ctx context.Context, task postgres.StageTask, source postgres.SourceBlob, path string) error {
	if err := p.verifySourceObject(ctx, task, source); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return &StageFailure{Code: "SCRATCH_CREATE_FAILED", Retryable: true, Err: err}
	}
	downloadCtx, cancel := context.WithTimeout(ctx, maxProbeDownload)
	info, downloadErr := p.store.DownloadObject(downloadCtx, source.ObjectKey, source.ObjectVersion, file, source.SizeBytes)
	cancel()
	closeErr := file.Close()
	if downloadErr != nil {
		return &StageFailure{Code: "SOURCE_DOWNLOAD_FAILED", Retryable: true, Err: downloadErr}
	}
	if closeErr != nil {
		return &StageFailure{Code: "SOURCE_DOWNLOAD_FAILED", Retryable: true, Err: closeErr}
	}
	if info.Size != source.SizeBytes || !objectVersionMatches(source.ObjectVersion, info.VersionID) {
		return p.permanentSourceFailure(ctx, task, source, "SOURCE_OBJECT_MISMATCH", errors.New("downloaded source object length changed"))
	}
	sum, size, err := hashFile(path, application.UploadMaxBytes)
	if err != nil {
		return &StageFailure{Code: "SOURCE_HASH_FAILED", Retryable: true, Err: err}
	}
	if size != source.SizeBytes || !equalBytes(sum, source.SHA256) {
		return p.permanentSourceFailure(ctx, task, source, "SOURCE_CHECKSUM_MISMATCH", errors.New("source object checksum differs from the PROBE checkpoint"))
	}
	return nil
}

func (p *Processor) verifyReusableAudio(ctx context.Context, videoID uuid.UUID, blob postgres.AudioBlob, inputHash, profileHash []byte) (bool, error) {
	if blob.Bucket != p.bucket || len(blob.SHA256) != sha256.Size || blob.SizeBytes <= 44 || blob.SizeBytes > media.MaxAudioBytes {
		return false, nil
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, blob.ObjectKey, blob.ObjectVersion)
	cancel()
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	return objectVersionMatches(blob.ObjectVersion, info.VersionID) &&
		validAudioObject(info, blob.SizeBytes, blob.SHA256, inputHash, profileHash, videoID), nil
}

func validAudioObject(info ossstore.ObjectInfo, size int64, sha256sum, inputHash, profileHash []byte, videoID uuid.UUID) bool {
	return info.Size == size && metadataEquals(info.Metadata, "sha256", hex.EncodeToString(sha256sum)) &&
		metadataEquals(info.Metadata, "artifact-fingerprint", hex.EncodeToString(inputHash)) &&
		metadataEquals(info.Metadata, "profile-hash", hex.EncodeToString(profileHash)) &&
		metadataEquals(info.Metadata, "video-id", videoID.String())
}

func objectVersionMatches(expected, actual string) bool {
	return expected == "" || actual == expected
}

func usableAudioCandidates(probe media.ProbeMetadata) []media.AudioCandidate {
	if len(probe.AudioCandidates) > 0 {
		return append([]media.AudioCandidate(nil), probe.AudioCandidates...)
	}
	if probe.HasAudio && probe.AudioStreamIndex != nil && probe.AudioCodec == "aac" {
		return []media.AudioCandidate{{StreamIndex: *probe.AudioStreamIndex, Codec: probe.AudioCodec,
			OriginMS: probe.AudioOriginMS, Default: true}}
	}
	return nil
}

func audioInputFingerprint(sourceSHA256 []byte, candidate media.AudioCandidate) ([]byte, error) {
	if len(sourceSHA256) != sha256.Size {
		return nil, errors.New("source SHA-256 is required to identify the audio artifact")
	}
	fingerprint, _, err := postgres.HashJSON(struct {
		SourceSHA256 string `json:"source_sha256"`
		StreamIndex  int    `json:"stream_index"`
		Codec        string `json:"codec"`
		OriginMS     int64  `json:"origin_ms"`
	}{hex.EncodeToString(sourceSHA256), candidate.StreamIndex, candidate.Codec, candidate.OriginMS})
	return fingerprint, err
}

func metadataEquals(metadata map[string]string, name, expected string) bool {
	for key, value := range metadata {
		if strings.EqualFold(key, name) {
			return value == expected
		}
	}
	return false
}

func hashFile(path string, maxBytes int64) ([]byte, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open file for SHA-256: %w", err)
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat file for SHA-256: %w", err)
	}
	if !stat.Mode().IsRegular() || stat.Size() <= 0 || stat.Size() > maxBytes {
		return nil, 0, fmt.Errorf("file size is outside the %d-byte hashing limit", maxBytes)
	}
	hasher := sha256.New()
	count, err := io.Copy(hasher, io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, 0, fmt.Errorf("hash file: %w", err)
	}
	if count != stat.Size() || count > maxBytes {
		return nil, 0, errors.New("file changed or exceeded its hashing limit")
	}
	return hasher.Sum(nil), count, nil
}

func secureTempRoot(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("MEDIA_TEMP_DIR must not be empty")
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return fmt.Errorf("create media scratch root: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect media scratch root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("media scratch root must be a real directory, not a symlink")
	}
	if err := os.Chmod(path, 0700); err != nil {
		return fmt.Errorf("restrict media scratch root permissions: %w", err)
	}
	return nil
}

func equalBytes(left, right []byte) bool {
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
