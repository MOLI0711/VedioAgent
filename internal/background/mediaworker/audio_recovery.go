package mediaworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/media"
	"mirrorcore/internal/repository/postgres"
)

// recoverStagedAudio closes the object-store/DB crash window. The object key is
// deterministic for one task, immutable input and extraction profile, so a new
// lease can verify and register an upload whose previous worker lost its lease
// before CompleteAudio committed.
func (p *Processor) recoverStagedAudio(ctx context.Context, task postgres.StageTask, source postgres.SourceBlob,
	workDir string, candidate media.AudioCandidate, inputFingerprint, profileHash []byte) (postgres.AudioBlob, bool, error) {
	key := audioArtifactObjectKey(task.VideoID, task.ID, inputFingerprint, profileHash)
	checkCtx, cancelCheck := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, key, "")
	cancelCheck()
	if err != nil {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_HEAD_FAILED", Retryable: true, Err: err}
	}
	if !exists {
		return postgres.AudioBlob{}, false, nil
	}
	if info.Size <= 44 || info.Size > media.MaxAudioBytes ||
		!metadataEquals(info.Metadata, "artifact-fingerprint", hex.EncodeToString(inputFingerprint)) ||
		!metadataEquals(info.Metadata, "profile-hash", hex.EncodeToString(profileHash)) ||
		!metadataEquals(info.Metadata, "video-id", task.VideoID.String()) {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_OBJECT_IDENTITY_MISMATCH", Retryable: false,
			Err: errors.New("uncheckpointed audio object does not match the current input and profile")}
	}
	shaText := info.Metadata["sha256"]
	sha, err := hex.DecodeString(shaText)
	if err != nil || len(sha) != sha256.Size {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_OBJECT_METADATA_INVALID", Retryable: false,
			Err: errors.New("uncheckpointed audio object has no valid SHA-256 metadata")}
	}
	path := filepath.Join(workDir, fmt.Sprintf("staged-audio-%d.wav", candidate.StreamIndex))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_SCRATCH_CREATE_FAILED", Retryable: true, Err: err}
	}
	downloadCtx, cancelDownload := context.WithTimeout(ctx, maxObjectTransfer)
	objectInfo, downloadErr := p.store.DownloadObject(downloadCtx, key, info.VersionID, file, media.MaxAudioBytes)
	cancelDownload()
	closeErr := file.Close()
	if downloadErr != nil || closeErr != nil {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_DOWNLOAD_FAILED", Retryable: true,
			Err: errors.Join(downloadErr, closeErr)}
	}
	if objectInfo.Size != info.Size || !objectVersionMatches(info.VersionID, objectInfo.VersionID) {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_DOWNLOAD_MISMATCH", Retryable: false,
			Err: errors.New("downloaded uncheckpointed audio does not match its recorded size and version")}
	}
	actualSHA, actualSize, err := hashFile(path, media.MaxAudioBytes)
	if err != nil {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_HASH_FAILED", Retryable: true, Err: err}
	}
	if actualSize != info.Size || !equalBytes(actualSHA, sha) ||
		!metadataEquals(info.Metadata, "sha256", shaText) {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_CHECKSUM_MISMATCH", Retryable: false,
			Err: errors.New("uncheckpointed audio bytes do not match their object metadata")}
	}
	wavCtx, cancelWAV := context.WithTimeout(ctx, maxProbeRuntime)
	wav, err := media.ValidateWAV(wavCtx, p.config.FFprobePath, path)
	cancelWAV()
	if err != nil {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_WAV_INVALID", Retryable: false, Err: err}
	}
	if wav.Codec != "pcm_s16le" || wav.SampleRate != 16000 || wav.Channels != 1 || wav.SizeBytes != info.Size {
		return postgres.AudioBlob{}, false, &StageFailure{Code: "AUDIO_STAGED_WAV_PROFILE_MISMATCH", Retryable: false,
			Err: errors.New("uncheckpointed audio does not match the configured PCM WAV profile")}
	}
	metadata, err := json.Marshal(map[string]any{
		"profile": media.AudioProfile, "codec": wav.Codec, "sample_rate": wav.SampleRate,
		"channels": wav.Channels, "duration_ms": wav.DurationMS, "size_bytes": wav.SizeBytes,
		"sha256": shaText, "source_sha256": hex.EncodeToString(source.SHA256),
		"audio_stream_index": candidate.StreamIndex, "audio_origin_ms": candidate.OriginMS,
	})
	if err != nil {
		return postgres.AudioBlob{}, false, err
	}
	return postgres.AudioBlob{ID: uuid.New(), Bucket: p.bucket, ObjectKey: key, ObjectVersion: info.VersionID,
		SizeBytes: info.Size, State: "READY", SHA256: sha, ArtifactFingerprint: inputFingerprint,
		ProfileHash: profileHash, Metadata: metadata}, true, nil
}

func audioArtifactObjectKey(videoID, taskID uuid.UUID, inputFingerprint, profileHash []byte) string {
	return fmt.Sprintf("artifacts/audio/%s/%s/%s/%s.wav", videoID,
		hex.EncodeToString(inputFingerprint), hex.EncodeToString(profileHash), taskID)
}
