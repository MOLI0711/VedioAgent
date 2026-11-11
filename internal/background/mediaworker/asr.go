package mediaworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/providers/funasr"
	"mirrorcore/internal/repository/postgres"
)

const (
	asrProfile       = "fun-asr-recorded-v1"
	asrModel         = "fun-asr"
	asrURLLifetime   = 6 * time.Hour
	asrSubmitTimeout = 20 * time.Second
	asrQueryTimeout  = 10 * time.Second
	asrResultTimeout = 60 * time.Second
)

type ASRProcessor struct {
	repository *postgres.StageRepository
	store      ArtifactStore
	provider   ASRProvider
	bucket     string
	identity   ASRIdentity
	tempDir    string
}

type ASRProvider interface {
	Submit(context.Context, string) (funasr.Task, error)
	GetTask(context.Context, string) (funasr.Task, error)
	DownloadResult(context.Context, string) ([]byte, error)
}

type ASRIdentity struct {
	Profile   string
	Provider  string
	Region    string
	Workspace string
	Model     string
}

func NewASRProcessor(repository *postgres.StageRepository, store ArtifactStore,
	provider *funasr.Client, bucket, region, workspace, tempDir string) (*ASRProcessor, error) {
	return NewASRProcessorWithIdentity(repository, store, provider, bucket, ASRIdentity{
		Profile: asrProfile, Provider: "aliyun-bailian", Region: region, Workspace: workspace, Model: asrModel,
	}, tempDir)
}

func NewASRProcessorWithIdentity(repository *postgres.StageRepository, store ArtifactStore,
	provider ASRProvider, bucket string, identity ASRIdentity, tempDir string) (*ASRProcessor, error) {
	if repository == nil || store == nil || provider == nil || bucket == "" || identity.Profile == "" ||
		identity.Provider == "" || identity.Region == "" || identity.Workspace == "" || identity.Model == "" {
		return nil, errors.New("ASR processor requires repository, artifact store, provider, bucket, and complete identity")
	}
	if err := secureTempRoot(tempDir); err != nil {
		return nil, err
	}
	return &ASRProcessor{repository: repository, store: store, provider: provider,
		bucket: bucket, identity: identity, tempDir: tempDir}, nil
}

func (p *ASRProcessor) Process(ctx context.Context, task tasks.Task) error {
	stageTask := postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID,
		Stage: task.Stage, RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}
	input, err := p.repository.LoadASRInput(ctx, stageTask)
	if err != nil {
		return &StageFailure{Code: "ASR_INPUT_NOT_READY", Retryable: false, Err: err}
	}
	profileHash, err := asrProfileHash(p.identity)
	if err != nil {
		return err
	}
	var inputFingerprint []byte
	if input.AudioState == "NO_AUDIO" {
		inputFingerprint, _, err = postgres.HashJSON(struct {
			NoAudio      bool      `json:"no_audio"`
			SourceBlobID uuid.UUID `json:"source_blob_id"`
		}{true, input.SourceBlobID})
	} else {
		if input.Audio.Bucket != p.bucket || input.Audio.State != "READY" || input.Audio.SizeBytes <= 44 || len(input.Audio.SHA256) != sha256.Size {
			return &StageFailure{Code: "ASR_AUDIO_ARTIFACT_INVALID", Retryable: false, Err: errors.New("audio artifact metadata is incomplete or invalid")}
		}
		var audioMetadata struct {
			AudioOriginMS int64 `json:"audio_origin_ms"`
		}
		if err := json.Unmarshal(input.Audio.Metadata, &audioMetadata); err != nil {
			return &StageFailure{Code: "ASR_AUDIO_METADATA_INVALID", Retryable: false, Err: err}
		}
		inputFingerprint, _, err = postgres.HashJSON(struct {
			AudioSHA256   string `json:"audio_sha256"`
			AudioOriginMS int64  `json:"audio_origin_ms"`
		}{hex.EncodeToString(input.Audio.SHA256), audioMetadata.AudioOriginMS})
	}
	if err != nil {
		return err
	}
	if _, err := p.repository.StartStage(ctx, stageTask, input.SourceBlobID, inputFingerprint, profileHash); err != nil {
		return err
	}
	if input.AudioState == "NO_AUDIO" {
		metadata, _ := json.Marshal(map[string]any{"profile": p.identity.Profile, "provider": p.identity.Provider,
			"model": p.identity.Model, "reason": "NO_AUDIO_STREAM"})
		return p.repository.CompleteASRNoAudio(ctx, stageTask, input, inputFingerprint, profileHash, metadata)
	}
	submission, exists, err := p.repository.FindASRSubmission(ctx, task.ID, inputFingerprint, profileHash)
	if err != nil {
		return &StageFailure{Code: "ASR_LEDGER_READ_FAILED", Retryable: true, Err: err}
	}
	if exists {
		switch submission.State {
		case "SUBMITTING":
			unknownAt := submission.PreparedAt.Add(tasks.LeaseDuration)
			if time.Now().Before(unknownAt) {
				return p.wait(ctx, stageTask, submission.ID, "ASR_UNKNOWN", unknownAt)
			}
			return p.repository.MarkASRUnknownAndWait(ctx, stageTask, submission.ID,
				"SUBMISSION_ORPHANED", "worker lease expired before a provider task ID was persisted")
		case "UNKNOWN":
			if submission.UnknownSince == nil {
				return &StageFailure{Code: "ASR_LEDGER_INVALID", Retryable: false, Err: errors.New("unknown submission has no unknown_since timestamp")}
			}
			resubmitAt := submission.NextPollAt
			if resubmitAt.IsZero() {
				return &StageFailure{Code: "ASR_LEDGER_INVALID", Retryable: false, Err: errors.New("unknown submission has no persisted resubmission time")}
			}
			if time.Now().Before(resubmitAt) {
				return p.wait(ctx, stageTask, submission.ID, "ASR_UNKNOWN", resubmitAt)
			}
			if submission.Sequence >= 2 {
				return &StageFailure{Code: "ASR_ACCEPTANCE_UNKNOWN", Retryable: false,
					Err: errors.New("both permitted submissions have an unconfirmed provider acceptance")}
			}
			return p.submit(ctx, task, stageTask, input, inputFingerprint, profileHash)
		case "ACCEPTED", "RUNNING", "RESULT_READY":
			recovered, recoverErr := p.recoverStoredResult(ctx, stageTask, input, inputFingerprint, profileHash, submission)
			if recoverErr != nil {
				return recoverErr
			}
			if recovered {
				return nil
			}
			if submission.State != "RESULT_READY" && !submission.DeadlineAt.After(time.Now()) {
				return &StageFailure{Code: "ASR_PROVIDER_DEADLINE_EXCEEDED", Retryable: false,
					Err: errors.New("ASR task exceeded its persisted two-hour wait budget")}
			}
			return p.poll(ctx, task, stageTask, input, inputFingerprint, profileHash, submission)
		case "SUCCEEDED", "FAILED", "REJECTED":
			// A terminal remote attempt can be followed by reuse of another valid
			// artifact for the same immutable audio and model profile.
		default:
			return &StageFailure{Code: "ASR_LEDGER_INVALID", Retryable: false, Err: fmt.Errorf("unsupported ASR submission state %q", submission.State)}
		}
	}
	if reused, reuseErr := p.tryReuseASR(ctx, task, stageTask, input, inputFingerprint, profileHash); reuseErr != nil {
		return reuseErr
	} else if reused {
		return nil
	}
	if exists {
		switch submission.State {
		case "SUCCEEDED":
			return &StageFailure{Code: "ASR_RESULT_CHECKPOINT_MISSING", Retryable: false,
				Err: errors.New("completed ASR submission has no valid reusable raw result")}
		case "REJECTED":
			var errorInfo struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(submission.LastError, &errorInfo)
			if errorInfo.Code != "PROVIDER_RATE_LIMITED" {
				return &StageFailure{Code: "ASR_PROVIDER_REJECTED", Retryable: false,
					Err: errors.New("ASR provider explicitly rejected the submission")}
			}
			if time.Now().Before(submission.NextPollAt) {
				return p.wait(ctx, stageTask, submission.ID, "ASR_BACKOFF", submission.NextPollAt)
			}
			if submission.Sequence >= 2 {
				return &StageFailure{Code: "ASR_SUBMISSION_BUDGET_EXHAUSTED", Retryable: false,
					Err: errors.New("ASR rate limit persisted through the permitted second submission")}
			}
			return p.submit(ctx, task, stageTask, input, inputFingerprint, profileHash)
		case "FAILED":
			return &StageFailure{Code: "ASR_PROVIDER_FAILED", Retryable: false, Err: errors.New("ASR task failed remotely")}
		}
	}
	return p.submit(ctx, task, stageTask, input, inputFingerprint, profileHash)
}

func asrProfileHash(identity ASRIdentity) ([]byte, error) {
	if identity.Provider == "aliyun-bailian" {
		// Preserve the existing production profile hash so deployments can roll
		// this interface refactor without stranding in-flight ASR submissions.
		hash, _, err := postgres.HashJSON(struct {
			Profile string `json:"profile"`
			Model   string `json:"model"`
			Region  string `json:"region"`
			Channel []int  `json:"channel_id"`
		}{identity.Profile, identity.Model, identity.Region, []int{0}})
		return hash, err
	}
	hash, _, err := postgres.HashJSON(struct {
		Profile   string `json:"profile"`
		Provider  string `json:"provider"`
		Model     string `json:"model"`
		Region    string `json:"region"`
		Workspace string `json:"workspace"`
		Channel   []int  `json:"channel_id"`
	}{identity.Profile, identity.Provider, identity.Model, identity.Region, identity.Workspace, []int{0}})
	return hash, err
}

func (p *ASRProcessor) tryReuseASR(ctx context.Context, task tasks.Task, stageTask postgres.StageTask,
	input postgres.ASRInput, inputFingerprint, profileHash []byte) (bool, error) {
	reusable, found, err := p.repository.FindReusableASR(ctx, task.VideoID, inputFingerprint, profileHash)
	if err != nil {
		return false, &StageFailure{Code: "ASR_REUSE_LOOKUP_FAILED", Retryable: true, Err: err}
	}
	if !found {
		return false, nil
	}
	valid, err := p.verifyASRObject(ctx, task.VideoID, reusable, inputFingerprint, profileHash)
	if err != nil {
		return false, &StageFailure{Code: "ASR_REUSE_CHECK_FAILED", Retryable: true, Err: err}
	}
	if !valid {
		if err := p.repository.MarkASRBlobBroken(ctx, stageTask, reusable.ID); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := p.repository.ReuseASR(ctx, stageTask, input, inputFingerprint, profileHash, reusable); err != nil {
		return false, err
	}
	return true, nil
}

func (p *ASRProcessor) submit(ctx context.Context, task tasks.Task, stageTask postgres.StageTask, input postgres.ASRInput,
	inputFingerprint, profileHash []byte) error {
	if input.Audio.Bucket != p.bucket {
		return &StageFailure{Code: "ASR_AUDIO_BUCKET_MISMATCH", Retryable: false, Err: errors.New("audio artifact belongs to an unexpected OSS bucket")}
	}
	if err := p.verifyAudioObject(ctx, task, input.Audio); err != nil {
		return err
	}
	linkCtx, cancelLink := context.WithTimeout(ctx, 10*time.Second)
	link, err := p.store.PresignGet(linkCtx, input.Audio.ObjectKey, input.Audio.ObjectVersion, asrURLLifetime)
	cancelLink()
	if err != nil {
		return &StageFailure{Code: "ASR_AUDIO_URL_SIGN_FAILED", Retryable: false, Err: err}
	}
	if !link.ExpiresAt.After(time.Now().Add(150 * time.Minute)) {
		return &StageFailure{Code: "ASR_AUDIO_URL_WINDOW_TOO_SHORT", Retryable: false,
			Err: errors.New("signed audio URL does not cover the provider wait budget and safety margin")}
	}
	submission, err := p.repository.ReserveASRSubmission(ctx, stageTask, input, inputFingerprint, profileHash,
		p.identity.Region, p.identity.Workspace, p.identity.Model)
	if errors.Is(err, postgres.ErrASRProviderAtCapacity) {
		return p.wait(ctx, stageTask, uuid.Nil, "PROVIDER_CAPACITY", time.Now().Add(10*time.Second))
	}
	if errors.Is(err, postgres.ErrASRSubmissionBudget) {
		return &StageFailure{Code: "ASR_SUBMISSION_BUDGET_EXHAUSTED", Retryable: false, Err: err}
	}
	if errors.Is(err, postgres.ErrASRResubmitNotReady) {
		return p.wait(ctx, stageTask, uuid.Nil, "ASR_BACKOFF", time.Now().Add(15*time.Second))
	}
	if errors.Is(err, postgres.ErrASRProviderDeadline) {
		return &StageFailure{Code: "ASR_PROVIDER_DEADLINE_EXCEEDED", Retryable: false, Err: err}
	}
	if err != nil {
		return err
	}
	submitCtx, cancelSubmit := context.WithTimeout(ctx, asrSubmitTimeout)
	providerTask, submitErr := p.provider.Submit(submitCtx, link.URL)
	cancelSubmit()
	if submitErr != nil {
		var apiErr *funasr.APIError
		if errors.As(submitErr, &apiErr) && apiErr.StatusCode == 429 {
			if recordErr := p.repository.RecordASRRejected(ctx, stageTask, submission.ID,
				"PROVIDER_RATE_LIMITED", apiErr.Error(), apiErr.RetryAfter); recordErr != nil {
				return recordErr
			}
			return p.wait(ctx, stageTask, submission.ID, "ASR_BACKOFF", time.Now().Add(apiErr.RetryAfter))
		}
		if errors.As(submitErr, &apiErr) && !apiErr.Ambiguous {
			if recordErr := p.repository.RecordASRRejected(ctx, stageTask, submission.ID,
				apiErr.Code, apiErr.Error(), 0); recordErr != nil {
				return recordErr
			}
			return &StageFailure{Code: "ASR_PROVIDER_REJECTED", Retryable: false, Err: submitErr}
		}
		if recordErr := p.repository.MarkASRUnknownAndWait(ctx, stageTask, submission.ID,
			"SUBMIT_OUTCOME_UNKNOWN", "ASR submission result could not confirm whether the provider accepted the task"); recordErr != nil {
			return errors.Join(submitErr, recordErr)
		}
		return nil
	}
	if err := p.repository.RecordASRAccepted(ctx, stageTask, submission.ID,
		providerTask.ID, providerTask.RequestID, providerTask.Status); err != nil {
		return err
	}
	next := time.Now().Add(5 * time.Second)
	return p.wait(ctx, stageTask, submission.ID, "ASR_RUNNING", next)
}

func (p *ASRProcessor) poll(ctx context.Context, task tasks.Task, stageTask postgres.StageTask, input postgres.ASRInput,
	inputFingerprint, profileHash []byte, submission postgres.ASRSubmission) error {
	queryCtx, cancelQuery := context.WithTimeout(ctx, asrQueryTimeout)
	providerTask, err := p.provider.GetTask(queryCtx, submission.ProviderTaskID)
	cancelQuery()
	if err != nil {
		var apiErr *funasr.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 && apiErr.StatusCode != 429 {
			return &StageFailure{Code: "ASR_PROVIDER_QUERY_REJECTED", Retryable: false, Err: err}
		}
		delay := pollDelay(submission.PollCount, task.ID)
		if errors.As(err, &apiErr) && apiErr.StatusCode == 429 && apiErr.RetryAfter > delay {
			delay = apiErr.RetryAfter
		}
		return p.wait(ctx, stageTask, submission.ID, "ASR_BACKOFF", time.Now().Add(delay))
	}
	status := strings.ToUpper(providerTask.Status)
	subtaskStatus := strings.ToUpper(providerTask.SubtaskStatus)
	switch status {
	case "SUCCEEDED":
		if subtaskStatus == "FAILED" || subtaskStatus == "ERROR" {
			if err := p.repository.MarkASRRemoteFailed(ctx, stageTask, submission.ID,
				subtaskStatus, providerTask.Code); err != nil {
				return err
			}
			return &StageFailure{Code: "ASR_PROVIDER_SUBTASK_FAILED", Retryable: false,
				Err: errors.New("ASR file subtask failed")}
		}
		if err := p.repository.MarkASRResultReady(ctx, stageTask, submission.ID, providerTask.RequestID); err != nil {
			return err
		}
		submission.State = "RESULT_READY"
		if subtaskStatus != "SUCCEEDED" || providerTask.ResultURL == "" {
			return p.wait(ctx, stageTask, submission.ID, "ASR_RUNNING", time.Now().Add(5*time.Second))
		}
		return p.downloadAndSaveResult(ctx, task, stageTask, input, inputFingerprint, profileHash, submission, providerTask)
	case "FAILED", "CANCELED", "CANCELLED", "EXPIRED":
		if err := p.repository.MarkASRRemoteFailed(ctx, stageTask, submission.ID, status, providerTask.Code); err != nil {
			return err
		}
		return &StageFailure{Code: "ASR_PROVIDER_FAILED", Retryable: false, Err: errors.New("ASR task failed remotely")}
	default:
		if err := p.repository.RecordASRPoll(ctx, stageTask, submission.ID, status, providerTask.RequestID,
			time.Now().Add(pollDelay(submission.PollCount, task.ID))); err != nil {
			return err
		}
		return p.wait(ctx, stageTask, submission.ID, "ASR_RUNNING",
			time.Now().Add(pollDelay(submission.PollCount, task.ID)))
	}
}

func (p *ASRProcessor) downloadAndSaveResult(ctx context.Context, task tasks.Task, stageTask postgres.StageTask,
	input postgres.ASRInput, inputFingerprint, profileHash []byte, submission postgres.ASRSubmission, providerTask funasr.Task) error {
	attempt, err := p.repository.ReserveASRResultDownload(ctx, stageTask, submission.ID)
	if err != nil {
		return err
	}
	downloadCtx, cancelDownload := context.WithTimeout(ctx, asrResultTimeout)
	result, downloadErr := p.provider.DownloadResult(downloadCtx, providerTask.ResultURL)
	cancelDownload()
	if downloadErr != nil {
		var apiErr *funasr.APIError
		if errors.As(downloadErr, &apiErr) && (apiErr.StatusCode == 403 || apiErr.StatusCode == 404) {
			if markErr := p.repository.MarkASRRemoteFailed(ctx, stageTask, submission.ID, "ASR_RESULT_EXPIRED", "provider result link is no longer accessible"); markErr != nil {
				return errors.Join(downloadErr, markErr)
			}
			return &StageFailure{Code: "ASR_RESULT_EXPIRED", Retryable: false, Err: downloadErr}
		}
		if attempt >= 3 {
			if markErr := p.repository.MarkASRRemoteFailed(ctx, stageTask, submission.ID, "ASR_RESULT_DOWNLOAD_EXHAUSTED", "provider result download attempt budget exhausted"); markErr != nil {
				return errors.Join(downloadErr, markErr)
			}
			return &StageFailure{Code: "ASR_RESULT_DOWNLOAD_FAILED", Retryable: false, Err: downloadErr}
		}
		delay := time.Duration(attempt*10) * time.Second
		if errors.As(downloadErr, &apiErr) && apiErr.RetryAfter > delay {
			delay = apiErr.RetryAfter
		}
		return p.wait(ctx, stageTask, submission.ID, "ASR_BACKOFF", time.Now().Add(delay))
	}
	return p.storeAndCompleteResult(ctx, task, stageTask, input, inputFingerprint, profileHash,
		submission, providerTask, result)
}

func (p *ASRProcessor) recoverStoredResult(ctx context.Context, task postgres.StageTask, input postgres.ASRInput,
	inputFingerprint, profileHash []byte, submission postgres.ASRSubmission) (bool, error) {
	if submission.Sequence == 0 || submission.State == "REJECTED" || submission.State == "UNKNOWN" || submission.State == "SUBMITTING" || submission.State == "FAILED" {
		return false, nil
	}
	key := asrResultKey(task, submission)
	checkCtx, cancelCheck := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, key, "")
	cancelCheck()
	if err != nil {
		return false, &StageFailure{Code: "ASR_STAGED_RESULT_CHECK_FAILED", Retryable: true, Err: err}
	}
	if !exists {
		return false, nil
	}
	if info.Size <= 0 || info.Size > funasr.MaxResultBytes ||
		!metadataEquals(info.Metadata, "artifact-fingerprint", hex.EncodeToString(inputFingerprint)) ||
		!metadataEquals(info.Metadata, "profile-hash", hex.EncodeToString(profileHash)) ||
		!metadataEquals(info.Metadata, "video-id", task.VideoID.String()) ||
		!metadataEquals(info.Metadata, "submission-id", submission.ID.String()) {
		return false, &StageFailure{Code: "ASR_STORED_RESULT_MISMATCH", Retryable: false,
			Err: errors.New("staged ASR result object did not match its durable operation")}
	}
	var buffer bytes.Buffer
	download, cancel := context.WithTimeout(ctx, 60*time.Second)
	objectInfo, err := p.store.DownloadObject(download, key, info.VersionID, &buffer, funasr.MaxResultBytes)
	cancel()
	if err != nil || objectInfo.Size != info.Size || !objectVersionMatches(info.VersionID, objectInfo.VersionID) {
		return false, &StageFailure{Code: "ASR_STORED_RESULT_READ_FAILED", Retryable: true, Err: err}
	}
	if err := funasr.ValidateResult(buffer.Bytes()); err != nil {
		return false, &StageFailure{Code: "ASR_STORED_RESULT_INVALID", Retryable: false, Err: err}
	}
	sha := sha256.Sum256(buffer.Bytes())
	if !metadataEquals(info.Metadata, "sha256", hex.EncodeToString(sha[:])) {
		return false, &StageFailure{Code: "ASR_STORED_RESULT_CHECKSUM_MISMATCH", Retryable: false,
			Err: errors.New("staged ASR result SHA-256 did not match object metadata")}
	}
	var audioMetadata struct {
		AudioOriginMS int64 `json:"audio_origin_ms"`
	}
	_ = json.Unmarshal(input.Audio.Metadata, &audioMetadata)
	metadata, _ := json.Marshal(map[string]any{"profile": p.identity.Profile, "provider": p.identity.Provider, "model": p.identity.Model,
		"provider_task_id": submission.ProviderTaskID, "provider_request_id": submission.ProviderRequestID,
		"audio_origin_ms": audioMetadata.AudioOriginMS, "sha256": hex.EncodeToString(sha[:]), "size_bytes": info.Size})
	raw := postgres.AudioBlob{ID: uuid.New(), Bucket: p.bucket, ObjectKey: key, ObjectVersion: info.VersionID,
		SizeBytes: info.Size, State: "READY", SHA256: sha[:], ArtifactFingerprint: inputFingerprint,
		ProfileHash: profileHash, Metadata: metadata}
	err = p.repository.CompleteASR(ctx, task, input, submission.ID, inputFingerprint, profileHash, sha[:], raw, metadata)
	return err == nil, err
}

func (p *ASRProcessor) storeAndCompleteResult(ctx context.Context, task tasks.Task, stageTask postgres.StageTask,
	input postgres.ASRInput, inputFingerprint, profileHash []byte, submission postgres.ASRSubmission,
	providerTask funasr.Task, result []byte) error {
	sha := sha256.Sum256(result)
	key := asrResultKey(stageTask, submission)
	metadata := map[string]string{"artifact-fingerprint": hex.EncodeToString(inputFingerprint),
		"profile-hash": hex.EncodeToString(profileHash), "sha256": hex.EncodeToString(sha[:]),
		"video-id": task.VideoID.String(), "submission-id": submission.ID.String()}
	path := filepath.Join(p.tempDir, fmt.Sprintf("asr-result-%s-%d.json", task.ID, submission.Sequence))
	if err := os.WriteFile(path, result, 0600); err != nil {
		return &StageFailure{Code: "ASR_RESULT_SCRATCH_WRITE_FAILED", Retryable: true, Err: err}
	}
	defer os.Remove(path)
	putCtx, cancelPut := context.WithTimeout(ctx, 60*time.Second)
	putInfo, err := p.store.PutObjectFile(putCtx, key, path, "application/json", metadata)
	cancelPut()
	if err != nil {
		return &StageFailure{Code: "ASR_RESULT_PERSIST_FAILED", Retryable: true, Err: err}
	}
	if putInfo.Size != int64(len(result)) {
		return &StageFailure{Code: "ASR_RESULT_PERSIST_MISMATCH", Retryable: true, Err: errors.New("raw transcript object size did not match the downloaded response")}
	}
	info, exists, err := p.store.HeadObjectVersion(ctx, key, putInfo.VersionID)
	if err != nil || !exists || !objectVersionMatches(putInfo.VersionID, info.VersionID) ||
		info.Size != int64(len(result)) || !metadataEquals(info.Metadata, "sha256", hex.EncodeToString(sha[:])) {
		return &StageFailure{Code: "ASR_RESULT_PERSIST_VERIFY_FAILED", Retryable: true, Err: err}
	}
	var audioMetadata struct {
		AudioOriginMS int64 `json:"audio_origin_ms"`
	}
	_ = json.Unmarshal(input.Audio.Metadata, &audioMetadata)
	resultMetadata, _ := json.Marshal(map[string]any{"profile": p.identity.Profile, "provider": p.identity.Provider, "model": p.identity.Model,
		"provider_task_id": providerTask.ID, "provider_request_id": providerTask.RequestID,
		"audio_origin_ms": audioMetadata.AudioOriginMS, "sha256": hex.EncodeToString(sha[:]), "size_bytes": len(result)})
	raw := postgres.AudioBlob{ID: uuid.New(), Bucket: p.bucket, ObjectKey: key, ObjectVersion: putInfo.VersionID,
		SizeBytes: int64(len(result)), State: "READY", SHA256: sha[:], ArtifactFingerprint: inputFingerprint,
		ProfileHash: profileHash, Metadata: resultMetadata}
	return p.repository.CompleteASR(ctx, stageTask, input, submission.ID, inputFingerprint, profileHash, sha[:], raw, resultMetadata)
}

func (p *ASRProcessor) verifyAudioObject(ctx context.Context, task tasks.Task, audio postgres.AudioBlob) error {
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, audio.ObjectKey, audio.ObjectVersion)
	cancel()
	if err != nil {
		return &StageFailure{Code: "ASR_AUDIO_HEAD_FAILED", Retryable: true, Err: err}
	}
	if !exists || info.Size != audio.SizeBytes || !objectVersionMatches(audio.ObjectVersion, info.VersionID) ||
		!validAudioObject(info, audio.SizeBytes, audio.SHA256, audio.ArtifactFingerprint, audio.ProfileHash, task.VideoID) {
		return &StageFailure{Code: "ASR_AUDIO_OBJECT_MISMATCH", Retryable: false,
			Err: errors.New("audio object did not match its committed checkpoint")}
	}
	return nil
}

func (p *ASRProcessor) verifyASRObject(ctx context.Context, videoID uuid.UUID, blob postgres.AudioBlob,
	fingerprint, profileHash []byte) (bool, error) {
	if blob.Bucket != p.bucket || blob.SizeBytes <= 0 || len(blob.SHA256) != sha256.Size {
		return false, nil
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	info, exists, err := p.store.HeadObjectVersion(checkCtx, blob.ObjectKey, blob.ObjectVersion)
	cancel()
	if err != nil || !exists {
		return false, err
	}
	valid := info.Size == blob.SizeBytes && objectVersionMatches(blob.ObjectVersion, info.VersionID) &&
		metadataEquals(info.Metadata, "artifact-fingerprint", hex.EncodeToString(fingerprint)) &&
		metadataEquals(info.Metadata, "profile-hash", hex.EncodeToString(profileHash)) &&
		metadataEquals(info.Metadata, "sha256", hex.EncodeToString(blob.SHA256)) &&
		metadataEquals(info.Metadata, "video-id", videoID.String())
	return valid, nil
}

func (p *ASRProcessor) wait(ctx context.Context, task postgres.StageTask, submissionID uuid.UUID, reason string, next time.Time) error {
	return p.repository.WaitASR(ctx, task, submissionID, reason, next)
}

func pollDelay(pollCount int, taskID uuid.UUID) time.Duration {
	seconds := []int{5, 10, 20, 30}
	index := min(pollCount, len(seconds)-1)
	jitter := time.Duration(taskID[15]%6) * time.Second
	return time.Duration(seconds[index])*time.Second + jitter
}

func asrResultKey(task postgres.StageTask, submission postgres.ASRSubmission) string {
	return fmt.Sprintf("artifacts/asr/%s/%s/%s-%d.json", task.VideoID,
		hex.EncodeToString(submission.AudioFingerprint), task.ID, submission.Sequence)
}
