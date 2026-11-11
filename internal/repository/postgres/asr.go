package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrASRProviderAtCapacity = errors.New("Fun-ASR provider concurrency is full")
	ErrASRSubmissionBudget   = errors.New("Fun-ASR submission budget is exhausted or not yet available")
	ErrASRResubmitNotReady   = errors.New("Fun-ASR supplementary submission is not yet eligible")
	ErrASRProviderDeadline   = errors.New("Fun-ASR provider deadline has elapsed")
)

type ASRInput struct {
	AudioState   string
	SourceBlobID uuid.UUID
	Audio        AudioBlob
}

type ASRSubmission struct {
	ID                uuid.UUID
	AudioFingerprint  []byte
	ProfileHash       []byte
	AudioBlobID       uuid.UUID
	Sequence          int16
	State             string
	ProviderTaskID    string
	ProviderRequestID string
	PreparedAt        time.Time
	UnknownSince      *time.Time
	DeadlineAt        time.Time
	NextPollAt        time.Time
	PollCount         int
	DownloadAttempts  int
	ProviderStatus    string
	LastError         json.RawMessage
}

func (r *StageRepository) LoadASRInput(ctx context.Context, task StageTask) (ASRInput, error) {
	var input ASRInput
	var outputID *uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT s.state,s.source_blob_id,s.output_blob_id,
		COALESCE(b.id,'00000000-0000-0000-0000-000000000000'::uuid),COALESCE(b.bucket,''),
		COALESCE(b.object_key,''),COALESCE(b.object_version,''),COALESCE(b.size_bytes,0),COALESCE(b.state,''),
		COALESCE(b.sha256,decode('', 'hex')),COALESCE(b.artifact_fingerprint,decode('', 'hex')),
		COALESCE(b.profile_hash,decode('', 'hex')),COALESCE(b.metadata,'{}'::jsonb)
		FROM task_steps s LEFT JOIN blobs b ON b.id=s.output_blob_id AND b.video_id=s.video_id
		WHERE s.task_id=$1 AND s.video_id=$2 AND s.stage='AUDIO'`, task.ID, task.VideoID).
		Scan(&input.AudioState, &input.SourceBlobID, &outputID, &input.Audio.ID, &input.Audio.Bucket,
			&input.Audio.ObjectKey, &input.Audio.ObjectVersion, &input.Audio.SizeBytes, &input.Audio.State, &input.Audio.SHA256,
			&input.Audio.ArtifactFingerprint, &input.Audio.ProfileHash, &input.Audio.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return ASRInput{}, errors.New("ASR stage requires a committed AUDIO checkpoint")
	}
	if err != nil {
		return ASRInput{}, fmt.Errorf("load ASR audio checkpoint: %w", err)
	}
	if input.AudioState == "READY" && (outputID == nil || input.Audio.ID == uuid.Nil || input.Audio.State == "BROKEN") {
		return ASRInput{}, errors.New("AUDIO checkpoint points to a missing artifact")
	}
	if input.AudioState == "NO_AUDIO" {
		return input, nil
	}
	if input.AudioState != "READY" {
		return ASRInput{}, fmt.Errorf("AUDIO checkpoint is %s", input.AudioState)
	}
	return input, nil
}

func (r *StageRepository) FindReusableASR(ctx context.Context, videoID uuid.UUID, inputFingerprint, profileHash []byte) (AudioBlob, bool, error) {
	var blob AudioBlob
	err := r.pool.QueryRow(ctx, `SELECT id,bucket,object_key,COALESCE(object_version,''),size_bytes,
		COALESCE(sha256,decode('', 'hex')),artifact_fingerprint,profile_hash,metadata
		FROM blobs WHERE video_id=$1 AND kind='ASR_RAW' AND state='READY'
		AND artifact_fingerprint=$2 AND profile_hash=$3 LIMIT 1`, videoID, inputFingerprint, profileHash).
		Scan(&blob.ID, &blob.Bucket, &blob.ObjectKey, &blob.ObjectVersion, &blob.SizeBytes,
			&blob.SHA256, &blob.ArtifactFingerprint, &blob.ProfileHash, &blob.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return AudioBlob{}, false, nil
	}
	if err != nil {
		return AudioBlob{}, false, fmt.Errorf("find reusable ASR result: %w", err)
	}
	return blob, true, nil
}

func (r *StageRepository) FindASRSubmission(ctx context.Context, taskID uuid.UUID, inputFingerprint, profileHash []byte) (ASRSubmission, bool, error) {
	var submission ASRSubmission
	var unknown pgtype.Timestamptz
	err := r.pool.QueryRow(ctx, `SELECT id,asr_input_fingerprint,profile_hash,audio_blob_id,submit_seq,state,
		COALESCE(provider_task_id,''),COALESCE(provider_request_id,''),prepared_at,unknown_since,
		provider_deadline_at,next_poll_at,poll_count,result_download_attempts,COALESCE(provider_status,''),
		COALESCE(last_error,'{}'::jsonb)
		FROM asr_submissions WHERE task_id=$1 ORDER BY submit_seq DESC LIMIT 1`, taskID).
		Scan(&submission.ID, &submission.AudioFingerprint, &submission.ProfileHash, &submission.AudioBlobID,
			&submission.Sequence, &submission.State, &submission.ProviderTaskID, &submission.ProviderRequestID,
			&submission.PreparedAt, &unknown, &submission.DeadlineAt, &submission.NextPollAt,
			&submission.PollCount, &submission.DownloadAttempts, &submission.ProviderStatus, &submission.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return ASRSubmission{}, false, nil
	}
	if err != nil {
		return ASRSubmission{}, false, fmt.Errorf("load Fun-ASR submission ledger: %w", err)
	}
	if !bytes.Equal(submission.AudioFingerprint, inputFingerprint) || !bytes.Equal(submission.ProfileHash, profileHash) {
		return ASRSubmission{}, false, errors.New("ASR submission ledger does not match the current immutable input profile")
	}
	if unknown.Valid {
		value := unknown.Time
		submission.UnknownSince = &value
	}
	return submission, true, nil
}

// ReserveASRSubmission creates a durable POST budget record before any HTTP call.
// The gate lock is always acquired before the owner/video/task locks.
func (r *StageRepository) ReserveASRSubmission(ctx context.Context, task StageTask, input ASRInput,
	inputFingerprint, profileHash []byte, region, workspace, model string) (ASRSubmission, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ASRSubmission{}, fmt.Errorf("begin Fun-ASR submission reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	gateName := "fun_asr"
	localDemo := region == "local" && workspace == "local-demo"
	if localDemo {
		gateName = "local_demo_asr"
	}
	var maxInflight int
	if err := tx.QueryRow(ctx, `SELECT max_inflight FROM runtime_gates WHERE name=$1 FOR UPDATE`, gateName).Scan(&maxInflight); err != nil {
		return ASRSubmission{}, fmt.Errorf("lock Fun-ASR concurrency gate: %w", err)
	}
	if err := lockStageTask(ctx, tx, task); err != nil {
		return ASRSubmission{}, err
	}
	if input.AudioState != "READY" || input.Audio.ID == uuid.Nil {
		return ASRSubmission{}, errors.New("cannot submit Fun-ASR without a READY audio artifact")
	}
	var active int
	activeQuery := `SELECT count(*) FROM asr_submissions WHERE state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING')`
	if localDemo {
		activeQuery += ` AND region='local' AND workspace_id='local-demo'`
	}
	if err := tx.QueryRow(ctx, activeQuery).Scan(&active); err != nil {
		return ASRSubmission{}, fmt.Errorf("count in-flight Fun-ASR submissions: %w", err)
	}
	if active >= maxInflight {
		return ASRSubmission{}, ErrASRProviderAtCapacity
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM asr_submissions WHERE task_id=$1`, task.ID).Scan(&count); err != nil {
		return ASRSubmission{}, fmt.Errorf("count task Fun-ASR submissions: %w", err)
	}
	if count >= 2 {
		return ASRSubmission{}, ErrASRSubmissionBudget
	}
	sequence := int16(count + 1)
	var providerDeadline time.Time
	if sequence == 1 {
		if err := tx.QueryRow(ctx, `SELECT LEAST(clock_timestamp()+interval '2 hours',deadline_at)
			FROM parse_tasks WHERE id=$1`, task.ID).Scan(&providerDeadline); err != nil {
			return ASRSubmission{}, fmt.Errorf("load ASR provider deadline: %w", err)
		}
	} else {
		var priorState, rateCode string
		var priorFingerprint, priorProfile []byte
		var unknownReady, rateLimitReady bool
		if err := tx.QueryRow(ctx, `SELECT state,provider_deadline_at,
			COALESCE(last_error->>'code',''),asr_input_fingerprint,profile_hash,
			COALESCE(unknown_since <= clock_timestamp()-interval '5 minutes',false),
		next_poll_at <= clock_timestamp()
			FROM asr_submissions WHERE task_id=$1 AND submit_seq=1 FOR UPDATE`, task.ID).
			Scan(&priorState, &providerDeadline, &rateCode, &priorFingerprint, &priorProfile,
				&unknownReady, &rateLimitReady); err != nil {
			return ASRSubmission{}, fmt.Errorf("load prior ASR attempt: %w", err)
		}
		if !bytes.Equal(priorFingerprint, inputFingerprint) || !bytes.Equal(priorProfile, profileHash) {
			return ASRSubmission{}, errors.New("second ASR submission must preserve the first attempt input and profile")
		}
		unknownReady = priorState == "UNKNOWN" && unknownReady
		rateLimitReady = priorState == "REJECTED" && rateCode == "PROVIDER_RATE_LIMITED" && rateLimitReady
		if !unknownReady && !rateLimitReady {
			return ASRSubmission{}, ErrASRResubmitNotReady
		}
	}
	var providerExpired bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz <= clock_timestamp()`, providerDeadline).Scan(&providerExpired); err != nil {
		return ASRSubmission{}, fmt.Errorf("check ASR provider deadline: %w", err)
	}
	if providerExpired {
		return ASRSubmission{}, ErrASRProviderDeadline
	}
	submission := ASRSubmission{ID: uuid.New(), AudioFingerprint: inputFingerprint, ProfileHash: profileHash,
		AudioBlobID: input.Audio.ID, Sequence: sequence, State: "SUBMITTING", DeadlineAt: providerDeadline,
		NextPollAt: time.Now()}
	_, err = tx.Exec(ctx, `INSERT INTO asr_submissions(id,task_id,video_id,asr_input_fingerprint,profile_hash,audio_blob_id,
		submit_seq,operation_id,state,region,workspace_id,model,provider_deadline_at,next_poll_at,last_run_epoch)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'SUBMITTING',$9,$10,$11,$12,clock_timestamp(),$13)`,
		submission.ID, task.ID, task.VideoID, inputFingerprint, profileHash, input.Audio.ID,
		sequence, uuid.New(), region, workspace, model, providerDeadline, task.RunEpoch)
	if err != nil {
		return ASRSubmission{}, fmt.Errorf("persist Fun-ASR submission budget: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ASRSubmission{}, fmt.Errorf("commit Fun-ASR submission budget: %w", err)
	}
	return submission, nil
}

func (r *StageRepository) RecordASRAccepted(ctx context.Context, task StageTask, submissionID uuid.UUID, providerTaskID, requestID, status string) error {
	if providerTaskID == "" {
		return errors.New("Fun-ASR accepted response must include task_id")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin accepted Fun-ASR task checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var manuallyResolved bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM asr_manual_resolutions WHERE submission_id=$1)
		FROM asr_submissions WHERE id=$1 AND task_id=$2 FOR UPDATE`, submissionID, task.ID).Scan(&manuallyResolved); err != nil {
		return fmt.Errorf("lock Fun-ASR submission acceptance: %w", err)
	}
	updated, err := tx.Exec(ctx, `UPDATE asr_submissions SET state='ACCEPTED',provider_task_id=$3,unknown_since=NULL,
		provider_request_id=NULLIF($4,''),provider_status=NULLIF($5,''),submitted_at=clock_timestamp(),
		next_poll_at=clock_timestamp()+interval '5 seconds',last_error=NULL,last_run_epoch=$6,
		remote_terminal_at=NULL,reconcile_owner=NULL,reconcile_lease_until=NULL,updated_at=clock_timestamp()
		WHERE id=$1 AND task_id=$2 AND (state IN ('SUBMITTING','UNKNOWN') OR
			(state='REMOTE_TERMINAL' AND provider_task_id IS NULL AND EXISTS(
				SELECT 1 FROM asr_manual_resolutions WHERE submission_id=$1 AND superseded_at IS NULL)))`,
		submissionID, task.ID, providerTaskID, requestID, status, task.RunEpoch)
	if err != nil {
		return fmt.Errorf("save accepted Fun-ASR task ID: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return errors.New("ASR submission is no longer awaiting acceptance")
	}
	if manuallyResolved {
		audit, err := tx.Exec(ctx, `UPDATE asr_manual_resolutions SET superseded_at=clock_timestamp(),
			superseded_reason='LATE_PROVIDER_ACCEPTANCE',superseded_by_provider_task_id=$2
			WHERE submission_id=$1 AND superseded_at IS NULL`, submissionID, providerTaskID)
		if err != nil {
			return fmt.Errorf("supersede manual ASR resolution after late acceptance: %w", err)
		}
		if audit.RowsAffected() != 1 {
			return errors.New("manual ASR resolution audit changed before late acceptance")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit accepted Fun-ASR task checkpoint: %w", err)
	}
	return nil
}

func (r *StageRepository) RecordASRRejected(ctx context.Context, task StageTask, submissionID uuid.UUID, code, message string, retryAfter time.Duration) error {
	lastError, _ := json.Marshal(map[string]any{"code": code, "message": boundedError(message)})
	if retryAfter < 0 {
		retryAfter = 0
	}
	result, err := r.pool.Exec(ctx, `UPDATE asr_submissions SET state='REJECTED',unknown_since=NULL,provider_status=$3,
		last_error=$4::jsonb,next_poll_at=clock_timestamp()+($5 * interval '1 millisecond'),
		last_run_epoch=$6,updated_at=clock_timestamp() WHERE id=$1 AND task_id=$2 AND state IN ('SUBMITTING','UNKNOWN')`,
		submissionID, task.ID, code, string(lastError), retryAfter.Milliseconds(), task.RunEpoch)
	if err != nil {
		return fmt.Errorf("save Fun-ASR rejection: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("ASR submission is no longer awaiting rejection")
	}
	return nil
}

func (r *StageRepository) RecordASRPoll(ctx context.Context, task StageTask, submissionID uuid.UUID,
	status, requestID string, nextPoll time.Time) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin record Fun-ASR poll: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE asr_submissions SET state='RUNNING',provider_status=NULLIF($3,''),
		provider_request_id=COALESCE(NULLIF($4,''),provider_request_id),poll_count=poll_count+1,
		next_poll_at=$5,last_run_epoch=$6,updated_at=clock_timestamp()
		WHERE id=$1 AND task_id=$2 AND state IN ('ACCEPTED','RUNNING')`,
		submissionID, task.ID, status, requestID, nextPoll, task.RunEpoch)
	if err != nil {
		return fmt.Errorf("save Fun-ASR poll state: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("Fun-ASR submission is not queryable")
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) MarkASRResultReady(ctx context.Context, task StageTask, submissionID uuid.UUID, requestID string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin mark Fun-ASR result ready: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE asr_submissions SET state='RESULT_READY',provider_status='SUCCEEDED',
		provider_request_id=COALESCE(NULLIF($3,''),provider_request_id),poll_count=poll_count+1,
		next_poll_at=clock_timestamp(),last_run_epoch=$4,updated_at=clock_timestamp()
		WHERE id=$1 AND task_id=$2 AND state IN ('ACCEPTED','RUNNING')`, submissionID, task.ID, requestID, task.RunEpoch)
	if err != nil {
		return fmt.Errorf("persist Fun-ASR completed state: %w", err)
	}
	if result.RowsAffected() != 1 {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM asr_submissions WHERE id=$1 AND task_id=$2`, submissionID, task.ID).Scan(&state); err != nil {
			return fmt.Errorf("confirm Fun-ASR result-ready state: %w", err)
		}
		if state != "RESULT_READY" {
			return fmt.Errorf("cannot mark Fun-ASR result ready from state %s", state)
		}
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) WaitASR(ctx context.Context, task StageTask, submissionID uuid.UUID, reason string, nextPoll time.Time) error {
	if reason == "" {
		return errors.New("ASR wait reason is required")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin persist ASR wait: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	if submissionID != uuid.Nil {
		if _, err := tx.Exec(ctx, `UPDATE asr_submissions SET next_poll_at=GREATEST(next_poll_at,$3),
			last_run_epoch=$4,updated_at=clock_timestamp() WHERE id=$1 AND task_id=$2`,
			submissionID, task.ID, nextPoll, task.RunEpoch); err != nil {
			return fmt.Errorf("save next Fun-ASR check time: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='PENDING',updated_at=clock_timestamp()
		WHERE task_id=$1 AND stage='ASR'`, task.ID); err != nil {
		return fmt.Errorf("release ASR checkpoint to waiting: %w", err)
	}
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='WAITING',wait_reason=$4,next_run_at=LEAST($5,deadline_at),
		lease_owner=NULL,lease_until=NULL WHERE id=$1 AND video_id=$2 AND run_epoch=$3 AND status='RUNNING' AND lease_owner=$6`,
		task.ID, task.VideoID, task.RunEpoch, reason, nextPoll, task.LeaseOwner)
	if err != nil {
		return fmt.Errorf("persist ASR wait state: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrStageLeaseLost
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) MarkASRUnknownAndWait(ctx context.Context, task StageTask, submissionID uuid.UUID, code, message string) error {
	lastError, _ := json.Marshal(map[string]string{"code": code, "message": boundedError(message)})
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin persist unknown ASR acceptance: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var next time.Time
	err = tx.QueryRow(ctx, `UPDATE asr_submissions SET state='UNKNOWN',unknown_since=COALESCE(unknown_since,clock_timestamp()),
		next_poll_at=clock_timestamp()+interval '5 minutes',last_error=$3::jsonb,last_run_epoch=$4,updated_at=clock_timestamp()
		WHERE id=$1 AND task_id=$2 AND state='SUBMITTING' RETURNING next_poll_at`, submissionID, task.ID, string(lastError), task.RunEpoch).Scan(&next)
	if err != nil {
		return fmt.Errorf("record unknown Fun-ASR acceptance: %w", err)
	}
	if next.IsZero() {
		return errors.New("ASR submission was not awaiting its initial response")
	}
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='WAITING',wait_reason='ASR_UNKNOWN',next_run_at=LEAST($4,deadline_at),
		lease_owner=NULL,lease_until=NULL WHERE id=$1 AND video_id=$2 AND run_epoch=$3 AND status='RUNNING' AND lease_owner=$5`,
		task.ID, task.VideoID, task.RunEpoch, next, task.LeaseOwner)
	if err != nil {
		return fmt.Errorf("persist ASR unknown wait: %w", err)
	}
	if result.RowsAffected() == 1 {
		if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='PENDING',updated_at=clock_timestamp()
			WHERE task_id=$1 AND stage='ASR'`, task.ID); err != nil {
			return fmt.Errorf("release unknown ASR checkpoint: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) ReserveASRResultDownload(ctx context.Context, task StageTask, submissionID uuid.UUID) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin result download reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return 0, err
	}
	var attempt int
	err = tx.QueryRow(ctx, `UPDATE asr_submissions SET result_download_attempts=result_download_attempts+1,
		last_run_epoch=$3,updated_at=clock_timestamp() WHERE id=$1 AND task_id=$2 AND state='RESULT_READY'
		AND result_download_attempts<3 RETURNING result_download_attempts`, submissionID, task.ID, task.RunEpoch).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrASRSubmissionBudget
	}
	if err != nil {
		return 0, fmt.Errorf("reserve Fun-ASR result download attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit result download attempt: %w", err)
	}
	return attempt, nil
}

func (r *StageRepository) MarkASRRemoteFailed(ctx context.Context, task StageTask, submissionID uuid.UUID, status, message string) error {
	lastError, _ := json.Marshal(map[string]string{"code": status, "message": boundedError(message)})
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin record remote ASR failure: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE asr_submissions SET state='FAILED',provider_status=$3,last_error=$4::jsonb,
		updated_at=clock_timestamp() WHERE id=$1 AND task_id=$2 AND state IN ('ACCEPTED','RUNNING','RESULT_READY')`,
		submissionID, task.ID, status, string(lastError))
	if err != nil {
		return fmt.Errorf("save remote Fun-ASR failure: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("remote ASR submission is no longer active")
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) CompleteASRNoAudio(ctx context.Context, task StageTask, input ASRInput,
	inputFingerprint, profileHash []byte, metadata json.RawMessage) error {
	if input.AudioState != "NO_AUDIO" || !json.Valid(metadata) {
		return errors.New("ASR no-audio completion requires a valid no-audio checkpoint")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin no-audio ASR checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	if err := saveASRStepAndAdvance(ctx, tx, task, input.SourceBlobID, nil, nil, "NO_AUDIO", inputFingerprint, profileHash, metadata); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) CompleteASR(ctx context.Context, task StageTask, input ASRInput, submissionID uuid.UUID,
	inputFingerprint, profileHash, resultSHA []byte, raw AudioBlob, metadata json.RawMessage) error {
	if len(inputFingerprint) != 32 || len(profileHash) != 32 || len(resultSHA) != 32 || raw.ID == uuid.Nil || raw.Bucket == "" || raw.ObjectKey == "" || raw.SizeBytes <= 0 || !json.Valid(metadata) {
		return errors.New("ASR result checkpoint metadata or object reference is invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin ASR result checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var blobID uuid.UUID
	err = tx.QueryRow(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,object_version,sha256,size_bytes,state,
		created_by_task,artifact_fingerprint,profile_hash,metadata)
		VALUES($1,$2,'ASR_RAW',$3,$4,NULLIF($5,''),$6,$7,'READY',$8,$9,$10,$11::jsonb)
		ON CONFLICT DO NOTHING RETURNING id`, raw.ID, task.VideoID, raw.Bucket, raw.ObjectKey, raw.ObjectVersion,
		resultSHA, raw.SizeBytes, task.ID, inputFingerprint, profileHash, string(metadata)).Scan(&blobID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT id,metadata FROM blobs WHERE video_id=$1 AND kind='ASR_RAW' AND state='READY'
			AND artifact_fingerprint=$2 AND profile_hash=$3 LIMIT 1`, task.VideoID, inputFingerprint, profileHash).
			Scan(&blobID, &metadata)
	}
	if err != nil {
		return fmt.Errorf("register raw ASR result: %w", err)
	}
	if submissionID != uuid.Nil {
		result, err := tx.Exec(ctx, `UPDATE asr_submissions SET state='SUCCEEDED',raw_result_blob_id=$3,
			provider_status='SUCCEEDED',updated_at=clock_timestamp()
			WHERE id=$1 AND task_id=$2 AND state='RESULT_READY'`, submissionID, task.ID, blobID)
		if err != nil {
			return fmt.Errorf("complete Fun-ASR submission: %w", err)
		}
		if result.RowsAffected() != 1 {
			return errors.New("Fun-ASR submission was not active when its result completed")
		}
	}
	var stepSubmission *uuid.UUID
	if submissionID != uuid.Nil {
		stepSubmission = &submissionID
	}
	if err := saveASRStepAndAdvance(ctx, tx, task, input.SourceBlobID, &blobID, stepSubmission, "READY", inputFingerprint, profileHash, metadata); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) ReuseASR(ctx context.Context, task StageTask, input ASRInput, inputFingerprint, profileHash []byte, raw AudioBlob) error {
	metadata := raw.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin reused ASR checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	if err := saveASRStepAndAdvance(ctx, tx, task, input.SourceBlobID, &raw.ID, nil, "READY", inputFingerprint, profileHash, metadata); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) MarkASRBlobBroken(ctx context.Context, task StageTask, blobID uuid.UUID) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin mark ASR result broken: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE blobs SET state='BROKEN' WHERE id=$1 AND video_id=$2 AND kind='ASR_RAW' AND state='READY'`, blobID, task.VideoID)
	if err != nil {
		return fmt.Errorf("mark raw ASR result broken: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("raw ASR result changed before marking broken")
	}
	return tx.Commit(ctx)
}

func saveASRStepAndAdvance(ctx context.Context, tx pgx.Tx, task StageTask, sourceBlobID uuid.UUID,
	outputBlobID, submissionID *uuid.UUID, state string, inputFingerprint, profileHash []byte, metadata json.RawMessage) error {
	if len(inputFingerprint) != 32 || len(profileHash) != 32 || !json.Valid(metadata) {
		return errors.New("ASR checkpoint hashes or metadata are invalid")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_blob_id,output_asr_submission_id,output_metadata,failure_count,last_run_epoch,
		started_at,completed_at,updated_at)
		VALUES($1,$2,'ASR',$3,$4,$5,$6,$7,$8,$9::jsonb,0,$10,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state=EXCLUDED.state,input_fingerprint=EXCLUDED.input_fingerprint,
		config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=EXCLUDED.output_blob_id,
		output_asr_submission_id=EXCLUDED.output_asr_submission_id,output_metadata=EXCLUDED.output_metadata,
		last_run_epoch=EXCLUDED.last_run_epoch,completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`,
		task.ID, task.VideoID, state, inputFingerprint, profileHash, sourceBlobID, outputBlobID, submissionID, string(metadata), task.RunEpoch); err != nil {
		return fmt.Errorf("save ASR checkpoint: %w", err)
	}
	return advanceStage(ctx, tx, task, "TRANSCRIPT")
}
