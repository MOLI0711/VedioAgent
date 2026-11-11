package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
)

var ErrStageLeaseLost = errors.New("parse task lease is no longer valid")

type StageRepository struct{ pool *pgxpool.Pool }

func NewStageRepository(pool *pgxpool.Pool) *StageRepository {
	return &StageRepository{pool: pool}
}

type StageTask struct {
	ID         uuid.UUID
	VideoID    uuid.UUID
	OwnerID    uuid.UUID
	Stage      string
	RunEpoch   int64
	LeaseOwner string
}

type SourceBlob struct {
	ID                  uuid.UUID
	Bucket              string
	ObjectKey           string
	ObjectVersion       string
	SizeBytes           int64
	State               string
	SHA256              []byte
	ArtifactFingerprint []byte
	ProfileHash         []byte
	Metadata            json.RawMessage
	Probe               json.RawMessage
	MediaDurationMS     int64
}

type StageStep struct {
	State                string
	InputFingerprint     []byte
	ConfigHash           []byte
	OutputBlobID         *uuid.UUID
	OutputChunkSetID     *uuid.UUID
	OutputEmbeddingSetID *uuid.UUID
	OutputResultID       *uuid.UUID
	OutputMetadata       json.RawMessage
}

type AudioBlob struct {
	ID                  uuid.UUID
	Bucket              string
	ObjectKey           string
	ObjectVersion       string
	SizeBytes           int64
	State               string
	SHA256              []byte
	ArtifactFingerprint []byte
	ProfileHash         []byte
	Metadata            json.RawMessage
}

func (r *StageRepository) LoadSourceBlob(ctx context.Context, task StageTask) (SourceBlob, error) {
	var blob SourceBlob
	err := r.pool.QueryRow(ctx, `SELECT b.id,b.bucket,b.object_key,COALESCE(b.object_version,''),b.size_bytes,b.state,
		COALESCE(b.sha256,decode('', 'hex')),COALESCE(b.artifact_fingerprint,decode('', 'hex')),
		COALESCE(b.profile_hash,decode('', 'hex')),b.metadata,v.probe_metadata,COALESCE(v.media_duration_ms,0)
		FROM parse_tasks t JOIN videos v ON v.id=t.video_id AND v.owner_id=t.owner_id
		JOIN blobs b ON b.id=v.source_blob_id AND b.video_id=v.id AND b.kind='SOURCE_VIDEO'
		WHERE t.id=$1 AND t.video_id=$2 AND t.owner_id=$3 AND v.lifecycle='ACTIVE'
		  AND v.file_state IN ('UPLOADED','VALID')`, task.ID, task.VideoID, task.OwnerID).
		Scan(&blob.ID, &blob.Bucket, &blob.ObjectKey, &blob.ObjectVersion, &blob.SizeBytes, &blob.State,
			&blob.SHA256, &blob.ArtifactFingerprint, &blob.ProfileHash, &blob.Metadata, &blob.Probe, &blob.MediaDurationMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return SourceBlob{}, &application.ServiceError{Status: 409, Code: "SOURCE_NOT_READY", Message: "源视频尚未就绪"}
	}
	if err != nil {
		return SourceBlob{}, fmt.Errorf("load source blob for parse stage: %w", err)
	}
	return blob, nil
}

func (r *StageRepository) FindTaskStep(ctx context.Context, taskID uuid.UUID, stage string) (StageStep, bool, error) {
	var step StageStep
	err := r.pool.QueryRow(ctx, `SELECT state,input_fingerprint,config_hash,output_blob_id,output_chunk_set_id,
		output_embedding_set_id,output_result_id,output_metadata
		FROM task_steps WHERE task_id=$1 AND stage=$2`, taskID, stage).
		Scan(&step.State, &step.InputFingerprint, &step.ConfigHash, &step.OutputBlobID, &step.OutputChunkSetID,
			&step.OutputEmbeddingSetID, &step.OutputResultID, &step.OutputMetadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return StageStep{}, false, nil
	}
	if err != nil {
		return StageStep{}, false, fmt.Errorf("load parse task stage checkpoint: %w", err)
	}
	return step, true, nil
}

func (r *StageRepository) FindReusableAudio(ctx context.Context, videoID uuid.UUID, inputFingerprint, configHash []byte) (AudioBlob, bool, error) {
	var blob AudioBlob
	err := r.pool.QueryRow(ctx, `SELECT id,bucket,object_key,COALESCE(object_version,''),size_bytes,
		COALESCE(sha256,decode('', 'hex')),artifact_fingerprint,profile_hash,metadata
		FROM blobs WHERE video_id=$1 AND kind='AUDIO' AND state='READY'
		  AND artifact_fingerprint=$2 AND profile_hash=$3 LIMIT 1`, videoID, inputFingerprint, configHash).
		Scan(&blob.ID, &blob.Bucket, &blob.ObjectKey, &blob.ObjectVersion, &blob.SizeBytes,
			&blob.SHA256, &blob.ArtifactFingerprint, &blob.ProfileHash, &blob.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return AudioBlob{}, false, nil
	}
	if err != nil {
		return AudioBlob{}, false, fmt.Errorf("find reusable audio artifact: %w", err)
	}
	return blob, true, nil
}

func (r *StageRepository) StartStage(ctx context.Context, task StageTask, sourceBlobID uuid.UUID, inputFingerprint, configHash []byte) (StageStep, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return StageStep{}, fmt.Errorf("begin parse stage: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return StageStep{}, err
	}
	var step StageStep
	err = tx.QueryRow(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,last_run_epoch,started_at,updated_at)
		VALUES ($1,$2,$3,'RUNNING',$4,$5,$6,$7,clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state='RUNNING',input_fingerprint=EXCLUDED.input_fingerprint,
			config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=NULL,
			output_asr_submission_id=NULL,output_transcript_id=NULL,output_chunk_set_id=NULL,output_embedding_set_id=NULL,output_result_id=NULL,
			output_metadata='{}'::jsonb,last_run_epoch=EXCLUDED.last_run_epoch,
			started_at=clock_timestamp(),completed_at=NULL,last_error=NULL,updated_at=clock_timestamp()
		RETURNING state,input_fingerprint,config_hash,output_blob_id,output_chunk_set_id,output_embedding_set_id,output_result_id,output_metadata`,
		task.ID, task.VideoID, task.Stage, inputFingerprint, configHash, sourceBlobID, task.RunEpoch).
		Scan(&step.State, &step.InputFingerprint, &step.ConfigHash, &step.OutputBlobID, &step.OutputChunkSetID,
			&step.OutputEmbeddingSetID, &step.OutputResultID, &step.OutputMetadata)
	if err != nil {
		return StageStep{}, fmt.Errorf("record parse stage start: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return StageStep{}, fmt.Errorf("commit parse stage start: %w", err)
	}
	return step, nil
}

func (r *StageRepository) CompleteProbe(ctx context.Context, task StageTask, source SourceBlob,
	inputFingerprint, configHash, sourceSHA256 []byte, metadata json.RawMessage) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin probe checkpoint commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var probe struct {
		DurationMS int64 `json:"duration_ms"`
	}
	if err := json.Unmarshal(metadata, &probe); err != nil || probe.DurationMS <= 0 {
		return errors.New("probe metadata is invalid")
	}
	if len(inputFingerprint) != 32 || len(configHash) != 32 || len(sourceSHA256) != 32 {
		return errors.New("probe fingerprints must be SHA-256 values")
	}
	blobResult, err := tx.Exec(ctx, `UPDATE blobs SET state='READY',sha256=$3,artifact_fingerprint=$3,
		profile_hash=$4,metadata=$5::jsonb WHERE id=$1 AND video_id=$2 AND kind='SOURCE_VIDEO' AND state IN ('STORED','READY')`,
		source.ID, task.VideoID, sourceSHA256, configHash, string(metadata))
	if err != nil {
		return fmt.Errorf("mark source blob verified: %w", err)
	}
	if blobResult.RowsAffected() != 1 {
		return ErrStageLeaseLost
	}
	videoResult, err := tx.Exec(ctx, `UPDATE videos SET file_state='VALID',media_duration_ms=$3,probe_metadata=$4::jsonb
		WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE' AND file_state IN ('UPLOADED','VALID')`,
		task.VideoID, task.OwnerID, probe.DurationMS, string(metadata))
	if err != nil {
		return fmt.Errorf("save probed video metadata: %w", err)
	}
	if videoResult.RowsAffected() != 1 {
		return ErrStageLeaseLost
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_blob_id,output_metadata,failure_count,last_run_epoch,started_at,completed_at,updated_at)
		VALUES ($1,$2,'PROBE','READY',$3,$4,$5,$5,$6::jsonb,0,$7,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state='READY',input_fingerprint=EXCLUDED.input_fingerprint,
			config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=EXCLUDED.output_blob_id,
			output_metadata=EXCLUDED.output_metadata,last_run_epoch=EXCLUDED.last_run_epoch,
			completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`,
		task.ID, task.VideoID, inputFingerprint, configHash, source.ID, string(metadata), task.RunEpoch); err != nil {
		return fmt.Errorf("save probe checkpoint: %w", err)
	}
	if err := advanceStage(ctx, tx, task, "AUDIO"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit probe checkpoint: %w", err)
	}
	return nil
}

func (r *StageRepository) CompleteAudio(ctx context.Context, task StageTask, source SourceBlob,
	inputFingerprint, configHash []byte, audio AudioBlob, metadata json.RawMessage, noAudio bool) (AudioBlob, error) {
	if len(inputFingerprint) != 32 || len(configHash) != 32 || !json.Valid(metadata) {
		return AudioBlob{}, errors.New("audio checkpoint fingerprints or metadata are invalid")
	}
	if !noAudio && (audio.ID == uuid.Nil || audio.Bucket == "" || audio.ObjectKey == "" || audio.SizeBytes <= 44 || len(audio.SHA256) != 32) {
		return AudioBlob{}, errors.New("ready audio checkpoint requires a complete object reference and SHA-256")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return AudioBlob{}, fmt.Errorf("begin audio checkpoint commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return AudioBlob{}, err
	}
	var outputID *uuid.UUID
	if !noAudio {
		var id uuid.UUID
		err := tx.QueryRow(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,object_version,sha256,size_bytes,state,
			created_by_task,artifact_fingerprint,profile_hash,metadata)
			VALUES ($1,$2,'AUDIO',$3,$4,NULLIF($5,''),$6,$7,'READY',$8,$9,$10,$11::jsonb)
			ON CONFLICT DO NOTHING RETURNING id`, audio.ID, task.VideoID, audio.Bucket, audio.ObjectKey,
			audio.ObjectVersion, audio.SHA256, audio.SizeBytes, task.ID, inputFingerprint, configHash, string(metadata)).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `SELECT id FROM blobs WHERE video_id=$1 AND kind='AUDIO' AND state='READY'
				AND artifact_fingerprint=$2 AND profile_hash=$3 LIMIT 1`, task.VideoID, inputFingerprint, configHash).Scan(&id)
			if err != nil {
				return AudioBlob{}, fmt.Errorf("resolve concurrent reusable audio artifact: %w", err)
			}
		} else if err != nil {
			return AudioBlob{}, fmt.Errorf("register audio artifact: %w", err)
		}
		outputID = &id
		if id != audio.ID {
			if err := tx.QueryRow(ctx, `SELECT bucket,object_key,COALESCE(object_version,''),size_bytes,
				COALESCE(sha256,decode('', 'hex')),artifact_fingerprint,profile_hash,metadata FROM blobs WHERE id=$1`, id).
				Scan(&audio.Bucket, &audio.ObjectKey, &audio.ObjectVersion, &audio.SizeBytes,
					&audio.SHA256, &audio.ArtifactFingerprint, &audio.ProfileHash, &audio.Metadata); err != nil {
				return AudioBlob{}, fmt.Errorf("load concurrent reusable audio artifact: %w", err)
			}
			audio.ID = id
			metadata = audio.Metadata
		} else {
			audio.ArtifactFingerprint, audio.ProfileHash, audio.Metadata = inputFingerprint, configHash, metadata
		}
	}
	state := "READY"
	if noAudio {
		state = "NO_AUDIO"
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_blob_id,output_metadata,failure_count,last_run_epoch,started_at,completed_at,updated_at)
		VALUES ($1,$2,'AUDIO',$3,$4,$5,$6,$7,$8::jsonb,0,$9,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state=EXCLUDED.state,input_fingerprint=EXCLUDED.input_fingerprint,
			config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=EXCLUDED.output_blob_id,
			output_metadata=EXCLUDED.output_metadata,last_run_epoch=EXCLUDED.last_run_epoch,
			completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`,
		task.ID, task.VideoID, state, inputFingerprint, configHash, source.ID, outputID, string(metadata), task.RunEpoch); err != nil {
		return AudioBlob{}, fmt.Errorf("save audio checkpoint: %w", err)
	}
	if err := advanceStage(ctx, tx, task, "ASR"); err != nil {
		return AudioBlob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AudioBlob{}, fmt.Errorf("commit audio checkpoint: %w", err)
	}
	return audio, nil
}

func (r *StageRepository) RecordStageFailure(ctx context.Context, task StageTask, code, message string, retryable bool, retryAfter time.Duration) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin parse stage failure: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		last_run_epoch,started_at,updated_at)
		VALUES ($1,$2,$3,'PENDING',decode(repeat('00',32),'hex'),decode(repeat('00',32),'hex'),$4,clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO NOTHING`, task.ID, task.VideoID, task.Stage, task.RunEpoch); err != nil {
		return 0, fmt.Errorf("ensure stage failure checkpoint: %w", err)
	}
	var failures int
	err = tx.QueryRow(ctx, `UPDATE task_steps SET failure_count=failure_count+1,state='PENDING',
		last_error=jsonb_build_object('code',$3::text,'message',$4::text,'retryable',$5::boolean),updated_at=clock_timestamp()
		WHERE task_id=$1 AND stage=$2 RETURNING failure_count`, task.ID, task.Stage, code, boundedError(message), retryable).
		Scan(&failures)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("stage failure recorded before stage checkpoint exists")
	}
	if err != nil {
		return 0, fmt.Errorf("increment parse stage failure: %w", err)
	}
	terminal := !retryable || failures >= 5
	if terminal {
		if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='FAILED',updated_at=clock_timestamp() WHERE task_id=$1 AND stage=$2`, task.ID, task.Stage); err != nil {
			return 0, fmt.Errorf("mark parse stage failed: %w", err)
		}
	}
	status, delay := "RETRY_WAIT", time.Duration(min(1<<min(failures-1, 6), 60))*time.Second
	if retryAfter > delay {
		delay = min(retryAfter, 15*time.Minute)
	}
	if terminal {
		status, delay = "FAILED", 0
	}
	lastError, _ := json.Marshal(map[string]any{"code": code, "message": boundedError(message), "retryable": !terminal && retryable})
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status=$4,last_error=$5::jsonb,
		next_run_at=clock_timestamp()+($6 * interval '1 millisecond'),completed_at=CASE WHEN $4='FAILED' THEN clock_timestamp() ELSE NULL END,
		lease_owner=NULL,lease_until=NULL WHERE id=$1 AND video_id=$2 AND run_epoch=$3 AND status='RUNNING' AND lease_owner=$7`,
		task.ID, task.VideoID, task.RunEpoch, status, string(lastError), delay.Milliseconds(), task.LeaseOwner)
	if err != nil {
		return 0, fmt.Errorf("schedule parse stage retry: %w", err)
	}
	if result.RowsAffected() != 1 {
		return 0, ErrStageLeaseLost
	}
	if !retryable && rejectsSourceMedia(code) {
		if _, err := tx.Exec(ctx, `UPDATE videos SET file_state='REJECTED'
			WHERE id=$1 AND owner_id=$2 AND file_state IN ('UPLOADED','VALID')`, task.VideoID, task.OwnerID); err != nil {
			return 0, fmt.Errorf("mark invalid source media rejected: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit parse stage failure: %w", err)
	}
	return failures, nil
}

func rejectsSourceMedia(code string) bool {
	switch code {
	case "UNSUPPORTED_MEDIA", "SOURCE_SIZE_INVALID", "SOURCE_BLOB_UNAVAILABLE",
		"SOURCE_OBJECT_MISSING_OR_MISMATCHED", "SOURCE_OBJECT_MISMATCH", "SOURCE_CHECKSUM_MISMATCH",
		"AUDIO_EXTRACTION_FAILED":
		return true
	default:
		return false
	}
}

func (r *StageRepository) MarkAudioBlobBroken(ctx context.Context, task StageTask, blobID uuid.UUID) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin mark reusable audio broken: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE blobs SET state='BROKEN'
		WHERE id=$1 AND video_id=$2 AND kind='AUDIO' AND state='READY'`, blobID, task.VideoID)
	if err != nil {
		return fmt.Errorf("mark reusable audio artifact broken: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("reusable audio artifact changed before it could be marked broken")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reusable audio broken state: %w", err)
	}
	return nil
}

func (r *StageRepository) MarkSourceBlobBroken(ctx context.Context, task StageTask, blobID uuid.UUID) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin mark source broken: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE blobs SET state='BROKEN'
		WHERE id=$1 AND video_id=$2 AND kind='SOURCE_VIDEO' AND state IN ('STORED','READY')`, blobID, task.VideoID)
	if err != nil {
		return fmt.Errorf("mark source object broken: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("source object changed before it could be marked broken")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source broken state: %w", err)
	}
	return nil
}

func lockStageTask(ctx context.Context, tx pgx.Tx, task StageTask) error {
	var locked uuid.UUID
	// Stage commits need to serialize with deletion and publication for this
	// video, but must not serialize unrelated videos owned by the same user.
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE' FOR UPDATE`,
		task.VideoID, task.OwnerID).Scan(&locked); err != nil {
		return fmt.Errorf("lock parse task video: %w", err)
	}
	var status, stage, leaseOwner string
	var epoch int64
	var validLease bool
	err := tx.QueryRow(ctx, `SELECT status,current_stage,COALESCE(lease_owner,''),run_epoch,
		lease_until>clock_timestamp() AND deadline_at>clock_timestamp()
		FROM parse_tasks WHERE id=$1 AND video_id=$2 AND owner_id=$3 FOR UPDATE`,
		task.ID, task.VideoID, task.OwnerID).Scan(&status, &stage, &leaseOwner, &epoch, &validLease)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStageLeaseLost
	}
	if err != nil {
		return fmt.Errorf("lock parse task lease: %w", err)
	}
	if status != "RUNNING" || stage != task.Stage || leaseOwner != task.LeaseOwner || epoch != task.RunEpoch || !validLease {
		return ErrStageLeaseLost
	}
	return nil
}

func advanceStage(ctx context.Context, tx pgx.Tx, task StageTask, nextStage string) error {
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET current_stage=$4,status='READY',lease_owner=NULL,lease_until=NULL,
		wait_reason=NULL,last_error=NULL WHERE id=$1 AND video_id=$2 AND run_epoch=$3 AND status='RUNNING'
		AND lease_owner=$5 AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		task.ID, task.VideoID, task.RunEpoch, nextStage, task.LeaseOwner)
	if err != nil {
		return fmt.Errorf("advance parse task stage: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrStageLeaseLost
	}
	return nil
}

func boundedError(message string) string {
	if len(message) > 512 {
		return message[:512]
	}
	return message
}

func HashJSON(value any) ([]byte, json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	hash := sha256.Sum256(encoded)
	return hash[:], encoded, nil
}

func HexHash(value []byte) string { return hex.EncodeToString(value) }
