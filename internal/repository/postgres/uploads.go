package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/storage/ossstore"
)

type UploadRepository struct {
	pool   *pgxpool.Pool
	bucket string
}

func NewUploadRepository(pool *pgxpool.Pool, bucket string) *UploadRepository {
	return &UploadRepository{pool: pool, bucket: bucket}
}

func (r *UploadRepository) CreateUpload(ctx context.Context, record application.UploadRecord) (application.UploadSession, bool, error) {
	input := record.Input
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.UploadSession{}, false, fmt.Errorf("begin upload creation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var lockedUser uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, input.OwnerID).Scan(&lockedUser); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return application.UploadSession{}, false, &application.ServiceError{Status: 401, Code: "UNAUTHENTICATED", Message: "请先登录"}
		}
		return application.UploadSession{}, false, fmt.Errorf("lock user upload quota: %w", err)
	}

	var existingID uuid.UUID
	var existingHash []byte
	err = tx.QueryRow(ctx, `SELECT id,request_hash FROM upload_sessions WHERE owner_id=$1 AND create_request_key=$2`, input.OwnerID, input.RequestKey).
		Scan(&existingID, &existingHash)
	if err == nil {
		if !equalBytes(existingHash, record.RequestHash) {
			return application.UploadSession{}, false, &application.ServiceError{Status: 409, Code: "IDEMPOTENCY_KEY_REUSED", Message: "该请求键已经用于不同的上传请求"}
		}
		session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2`, existingID, input.OwnerID))
		if err != nil {
			return application.UploadSession{}, false, fmt.Errorf("load existing upload: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return application.UploadSession{}, false, fmt.Errorf("finish idempotent upload replay: %w", err)
		}
		return session, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return application.UploadSession{}, false, fmt.Errorf("check upload idempotency: %w", err)
	}

	var activeTasks, reservedUploads int
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM parse_tasks WHERE owner_id=$1 AND status IN ('WAIT_DISPATCH','READY','RUNNING','WAITING','RETRY_WAIT')),
		(SELECT count(*) FROM upload_sessions WHERE owner_id=$1 AND parse_slot_reserved
		 AND expires_at>clock_timestamp() AND status NOT IN ('COMPLETE','ABORTED','EXPIRED'))`, input.OwnerID).
		Scan(&activeTasks, &reservedUploads); err != nil {
		return application.UploadSession{}, false, fmt.Errorf("count upload reservation slots: %w", err)
	}
	if activeTasks+reservedUploads >= 2 {
		return application.UploadSession{}, false, &application.ServiceError{Status: 429, Code: "PARSE_QUOTA_EXCEEDED", Message: "当前解析任务较多，请稍后重试", Retryable: true, RetryAfterMS: 30000}
	}

	if _, err := tx.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES ($1,$2,'UPLOADING',$3,$4)`, record.VideoID, input.OwnerID, input.SourceName, input.SizeBytes); err != nil {
		return application.UploadSession{}, false, fmt.Errorf("insert upload video: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO upload_sessions(
		id,video_id,owner_id,create_request_key,request_hash,object_key,status,operation_epoch,
		operation_lease_until,expected_size_bytes,expires_at,part_size_bytes,declared_content_type
	) VALUES ($1,$2,$3,$4,$5,$6,'INITIATING',1,clock_timestamp()+interval '2 minutes',
		$7,clock_timestamp()+interval '24 hours',$8,$9)`, record.SessionID, record.VideoID, input.OwnerID,
		input.RequestKey, record.RequestHash, record.ObjectKey, input.SizeBytes, application.UploadPartSize, input.ContentType); err != nil {
		return application.UploadSession{}, false, fmt.Errorf("insert upload session: %w", err)
	}
	session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2`, record.SessionID, input.OwnerID))
	if err != nil {
		return application.UploadSession{}, false, fmt.Errorf("load created upload session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return application.UploadSession{}, false, fmt.Errorf("commit upload creation: %w", err)
	}
	return session, true, nil
}

func (r *UploadRepository) ClaimInitiation(ctx context.Context, ownerID, sessionID uuid.UUID) (application.UploadSession, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.UploadSession{}, false, fmt.Errorf("begin upload init claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockUploadOwner(ctx, tx, ownerID, sessionID); err != nil {
		return application.UploadSession{}, false, err
	}
	session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2 FOR UPDATE OF s`, sessionID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return application.UploadSession{}, false, uploadNotFound()
	}
	if err != nil {
		return application.UploadSession{}, false, fmt.Errorf("lock upload session for initialization: %w", err)
	}
	var claimed bool
	if session.Status == "INITIATING" {
		if session.TerminationReason != "" {
			if err := tx.Commit(ctx); err != nil {
				return application.UploadSession{}, false, fmt.Errorf("finish cancelled upload init read: %w", err)
			}
			return session, false, nil
		}
		err = tx.QueryRow(ctx, `UPDATE upload_sessions SET operation_epoch=operation_epoch+1,
			operation_lease_until=clock_timestamp()+interval '2 minutes'
			WHERE id=$1 AND owner_id=$2 AND status='INITIATING'
			  AND (operation_lease_until IS NULL OR operation_lease_until<=clock_timestamp())
		RETURNING operation_epoch,operation_lease_until`, sessionID, ownerID).
			Scan(&session.OperationEpoch, &session.OperationLeaseUntil)
		if err == nil {
			claimed = true
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return application.UploadSession{}, false, fmt.Errorf("claim upload initialization: %w", err)
		}
	} else if session.Status != "UPLOADING" {
		return application.UploadSession{}, false, uploadStateConflict(session.Status)
	}
	if err := tx.Commit(ctx); err != nil {
		return application.UploadSession{}, false, fmt.Errorf("commit upload init claim: %w", err)
	}
	return session, claimed, nil
}

func (r *UploadRepository) MarkInitiated(ctx context.Context, sessionID uuid.UUID, epoch int64, uploadID string) error {
	result, err := r.pool.Exec(ctx, `UPDATE upload_sessions SET status='UPLOADING',upload_id=$3,operation_lease_until=NULL
		WHERE id=$1 AND operation_epoch=$2 AND status='INITIATING'
		  AND operation_lease_until>clock_timestamp()`, sessionID, epoch, uploadID)
	if err != nil {
		return fmt.Errorf("persist OSS upload ID: %w", err)
	}
	if result.RowsAffected() != 1 {
		return &application.ServiceError{Status: 409, Code: "UPLOAD_OPERATION_LOST", Message: "上传初始化执行权已失效，请查询上传会话"}
	}
	return nil
}

func (r *UploadRepository) GetUpload(ctx context.Context, ownerID, sessionID uuid.UUID) (application.UploadSession, error) {
	session, err := scanUpload(r.pool.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2 AND v.lifecycle='ACTIVE'`, sessionID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return application.UploadSession{}, uploadNotFound()
	}
	if err != nil {
		return application.UploadSession{}, fmt.Errorf("get upload session: %w", err)
	}
	return session, nil
}

func (r *UploadRepository) CanIssuePartURLs(ctx context.Context, ownerID, sessionID uuid.UUID) (bool, error) {
	var canIssue bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM upload_sessions s JOIN videos v ON v.id=s.video_id
		WHERE s.id=$1 AND s.owner_id=$2 AND s.status='UPLOADING'
		  AND s.termination_reason IS NULL AND s.expires_at>clock_timestamp()
		  AND v.lifecycle='ACTIVE')`, sessionID, ownerID).Scan(&canIssue)
	if err != nil {
		return false, fmt.Errorf("check upload URL eligibility: %w", err)
	}
	return canIssue, nil
}

func (r *UploadRepository) GetUploadByRequestKey(ctx context.Context, ownerID uuid.UUID, requestKey string) (application.UploadSession, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT id FROM upload_sessions WHERE owner_id=$1 AND create_request_key=$2`, ownerID, requestKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.UploadSession{}, uploadNotFound()
	}
	if err != nil {
		return application.UploadSession{}, fmt.Errorf("lookup upload session: %w", err)
	}
	return r.GetUpload(ctx, ownerID, id)
}

func (r *UploadRepository) ClaimCompletion(ctx context.Context, ownerID, sessionID uuid.UUID) (application.UploadSession, bool, *application.UploadTask, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.UploadSession{}, false, nil, fmt.Errorf("begin upload completion claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockUploadOwner(ctx, tx, ownerID, sessionID); err != nil {
		return application.UploadSession{}, false, nil, err
	}
	session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2 FOR UPDATE OF s`, sessionID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return application.UploadSession{}, false, nil, uploadNotFound()
	}
	if err != nil {
		return application.UploadSession{}, false, nil, fmt.Errorf("lock upload session for completion: %w", err)
	}
	if session.Status == "COMPLETE" && session.InitialTaskID != nil {
		task, err := getInitialUploadTask(ctx, tx, *session.InitialTaskID)
		if err != nil {
			return application.UploadSession{}, false, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return application.UploadSession{}, false, nil, fmt.Errorf("finish completed upload replay: %w", err)
		}
		return session, false, &task, nil
	}
	if session.Status == "UPLOADING" {
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT expires_at<=clock_timestamp() FROM upload_sessions WHERE id=$1`, sessionID).Scan(&expired); err != nil {
			return application.UploadSession{}, false, nil, fmt.Errorf("check upload expiration: %w", err)
		}
		if expired {
			if _, err := tx.Exec(ctx, `UPDATE upload_sessions SET status='EXPIRED',parse_slot_reserved=false
				WHERE id=$1 AND status='UPLOADING' AND expires_at<=clock_timestamp()`, sessionID); err != nil {
				return application.UploadSession{}, false, nil, fmt.Errorf("expire upload reservation: %w", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE videos SET file_state='EXPIRED' WHERE id=$1 AND owner_id=$2 AND file_state='UPLOADING'`, session.VideoID, ownerID); err != nil {
				return application.UploadSession{}, false, nil, fmt.Errorf("expire upload video: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return application.UploadSession{}, false, nil, fmt.Errorf("commit expired upload: %w", err)
			}
			return application.UploadSession{}, false, nil, &application.ServiceError{Status: 409, Code: "UPLOAD_EXPIRED", Message: "上传会话已过期"}
		}
		err = tx.QueryRow(ctx, `UPDATE upload_sessions SET status='COMPLETING',operation_epoch=operation_epoch+1,
			operation_lease_until=clock_timestamp()+interval '2 minutes'
			WHERE id=$1 AND owner_id=$2 AND status='UPLOADING' AND expires_at>clock_timestamp()
		RETURNING operation_epoch,operation_lease_until`, sessionID, ownerID).
			Scan(&session.OperationEpoch, &session.OperationLeaseUntil)
		if errors.Is(err, pgx.ErrNoRows) {
			return application.UploadSession{}, false, nil, &application.ServiceError{Status: 409, Code: "UPLOAD_EXPIRED", Message: "上传会话已过期"}
		}
		if err != nil {
			return application.UploadSession{}, false, nil, fmt.Errorf("claim upload completion: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return application.UploadSession{}, false, nil, fmt.Errorf("commit upload completion claim: %w", err)
		}
		return session, true, nil, nil
	}
	if session.Status == "COMPLETING" || session.Status == "VERIFYING" {
		err = tx.QueryRow(ctx, `UPDATE upload_sessions SET status='VERIFYING',operation_epoch=operation_epoch+1,
			operation_lease_until=clock_timestamp()+interval '2 minutes'
			WHERE id=$1 AND owner_id=$2 AND status IN ('COMPLETING','VERIFYING')
			  AND (operation_lease_until IS NULL OR operation_lease_until<=clock_timestamp())
		RETURNING operation_epoch,operation_lease_until`, sessionID, ownerID).
			Scan(&session.OperationEpoch, &session.OperationLeaseUntil)
		if err == nil {
			if err := tx.Commit(ctx); err != nil {
				return application.UploadSession{}, false, nil, fmt.Errorf("commit upload verification claim: %w", err)
			}
			return session, true, nil, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return application.UploadSession{}, false, nil, fmt.Errorf("claim upload verification: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return application.UploadSession{}, false, nil, fmt.Errorf("finish active upload verification read: %w", err)
		}
		return session, false, nil, nil
	}
	return application.UploadSession{}, false, nil, uploadStateConflict(session.Status)
}

func (r *UploadRepository) RestoreUploading(ctx context.Context, sessionID uuid.UUID, epoch int64) error {
	result, err := r.pool.Exec(ctx, `UPDATE upload_sessions SET status='UPLOADING',operation_lease_until=NULL
		WHERE id=$1 AND status IN ('COMPLETING','VERIFYING') AND operation_epoch=$2
		  AND operation_lease_until>clock_timestamp()`, sessionID, epoch)
	if err != nil {
		return fmt.Errorf("restore upload session: %w", err)
	}
	if result.RowsAffected() != 1 {
		return &application.ServiceError{Status: 409, Code: "UPLOAD_OPERATION_LOST", Message: "上传完成执行权已失效，请查询上传会话"}
	}
	return nil
}

func (r *UploadRepository) MarkVerifying(ctx context.Context, sessionID uuid.UUID, epoch int64) error {
	result, err := r.pool.Exec(ctx, `UPDATE upload_sessions SET status='VERIFYING',operation_lease_until=NULL
		WHERE id=$1 AND status IN ('COMPLETING','VERIFYING') AND operation_epoch=$2
		  AND operation_lease_until>clock_timestamp()`, sessionID, epoch)
	if err != nil {
		return fmt.Errorf("mark upload for verification: %w", err)
	}
	if result.RowsAffected() != 1 {
		return &application.ServiceError{Status: 409, Code: "UPLOAD_OPERATION_LOST", Message: "上传核验执行权已失效，请查询上传会话"}
	}
	return nil
}

func (r *UploadRepository) FinalizeUpload(ctx context.Context, sessionID, ownerID uuid.UUID, epoch int64, info ossstore.ObjectInfo) (application.UploadTask, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("begin upload finalization: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockUploadOwner(ctx, tx, ownerID, sessionID); err != nil {
		return application.UploadTask{}, err
	}
	session, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE s.id=$1 AND s.owner_id=$2 FOR UPDATE OF s`, sessionID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return application.UploadTask{}, uploadNotFound()
	}
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("lock upload session for finalization: %w", err)
	}
	if session.Status == "COMPLETE" && session.InitialTaskID != nil {
		task, err := getInitialUploadTask(ctx, tx, *session.InitialTaskID)
		if err != nil {
			return application.UploadTask{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return application.UploadTask{}, fmt.Errorf("finish upload finalization replay: %w", err)
		}
		return task, nil
	}
	if session.OperationEpoch != epoch || (session.Status != "COMPLETING" && session.Status != "VERIFYING") || session.OperationLeaseUntil == nil {
		return application.UploadTask{}, &application.ServiceError{Status: 409, Code: "UPLOAD_OPERATION_LOST", Message: "上传完成执行权已失效，请查询上传会话"}
	}

	var lifecycle, fileState string
	var nextVersion int32
	if err := tx.QueryRow(ctx, `SELECT lifecycle,file_state,next_task_version FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, session.VideoID, ownerID).
		Scan(&lifecycle, &fileState, &nextVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return application.UploadTask{}, uploadNotFound()
		}
		return application.UploadTask{}, fmt.Errorf("lock video for upload finalization: %w", err)
	}
	if lifecycle != "ACTIVE" || fileState != "UPLOADING" || nextVersion <= 0 {
		return application.UploadTask{}, &application.ServiceError{Status: 409, Code: "VIDEO_NOT_UPLOADABLE", Message: "视频当前不可完成上传"}
	}

	taskID, eventID, blobID := uuid.New(), uuid.New(), uuid.New()
	configSnapshot, err := json.Marshal(map[string]any{
		"profile_id": "voice-v1", "reuse_mode": "reuse_ready", "asr_provider": "aliyun-bailian",
		"asr_model": "fun-asr", "asr_region": "cn-beijing", "embedding_model": "text-embedding-v4",
		"embedding_dimensions": 1024, "generation_model": "qwen3.7-plus-2026-05-26", "enable_thinking": false,
	})
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("encode initial task config: %w", err)
	}
	var task application.UploadTask
	var createdAt time.Time
	err = tx.QueryRow(ctx, `INSERT INTO parse_tasks(
		id,video_id,owner_id,task_version,create_request_key,request_hash,config_snapshot,status,current_stage,deadline_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,'WAIT_DISPATCH','PROBE',clock_timestamp()+interval '6 hours')
	RETURNING id,video_id,task_version,status,current_stage,created_at`, taskID, session.VideoID, ownerID,
		nextVersion, "upload:"+session.ID.String(), session.RequestHash, configSnapshot).
		Scan(&task.ID, &task.VideoID, &task.TaskVersion, &task.Status, &task.Stage, &createdAt)
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("create initial parse task: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,object_version,size_bytes,state,created_by_task)
		VALUES ($1,$2,'SOURCE_VIDEO',$3,$4,NULLIF($5,''),$6,'STORED',$7)`, blobID, session.VideoID, r.bucket, session.ObjectKey,
		info.VersionID, info.Size, taskID); err != nil {
		return application.UploadTask{}, fmt.Errorf("record uploaded source blob: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE videos SET source_blob_id=$1,file_state='UPLOADED',next_task_version=next_task_version+1,
		latest_task_id=$2 WHERE id=$3 AND owner_id=$4`, blobID, taskID, session.VideoID, ownerID); err != nil {
		return application.UploadTask{}, fmt.Errorf("attach source blob and initial task: %w", err)
	}
	eventPayload, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": parseEventType, "schema_version": 1,
		"task_id": task.ID, "video_id": task.VideoID, "task_version": task.TaskVersion,
		"occurred_at": createdAt.UTC().Format(time.RFC3339Nano), "trace_id": uuid.NewString(),
	})
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("encode initial parse event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_id,event_type,schema_version,topic,event_key,payload,status)
		VALUES ($1,$2,$3,1,$4,$5,$6,'PENDING')`, eventID, task.ID, parseEventType, parseTopic, task.VideoID.String(), eventPayload); err != nil {
		return application.UploadTask{}, fmt.Errorf("insert initial parse event: %w", err)
	}
	result, err := tx.Exec(ctx, `UPDATE upload_sessions SET status='COMPLETE',parse_slot_reserved=false,
		initial_task_id=$3,operation_lease_until=NULL
		WHERE id=$1 AND owner_id=$2 AND operation_epoch=$4 AND status IN ('COMPLETING','VERIFYING')
		  AND operation_lease_until>clock_timestamp()`, sessionID, ownerID, taskID, epoch)
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("complete upload session: %w", err)
	}
	if result.RowsAffected() != 1 {
		return application.UploadTask{}, &application.ServiceError{Status: 409, Code: "UPLOAD_OPERATION_LOST", Message: "上传完成执行权已失效，请查询上传会话"}
	}
	if err := tx.Commit(ctx); err != nil {
		return application.UploadTask{}, fmt.Errorf("commit upload finalization: %w", err)
	}
	return task, nil
}

const uploadSelect = `SELECT s.id,s.video_id,s.owner_id,s.status,v.source_name,s.expected_size_bytes,
	s.part_size_bytes,s.declared_content_type,s.expires_at,s.initial_task_id,s.object_key,
	COALESCE(s.upload_id,''),s.operation_epoch,s.operation_lease_until,COALESCE(s.termination_reason,''),
	s.termination_after,s.request_hash
	FROM upload_sessions s JOIN videos v ON v.id=s.video_id`

func scanUpload(row interface{ Scan(...any) error }) (application.UploadSession, error) {
	var session application.UploadSession
	err := row.Scan(&session.ID, &session.VideoID, &session.OwnerID, &session.Status, &session.SourceName,
		&session.ExpectedSizeBytes, &session.PartSizeBytes, &session.DeclaredContentType, &session.ExpiresAt,
		&session.InitialTaskID, &session.ObjectKey, &session.UploadID, &session.OperationEpoch,
		&session.OperationLeaseUntil, &session.TerminationReason, &session.TerminationAfter, &session.RequestHash)
	session.CancellationRequested = session.TerminationReason != ""
	return session, err
}

func lockUploadOwner(ctx context.Context, tx pgx.Tx, ownerID, sessionID uuid.UUID) error {
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, ownerID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uploadNotFound()
		}
		return fmt.Errorf("lock upload owner: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=(SELECT video_id FROM upload_sessions WHERE id=$1 AND owner_id=$2)
		AND owner_id=$2 AND lifecycle='ACTIVE' FOR UPDATE`, sessionID, ownerID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uploadNotFound()
		}
		return fmt.Errorf("lock upload video: %w", err)
	}
	return nil
}

func getInitialUploadTask(ctx context.Context, tx pgx.Tx, taskID uuid.UUID) (application.UploadTask, error) {
	var task application.UploadTask
	err := tx.QueryRow(ctx, `SELECT id,video_id,task_version,status,current_stage FROM parse_tasks WHERE id=$1`, taskID).
		Scan(&task.ID, &task.VideoID, &task.TaskVersion, &task.Status, &task.Stage)
	if err != nil {
		return application.UploadTask{}, fmt.Errorf("load initial parse task: %w", err)
	}
	return task, nil
}

func uploadNotFound() error {
	return &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "上传会话不存在"}
}

func uploadStateConflict(status string) error {
	return &application.ServiceError{Status: 409, Code: "UPLOAD_STATE_CONFLICT", Message: "上传会话当前状态不允许此操作"}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
