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
)

const (
	parseTopic      = "mirrorcore.parse.requested.v1"
	parseEventType  = "parse.requested"
	retryAllowedSQL = `COALESCE(t.status='FAILED' AND t.manual_retry_count=0 AND t.retry_request_key IS NULL
		AND v.lifecycle='ACTIVE' AND v.file_state='VALID' AND source.state='READY'
		AND (t.last_error->>'code'='TASK_DEADLINE_EXCEEDED' OR EXISTS(
			SELECT 1 FROM task_steps step WHERE step.task_id=t.id AND step.state='FAILED'
				AND step.last_error->>'retryable'='true')),false)`
)

type ResourceRepository struct{ pool *pgxpool.Pool }

func NewResourceRepository(pool *pgxpool.Pool) *ResourceRepository {
	return &ResourceRepository{pool: pool}
}

func (r *ResourceRepository) CreateParseTask(ctx context.Context, record application.CreateParseTaskRecord) (application.ParseTask, bool, error) {
	input := record.Input
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("begin parse task transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var lockedUser uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id = $1 FOR UPDATE`, input.OwnerID).Scan(&lockedUser); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return application.ParseTask{}, false, &application.ServiceError{Status: 401, Code: "UNAUTHENTICATED", Message: "请先登录"}
		}
		return application.ParseTask{}, false, fmt.Errorf("lock user quota row: %w", err)
	}

	var existing application.ParseTask
	var existingHash []byte
	err = tx.QueryRow(ctx, `SELECT id, video_id, task_version, status, current_stage, next_run_at, created_at, completed_at, request_hash
		FROM parse_tasks WHERE owner_id = $1 AND create_request_key = $2`, input.OwnerID, input.RequestKey).
		Scan(&existing.ID, &existing.VideoID, &existing.TaskVersion, &existing.Status, &existing.Stage,
			&existing.NextRunAt, &existing.CreatedAt, &existing.CompletedAt, &existingHash)
	if err == nil {
		if string(existingHash) != string(record.RequestHash) || existing.VideoID != input.VideoID {
			return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "IDEMPOTENCY_KEY_REUSED", Message: "该请求键已经用于不同的解析请求"}
		}
		if err := tx.Commit(ctx); err != nil {
			return application.ParseTask{}, false, fmt.Errorf("finish idempotent replay: %w", err)
		}
		return existing, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, false, fmt.Errorf("check parse task idempotency: %w", err)
	}

	var activeTasks, reservedUploads int
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM parse_tasks WHERE owner_id = $1 AND status IN ('WAIT_DISPATCH','READY','RUNNING','WAITING','RETRY_WAIT')),
		(SELECT count(*) FROM upload_sessions WHERE owner_id = $1 AND parse_slot_reserved AND expires_at > clock_timestamp()
		 AND status NOT IN ('COMPLETE','ABORTED','EXPIRED'))`, input.OwnerID).Scan(&activeTasks, &reservedUploads); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("count active parse slots: %w", err)
	}
	if activeTasks+reservedUploads >= 2 {
		return application.ParseTask{}, false, &application.ServiceError{Status: 429, Code: "PARSE_QUOTA_EXCEEDED", Message: "当前解析任务较多，请稍后重试", Retryable: true, RetryAfterMS: 30000}
	}

	var fileState, lifecycle string
	var nextVersion int32
	err = tx.QueryRow(ctx, `SELECT file_state, lifecycle, next_task_version FROM videos
		WHERE id = $1 AND owner_id = $2 FOR UPDATE`, input.VideoID, input.OwnerID).
		Scan(&fileState, &lifecycle, &nextVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, false, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "视频不存在"}
	}
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("lock video: %w", err)
	}
	if lifecycle != "ACTIVE" || (fileState != "UPLOADED" && fileState != "VALID") {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "VIDEO_NOT_READY", Message: "视频尚未完成上传或当前不可处理"}
	}
	if nextVersion <= 0 {
		return application.ParseTask{}, false, fmt.Errorf("invalid next task version %d", nextVersion)
	}

	taskID := uuid.New()
	eventID := uuid.New()
	var task application.ParseTask
	err = tx.QueryRow(ctx, `INSERT INTO parse_tasks(
		id, video_id, owner_id, task_version, create_request_key, request_hash, config_snapshot,
		status, current_stage, deadline_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, 'WAIT_DISPATCH', 'PROBE', clock_timestamp() + interval '6 hours'
	) RETURNING id, video_id, task_version, status, current_stage, next_run_at, created_at, completed_at, deadline_at`,
		taskID, input.VideoID, input.OwnerID, nextVersion, input.RequestKey, record.RequestHash, record.ConfigSnapshot).
		Scan(&task.ID, &task.VideoID, &task.TaskVersion, &task.Status, &task.Stage, &task.NextRunAt, &task.CreatedAt,
			&task.CompletedAt, &task.DeadlineAt)
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("insert parse task: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE videos SET next_task_version = next_task_version + 1, latest_task_id = $1 WHERE id = $2`, task.ID, input.VideoID); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("advance video task version: %w", err)
	}
	eventPayload, err := jsonMarshalParseEvent(eventID, task, input.TraceID)
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("encode parse event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(
		event_id, aggregate_id, event_type, schema_version, topic, event_key, payload, status
	) VALUES ($1,$2,$3,1,$4,$5,$6,'PENDING')`, eventID, task.ID, parseEventType, parseTopic, task.VideoID.String(), eventPayload); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("insert parse outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("commit parse task: %w", err)
	}
	return task, false, nil
}

func jsonMarshalParseEvent(eventID uuid.UUID, task application.ParseTask, traceID string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"event_id": eventID, "event_type": parseEventType, "schema_version": 1,
		"task_id": task.ID, "video_id": task.VideoID, "task_version": task.TaskVersion,
		"occurred_at": task.CreatedAt.UTC().Format(time.RFC3339Nano), "trace_id": traceID,
	})
}

func (r *ResourceRepository) ListVideos(ctx context.Context, ownerID uuid.UUID, limit int, cursorCreatedAt *time.Time, cursorID *uuid.UUID) (application.Page, error) {
	query := `SELECT v.id, v.file_state, v.lifecycle, v.source_name, v.source_size_bytes, v.active_result_id,
		v.created_at, t.id, t.task_version, t.status, t.current_stage, t.next_run_at, t.created_at, t.completed_at, ` + retryAllowedSQL + `
		FROM videos v LEFT JOIN parse_tasks t ON t.id = v.latest_task_id
		LEFT JOIN blobs source ON source.id=v.source_blob_id AND source.video_id=v.id AND source.kind='SOURCE_VIDEO'
		WHERE v.owner_id = $1 AND v.lifecycle = 'ACTIVE'`
	args := []any{ownerID}
	if cursorCreatedAt != nil && cursorID != nil {
		query += ` AND (v.created_at, v.id) < ($2, $3)`
		args = append(args, *cursorCreatedAt, *cursorID)
	}
	query += fmt.Sprintf(` ORDER BY v.created_at DESC, v.id DESC LIMIT $%d`, len(args)+1)
	args = append(args, limit+1)
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return application.Page{}, fmt.Errorf("list videos: %w", err)
	}
	defer rows.Close()
	page := application.Page{Videos: make([]application.Video, 0, limit)}
	for rows.Next() {
		video, err := scanVideo(rows)
		if err != nil {
			return application.Page{}, fmt.Errorf("scan video list: %w", err)
		}
		page.Videos = append(page.Videos, video)
	}
	if err := rows.Err(); err != nil {
		return application.Page{}, fmt.Errorf("iterate videos: %w", err)
	}
	if len(page.Videos) > limit {
		page.Videos = page.Videos[:limit]
		last := page.Videos[len(page.Videos)-1]
		page.NextCursor = application.EncodeCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

func (r *ResourceRepository) GetVideo(ctx context.Context, ownerID, videoID uuid.UUID) (application.Video, error) {
	row := r.pool.QueryRow(ctx, `SELECT v.id, v.file_state, v.lifecycle, v.source_name, v.source_size_bytes, v.active_result_id,
		v.created_at, t.id, t.task_version, t.status, t.current_stage, t.next_run_at, t.created_at, t.completed_at, `+retryAllowedSQL+`
		FROM videos v LEFT JOIN parse_tasks t ON t.id = v.latest_task_id
		LEFT JOIN blobs source ON source.id=v.source_blob_id AND source.video_id=v.id AND source.kind='SOURCE_VIDEO'
		WHERE v.id = $1 AND v.owner_id = $2 AND v.lifecycle = 'ACTIVE'`, videoID, ownerID)
	video, err := scanVideo(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.Video{}, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "视频不存在"}
	}
	if err != nil {
		return application.Video{}, fmt.Errorf("get video: %w", err)
	}
	return video, nil
}

func scanVideo(row interface{ Scan(...any) error }) (application.Video, error) {
	var video application.Video
	var activeResult *uuid.UUID
	var taskID *uuid.UUID
	var taskVersion *int32
	var taskStatus, taskStage *string
	var taskNextRun, taskCreated *time.Time
	var taskCompleted *time.Time
	var taskRetryAllowed bool
	err := row.Scan(&video.ID, &video.FileState, &video.Lifecycle, &video.SourceName, &video.SourceSize,
		&activeResult, &video.CreatedAt, &taskID, &taskVersion, &taskStatus, &taskStage, &taskNextRun, &taskCreated, &taskCompleted,
		&taskRetryAllowed)
	if err != nil {
		return application.Video{}, err
	}
	video.ActiveResultID = activeResult
	if taskID != nil {
		video.LatestTask = &application.ParseTask{ID: *taskID, VideoID: video.ID, TaskVersion: *taskVersion,
			Status: *taskStatus, Stage: *taskStage, NextRunAt: *taskNextRun, CreatedAt: *taskCreated, CompletedAt: taskCompleted,
			RetryAllowed: taskRetryAllowed}
	}
	return video, nil
}

func (r *ResourceRepository) GetParseTask(ctx context.Context, ownerID, taskID uuid.UUID) (application.ParseTask, error) {
	return loadParseTask(ctx, r.pool, ownerID, taskID)
}

func (r *ResourceRepository) LookupParseTask(ctx context.Context, ownerID uuid.UUID, requestKey string) (application.ParseTask, error) {
	var taskID uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT id FROM parse_tasks WHERE owner_id=$1 AND create_request_key=$2`,
		ownerID, requestKey).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "解析任务不存在"}
	}
	if err != nil {
		return application.ParseTask{}, fmt.Errorf("lookup parse task: %w", err)
	}
	return loadParseTask(ctx, r.pool, ownerID, taskID)
}

func (r *ResourceRepository) RetryParseTask(ctx context.Context, ownerID, taskID uuid.UUID,
	requestKey string) (application.ParseTask, bool, error) {
	var videoID uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT video_id FROM parse_tasks WHERE id=$1 AND owner_id=$2`, taskID, ownerID).
		Scan(&videoID)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, false, taskNotFound()
	}
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("find task before retry: %w", err)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("begin manual parse retry: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockParseTaskOwnerAndVideo(ctx, tx, ownerID, videoID); err != nil {
		return application.ParseTask{}, false, err
	}
	var status, stage string
	var retryCount int
	var retryRequestKey *string
	var deadlineAt time.Time
	var lastError []byte
	err = tx.QueryRow(ctx, `SELECT status,current_stage,manual_retry_count,retry_request_key,deadline_at,
		COALESCE(last_error,'{}'::jsonb) FROM parse_tasks WHERE id=$1 AND owner_id=$2 AND video_id=$3 FOR UPDATE`,
		taskID, ownerID, videoID).Scan(&status, &stage, &retryCount, &retryRequestKey, &deadlineAt, &lastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, false, taskNotFound()
	}
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("lock parse task for retry: %w", err)
	}
	if retryRequestKey != nil && *retryRequestKey == requestKey {
		task, loadErr := loadParseTask(ctx, tx, ownerID, taskID)
		if loadErr != nil {
			return application.ParseTask{}, false, loadErr
		}
		if err := tx.Commit(ctx); err != nil {
			return application.ParseTask{}, false, fmt.Errorf("commit manual retry replay: %w", err)
		}
		return task, true, nil
	}
	if retryCount >= 1 {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "MANUAL_RETRY_EXHAUSTED", Message: "该解析任务的人工继续次数已用完"}
	}
	if status != "FAILED" {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "TASK_NOT_RETRYABLE", Message: "只有可恢复的失败任务可以人工继续"}
	}
	var lifecycle, fileState string
	var sourceBlobID *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT lifecycle,file_state,source_blob_id FROM videos WHERE id=$1 AND owner_id=$2`,
		videoID, ownerID).Scan(&lifecycle, &fileState, &sourceBlobID); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("read video state for retry: %w", err)
	}
	if lifecycle != "ACTIVE" || fileState != "VALID" || sourceBlobID == nil {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "SOURCE_NOT_VALID", Message: "源视频不可用，不能继续解析"}
	}
	var sourceReady bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM blobs WHERE id=$1 AND video_id=$2
		AND kind='SOURCE_VIDEO' AND state='READY')`, *sourceBlobID, videoID).Scan(&sourceReady); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("verify source blob for retry: %w", err)
	}
	var retryable bool
	if err := tx.QueryRow(ctx, `SELECT
		($2::jsonb->>'code'='TASK_DEADLINE_EXCEEDED') OR EXISTS(
			SELECT 1 FROM task_steps WHERE task_id=$1 AND state='FAILED' AND last_error->>'retryable'='true'
		)`, taskID, string(lastError)).Scan(&retryable); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("check parse retry eligibility: %w", err)
	}
	if !sourceReady {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "SOURCE_NOT_VALID", Message: "源视频产物不可用，不能继续解析"}
	}
	if !retryable {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "TASK_NOT_RETRYABLE", Message: "该失败原因不支持人工继续；请创建新的解析版本"}
	}
	var activeTasks, reservedUploads int
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM parse_tasks WHERE owner_id=$1 AND status IN ('WAIT_DISPATCH','READY','RUNNING','WAITING','RETRY_WAIT')),
		(SELECT count(*) FROM upload_sessions WHERE owner_id=$1 AND parse_slot_reserved AND expires_at>clock_timestamp()
		 AND status NOT IN ('COMPLETE','ABORTED','EXPIRED'))`, ownerID).Scan(&activeTasks, &reservedUploads); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("count active slots for retry: %w", err)
	}
	if activeTasks+reservedUploads >= 2 {
		return application.ParseTask{}, false, &application.ServiceError{Status: 429, Code: "PARSE_QUOTA_EXCEEDED", Message: "当前解析任务较多，请稍后重试", Retryable: true, RetryAfterMS: 30000}
	}
	var stageSnapshot []byte
	if err := tx.QueryRow(ctx, `SELECT COALESCE(jsonb_object_agg(stage,jsonb_build_object(
		'state',state,'failure_count',failure_count,'last_error',last_error)), '{}'::jsonb)
		FROM task_steps WHERE task_id=$1`, taskID).Scan(&stageSnapshot); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("snapshot task stages before manual retry: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO parse_task_retry_history(task_id,retry_no,request_key,previous_stage,
		previous_deadline_at,previous_error,stage_failure_snapshot)
		VALUES($1,1,$2,$3,$4,$5::jsonb,$6::jsonb)`, taskID, requestKey, stage, deadlineAt, string(lastError), string(stageSnapshot)); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("record manual parse retry: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='PENDING',failure_count=0,last_error=NULL,
		completed_at=NULL,updated_at=clock_timestamp()
		WHERE task_id=$1 AND state='FAILED' AND last_error->>'retryable'='true'`, taskID); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("reset retryable stage counters: %w", err)
	}
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='READY',retry_request_key=$2,manual_retry_count=1,
		deadline_at=clock_timestamp()+interval '6 hours',next_run_at=clock_timestamp(),wait_reason=NULL,
		last_error=NULL,lease_owner=NULL,lease_until=NULL,completed_at=NULL
		WHERE id=$1 AND owner_id=$3 AND status='FAILED' AND manual_retry_count=0`, taskID, requestKey, ownerID)
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("schedule manual parse retry: %w", err)
	}
	if result.RowsAffected() != 1 {
		return application.ParseTask{}, false, &application.ServiceError{Status: 409, Code: "TASK_NOT_RETRYABLE", Message: "解析任务状态已经变化，请重新查询"}
	}
	task, err := loadParseTask(ctx, tx, ownerID, taskID)
	if err != nil {
		return application.ParseTask{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("commit manual parse retry: %w", err)
	}
	return task, false, nil
}

func (r *ResourceRepository) CancelParseTask(ctx context.Context, ownerID, taskID uuid.UUID) (application.ParseTask, bool, error) {
	var videoID uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT video_id FROM parse_tasks WHERE id=$1 AND owner_id=$2`, taskID, ownerID).
		Scan(&videoID)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, false, taskNotFound()
	}
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("find task before cancellation: %w", err)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("begin parse task cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockParseTaskOwnerAndVideo(ctx, tx, ownerID, videoID); err != nil {
		return application.ParseTask{}, false, err
	}
	var status, currentStage string
	err = tx.QueryRow(ctx, `SELECT status,current_stage FROM parse_tasks WHERE id=$1 AND owner_id=$2 AND video_id=$3 FOR UPDATE`,
		taskID, ownerID, videoID).Scan(&status, &currentStage)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, false, taskNotFound()
	}
	if err != nil {
		return application.ParseTask{}, false, fmt.Errorf("lock parse task for cancellation: %w", err)
	}
	changed := false
	if status == "WAIT_DISPATCH" || status == "READY" || status == "RUNNING" || status == "WAITING" || status == "RETRY_WAIT" {
		result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='CANCELLED',current_stage='DONE',wait_reason=NULL,
			run_epoch=run_epoch+1,lease_owner=NULL,lease_until=NULL,last_error='{"code":"TASK_CANCELLED","retryable":false}'::jsonb,
			completed_at=clock_timestamp() WHERE id=$1 AND owner_id=$2 AND video_id=$3 AND status=$4`,
			taskID, ownerID, videoID, status)
		if err != nil {
			return application.ParseTask{}, false, fmt.Errorf("cancel parse task: %w", err)
		}
		changed = result.RowsAffected() == 1
		if changed {
			if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='CANCELLED',
				last_error='{"code":"TASK_CANCELLED","retryable":false}'::jsonb,
				completed_at=COALESCE(completed_at,clock_timestamp()),updated_at=clock_timestamp()
				WHERE task_id=$1 AND video_id=$2 AND stage=$3 AND state IN ('PENDING','RUNNING')`,
				taskID, videoID, currentStage); err != nil {
				return application.ParseTask{}, false, fmt.Errorf("cancel active parse stage: %w", err)
			}
		}
	}
	task, err := loadParseTask(ctx, tx, ownerID, taskID)
	if err != nil {
		return application.ParseTask{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return application.ParseTask{}, false, fmt.Errorf("commit parse task cancellation: %w", err)
	}
	return task, changed, nil
}

type parseTaskQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadParseTask(ctx context.Context, queryer parseTaskQueryer, ownerID, taskID uuid.UUID) (application.ParseTask, error) {
	var task application.ParseTask
	var errorCode *string
	err := queryer.QueryRow(ctx, `SELECT t.id,t.video_id,t.task_version,t.status,t.current_stage,COALESCE(t.wait_reason,''),
		t.next_run_at,t.created_at,t.completed_at,t.deadline_at, `+retryAllowedSQL+`,
		t.last_error->>'code'
		FROM parse_tasks t JOIN videos v ON v.id=t.video_id
		LEFT JOIN blobs source ON source.id=v.source_blob_id AND source.video_id=v.id AND source.kind='SOURCE_VIDEO'
		WHERE t.id=$1 AND t.owner_id=$2`, taskID, ownerID).Scan(&task.ID, &task.VideoID, &task.TaskVersion,
		&task.Status, &task.Stage, &task.WaitReason, &task.NextRunAt, &task.CreatedAt, &task.CompletedAt,
		&task.DeadlineAt, &task.RetryAllowed, &errorCode)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ParseTask{}, taskNotFound()
	}
	if err != nil {
		return application.ParseTask{}, fmt.Errorf("load parse task: %w", err)
	}
	if errorCode != nil && *errorCode != "" {
		task.LastError = &application.ParseTaskError{Code: *errorCode, Retryable: task.RetryAllowed}
	}
	rows, err := queryer.Query(ctx, `SELECT s.stage,s.state,s.failure_count,
		CASE s.stage
		 WHEN 'TRANSCRIPT' THEN CASE WHEN s.output_transcript_id IS NOT NULL THEN
			COALESCE((SELECT last_committed_seq FROM transcripts WHERE id=s.output_transcript_id),0)
		 ELSE COALESCE((SELECT last_committed_seq FROM transcripts WHERE created_by_task=s.task_id AND state='BUILDING'
			ORDER BY updated_at DESC LIMIT 1),0) END
		 WHEN 'CHUNK' THEN CASE WHEN s.output_chunk_set_id IS NOT NULL THEN
			(SELECT count(*) FROM chunks WHERE chunk_set_id=s.output_chunk_set_id)
		 ELSE COALESCE((SELECT count(*) FROM chunks c JOIN chunk_sets cs ON cs.id=c.chunk_set_id
			WHERE cs.created_by_task=s.task_id AND cs.state='BUILDING'),0) END
		 WHEN 'EMBEDDING' THEN CASE WHEN s.output_embedding_set_id IS NOT NULL THEN
			(SELECT count(*) FROM chunk_embeddings WHERE embedding_set_id=s.output_embedding_set_id)
		 ELSE COALESCE((SELECT count(*) FROM chunk_embeddings e JOIN embedding_sets es ON es.id=e.embedding_set_id
			WHERE es.created_by_task=s.task_id AND es.state='BUILDING'),0) END
		 ELSE CASE WHEN s.state IN ('READY','NO_AUDIO','NO_SPEECH','NO_CONTENT') THEN 1 ELSE 0 END END AS committed_count,
		CASE s.stage
		 WHEN 'TRANSCRIPT' THEN CASE WHEN s.output_transcript_id IS NOT NULL THEN
			COALESCE((SELECT expected_segment_count FROM transcripts WHERE id=s.output_transcript_id),0)
		 ELSE COALESCE((SELECT expected_segment_count FROM transcripts WHERE created_by_task=s.task_id AND state='BUILDING'
			ORDER BY updated_at DESC LIMIT 1),0) END
		 WHEN 'CHUNK' THEN CASE WHEN s.output_chunk_set_id IS NOT NULL THEN
			COALESCE((SELECT expected_chunk_count FROM chunk_sets WHERE id=s.output_chunk_set_id),0)
		 ELSE COALESCE((SELECT expected_chunk_count FROM chunk_sets WHERE created_by_task=s.task_id AND state='BUILDING'
			ORDER BY updated_at DESC LIMIT 1),0) END
		 WHEN 'EMBEDDING' THEN CASE WHEN s.output_embedding_set_id IS NOT NULL THEN
			COALESCE((SELECT expected_embedding_count FROM embedding_sets WHERE id=s.output_embedding_set_id),0)
		 ELSE COALESCE((SELECT expected_embedding_count FROM embedding_sets WHERE created_by_task=s.task_id AND state='BUILDING'
			ORDER BY updated_at DESC LIMIT 1),0) END
		 ELSE 1 END AS expected_count
		FROM task_steps s WHERE s.task_id=$1 ORDER BY s.stage`, taskID)
	if err != nil {
		return application.ParseTask{}, fmt.Errorf("load parse task progress: %w", err)
	}
	task.StageProgress = make(map[string]application.ParseTaskStageProgress)
	defer rows.Close()
	for rows.Next() {
		var stage string
		var progress application.ParseTaskStageProgress
		if err := rows.Scan(&stage, &progress.State, &progress.FailureCount, &progress.CommittedCount, &progress.ExpectedCount); err != nil {
			return application.ParseTask{}, fmt.Errorf("scan parse task progress: %w", err)
		}
		task.StageProgress[stage] = progress
	}
	if err := rows.Err(); err != nil {
		return application.ParseTask{}, fmt.Errorf("iterate parse task progress: %w", err)
	}
	return task, nil
}

func lockParseTaskOwnerAndVideo(ctx context.Context, tx pgx.Tx, ownerID, videoID uuid.UUID) error {
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, ownerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return taskNotFound()
	} else if err != nil {
		return fmt.Errorf("lock parse task owner: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, videoID, ownerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return taskNotFound()
	} else if err != nil {
		return fmt.Errorf("lock parse task video: %w", err)
	}
	return nil
}

func taskNotFound() error {
	return &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "解析任务不存在"}
}
