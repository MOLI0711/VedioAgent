package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
)

const (
	videoDeleteGrace       = 30 * time.Minute
	videoGCBatchSize       = 500
	VideoGCBatchesPerClaim = 25
)

var videoPurgePhases = []string{
	"CLEAR_VIDEO_REFS",
	"PURGE_QUESTION_EVIDENCE",
	"REDACT_QUESTIONS",
	"PURGE_QUESTION_STEPS",
	"REDACT_CONVERSATIONS",
	"REDACT_UPLOADS",
	"REDACT_PARSE_TASKS",
	"PURGE_CONTENT_HEADS",
	"PURGE_CONTENT_SOURCE_REFS",
	"PURGE_CONTENT_ARTIFACTS",
	"PURGE_CONTENT_JOB_BATCHES",
	"PURGE_CONTENT_JOBS",
	"PURGE_TASK_STEPS",
	"PURGE_EMBEDDING_BATCHES",
	"PURGE_ASR_SUBMISSIONS",
	"PURGE_KAFKA_REJECTIONS",
	"PURGE_INBOX_EVENTS",
	"PURGE_OUTBOX_EVENTS",
	"PURGE_PARSE_RESULTS",
	"PURGE_CHUNK_EMBEDDINGS",
	"PURGE_EMBEDDING_SETS",
	"PURGE_CHUNK_SOURCES",
	"PURGE_CHUNKS",
	"PURGE_CHUNK_SETS",
	"PURGE_TRANSCRIPT_SEGMENTS",
	"PURGE_TRANSCRIPTS",
	"PURGE_BLOBS",
	"PURGE_GC_OBJECTS",
	"DISCOVER_OBJECTS_FINAL",
	"DELETE_OBJECTS_FINAL",
	"PURGE_GC_OBJECTS_FINAL",
	"FINALIZE",
}

func (r *ResourceRepository) DeleteVideo(ctx context.Context, ownerID, videoID uuid.UUID) (application.VideoDeletion, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.VideoDeletion{}, fmt.Errorf("begin video deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, ownerID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return application.VideoDeletion{}, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "视频不存在"}
	} else if err != nil {
		return application.VideoDeletion{}, fmt.Errorf("lock video owner for deletion: %w", err)
	}
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT lifecycle FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, videoID, ownerID).Scan(&lifecycle); errors.Is(err, pgx.ErrNoRows) {
		return application.VideoDeletion{}, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "视频不存在"}
	} else if err != nil {
		return application.VideoDeletion{}, fmt.Errorf("lock video for deletion: %w", err)
	}

	var result application.VideoDeletion
	if lifecycle == "ACTIVE" {
		if _, err := tx.Exec(ctx, `UPDATE videos SET lifecycle='DELETING',file_state='CANCELLED',
			active_result_id=NULL,latest_task_id=NULL,probe_metadata='{}'::jsonb
			WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE'`, videoID, ownerID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("hide video and stop new work: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='CANCELLED',current_stage='DONE',wait_reason=NULL,
			lease_owner=NULL,lease_until=NULL,run_epoch=run_epoch+1,completed_at=COALESCE(completed_at,clock_timestamp()),
			last_error='{"code":"VIDEO_DELETED"}'::jsonb
			WHERE video_id=$1 AND status IN ('WAIT_DISPATCH','READY','RUNNING','WAITING','RETRY_WAIT')`, videoID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("cancel video parse tasks: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE task_steps s SET state='CANCELLED',last_error='{"code":"VIDEO_DELETED"}'::jsonb,
			completed_at=COALESCE(s.completed_at,clock_timestamp()),updated_at=clock_timestamp()
			WHERE s.video_id=$1 AND s.state IN ('PENDING','RUNNING') AND EXISTS (
				SELECT 1 FROM parse_tasks t WHERE t.id=s.task_id AND t.video_id=s.video_id
					AND t.status='CANCELLED' AND t.last_error->>'code'='VIDEO_DELETED')`, videoID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("cancel video parse task steps: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE questions SET status='INTERRUPTED',owner_instance=NULL,lease_until=NULL,
			error='{"code":"VIDEO_DELETED"}'::jsonb,completed_at=COALESCE(completed_at,clock_timestamp())
			WHERE video_id=$1 AND status='RUNNING'`, videoID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("interrupt video questions: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE conversations SET archived_at=COALESCE(archived_at,clock_timestamp()),
			updated_at=clock_timestamp() WHERE video_id=$1`, videoID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("archive video conversations: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE content_jobs SET state='CANCELLED',stage='DONE',run_epoch=run_epoch+1,
			lease_owner=NULL,lease_until=NULL,error='{"code":"VIDEO_DELETED"}'::jsonb,
			completed_at=COALESCE(completed_at,clock_timestamp())
			WHERE video_id=$1 AND state IN ('READY','RUNNING','RETRY_WAIT')`, videoID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("cancel video content jobs: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE blobs SET state='DELETING' WHERE video_id=$1 AND state NOT IN ('DELETED','DELETING')`, videoID); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("mark video objects for deletion: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE upload_sessions SET status='ABORTING',termination_reason='CANCEL',
			termination_after=GREATEST(COALESCE(termination_after,'-infinity'::timestamptz),
				clock_timestamp()+($2 * interval '1 millisecond')),
			operation_epoch=operation_epoch+1,operation_lease_until=NULL,parse_slot_reserved=false
			WHERE video_id=$1 AND status IN ('UPLOADING','COMPLETING','VERIFYING','ABORTING')`,
			videoID, videoDeleteGrace.Milliseconds()); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("terminate video upload sessions: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE upload_sessions SET termination_reason=COALESCE(termination_reason,'CANCEL'),
			termination_after=GREATEST(COALESCE(termination_after,'-infinity'::timestamptz),
				clock_timestamp()+($2 * interval '1 millisecond'))
			WHERE video_id=$1 AND status='INITIATING'`, videoID, videoDeleteGrace.Milliseconds()); err != nil {
			return application.VideoDeletion{}, fmt.Errorf("defer initiating upload cleanup: %w", err)
		}
	}

	jobID := uuid.New()
	err = tx.QueryRow(ctx, `INSERT INTO video_deletion_jobs(id,video_id,owner_id,state,phase,next_run_at)
		VALUES($1,$2,$3,'PENDING','WAIT_UPLOADS',clock_timestamp()+interval '24 hours') ON CONFLICT(video_id) DO UPDATE
		SET id=video_deletion_jobs.id
		RETURNING id,video_id,state,phase,created_at`, jobID, videoID, ownerID).
		Scan(&result.DeletionID, &result.VideoID, &result.State, &result.Phase, &result.CreatedAt)
	if err != nil {
		return application.VideoDeletion{}, fmt.Errorf("create or load video deletion job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return application.VideoDeletion{}, fmt.Errorf("commit video deletion request: %w", err)
	}
	return result, nil
}

type VideoDeletionRepository struct{ pool *pgxpool.Pool }

func NewVideoDeletionRepository(pool *pgxpool.Pool) *VideoDeletionRepository {
	return &VideoDeletionRepository{pool: pool}
}

type VideoDeletionJob struct {
	ID                      uuid.UUID
	VideoID                 uuid.UUID
	OwnerID                 uuid.UUID
	State                   string
	Phase                   string
	RunEpoch                int64
	LeaseOwner              string
	ObjectScanPrefixIndex   int16
	ObjectScanKeyMarker     string
	ObjectScanVersionMarker string
}

type DiscoveredObject struct {
	Key       string
	VersionID string
}

type GCObject struct {
	ID      uuid.UUID
	Bucket  string
	Key     string
	Version string
}

func (r *VideoDeletionRepository) ClaimNext(ctx context.Context, owner string) (*VideoDeletionJob, error) {
	if owner == "" {
		return nil, errors.New("video deletion worker owner is required")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin video deletion claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var job VideoDeletionJob
	err = tx.QueryRow(ctx, `WITH candidate AS (
		SELECT id FROM video_deletion_jobs
		WHERE (state IN ('PENDING','RETRY_WAIT') AND next_run_at<=clock_timestamp())
		   OR (state='RUNNING' AND lease_until<=clock_timestamp())
		ORDER BY next_run_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1
	)
	UPDATE video_deletion_jobs j SET state='RUNNING',run_epoch=j.run_epoch+1,lease_owner=$1,
		lease_until=clock_timestamp()+interval '2 minutes',attempts=j.attempts+1,last_error=NULL
	FROM candidate c WHERE j.id=c.id
	RETURNING j.id,j.video_id,j.owner_id,j.state,j.phase,j.run_epoch,j.lease_owner,
		j.object_scan_prefix_index,COALESCE(j.object_scan_key_marker,''),COALESCE(j.object_scan_version_marker,'')`, owner).
		Scan(&job.ID, &job.VideoID, &job.OwnerID, &job.State, &job.Phase, &job.RunEpoch, &job.LeaseOwner,
			&job.ObjectScanPrefixIndex, &job.ObjectScanKeyMarker, &job.ObjectScanVersionMarker)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty video deletion claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim video deletion job: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE video_gc_objects SET state='PENDING'
		WHERE video_id=$1 AND state='RUNNING' AND lease_epoch<$2`, job.VideoID, job.RunEpoch); err != nil {
		return nil, fmt.Errorf("recover stale object deletion attempts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit video deletion claim: %w", err)
	}
	return &job, nil
}

func (r *VideoDeletionRepository) Renew(ctx context.Context, job VideoDeletionJob) (bool, error) {
	result, err := r.pool.Exec(ctx, `UPDATE video_deletion_jobs SET lease_until=clock_timestamp()+interval '2 minutes'
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return false, fmt.Errorf("renew video deletion lease: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func (r *VideoDeletionRepository) Get(ctx context.Context, ownerID, deletionID uuid.UUID) (application.VideoDeletion, error) {
	var item application.VideoDeletion
	err := r.pool.QueryRow(ctx, `SELECT j.id,j.video_id,j.state,j.phase,
		CASE WHEN j.phase='WAIT_UPLOADS' THEN COALESCE((
			SELECT CASE
				WHEN EXISTS (SELECT 1 FROM upload_sessions u WHERE u.video_id=j.video_id
					AND (u.status IN ('INITIATING','UPLOADING','COMPLETING','VERIFYING','ABORTING')
						OR u.operation_lease_until>clock_timestamp())) THEN 'UPLOAD_TERMINATION_PENDING'
				WHEN EXISTS (SELECT 1 FROM asr_submissions a WHERE a.video_id=j.video_id
					AND a.state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY')
					AND a.provider_task_id IS NOT NULL) THEN 'ASR_REMOTE_RECONCILIATION_PENDING'
				WHEN EXISTS (SELECT 1 FROM asr_submissions a WHERE a.video_id=j.video_id
					AND a.state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY')
					AND a.provider_task_id IS NULL) THEN 'ASR_TASK_ID_UNAVAILABLE'
				ELSE NULL END
		),j.last_error->>'code') ELSE j.last_error->>'code' END, j.created_at
		FROM video_deletion_jobs j WHERE j.id=$1 AND j.owner_id=$2`, deletionID, ownerID).
		Scan(&item.DeletionID, &item.VideoID, &item.State, &item.Phase, &item.BlockingReason, &item.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.VideoDeletion{}, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "删除任务不存在"}
	}
	if err != nil {
		return application.VideoDeletion{}, fmt.Errorf("get video deletion status: %w", err)
	}
	return item, nil
}

func (r *VideoDeletionRepository) UploadsSettled(ctx context.Context, job VideoDeletionJob) (bool, string, error) {
	var reason string
	err := r.pool.QueryRow(ctx, `SELECT COALESCE((
		SELECT CASE
			WHEN EXISTS (SELECT 1 FROM upload_sessions u WHERE u.video_id=$1
				AND (u.status IN ('INITIATING','UPLOADING','COMPLETING','VERIFYING','ABORTING')
					OR u.operation_lease_until>clock_timestamp())) THEN 'UPLOAD_TERMINATION_PENDING'
			WHEN EXISTS (SELECT 1 FROM asr_submissions a WHERE a.video_id=$1
				AND a.state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY')
				AND a.provider_task_id IS NOT NULL) THEN 'ASR_REMOTE_RECONCILIATION_PENDING'
			WHEN EXISTS (SELECT 1 FROM asr_submissions a WHERE a.video_id=$1
				AND a.state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY')
				AND a.provider_task_id IS NULL) THEN 'ASR_TASK_ID_UNAVAILABLE'
			ELSE NULL END
	), '')`, job.VideoID).Scan(&reason)
	if err != nil {
		return false, "", fmt.Errorf("check upload sessions before object deletion: %w", err)
	}
	return reason == "", reason, nil
}

func (r *VideoDeletionRepository) PrepareObjects(ctx context.Context, job VideoDeletionJob, uploadBucket string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin video object snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockDeletionJob(ctx, tx, job); err != nil {
		return err
	}
	var unsettled bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM upload_sessions WHERE video_id=$1
		AND (status IN ('INITIATING','UPLOADING','COMPLETING','VERIFYING','ABORTING')
			OR operation_lease_until>clock_timestamp())) OR EXISTS(
			SELECT 1 FROM asr_submissions WHERE video_id=$1
			AND state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY'))`, job.VideoID).Scan(&unsettled); err != nil {
		return fmt.Errorf("recheck upload sessions before object snapshot: %w", err)
	}
	if unsettled {
		return ErrDeletionWait
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_gc_objects(id,video_id,bucket,object_key,object_version,state)
		SELECT gen_random_uuid(),video_id,bucket,object_key,object_version,'PENDING'
		FROM blobs WHERE video_id=$1 AND state<>'DELETED'
		ON CONFLICT DO NOTHING`, job.VideoID); err != nil {
		return fmt.Errorf("snapshot video blobs for deletion: %w", err)
	}
	if uploadBucket != "" {
		if _, err := tx.Exec(ctx, `INSERT INTO video_gc_objects(id,video_id,bucket,object_key,object_version,state)
			SELECT gen_random_uuid(),s.video_id,$2,s.object_key,NULL,'PENDING'
			FROM upload_sessions s
			WHERE s.video_id=$1 AND s.status IN ('COMPLETE','ABORTED','EXPIRED')
			  AND NOT EXISTS (SELECT 1 FROM blobs b WHERE b.video_id=s.video_id AND b.bucket=$2 AND b.object_key=s.object_key)
			ON CONFLICT DO NOTHING`, job.VideoID, uploadBucket); err != nil {
			return fmt.Errorf("snapshot completed upload objects for deletion: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE video_deletion_jobs SET phase='DISCOVER_OBJECTS',object_scan_prefix_index=0,
		object_scan_key_marker=NULL,object_scan_version_marker=NULL,next_run_at=clock_timestamp()
		WHERE id=$1 AND run_epoch=$2 AND state='RUNNING' AND lease_owner=$3 AND lease_until>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner); err != nil {
		return fmt.Errorf("advance video deletion to object phase: %w", err)
	}
	return tx.Commit(ctx)
}

// CommitObjectScanPage persists discovered object versions and the OSS cursor
// in one transaction. Replaying a page after a process crash is idempotent
// because the object ledger has a unique identity index.
func (r *VideoDeletionRepository) CommitObjectScanPage(ctx context.Context, job VideoDeletionJob,
	bucket, prefix string, objects []DiscoveredObject, nextPrefixIndex int16, nextKeyMarker, nextVersionMarker string,
	truncated bool, finalScan bool) (string, error) {
	if prefix == "" || (finalScan && job.Phase != "DISCOVER_OBJECTS_FINAL") ||
		(!finalScan && job.Phase != "DISCOVER_OBJECTS") {
		return job.Phase, errors.New("OSS object scan phase or prefix is invalid")
	}
	if len(objects) > 500 {
		return job.Phase, errors.New("OSS object scan page exceeds the bounded ledger batch")
	}
	if truncated && nextPrefixIndex != job.ObjectScanPrefixIndex {
		return job.Phase, errors.New("truncated OSS scan page changed its prefix index")
	}
	if !truncated && (nextPrefixIndex != job.ObjectScanPrefixIndex+1 || nextKeyMarker != "" || nextVersionMarker != "") {
		return job.Phase, errors.New("completed OSS scan page has an invalid next cursor")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return job.Phase, fmt.Errorf("begin OSS object scan checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockDeletionJob(ctx, tx, job); err != nil {
		return job.Phase, err
	}
	var currentPrefix int16
	var currentKey, currentVersion string
	if err := tx.QueryRow(ctx, `SELECT object_scan_prefix_index,COALESCE(object_scan_key_marker,''),
		COALESCE(object_scan_version_marker,'') FROM video_deletion_jobs WHERE id=$1`, job.ID).
		Scan(&currentPrefix, &currentKey, &currentVersion); err != nil {
		return job.Phase, fmt.Errorf("read OSS object scan cursor: %w", err)
	}
	if currentPrefix != job.ObjectScanPrefixIndex || currentKey != job.ObjectScanKeyMarker ||
		currentVersion != job.ObjectScanVersionMarker {
		return job.Phase, errors.New("OSS object scan cursor changed during the claimed page")
	}
	keys := make([]string, 0, len(objects))
	versions := make([]string, 0, len(objects))
	for _, object := range objects {
		if object.Key == "" || !strings.HasPrefix(object.Key, prefix) {
			return job.Phase, errors.New("OSS object scan returned a key outside the requested video prefix")
		}
		keys = append(keys, object.Key)
		versions = append(versions, object.VersionID)
	}
	if len(objects) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO video_gc_objects(id,video_id,bucket,object_key,object_version,state)
			SELECT gen_random_uuid(),$1,$2,items.object_key,NULLIF(items.object_version,''),'PENDING'
			FROM unnest($3::text[],$4::text[]) AS items(object_key,object_version)
			ON CONFLICT DO NOTHING`, job.VideoID, bucket, keys, versions); err != nil {
			return job.Phase, fmt.Errorf("record discovered OSS object versions: %w", err)
		}
	}
	phase := job.Phase
	prefixIndex := nextPrefixIndex
	keyMarker, versionMarker := nextKeyMarker, nextVersionMarker
	if !truncated && nextPrefixIndex < 3 {
		keyMarker, versionMarker = "", ""
	}
	if !truncated && nextPrefixIndex >= 3 {
		if finalScan {
			phase = "DELETE_OBJECTS_FINAL"
		} else {
			phase = "DELETE_OBJECTS"
		}
		prefixIndex, keyMarker, versionMarker = 0, "", ""
	} else if truncated {
		prefixIndex = job.ObjectScanPrefixIndex
		if nextKeyMarker == job.ObjectScanKeyMarker && nextVersionMarker == job.ObjectScanVersionMarker {
			return job.Phase, errors.New("OSS object scan did not advance its continuation markers")
		}
	}
	updated, err := tx.Exec(ctx, `UPDATE video_deletion_jobs SET phase=$4,object_scan_prefix_index=$5,
		object_scan_key_marker=NULLIF($6,''),object_scan_version_marker=NULLIF($7,''),next_run_at=clock_timestamp()
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner, phase, prefixIndex, keyMarker, versionMarker)
	if err != nil {
		return job.Phase, fmt.Errorf("persist OSS object scan cursor: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return job.Phase, errors.New("video deletion lease lost while saving OSS object scan")
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Phase, fmt.Errorf("commit OSS object scan page: %w", err)
	}
	return phase, nil
}

func (r *VideoDeletionRepository) AdvanceAfterFinalObjectDelete(ctx context.Context, job VideoDeletionJob) error {
	result, err := r.pool.Exec(ctx, `UPDATE video_deletion_jobs SET phase='PURGE_GC_OBJECTS_FINAL'
		WHERE id=$1 AND state='RUNNING' AND phase='DELETE_OBJECTS_FINAL' AND run_epoch=$2 AND lease_owner=$3
		AND lease_until>clock_timestamp() AND NOT EXISTS (
			SELECT 1 FROM video_gc_objects WHERE video_id=$4 AND state<>'DELETED')`,
		job.ID, job.RunEpoch, job.LeaseOwner, job.VideoID)
	if err != nil {
		return fmt.Errorf("advance final OSS sweep to ledger cleanup: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("video deletion lease lost or final object list is incomplete")
	}
	return nil
}

var ErrDeletionWait = errors.New("video deletion is waiting for upload termination")

func (r *VideoDeletionRepository) ClaimObject(ctx context.Context, job VideoDeletionJob) (*GCObject, error) {
	var object GCObject
	err := r.pool.QueryRow(ctx, `WITH candidate AS (
		SELECT o.id FROM video_gc_objects o JOIN video_deletion_jobs j ON j.video_id=o.video_id
		WHERE o.video_id=$1 AND o.state='PENDING' AND j.id=$2 AND j.state='RUNNING'
		  AND j.run_epoch=$3 AND j.lease_owner=$4 AND j.lease_until>clock_timestamp()
		ORDER BY o.created_at,o.id FOR UPDATE OF o SKIP LOCKED LIMIT 1
	)
	UPDATE video_gc_objects o SET state='RUNNING',lease_epoch=$3,attempts=attempts+1,last_error=NULL
	FROM candidate c WHERE o.id=c.id RETURNING o.id,o.bucket,o.object_key,COALESCE(o.object_version,'')`,
		job.VideoID, job.ID, job.RunEpoch, job.LeaseOwner).Scan(&object.ID, &object.Bucket, &object.Key, &object.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim object for deletion: %w", err)
	}
	return &object, nil
}

func (r *VideoDeletionRepository) FinishObject(ctx context.Context, job VideoDeletionJob, object GCObject) error {
	result, err := r.pool.Exec(ctx, `UPDATE video_gc_objects o SET state='DELETED',deleted_at=clock_timestamp(),last_error=NULL
		WHERE o.id=$1 AND o.video_id=$2 AND o.state='RUNNING' AND o.lease_epoch=$3
		AND EXISTS (SELECT 1 FROM video_deletion_jobs j WHERE j.id=$4 AND j.video_id=o.video_id
			AND j.state='RUNNING' AND j.run_epoch=$3 AND j.lease_owner=$5 AND j.lease_until>clock_timestamp())`,
		object.ID, job.VideoID, job.RunEpoch, job.ID, job.LeaseOwner)
	if err != nil {
		return fmt.Errorf("record deleted object: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("video deletion lease lost after OSS object delete")
	}
	return nil
}

func (r *VideoDeletionRepository) FailObject(ctx context.Context, job VideoDeletionJob, object GCObject, cause error) error {
	result, err := r.pool.Exec(ctx, `UPDATE video_gc_objects SET state='PENDING',last_error=jsonb_build_object('code','OSS_DELETE_FAILED')
		WHERE id=$1 AND video_id=$2 AND state='RUNNING' AND lease_epoch=$3
		AND EXISTS (SELECT 1 FROM video_deletion_jobs j WHERE j.id=$4 AND j.video_id=$2
			AND j.state='RUNNING' AND j.run_epoch=$3 AND j.lease_owner=$5 AND j.lease_until>clock_timestamp())`,
		object.ID, job.VideoID, job.RunEpoch, job.ID, job.LeaseOwner)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("record object deletion failure: %w", err))
	}
	if result.RowsAffected() != 1 {
		return errors.Join(cause, errors.New("video deletion lease lost after OSS delete failure"))
	}
	return nil
}

func (r *VideoDeletionRepository) AdvanceToPurge(ctx context.Context, job VideoDeletionJob) error {
	result, err := r.pool.Exec(ctx, `UPDATE video_deletion_jobs SET phase='CLEAR_VIDEO_REFS'
		WHERE id=$1 AND state='RUNNING' AND phase='DELETE_OBJECTS' AND run_epoch=$2 AND lease_owner=$3
		AND lease_until>clock_timestamp() AND NOT EXISTS (
			SELECT 1 FROM video_gc_objects WHERE video_id=$4 AND state<>'DELETED')`,
		job.ID, job.RunEpoch, job.LeaseOwner, job.VideoID)
	if err != nil {
		return fmt.Errorf("advance video deletion to row purge: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("video deletion lease lost or object list is incomplete")
	}
	return nil
}

func (r *VideoDeletionRepository) Retry(ctx context.Context, job VideoDeletionJob, cause error, delay time.Duration) error {
	return r.RetryWithCode(ctx, job, cause, delay, "VIDEO_DELETION_RETRY")
}

func (r *VideoDeletionRepository) RetryWithCode(ctx context.Context, job VideoDeletionJob, cause error,
	delay time.Duration, code string) error {
	if delay < time.Second {
		delay = time.Second
	}
	if code == "" || len(code) > 80 {
		return errors.Join(cause, errors.New("video deletion retry code is invalid"))
	}
	_, err := r.pool.Exec(ctx, `UPDATE video_deletion_jobs SET state='RETRY_WAIT',lease_owner=NULL,lease_until=NULL,
		next_run_at=clock_timestamp()+($4 * interval '1 millisecond'),
		last_error=jsonb_build_object('code',$5::text)
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$2 AND lease_owner=$3`,
		job.ID, job.RunEpoch, job.LeaseOwner, delay.Milliseconds(), code)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("schedule video deletion retry: %w", err))
	}
	return nil
}

func (r *VideoDeletionRepository) PurgeBatch(ctx context.Context, job VideoDeletionJob) (string, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return job.Phase, false, fmt.Errorf("begin video metadata purge batch: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockDeletionJob(ctx, tx, job); err != nil {
		return job.Phase, false, err
	}
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT lifecycle FROM videos WHERE id=$1 AND owner_id=$2`, job.VideoID, job.OwnerID).Scan(&lifecycle); err != nil {
		return job.Phase, false, fmt.Errorf("read video during metadata purge: %w", err)
	}
	if lifecycle != "DELETING" {
		return job.Phase, false, errors.New("video lifecycle changed during deletion")
	}
	if job.Phase == "FINALIZE" {
		var gcObjectsRemain bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_gc_objects WHERE video_id=$1)`, job.VideoID).Scan(&gcObjectsRemain); err != nil {
			return job.Phase, false, fmt.Errorf("verify object ledger before deletion finalization: %w", err)
		}
		if gcObjectsRemain {
			return job.Phase, false, errors.New("video deletion still has object ledger rows")
		}
		updatedVideo, err := tx.Exec(ctx, `UPDATE videos SET lifecycle='DELETED',file_state='CANCELLED',source_name='deleted',
			source_size_bytes=1,media_duration_ms=NULL,probe_metadata='{}'::jsonb,source_blob_id=NULL,
			active_result_id=NULL,latest_task_id=NULL WHERE id=$1 AND owner_id=$2 AND lifecycle='DELETING'`, job.VideoID, job.OwnerID)
		if err != nil {
			return job.Phase, false, fmt.Errorf("redact deleted video: %w", err)
		}
		if updatedVideo.RowsAffected() != 1 {
			return job.Phase, false, errors.New("video lifecycle changed before deletion finalization")
		}
		updatedJob, err := tx.Exec(ctx, `UPDATE video_deletion_jobs SET state='SUCCEEDED',phase='DONE',lease_owner=NULL,
			lease_until=NULL,completed_at=clock_timestamp(),last_error=NULL WHERE id=$1 AND state='RUNNING'
			AND phase='FINALIZE' AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`,
			job.ID, job.RunEpoch, job.LeaseOwner)
		if err != nil {
			return job.Phase, false, fmt.Errorf("finish video deletion job: %w", err)
		}
		if updatedJob.RowsAffected() != 1 {
			return job.Phase, false, errors.New("video deletion lease lost before final commit")
		}
		if err := tx.Commit(ctx); err != nil {
			return job.Phase, false, fmt.Errorf("commit video deletion finalization: %w", err)
		}
		return "DONE", true, nil
	}

	phaseIndex := -1
	for i, phase := range videoPurgePhases {
		if phase == job.Phase {
			phaseIndex = i
			break
		}
	}
	if phaseIndex < 0 {
		return job.Phase, false, fmt.Errorf("unknown video purge phase %q", job.Phase)
	}
	if job.Phase == "CLEAR_VIDEO_REFS" {
		var pendingObjects bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_gc_objects WHERE video_id=$1 AND state<>'DELETED')`, job.VideoID).Scan(&pendingObjects); err != nil {
			return job.Phase, false, fmt.Errorf("verify OSS object cleanup before row purge: %w", err)
		}
		if pendingObjects {
			return job.Phase, false, errors.New("video deletion still has OSS objects to remove")
		}
	}
	statement, err := videoPurgeSQL(job.Phase)
	if err != nil {
		return job.Phase, false, err
	}
	args := []any{job.VideoID}
	if job.Phase != "CLEAR_VIDEO_REFS" {
		args = append(args, videoGCBatchSize)
	}
	result, err := tx.Exec(ctx, statement, args...)
	if err != nil {
		return job.Phase, false, fmt.Errorf("purge video phase %s: %w", job.Phase, err)
	}
	nextPhase := job.Phase
	if result.RowsAffected() < videoGCBatchSize {
		nextPhase = videoPurgePhases[phaseIndex+1]
		updated, err := tx.Exec(ctx, `UPDATE video_deletion_jobs SET phase=$4 WHERE id=$1 AND state='RUNNING'
			AND phase=$5 AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`,
			job.ID, job.RunEpoch, job.LeaseOwner, nextPhase, job.Phase)
		if err != nil {
			return job.Phase, false, fmt.Errorf("advance video purge phase: %w", err)
		}
		if updated.RowsAffected() != 1 {
			return job.Phase, false, errors.New("video deletion lease lost while advancing purge phase")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Phase, false, fmt.Errorf("commit video purge batch: %w", err)
	}
	return nextPhase, false, nil
}

func (r *VideoDeletionRepository) Yield(ctx context.Context, job VideoDeletionJob, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	result, err := r.pool.Exec(ctx, `UPDATE video_deletion_jobs SET state='PENDING',lease_owner=NULL,lease_until=NULL,
		next_run_at=clock_timestamp()+($4 * interval '1 millisecond'),last_error=NULL
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner, delay.Milliseconds())
	if err != nil {
		return fmt.Errorf("yield video deletion work: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("video deletion lease lost before yielding work")
	}
	return nil
}

func videoPurgeSQL(phase string) (string, error) {
	queries := map[string]string{
		"CLEAR_VIDEO_REFS": `UPDATE videos SET source_blob_id=NULL,active_result_id=NULL,latest_task_id=NULL WHERE id=$1 AND lifecycle='DELETING'`,
		"PURGE_QUESTION_EVIDENCE": `WITH batch AS (SELECT ctid FROM question_evidence WHERE video_id=$1 ORDER BY ctid LIMIT $2)
			DELETE FROM question_evidence target USING batch WHERE target.ctid=batch.ctid`,
		"REDACT_QUESTIONS": `WITH batch AS (SELECT ctid FROM questions WHERE video_id=$1 AND
			(question_text<>'[deleted]' OR retrieval_query IS NOT NULL OR result_id IS NOT NULL OR generation_profile IS NOT NULL
			OR retrieval_profile IS NOT NULL OR answer_json IS NOT NULL OR purged_at IS NULL OR status<>'INTERRUPTED'
			OR error IS DISTINCT FROM '{"code":"VIDEO_DELETED"}'::jsonb)
			ORDER BY id LIMIT $2)
			UPDATE questions target SET question_text='[deleted]',retrieval_query=NULL,context_question_ids='{}',
			context_snapshot='[]'::jsonb,result_id=NULL,generation_profile=NULL,retrieval_profile=NULL,answer_json=NULL,
			error='{"code":"VIDEO_DELETED"}'::jsonb,status='INTERRUPTED',owner_instance=NULL,execution_id=NULL,
			lease_until=NULL,completed_at=COALESCE(target.completed_at,clock_timestamp()),
			purged_at=COALESCE(target.purged_at,clock_timestamp()) FROM batch WHERE target.ctid=batch.ctid`,
		"PURGE_QUESTION_STEPS": `WITH batch AS (SELECT ctid FROM question_steps WHERE question_id IN
			(SELECT id FROM questions WHERE video_id=$1) ORDER BY ctid LIMIT $2)
			DELETE FROM question_steps target USING batch WHERE target.ctid=batch.ctid`,
		"REDACT_CONVERSATIONS": `WITH batch AS (SELECT ctid FROM conversations WHERE video_id=$1
			AND (title IS NOT NULL OR archived_at IS NULL) ORDER BY id LIMIT $2)
			UPDATE conversations target SET title=NULL,archived_at=COALESCE(target.archived_at,clock_timestamp()),
			updated_at=clock_timestamp() FROM batch WHERE target.ctid=batch.ctid`,
		"REDACT_UPLOADS": `WITH batch AS (SELECT ctid FROM upload_sessions WHERE video_id=$1 AND
			(status<>'ABORTED' OR parse_slot_reserved OR upload_id IS NOT NULL OR object_key NOT LIKE 'deleted/%'
			OR operation_lease_until IS NOT NULL OR termination_after IS NULL)
			ORDER BY id LIMIT $2)
			UPDATE upload_sessions target SET status='ABORTED',parse_slot_reserved=false,upload_id=NULL,
			object_key='deleted/'||target.id::text,operation_epoch=operation_epoch+1,operation_lease_until=NULL,
			termination_reason='CANCEL',termination_after=COALESCE(termination_after,clock_timestamp())
			FROM batch WHERE target.ctid=batch.ctid`,
		"REDACT_PARSE_TASKS": `WITH batch AS (SELECT ctid FROM parse_tasks WHERE video_id=$1 AND
			(status<>'CANCELLED' OR current_stage<>'DONE' OR lease_owner IS NOT NULL OR lease_until IS NOT NULL
			OR config_snapshot<>'{}'::jsonb OR last_error IS DISTINCT FROM '{"code":"VIDEO_DELETED"}'::jsonb)
			ORDER BY id LIMIT $2)
			UPDATE parse_tasks target SET status='CANCELLED',current_stage='DONE',wait_reason=NULL,
			lease_owner=NULL,lease_until=NULL,run_epoch=run_epoch+1,config_snapshot='{}'::jsonb,
			last_error='{"code":"VIDEO_DELETED"}'::jsonb,completed_at=COALESCE(target.completed_at,clock_timestamp())
			FROM batch WHERE target.ctid=batch.ctid`,
		"PURGE_CONTENT_HEADS": `WITH batch AS (SELECT ctid FROM result_content_heads WHERE video_id=$1 ORDER BY result_id LIMIT $2)
			DELETE FROM result_content_heads target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CONTENT_SOURCE_REFS": `WITH batch AS (SELECT ctid FROM content_source_refs WHERE video_id=$1 ORDER BY artifact_id,ref_id LIMIT $2)
			DELETE FROM content_source_refs target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CONTENT_ARTIFACTS": `WITH batch AS (SELECT ctid FROM content_artifacts WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM content_artifacts target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CONTENT_JOB_BATCHES": `WITH batch AS (SELECT ctb.ctid FROM content_job_batches ctb JOIN content_jobs cj ON cj.id=ctb.job_id
			WHERE cj.video_id=$1 ORDER BY ctb.job_id,ctb.batch_no LIMIT $2)
			DELETE FROM content_job_batches target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CONTENT_JOBS": `WITH batch AS (SELECT ctid FROM content_jobs WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM content_jobs target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_TASK_STEPS": `WITH batch AS (SELECT ctid FROM task_steps WHERE video_id=$1 ORDER BY task_id,stage LIMIT $2)
			DELETE FROM task_steps target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_EMBEDDING_BATCHES": `WITH batch AS (SELECT ctid FROM embedding_batches WHERE video_id=$1 ORDER BY task_id,chunk_set_id,config_hash,batch_index LIMIT $2)
			DELETE FROM embedding_batches target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_ASR_SUBMISSIONS": `WITH batch AS (SELECT ctid FROM asr_submissions WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM asr_submissions target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_KAFKA_REJECTIONS": `WITH batch AS (SELECT kr.ctid FROM kafka_rejections kr JOIN outbox_events oe ON oe.event_id=kr.dlq_event_id
			JOIN parse_tasks task ON task.id=oe.aggregate_id WHERE task.video_id=$1 ORDER BY kr.ctid LIMIT $2)
			DELETE FROM kafka_rejections target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_INBOX_EVENTS": `WITH batch AS (SELECT ie.ctid FROM inbox_events ie JOIN parse_tasks task ON task.id=ie.task_id
			WHERE task.video_id=$1 ORDER BY ie.ctid LIMIT $2)
			DELETE FROM inbox_events target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_OUTBOX_EVENTS": `WITH batch AS (SELECT oe.ctid FROM outbox_events oe JOIN parse_tasks task ON task.id=oe.aggregate_id
			WHERE task.video_id=$1 ORDER BY oe.ctid LIMIT $2)
			DELETE FROM outbox_events target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_PARSE_RESULTS": `WITH batch AS (SELECT ctid FROM parse_results WHERE video_id=$1 ORDER BY task_version,id LIMIT $2)
			DELETE FROM parse_results target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CHUNK_EMBEDDINGS": `WITH batch AS (SELECT ctid FROM chunk_embeddings WHERE video_id=$1 ORDER BY embedding_set_id,chunk_id LIMIT $2)
			DELETE FROM chunk_embeddings target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_EMBEDDING_SETS": `WITH batch AS (SELECT ctid FROM embedding_sets WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM embedding_sets target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CHUNK_SOURCES": `WITH batch AS (SELECT ctid FROM chunk_sources WHERE video_id=$1 ORDER BY chunk_id,transcript_segment_seq LIMIT $2)
			DELETE FROM chunk_sources target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CHUNKS": `WITH batch AS (SELECT ctid FROM chunks WHERE video_id=$1 ORDER BY chunk_set_id,seq LIMIT $2)
			DELETE FROM chunks target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_CHUNK_SETS": `WITH batch AS (SELECT ctid FROM chunk_sets WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM chunk_sets target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_TRANSCRIPT_SEGMENTS": `WITH batch AS (SELECT ctid FROM transcript_segments WHERE video_id=$1 ORDER BY transcript_id,seq LIMIT $2)
			DELETE FROM transcript_segments target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_TRANSCRIPTS": `WITH batch AS (SELECT ctid FROM transcripts WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM transcripts target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_BLOBS": `WITH batch AS (SELECT ctid FROM blobs WHERE video_id=$1 ORDER BY id LIMIT $2)
			DELETE FROM blobs target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_GC_OBJECTS": `WITH batch AS (SELECT ctid FROM video_gc_objects WHERE video_id=$1 AND state='DELETED' ORDER BY id LIMIT $2)
			DELETE FROM video_gc_objects target USING batch WHERE target.ctid=batch.ctid`,
		"PURGE_GC_OBJECTS_FINAL": `WITH batch AS (SELECT ctid FROM video_gc_objects WHERE video_id=$1 AND state='DELETED' ORDER BY id LIMIT $2)
			DELETE FROM video_gc_objects target USING batch WHERE target.ctid=batch.ctid`,
	}
	query, ok := queries[phase]
	if !ok {
		return "", fmt.Errorf("no purge statement for phase %q", phase)
	}
	return query, nil
}

func lockDeletionJob(ctx context.Context, tx pgx.Tx, job VideoDeletionJob) error {
	var currentEpoch int64
	var owner string
	var leaseValid bool
	if err := tx.QueryRow(ctx, `SELECT run_epoch,lease_owner,lease_until>clock_timestamp() FROM video_deletion_jobs
		WHERE id=$1 AND video_id=$2 AND state='RUNNING' AND phase=$3 FOR UPDATE`, job.ID, job.VideoID, job.Phase).
		Scan(&currentEpoch, &owner, &leaseValid); errors.Is(err, pgx.ErrNoRows) {
		return errors.New("video deletion job is no longer in the claimed phase")
	} else if err != nil {
		return fmt.Errorf("lock video deletion job: %w", err)
	}
	if currentEpoch != job.RunEpoch || owner != job.LeaseOwner || !leaseValid {
		return errors.New("video deletion lease lost")
	}
	return nil
}
