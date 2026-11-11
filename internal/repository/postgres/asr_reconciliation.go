package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ASRReconciliationRepository struct{ pool *pgxpool.Pool }

func NewASRReconciliationRepository(pool *pgxpool.Pool) *ASRReconciliationRepository {
	return &ASRReconciliationRepository{pool: pool}
}

type ASRReconciliationJob struct {
	ID                uuid.UUID
	VideoID           uuid.UUID
	State             string
	ProviderTaskID    string
	ProviderStatus    string
	PollCount         int
	ReconcileAttempts int
	ReconcileEpoch    int64
	ReconcileOwner    string
}

func (r *ASRReconciliationRepository) ClaimNext(ctx context.Context, owner, region, workspace string) (*ASRReconciliationJob, error) {
	if owner == "" || region == "" || workspace == "" {
		return nil, errors.New("ASR reconciliation owner, region, and workspace are required")
	}
	var job ASRReconciliationJob
	err := r.pool.QueryRow(ctx, `WITH candidate AS (
		SELECT s.id FROM asr_submissions s
		JOIN parse_tasks t ON t.id=s.task_id AND t.video_id=s.video_id
		JOIN videos v ON v.id=s.video_id
		WHERE s.state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY')
		  AND s.provider_task_id IS NOT NULL
		  AND s.region=$2 AND s.workspace_id=$3 AND s.model='fun-asr'
		  AND (
			 t.status IN ('SUCCEEDED','CANCELLED')
			 OR (t.status='FAILED' AND NOT (t.manual_retry_count < 1 AND (
				COALESCE(t.last_error->>'code'='TASK_DEADLINE_EXCEEDED',false) OR EXISTS(
					SELECT 1 FROM task_steps failed_step WHERE failed_step.task_id=t.id
						AND failed_step.state='FAILED' AND failed_step.last_error->>'retryable'='true'
				)
			 )))
			 OR v.lifecycle IN ('DELETING','DELETED')
			 OR EXISTS (SELECT 1 FROM asr_submissions later
				WHERE later.task_id=s.task_id AND later.submit_seq>s.submit_seq)
		  )
		  AND (s.state='RESULT_READY' OR s.next_poll_at<=clock_timestamp())
		  AND (s.reconcile_lease_until IS NULL OR s.reconcile_lease_until<=clock_timestamp())
		ORDER BY s.next_poll_at,s.prepared_at,s.id
		FOR UPDATE OF s SKIP LOCKED LIMIT 1
	)
	UPDATE asr_submissions s SET reconcile_epoch=s.reconcile_epoch+1,reconcile_owner=$1,
		reconcile_lease_until=clock_timestamp()+interval '45 seconds',reconcile_attempts=s.reconcile_attempts+1,
		updated_at=clock_timestamp()
	FROM candidate c WHERE s.id=c.id
	RETURNING s.id,s.video_id,s.state,s.provider_task_id,s.provider_status,s.poll_count,
		s.reconcile_attempts,s.reconcile_epoch,s.reconcile_owner`, owner, region, workspace).
		Scan(&job.ID, &job.VideoID, &job.State, &job.ProviderTaskID, &job.ProviderStatus, &job.PollCount,
			&job.ReconcileAttempts, &job.ReconcileEpoch, &job.ReconcileOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim orphaned Fun-ASR task for reconciliation: %w", err)
	}
	return &job, nil
}

func (r *ASRReconciliationRepository) RecordProviderStatus(ctx context.Context, job ASRReconciliationJob,
	status, requestID string, terminal bool, nextPoll time.Time) error {
	if status == "" || len(status) > 120 {
		return errors.New("Fun-ASR reconciliation status is invalid")
	}
	var result pgconnCommandTag
	var err error
	if terminal {
		result, err = r.pool.Exec(ctx, `UPDATE asr_submissions SET state='REMOTE_TERMINAL',provider_status=$4,
			provider_request_id=COALESCE(NULLIF($5,''),provider_request_id),
			remote_terminal_at=clock_timestamp(),next_poll_at=clock_timestamp(),last_error=NULL,
			reconcile_owner=NULL,reconcile_lease_until=NULL,updated_at=clock_timestamp()
			WHERE id=$1 AND state=$2 AND reconcile_epoch=$3 AND reconcile_owner=$6
			  AND reconcile_lease_until>clock_timestamp()`,
			job.ID, job.State, job.ReconcileEpoch, status, requestID, job.ReconcileOwner)
	} else {
		if nextPoll.IsZero() {
			return errors.New("nonterminal Fun-ASR reconciliation result requires next_poll_at")
		}
		result, err = r.pool.Exec(ctx, `UPDATE asr_submissions SET state='RUNNING',provider_status=$4,
			provider_request_id=COALESCE(NULLIF($6,''),provider_request_id),
			poll_count=poll_count+1,next_poll_at=$5,last_error=NULL,reconcile_owner=NULL,
			reconcile_lease_until=NULL,updated_at=clock_timestamp()
			WHERE id=$1 AND state=$2 AND reconcile_epoch=$3 AND reconcile_owner=$7
			  AND reconcile_lease_until>clock_timestamp()`,
			job.ID, job.State, job.ReconcileEpoch, status, nextPoll, requestID, job.ReconcileOwner)
	}
	if err != nil {
		return fmt.Errorf("persist Fun-ASR reconciliation status: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("Fun-ASR reconciliation lease was lost before status commit")
	}
	return nil
}

func (r *ASRReconciliationRepository) RecordQueryFailure(ctx context.Context, job ASRReconciliationJob,
	code string, nextPoll time.Time) error {
	if code == "" || len(code) > 80 || nextPoll.IsZero() {
		return errors.New("Fun-ASR reconciliation failure checkpoint is invalid")
	}
	result, err := r.pool.Exec(ctx, `UPDATE asr_submissions SET next_poll_at=$4,
		last_error=jsonb_build_object('code',$5::text),reconcile_owner=NULL,reconcile_lease_until=NULL,
		updated_at=clock_timestamp()
		WHERE id=$1 AND state=$2 AND reconcile_epoch=$3 AND reconcile_owner=$6
		  AND reconcile_lease_until>clock_timestamp()`,
		job.ID, job.State, job.ReconcileEpoch, nextPoll, code, job.ReconcileOwner)
	if err != nil {
		return fmt.Errorf("persist Fun-ASR reconciliation retry: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("Fun-ASR reconciliation lease was lost before retry checkpoint")
	}
	return nil
}

// pgconnCommandTag keeps the repository's SQL results behind a small local
// interface while allowing both status updates to share the fencing check.
type pgconnCommandTag interface {
	RowsAffected() int64
}
