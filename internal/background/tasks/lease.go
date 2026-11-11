package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	LeaseDuration     = 90 * time.Second
	LeaseRefresh      = 20 * time.Second
	MaintenancePeriod = 15 * time.Second
)

var ErrLeaseLost = errors.New("parse task lease is no longer valid")

// Task is the immutable execution input plus the current persisted checkpoint.
// Kafka messages are never treated as the source of task configuration.
type Task struct {
	ID             uuid.UUID
	VideoID        uuid.UUID
	OwnerID        uuid.UUID
	TaskVersion    int32
	Status         string
	Stage          string
	RunEpoch       int64
	LeaseOwner     string
	LeaseUntil     time.Time
	NextRunAt      time.Time
	DeadlineAt     time.Time
	ConfigSnapshot json.RawMessage
}

type Manager struct {
	pool   *pgxpool.Pool
	owner  string
	logger *slog.Logger
}

func NewManager(pool *pgxpool.Pool, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{pool: pool, owner: uuid.NewString(), logger: logger}
}

// ClaimNext atomically selects one due task and grants a new execution epoch.
// Call it only when the process has an available local execution slot.
func (m *Manager) ClaimNext(ctx context.Context) (*Task, error) {
	return m.ClaimNextStages(ctx, nil)
}

// ClaimNextStages limits a worker pool to stages for which it has processors.
// A task at a later, not-yet-configured stage remains durable and queryable.
func (m *Manager) ClaimNextStages(ctx context.Context, stages []string) (*Task, error) {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin parse task claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var task Task
	err = tx.QueryRow(ctx, `WITH candidate AS (
		SELECT id FROM parse_tasks
		WHERE status IN ('READY','WAITING','RETRY_WAIT')
		  AND next_run_at <= clock_timestamp()
		  AND deadline_at > clock_timestamp()
		  AND (COALESCE(array_length($3::text[],1),0)=0 OR current_stage=ANY($3::text[]))
		ORDER BY next_run_at, admitted_at NULLS LAST, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE parse_tasks t SET status='RUNNING', run_epoch=t.run_epoch+1,
		lease_owner=$1, lease_until=clock_timestamp()+($2 * interval '1 millisecond'), wait_reason=NULL
	FROM candidate c WHERE t.id=c.id
	RETURNING t.id,t.video_id,t.owner_id,t.task_version,t.status,t.current_stage,
		t.run_epoch,t.lease_owner,t.lease_until,t.next_run_at,t.deadline_at,t.config_snapshot`,
		m.owner, LeaseDuration.Milliseconds(), stages).
		Scan(&task.ID, &task.VideoID, &task.OwnerID, &task.TaskVersion, &task.Status, &task.Stage,
			&task.RunEpoch, &task.LeaseOwner, &task.LeaseUntil, &task.NextRunAt, &task.DeadlineAt, &task.ConfigSnapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("finish empty parse task claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim parse task: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit parse task claim: %w", err)
	}
	return &task, nil
}

// RenewLease uses PostgreSQL time and cannot revive an expired lease.
func (m *Manager) RenewLease(ctx context.Context, task Task) (bool, error) {
	result, err := m.pool.Exec(ctx, `UPDATE parse_tasks SET lease_until=clock_timestamp()+($4 * interval '1 millisecond')
		WHERE id=$1 AND status='RUNNING' AND run_epoch=$2 AND lease_owner=$3
		  AND lease_until > clock_timestamp() AND deadline_at > clock_timestamp()`,
		task.ID, task.RunEpoch, m.owner, LeaseDuration.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("renew parse task lease: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// Wait persists an external wait (for example, an ASR provider task) before
// releasing this process's execution slot. The next owner receives a new epoch.
func (m *Manager) Wait(ctx context.Context, task Task, reason string, nextRunAt time.Time) error {
	if reason == "" || len(reason) > 80 {
		return errors.New("wait reason must contain 1 to 80 bytes")
	}
	return m.release(ctx, task, "WAITING", reason, nextRunAt, nil)
}

// Retry schedules a retry of the current stage; it does not create a new
// task_version and does not change the persisted checkpoint.
func (m *Manager) Retry(ctx context.Context, task Task, nextRunAt time.Time, lastError json.RawMessage) error {
	if !json.Valid(lastError) || len(lastError) > 4096 {
		return errors.New("last error must be valid JSON and at most 4096 bytes")
	}
	return m.release(ctx, task, "RETRY_WAIT", "", nextRunAt, lastError)
}

func (m *Manager) release(ctx context.Context, task Task, status, waitReason string, nextRunAt time.Time, lastError json.RawMessage) error {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin parse task release: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status=$4, wait_reason=NULLIF($5,''),
		next_run_at=LEAST($6,deadline_at), last_error=COALESCE($7::jsonb,last_error),
		lease_owner=NULL, lease_until=NULL
		WHERE id=$1 AND status='RUNNING' AND run_epoch=$2 AND lease_owner=$3
		  AND lease_until > clock_timestamp() AND deadline_at > clock_timestamp()`,
		task.ID, task.RunEpoch, m.owner, status, waitReason, nextRunAt, nullableJSON(lastError))
	if err != nil {
		return fmt.Errorf("release parse task lease: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='PENDING',last_error=$3::jsonb,
		completed_at=NULL,updated_at=clock_timestamp()
		WHERE task_id=$1 AND stage=$2 AND state='RUNNING'`, task.ID, task.Stage, nullableJSON(lastError)); err != nil {
		return fmt.Errorf("release current parse stage checkpoint: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit parse task release: %w", err)
	}
	return nil
}

// Fail is a fenced terminal transition. It is used for permanent provider or
// media errors only; retryable failures should use Retry.
func (m *Manager) Fail(ctx context.Context, task Task, lastError json.RawMessage) error {
	if !json.Valid(lastError) || len(lastError) > 4096 {
		return errors.New("last error must be valid JSON and at most 4096 bytes")
	}
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin parse task failure: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='FAILED', last_error=$4::jsonb,
		completed_at=clock_timestamp(), wait_reason=NULL, lease_owner=NULL, lease_until=NULL
		WHERE id=$1 AND status='RUNNING' AND run_epoch=$2 AND lease_owner=$3
		AND lease_until > clock_timestamp() AND deadline_at > clock_timestamp()`, task.ID, task.RunEpoch, m.owner, string(lastError))
	if err != nil {
		return fmt.Errorf("fail parse task: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, `UPDATE task_steps SET state='FAILED',last_error=$3::jsonb,
		completed_at=clock_timestamp(),updated_at=clock_timestamp()
		WHERE task_id=$1 AND stage=$2 AND state='RUNNING'`, task.ID, task.Stage, string(lastError)); err != nil {
		return fmt.Errorf("fail current parse stage checkpoint: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit parse task failure: %w", err)
	}
	return nil
}

// MaintainOnce fences stale workers, fails admitted work past its deadline,
// and terminates tasks that were never admitted within the delivery window.
func (m *Manager) MaintainOnce(ctx context.Context) error {
	if err := m.failOverdue(ctx); err != nil {
		return err
	}
	if err := m.reapExpiredLeases(ctx); err != nil {
		return err
	}
	if err := m.failUndispatched(ctx); err != nil {
		return err
	}
	return nil
}

func (m *Manager) RunMaintenance(ctx context.Context) error {
	ticker := time.NewTicker(MaintenancePeriod)
	defer ticker.Stop()
	for {
		if err := m.MaintainOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.Error("parse task maintenance failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Manager) failOverdue(ctx context.Context) error {
	_, err := m.pool.Exec(ctx, `WITH failed AS (
		UPDATE parse_tasks SET status='FAILED',
			last_error=jsonb_build_object('code','TASK_DEADLINE_EXCEEDED','retryable',false),
			completed_at=clock_timestamp(), wait_reason=NULL, lease_owner=NULL, lease_until=NULL
		WHERE status IN ('READY','RUNNING','WAITING','RETRY_WAIT')
		  AND admitted_at IS NOT NULL AND deadline_at <= clock_timestamp()
		RETURNING id,current_stage
	)
	UPDATE task_steps s SET state='FAILED',
		last_error=jsonb_build_object('code','TASK_DEADLINE_EXCEEDED','retryable',false),
		completed_at=clock_timestamp(),updated_at=clock_timestamp()
	FROM failed f WHERE s.task_id=f.id AND s.stage=f.current_stage AND s.state IN ('PENDING','RUNNING')`)
	if err != nil {
		return fmt.Errorf("fail expired parse tasks: %w", err)
	}
	return nil
}

func (m *Manager) reapExpiredLeases(ctx context.Context) error {
	_, err := m.pool.Exec(ctx, `WITH expired AS (
		UPDATE parse_tasks SET status='RETRY_WAIT', next_run_at=clock_timestamp(),
			last_error=jsonb_build_object('code','WORKER_LEASE_EXPIRED','retryable',true,'previous',last_error),
			lease_owner=NULL, lease_until=NULL
		WHERE status='RUNNING' AND deadline_at > clock_timestamp()
		  AND (lease_until IS NULL OR lease_until <= clock_timestamp())
		RETURNING id,current_stage
	)
	UPDATE task_steps s SET state='PENDING',
		last_error=jsonb_build_object('code','WORKER_LEASE_EXPIRED','retryable',true),
		completed_at=NULL,updated_at=clock_timestamp()
	FROM expired e WHERE s.task_id=e.id AND s.stage=e.current_stage AND s.state='RUNNING'`)
	if err != nil {
		return fmt.Errorf("reap expired parse task leases: %w", err)
	}
	return nil
}

func (m *Manager) failUndispatched(ctx context.Context) error {
	_, err := m.pool.Exec(ctx, `WITH expired AS (
		UPDATE parse_tasks SET status='FAILED',
			last_error=jsonb_build_object('code','DELIVERY_TIMEOUT','retryable',false),
			completed_at=clock_timestamp(), wait_reason=NULL
		WHERE status='WAIT_DISPATCH' AND created_at <= clock_timestamp()-interval '24 hours'
		RETURNING id
	)
	UPDATE outbox_events o SET status='FAILED', lease_owner=NULL, lease_until=NULL
	FROM expired e WHERE o.aggregate_id=e.id AND o.event_type='parse.requested'
	  AND o.status IN ('PENDING','SENDING','SENT')`)
	if err != nil {
		return fmt.Errorf("fail undispatched parse tasks: %w", err)
	}
	return nil
}

func nullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}
