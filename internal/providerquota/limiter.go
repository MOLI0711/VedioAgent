package providerquota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rollingWindow = time.Minute
const reservationRetention = 2 * time.Minute
const reservationPruneBatchSize = 500
const reservationPruneTimeout = 5 * time.Second

var ErrEstimateExceedsTPM = errors.New("provider call reservation exceeds the configured TPM limit")

type Limiter struct {
	pool      *pgxpool.Pool
	workspace string
	model     string
	rpm       int
	tpm       int64
	observer  Observer
}

func New(ctx context.Context, pool *pgxpool.Pool, workspace, model string, rpm int, tpm int64,
	observers ...Observer) (*Limiter, error) {
	if pool == nil || workspace == "" || len(workspace) > 128 || strings.ContainsRune(workspace, '\x00') ||
		model == "" || len(model) > 128 || strings.ContainsRune(model, '\x00') || rpm <= 0 || rpm > 1_000_000 ||
		tpm <= 0 || tpm > 1_000_000_000 {
		return nil, errors.New("provider quota requires a database, workspace, model, and positive RPM/TPM limits")
	}
	var observer Observer
	if len(observers) > 0 {
		observer = observers[0]
	}
	limiter := &Limiter{pool: pool, workspace: workspace, model: model, rpm: rpm, tpm: tpm, observer: observer}
	if err := limiter.ensureGate(ctx); err != nil {
		return nil, err
	}
	return limiter, nil
}

func (l *Limiter) ensureGate(ctx context.Context) error {
	_, err := l.pool.Exec(ctx, `INSERT INTO provider_quota_gates(workspace_id,model,rpm_limit,tpm_limit)
		VALUES($1,$2,$3,$4) ON CONFLICT(workspace_id,model) DO NOTHING`,
		l.workspace, l.model, l.rpm, l.tpm)
	if err != nil {
		return fmt.Errorf("create provider quota gate: %w", err)
	}
	var rpm int
	var tpm int64
	if err := l.pool.QueryRow(ctx, `SELECT rpm_limit,tpm_limit FROM provider_quota_gates
		WHERE workspace_id=$1 AND model=$2`, l.workspace, l.model).Scan(&rpm, &tpm); err != nil {
		return fmt.Errorf("read provider quota gate: %w", err)
	}
	if rpm != l.rpm || tpm != l.tpm {
		return fmt.Errorf("configured %s RPM/TPM limits differ from the shared database gate; reconcile all API/background configurations before startup", l.model)
	}
	return nil
}

// Reserve serializes admission for one workspace/model across API and background
// processes. It counts every HTTP attempt, including failed and retried calls.
func (l *Limiter) Reserve(ctx context.Context, estimatedTokens int64) (uuid.UUID, error) {
	started := time.Now()
	waited := false
	finish := func(result string) {
		if l.observer != nil {
			l.observer.ObserveQuotaAdmission(l.model, result)
			if result == "reserved" {
				l.observer.ObserveProviderTokenReservation(l.model, estimatedTokens)
			}
			if waited {
				l.observer.ObserveQuotaWait(l.model, time.Since(started))
			}
		}
	}
	if estimatedTokens <= 0 || estimatedTokens > l.tpm {
		finish("tpm_rejected")
		return uuid.Nil, ErrEstimateExceedsTPM
	}
	for {
		id, wait, err := l.reserveOnce(ctx, estimatedTokens)
		if err != nil {
			if errors.Is(err, ErrEstimateExceedsTPM) {
				finish("tpm_rejected")
			} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				finish("canceled")
			} else {
				finish("error")
			}
			return uuid.Nil, err
		}
		if id != uuid.Nil {
			finish("reserved")
			return id, nil
		}
		waited = true
		if wait < 10*time.Millisecond {
			wait = 10 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			finish("canceled")
			return uuid.Nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type usageRow struct {
	createdAt time.Time
	tokens    int64
}

func (l *Limiter) reserveOnce(ctx context.Context, estimatedTokens int64) (uuid.UUID, time.Duration, error) {
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("begin provider quota admission: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var rpm int
	var tpm int64
	var now time.Time
	var cooldown pgtype.Timestamptz
	if err := tx.QueryRow(ctx, `SELECT rpm_limit,tpm_limit,clock_timestamp(),cooldown_until
		FROM provider_quota_gates WHERE workspace_id=$1 AND model=$2 FOR UPDATE`, l.workspace, l.model).
		Scan(&rpm, &tpm, &now, &cooldown); err != nil {
		return uuid.Nil, 0, fmt.Errorf("lock provider quota gate: %w", err)
	}
	if estimatedTokens > tpm {
		return uuid.Nil, 0, ErrEstimateExceedsTPM
	}
	rows, err := tx.Query(ctx, `SELECT created_at,COALESCE(actual_tokens,reserved_tokens)
		FROM provider_call_reservations WHERE workspace_id=$1 AND model=$2
		AND created_at>$3 ORDER BY created_at`, l.workspace, l.model, now.Add(-rollingWindow))
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("read provider quota window: %w", err)
	}
	usage := make([]usageRow, 0, rpm)
	var tokenSum int64
	for rows.Next() {
		var row usageRow
		if err := rows.Scan(&row.createdAt, &row.tokens); err != nil {
			rows.Close()
			return uuid.Nil, 0, fmt.Errorf("scan provider quota window: %w", err)
		}
		usage = append(usage, row)
		tokenSum += row.tokens
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return uuid.Nil, 0, fmt.Errorf("read provider quota window: %w", err)
	}
	rows.Close()

	waitUntil := now
	if cooldown.Valid && cooldown.Time.After(waitUntil) {
		waitUntil = cooldown.Time
	}
	if len(usage)+1 > rpm || tokenSum > tpm-estimatedTokens {
		remainingCount, remainingTokens := len(usage), tokenSum
		found := false
		for _, row := range usage {
			remainingCount--
			remainingTokens -= row.tokens
			availableAt := row.createdAt.Add(rollingWindow)
			if remainingCount+1 <= rpm && remainingTokens <= tpm-estimatedTokens {
				if availableAt.After(waitUntil) {
					waitUntil = availableAt
				}
				found = true
				break
			}
		}
		if !found {
			return uuid.Nil, 0, errors.New("provider quota window cannot admit this call")
		}
	}
	if waitUntil.After(now) {
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, 0, fmt.Errorf("release provider quota gate: %w", err)
		}
		return uuid.Nil, waitUntil.Sub(now), nil
	}
	id := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO provider_call_reservations(id,workspace_id,model,reserved_tokens)
		VALUES($1,$2,$3,$4)`, id, l.workspace, l.model, estimatedTokens); err != nil {
		return uuid.Nil, 0, fmt.Errorf("reserve provider RPM/TPM budget: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, 0, fmt.Errorf("commit provider quota reservation: %w", err)
	}
	return id, 0, nil
}

func (l *Limiter) Complete(ctx context.Context, id uuid.UUID, actualTokens int64, requestID string) error {
	if id == uuid.Nil || actualTokens < 0 {
		return errors.New("provider quota completion is invalid")
	}
	if actualTokens > l.tpm {
		actualTokens = l.tpm + 1
	}
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin provider quota completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := l.lockGate(ctx, tx); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE provider_call_reservations SET actual_tokens=$3,
		provider_request_id=NULLIF($4,''),outcome='SUCCEEDED',completed_at=clock_timestamp()
		WHERE id=$1 AND workspace_id=$2 AND model=$5 AND outcome='RESERVED'`,
		id, l.workspace, actualTokens, bounded(requestID, 128), l.model)
	if err != nil {
		return fmt.Errorf("record provider usage: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("provider quota reservation is missing or already completed")
	}
	return tx.Commit(ctx)
}

func (l *Limiter) Fail(ctx context.Context, id uuid.UUID, requestID, code string, retryAfter time.Duration) error {
	if id == uuid.Nil {
		return errors.New("provider quota failure reservation is invalid")
	}
	if code == "" {
		code = "PROVIDER_CALL_FAILED"
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	if retryAfter > 15*time.Minute {
		retryAfter = 15 * time.Minute
	}
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin provider quota failure record: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := l.lockGate(ctx, tx); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE provider_call_reservations SET
		provider_request_id=NULLIF($3,''),error_code=$4,outcome='FAILED',completed_at=clock_timestamp()
		WHERE id=$1 AND workspace_id=$2 AND model=$5 AND outcome='RESERVED'`,
		id, l.workspace, bounded(requestID, 128), bounded(code, 80), l.model)
	if err != nil {
		return fmt.Errorf("record failed provider attempt: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("provider quota reservation is missing or already completed")
	}
	if retryAfter > 0 {
		if _, err := tx.Exec(ctx, `UPDATE provider_quota_gates SET
			cooldown_until=GREATEST(COALESCE(cooldown_until,'-infinity'::timestamptz),
				clock_timestamp()+($3 * interval '1 millisecond')),updated_at=clock_timestamp()
			WHERE workspace_id=$1 AND model=$2`, l.workspace, l.model, retryAfter.Milliseconds()); err != nil {
			return fmt.Errorf("apply provider retry cooldown: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (l *Limiter) lockGate(ctx context.Context, tx pgx.Tx) error {
	var present bool
	if err := tx.QueryRow(ctx, `SELECT true FROM provider_quota_gates
		WHERE workspace_id=$1 AND model=$2 FOR UPDATE`, l.workspace, l.model).Scan(&present); err != nil {
		return fmt.Errorf("lock provider quota gate: %w", err)
	}
	return nil
}

func pruneReservations(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	tag, err := pool.Exec(ctx, `WITH expired AS (
		SELECT id FROM provider_call_reservations
		WHERE created_at<clock_timestamp()-($1 * interval '1 microsecond')
		ORDER BY created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM provider_call_reservations r USING expired e WHERE r.id=e.id`,
		reservationRetention.Microseconds(), reservationPruneBatchSize)
	if err != nil {
		return 0, fmt.Errorf("prune expired provider quota reservations: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RunLedgerPruner removes expired reservation rows for all workspaces and
// models independently of model traffic. Multiple API/background instances may
// run it concurrently; SKIP LOCKED lets each batch make progress without
// blocking another instance. Pruning failures do not block quota admission.
func RunLedgerPruner(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, onError func(error)) {
	if pool == nil {
		if onError != nil {
			onError(errors.New("provider quota ledger pruner requires a database pool"))
		}
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	prune := func() {
		pruneCtx, cancel := context.WithTimeout(ctx, reservationPruneTimeout)
		defer cancel()
		for batch := 0; batch < 10; batch++ {
			removed, err := pruneReservations(pruneCtx, pool)
			if err != nil {
				if ctx.Err() == nil && onError != nil {
					onError(err)
				}
				return
			}
			if removed < reservationPruneBatchSize {
				return
			}
		}
	}
	prune()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prune()
		}
	}
}

func bounded(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
