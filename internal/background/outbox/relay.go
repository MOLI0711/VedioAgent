package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Sender interface {
	Send(context.Context, string, []byte, []byte) error
}

type Relay struct {
	pool   *pgxpool.Pool
	sender Sender
	owner  string
	logger *slog.Logger
}

type event struct {
	ID       uuid.UUID
	Topic    string
	Key      []byte
	Payload  []byte
	Epoch    int64
	Attempts int
}

func New(pool *pgxpool.Pool, sender Sender, logger *slog.Logger) *Relay {
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{pool: pool, sender: sender, owner: uuid.NewString(), logger: logger}
}

func NewKgoSender(client *kgo.Client) Sender {
	return &kgoSender{client: client}
}

type kgoSender struct{ client *kgo.Client }

func (s *kgoSender) Send(ctx context.Context, topic string, key, payload []byte) error {
	record := &kgo.Record{Topic: topic, Key: key, Value: payload}
	return s.client.ProduceSync(ctx, record).FirstErr()
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	reconcileTicker := time.NewTicker(30 * time.Second)
	defer reconcileTicker.Stop()
	for {
		if err := r.PumpOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("outbox pump failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-reconcileTicker.C:
			if count, err := r.RequeueUnadmitted(ctx); err != nil {
				r.logger.Error("outbox admission reconciliation failed", "error", err)
			} else if count > 0 {
				r.logger.Warn("requeued parse events without inbox admission", "count", count)
			}
		}
	}
}

// RequeueUnadmitted republishes the same event identity when Kafka accepted a
// send but the task has not been durably admitted by the consumer. Inbox
// deduplication makes a late duplicate safe; it never resets a task's stage.
func (r *Relay) RequeueUnadmitted(ctx context.Context) (int64, error) {
	result, err := r.pool.Exec(ctx, `WITH candidates AS (
		SELECT o.event_id
		FROM outbox_events o
		JOIN parse_tasks t ON t.id=o.aggregate_id
		WHERE o.event_type='parse.requested' AND o.status='SENT'
		  AND o.sent_at <= clock_timestamp()-interval '60 seconds'
		  AND o.next_send_at <= clock_timestamp()
		  AND t.status='WAIT_DISPATCH' AND t.admitted_at IS NULL
		  AND t.created_at > clock_timestamp()-interval '24 hours'
		ORDER BY o.sent_at, o.event_id
		FOR UPDATE OF o SKIP LOCKED
	)
	UPDATE outbox_events o SET status='PENDING', sent_at=NULL,
		next_send_at=clock_timestamp()
	FROM candidates c WHERE o.event_id=c.event_id`)
	if err != nil {
		return 0, fmt.Errorf("requeue unadmitted parse events: %w", err)
	}
	return result.RowsAffected(), nil
}

// PumpOnce claims one due event in a short transaction, sends it without a DB
// transaction, then conditionally records the result using the claim epoch.
func (r *Relay) PumpOnce(ctx context.Context) error {
	return r.pump(ctx, nil)
}

// PumpEvent sends one particular due event; it is useful for targeted
// reconciliation and allows integration tests to avoid claiming other rows.
func (r *Relay) PumpEvent(ctx context.Context, eventID uuid.UUID) error {
	return r.pump(ctx, &eventID)
}

func (r *Relay) pump(ctx context.Context, eventID *uuid.UUID) error {
	claimed, err := r.claim(ctx, eventID)
	if err != nil || claimed == nil {
		return err
	}
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	sendErr := r.sender.Send(sendCtx, claimed.Topic, claimed.Key, claimed.Payload)
	cancel()
	if sendErr != nil {
		if err := r.scheduleRetry(ctx, *claimed); err != nil {
			return errors.Join(sendErr, err)
		}
		return fmt.Errorf("send outbox event %s: %w", claimed.ID, sendErr)
	}
	if err := r.markSent(ctx, *claimed); err != nil {
		return fmt.Errorf("mark outbox event %s sent: %w", claimed.ID, err)
	}
	return nil
}

func (r *Relay) claim(ctx context.Context, eventID *uuid.UUID) (*event, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var claimed event
	err = tx.QueryRow(ctx, `WITH candidate AS (
		SELECT event_id FROM outbox_events
		WHERE ($2::uuid IS NULL OR event_id=$2)
		  AND ((status = 'PENDING' AND next_send_at <= clock_timestamp())
		   OR (status = 'SENDING' AND lease_until <= clock_timestamp()))
		ORDER BY next_send_at, created_at, event_id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE outbox_events o SET
		status='SENDING', send_epoch=o.send_epoch+1, lease_owner=$1,
		lease_until=clock_timestamp()+interval '30 seconds', attempts=o.attempts+1
	FROM candidate c WHERE o.event_id=c.event_id
		RETURNING o.event_id,o.topic,o.event_key,o.payload,o.send_epoch,o.attempts`, r.owner, eventID).
		Scan(&claimed.ID, &claimed.Topic, &claimed.Key, &claimed.Payload, &claimed.Epoch, &claimed.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, tx.Commit(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("claim outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return &claimed, nil
}

func (r *Relay) markSent(ctx context.Context, claimed event) error {
	_, err := r.pool.Exec(ctx, `UPDATE outbox_events SET status='SENT', sent_at=clock_timestamp(),
		next_send_at=clock_timestamp()+interval '60 seconds', lease_owner=NULL, lease_until=NULL
		WHERE event_id=$1 AND status='SENDING' AND send_epoch=$2 AND lease_owner=$3`, claimed.ID, claimed.Epoch, r.owner)
	return err
}

func (r *Relay) scheduleRetry(ctx context.Context, claimed event) error {
	_, err := r.pool.Exec(ctx, `UPDATE outbox_events SET status='PENDING', lease_owner=NULL, lease_until=NULL,
		next_send_at=clock_timestamp()+($4 * interval '1 millisecond')
		WHERE event_id=$1 AND status='SENDING' AND send_epoch=$2 AND lease_owner=$3`,
		claimed.ID, claimed.Epoch, r.owner, retryDelay(claimed.Attempts).Milliseconds())
	return err
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := attempt - 1
	if exponent > 6 {
		exponent = 6
	}
	base := time.Second * time.Duration(1<<exponent)
	if base > time.Minute {
		base = time.Minute
	}
	jitter := base / 5
	delta := time.Duration(rand.Int64N(int64(jitter*2+1))) - jitter
	delay := base + delta
	if delay < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}
