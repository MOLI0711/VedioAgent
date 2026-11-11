package outbox

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/database"
	"mirrorcore/internal/repository/postgres"
)

type senderFunc func(context.Context, string, []byte, []byte) error

func (f senderFunc) Send(ctx context.Context, topic string, key, payload []byte) error {
	return f(ctx, topic, key, payload)
}

func TestExpiredLeaseFencesOldOutboxSender(t *testing.T) {
	pool, ctx, ownerID, videoID, taskID, eventID := prepareOutboxIntegration(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	oldSender := senderFunc(func(ctx context.Context, _ string, _, _ []byte) error {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	first := New(pool, oldSender, nil)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.PumpEvent(ctx, eventID) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first sender did not claim the event")
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1`, eventID); err != nil {
		t.Fatalf("expire first sender lease: %v", err)
	}
	second := New(pool, senderFunc(func(context.Context, string, []byte, []byte) error { return nil }), nil)
	if err := second.PumpEvent(ctx, eventID); err != nil {
		t.Fatalf("second sender: %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first sender completion: %v", err)
	}
	var status string
	var epoch int64
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT status,send_epoch,attempts FROM outbox_events WHERE event_id=$1`, eventID).
		Scan(&status, &epoch, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "SENT" || epoch != 2 || attempts != 2 {
		t.Fatalf("status=%s epoch=%d attempts=%d, want SENT/2/2 (task %s, owner %s, video %s)", status, epoch, attempts, taskID, ownerID, videoID)
	}
}

func TestFailedSendPersistsRetryBackoff(t *testing.T) {
	pool, ctx, _, _, _, eventID := prepareOutboxIntegration(t)
	relay := New(pool, senderFunc(func(context.Context, string, []byte, []byte) error { return errors.New("broker unavailable") }), nil)
	if err := relay.PumpEvent(ctx, eventID); err == nil {
		t.Fatal("expected send failure")
	}
	var status string
	var epoch int64
	var attempts int
	var dueLater bool
	if err := pool.QueryRow(ctx, `SELECT status,send_epoch,attempts,next_send_at > clock_timestamp()
		FROM outbox_events WHERE event_id=$1`, eventID).Scan(&status, &epoch, &attempts, &dueLater); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || epoch != 1 || attempts != 1 || !dueLater {
		t.Fatalf("status=%s epoch=%d attempts=%d due_later=%t, want PENDING/1/1/true", status, epoch, attempts, dueLater)
	}
}

func prepareOutboxIntegration(t *testing.T) (*pgxpool.Pool, context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate integration database: %v", err)
	}
	ownerID, videoID := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES ($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES ($1,$2,'VALID','outbox-integration.mp4',1024)`, videoID, ownerID)
	if err != nil {
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id=$1`, ownerID)
		t.Fatalf("insert video: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM parse_tasks WHERE owner_id=$1)`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `UPDATE videos SET latest_task_id=NULL WHERE owner_id=$1`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM parse_tasks WHERE owner_id=$1`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM videos WHERE owner_id=$1`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id=$1`, ownerID)
	})
	task, _, err := application.NewResourceService(postgres.NewResourceRepository(pool)).CreateParseTask(ctx, application.CreateParseTaskInput{
		VideoID: videoID, OwnerID: ownerID, RequestKey: uuid.NewString(), TraceID: uuid.NewString(), ProfileID: "voice-v1",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	var eventID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT event_id FROM outbox_events WHERE aggregate_id=$1`, task.ID).Scan(&eventID); err != nil {
		t.Fatalf("load outbox event: %v", err)
	}
	return pool, ctx, ownerID, videoID, task.ID, eventID
}
