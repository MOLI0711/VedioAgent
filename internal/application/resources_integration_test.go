package application_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/application"
	"mirrorcore/internal/database"
	"mirrorcore/internal/repository/postgres"
)

func TestCreateParseTaskIdempotencyVersionAndQuota(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate integration database: %v", err)
	}

	ownerID := uuid.New()
	videoID := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES ($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test")
	if err != nil {
		t.Fatalf("insert test user: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES ($1,$2,'VALID','integration.mp4',1024)`, videoID, ownerID)
	if err != nil {
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id=$1`, ownerID)
		t.Fatalf("insert test video: %v", err)
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

	service := application.NewResourceService(postgres.NewResourceRepository(pool))
	otherOwnerID := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES ($1,$2,'integration-test')`, otherOwnerID, otherOwnerID.String()+"@example.test")
	if err != nil {
		t.Fatalf("insert second test user: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id=$1`, otherOwnerID)
	})
	_, err = service.GetVideo(ctx, otherOwnerID, videoID)
	var serviceErr *application.ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.Status != 404 {
		t.Fatalf("other user's video lookup error = %v, want 404", err)
	}

	input := application.CreateParseTaskInput{VideoID: videoID, OwnerID: ownerID, RequestKey: "same-key", TraceID: uuid.NewString(), ProfileID: "voice-v1"}
	type result struct {
		task   application.ParseTask
		replay bool
		err    error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, replay, err := service.CreateParseTask(ctx, input)
			results <- result{task: task, replay: replay, err: err}
		}()
	}
	wg.Wait()
	close(results)
	var original application.ParseTask
	var created, replayed int
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent create: %v", result.err)
		}
		if original.ID != uuid.Nil && result.task.ID != original.ID {
			t.Fatalf("concurrent requests produced different tasks: %s and %s", original.ID, result.task.ID)
		}
		original = result.task
		if result.replay {
			replayed++
		} else {
			created++
		}
	}
	if created != 1 || replayed != 1 || original.TaskVersion != 1 {
		t.Fatalf("created=%d replayed=%d version=%d, want 1, 1, 1", created, replayed, original.TaskVersion)
	}

	conflictInput := input
	conflictInput.ReuseMode = "recompute_from"
	conflictInput.RecomputeFrom = "ASR"
	_, _, err = service.CreateParseTask(ctx, conflictInput)
	if !errors.As(err, &serviceErr) || serviceErr.Status != 409 {
		t.Fatalf("same key with different request error = %v, want 409", err)
	}

	secondInput := input
	secondInput.RequestKey = "second-key"
	second, replay, err := service.CreateParseTask(ctx, secondInput)
	if err != nil || replay || second.TaskVersion != 2 {
		t.Fatalf("second version task = (%+v, replay=%t, err=%v), want version 2", second, replay, err)
	}
	thirdInput := input
	thirdInput.RequestKey = "third-key"
	_, _, err = service.CreateParseTask(ctx, thirdInput)
	if !errors.As(err, &serviceErr) || serviceErr.Status != 429 {
		t.Fatalf("third active task error = %v, want quota 429", err)
	}

	var taskCount, outboxCount int
	var nextVersion int32
	var latestTaskID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM parse_tasks WHERE video_id=$1`, videoID).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id IN (SELECT id FROM parse_tasks WHERE video_id=$1)`, videoID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT next_task_version, latest_task_id FROM videos WHERE id=$1`, videoID).Scan(&nextVersion, &latestTaskID); err != nil {
		t.Fatal(err)
	}
	if taskCount != 2 || outboxCount != 2 || nextVersion != 3 || latestTaskID != second.ID {
		t.Fatalf("task_count=%d outbox_count=%d next_version=%d latest_task=%s", taskCount, outboxCount, nextVersion, latestTaskID)
	}
}
