package asrreconcile

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/database"
	"mirrorcore/internal/providers/funasr"
	"mirrorcore/internal/repository/postgres"
)

func TestLateASRAcceptanceAfterDeletionIsReconciledToRemoteTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool := openIsolatedASRDatabase(t, ctx)

	ownerID, videoID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO users(id,email,password_hash)
		VALUES($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test"); err != nil {
		t.Fatalf("insert ASR owner: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES($1,$2,'VALID','asr-reconcile.mp4',1024)`, videoID, ownerID); err != nil {
		t.Fatalf("insert ASR video: %v", err)
	}

	task, _, err := application.NewResourceService(postgres.NewResourceRepository(pool)).CreateParseTask(ctx,
		application.CreateParseTaskInput{OwnerID: ownerID, VideoID: videoID, RequestKey: uuid.NewString(),
			TraceID: uuid.NewString(), ProfileID: "voice-v1"})
	if err != nil {
		t.Fatalf("create ASR parse task: %v", err)
	}
	const runOwner = "asr-worker-old-epoch"
	if _, err := pool.Exec(ctx, `UPDATE parse_tasks SET status='RUNNING',current_stage='ASR',run_epoch=1,
		lease_owner=$2,lease_until=clock_timestamp()+interval '1 minute',admitted_at=clock_timestamp()
		WHERE id=$1`, task.ID, runOwner); err != nil {
		t.Fatalf("prepare active ASR execution: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		last_run_epoch,started_at) VALUES($1,$2,'ASR','RUNNING',$3,$3,1,clock_timestamp())`,
		task.ID, videoID, make([]byte, 32)); err != nil {
		t.Fatalf("insert ASR stage checkpoint: %v", err)
	}
	audioBlobID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,size_bytes,state)
		VALUES($1,$2,'AUDIO','integration-bucket',$3,64,'READY')`, audioBlobID, videoID,
		"artifacts/audio/"+videoID.String()+"/audio.wav"); err != nil {
		t.Fatalf("insert ready audio blob: %v", err)
	}
	stageTask := postgres.StageTask{ID: task.ID, VideoID: videoID, OwnerID: ownerID,
		Stage: "ASR", RunEpoch: 1, LeaseOwner: runOwner}
	stageRepository := postgres.NewStageRepository(pool)
	submission, err := stageRepository.ReserveASRSubmission(ctx, stageTask,
		postgres.ASRInput{AudioState: "READY", Audio: postgres.AudioBlob{ID: audioBlobID}},
		make([]byte, 32), make([]byte, 32), "cn-beijing", "integration-workspace", "fun-asr")
	if err != nil {
		t.Fatalf("reserve persistent ASR submission: %v", err)
	}
	submissionID := submission.ID
	if err := stageRepository.MarkASRUnknownAndWait(ctx, stageTask, submissionID,
		"SUBMIT_TRANSPORT_UNKNOWN", "connection reset after request write"); err != nil {
		t.Fatalf("persist unknown provider acceptance: %v", err)
	}
	assertDeletionBlocked(t, ctx, pool, videoID, "ASR_TASK_ID_UNAVAILABLE")

	deletion, err := application.NewResourceService(postgres.NewResourceRepository(pool)).DeleteVideo(ctx, ownerID, videoID)
	if err != nil {
		t.Fatalf("delete video with unknown ASR acceptance: %v", err)
	}
	if deletion.State != "PENDING" || deletion.Phase != "WAIT_UPLOADS" {
		t.Fatalf("deletion state = %s/%s, want PENDING/WAIT_UPLOADS", deletion.State, deletion.Phase)
	}
	var taskStatus string
	var epoch int64
	if err := pool.QueryRow(ctx, `SELECT status,run_epoch FROM parse_tasks WHERE id=$1`, task.ID).
		Scan(&taskStatus, &epoch); err != nil {
		t.Fatalf("read task after video deletion: %v", err)
	}
	if taskStatus != "CANCELLED" || epoch != 2 {
		t.Fatalf("task after deletion = %s/epoch%d, want CANCELLED/epoch2", taskStatus, epoch)
	}
	assertDeletionBlocked(t, ctx, pool, videoID, "ASR_TASK_ID_UNAVAILABLE")

	const lateProviderTaskID = "funasr-late-accepted-task"
	if err := stageRepository.RecordASRAccepted(ctx, stageTask, submissionID,
		lateProviderTaskID, "late-request-id", "PENDING"); err != nil {
		t.Fatalf("persist late acceptance from stale task epoch: %v", err)
	}
	assertDeletionBlocked(t, ctx, pool, videoID, "ASR_REMOTE_RECONCILIATION_PENDING")
	if _, err := pool.Exec(ctx, `UPDATE asr_submissions SET next_poll_at=clock_timestamp() WHERE id=$1`, submissionID); err != nil {
		t.Fatalf("make remote task due for reconciliation: %v", err)
	}

	provider := &terminalTaskQuerier{status: "CANCELED", requestID: "terminal-request-id"}
	worker, err := NewWorker(postgres.NewASRReconciliationRepository(pool), provider,
		"cn-beijing", "integration-workspace", nil)
	if err != nil {
		t.Fatalf("create ASR reconciliation worker: %v", err)
	}
	if err := worker.ReconcileOnce(ctx); err != nil {
		t.Fatalf("reconcile late-accepted ASR task: %v", err)
	}
	if len(provider.taskIDs) != 1 || provider.taskIDs[0] != lateProviderTaskID {
		t.Fatalf("provider task queries = %v, want [%s]", provider.taskIDs, lateProviderTaskID)
	}

	var state, providerStatus, providerRequestID string
	var remoteTerminalAt *time.Time
	var reconcileOwner *string
	var reconcileLeaseUntil *time.Time
	var attempts, reconcileEpoch int64
	if err := pool.QueryRow(ctx, `SELECT state,provider_status,provider_request_id,remote_terminal_at,
		reconcile_owner,reconcile_lease_until,reconcile_attempts,reconcile_epoch
		FROM asr_submissions WHERE id=$1`, submissionID).Scan(&state, &providerStatus, &providerRequestID,
		&remoteTerminalAt, &reconcileOwner, &reconcileLeaseUntil, &attempts, &reconcileEpoch); err != nil {
		t.Fatalf("read final ASR reconciliation state: %v", err)
	}
	if state != "REMOTE_TERMINAL" || providerStatus != "CANCELED" ||
		providerRequestID != "terminal-request-id" || remoteTerminalAt == nil ||
		reconcileOwner != nil || reconcileLeaseUntil != nil || attempts != 1 || reconcileEpoch != 1 {
		t.Fatalf("ASR final state=%s status=%s request=%s terminal_at=%v owner=%v lease=%v attempts=%d epoch=%d",
			state, providerStatus, providerRequestID, remoteTerminalAt, reconcileOwner, reconcileLeaseUntil, attempts, reconcileEpoch)
	}
	assertDeletionSettled(t, ctx, pool, videoID)
}

func TestASRSupplementBudgetSurvivesWorkerEpochChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool := openIsolatedASRDatabase(t, ctx)

	ownerID, videoID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO users(id,email,password_hash)
		VALUES($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test"); err != nil {
		t.Fatalf("insert ASR owner: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES($1,$2,'VALID','asr-budget.mp4',1024)`, videoID, ownerID); err != nil {
		t.Fatalf("insert ASR video: %v", err)
	}
	parseTask, _, err := application.NewResourceService(postgres.NewResourceRepository(pool)).CreateParseTask(ctx,
		application.CreateParseTaskInput{OwnerID: ownerID, VideoID: videoID, RequestKey: uuid.NewString(),
			TraceID: uuid.NewString(), ProfileID: "voice-v1"})
	if err != nil {
		t.Fatalf("create ASR parse task: %v", err)
	}
	const firstOwner = "asr-budget-epoch-one"
	if _, err := pool.Exec(ctx, `UPDATE parse_tasks SET status='RUNNING',current_stage='ASR',run_epoch=1,
		lease_owner=$2,lease_until=clock_timestamp()+interval '1 minute',admitted_at=clock_timestamp()
		WHERE id=$1`, parseTask.ID, firstOwner); err != nil {
		t.Fatalf("prepare first ASR execution: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		last_run_epoch,started_at) VALUES($1,$2,'ASR','RUNNING',$3,$3,1,clock_timestamp())`,
		parseTask.ID, videoID, make([]byte, 32)); err != nil {
		t.Fatalf("insert ASR stage checkpoint: %v", err)
	}
	audioBlobID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,size_bytes,state)
		VALUES($1,$2,'AUDIO','integration-bucket',$3,64,'READY')`, audioBlobID, videoID,
		"artifacts/audio/"+videoID.String()+"/audio.wav"); err != nil {
		t.Fatalf("insert ready audio blob: %v", err)
	}
	stageRepository := postgres.NewStageRepository(pool)
	input := postgres.ASRInput{AudioState: "READY", Audio: postgres.AudioBlob{ID: audioBlobID}}
	fingerprint, profileHash := make([]byte, 32), make([]byte, 32)
	firstStage := postgres.StageTask{ID: parseTask.ID, VideoID: videoID, OwnerID: ownerID,
		Stage: "ASR", RunEpoch: 1, LeaseOwner: firstOwner}
	first, err := stageRepository.ReserveASRSubmission(ctx, firstStage, input, fingerprint, profileHash,
		"cn-beijing", "integration-workspace", "fun-asr")
	if err != nil || first.Sequence != 1 {
		t.Fatalf("reserve first ASR submit = sequence %d, err=%v", first.Sequence, err)
	}
	if err := stageRepository.MarkASRUnknownAndWait(ctx, firstStage, first.ID,
		"SUBMIT_TRANSPORT_UNKNOWN", "connection reset after request write"); err != nil {
		t.Fatalf("persist first unknown acceptance: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE asr_submissions SET unknown_since=clock_timestamp()-interval '6 minutes'
		WHERE id=$1`, first.ID); err != nil {
		t.Fatalf("elapse unknown acceptance buffer in test: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE parse_tasks SET next_run_at=clock_timestamp() WHERE id=$1`, parseTask.ID); err != nil {
		t.Fatalf("make waiting ASR task due: %v", err)
	}

	manager := tasks.NewManager(pool, nil)
	secondExecution, err := manager.ClaimNextStages(ctx, []string{"ASR"})
	if err != nil || secondExecution == nil || secondExecution.RunEpoch != 2 {
		t.Fatalf("claim second ASR execution = %+v, err=%v; want epoch 2", secondExecution, err)
	}
	secondStage := postgres.StageTask{ID: secondExecution.ID, VideoID: secondExecution.VideoID,
		OwnerID: secondExecution.OwnerID, Stage: secondExecution.Stage,
		RunEpoch: secondExecution.RunEpoch, LeaseOwner: secondExecution.LeaseOwner}
	second, err := stageRepository.ReserveASRSubmission(ctx, secondStage, input, fingerprint, profileHash,
		"cn-beijing", "integration-workspace", "fun-asr")
	if err != nil || second.Sequence != 2 {
		t.Fatalf("reserve one allowed ASR supplement = sequence %d, err=%v", second.Sequence, err)
	}
	if err := stageRepository.RecordASRRejected(ctx, secondStage, second.ID,
		"PROVIDER_RATE_LIMITED", "rate limited", time.Second); err != nil {
		t.Fatalf("record second ASR rejection: %v", err)
	}
	if err := stageRepository.WaitASR(ctx, secondStage, second.ID, "ASR_RATE_LIMIT", time.Now().Add(time.Second)); err != nil {
		t.Fatalf("persist wait after second ASR attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE parse_tasks SET next_run_at=clock_timestamp() WHERE id=$1`, parseTask.ID); err != nil {
		t.Fatalf("make task due for next execution epoch: %v", err)
	}
	thirdExecution, err := manager.ClaimNextStages(ctx, []string{"ASR"})
	if err != nil || thirdExecution == nil || thirdExecution.RunEpoch != 3 {
		t.Fatalf("claim third ASR execution = %+v, err=%v; want epoch 3", thirdExecution, err)
	}
	thirdStage := postgres.StageTask{ID: thirdExecution.ID, VideoID: thirdExecution.VideoID,
		OwnerID: thirdExecution.OwnerID, Stage: thirdExecution.Stage,
		RunEpoch: thirdExecution.RunEpoch, LeaseOwner: thirdExecution.LeaseOwner}
	_, err = stageRepository.ReserveASRSubmission(ctx, thirdStage, input, fingerprint, profileHash,
		"cn-beijing", "integration-workspace", "fun-asr")
	if !errors.Is(err, postgres.ErrASRSubmissionBudget) {
		t.Fatalf("third ASR reservation error = %v, want persistent two-submission budget rejection", err)
	}

	var count int
	var taskVersion int32
	var finalEpoch int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM asr_submissions WHERE task_id=$1`, parseTask.ID).Scan(&count); err != nil {
		t.Fatalf("count persisted ASR submissions: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT task_version,run_epoch FROM parse_tasks WHERE id=$1`, parseTask.ID).
		Scan(&taskVersion, &finalEpoch); err != nil {
		t.Fatalf("read task version and epoch: %v", err)
	}
	if count != 2 || taskVersion != 1 || finalEpoch != 3 {
		t.Fatalf("submissions=%d task_version=%d run_epoch=%d, want 2/1/3", count, taskVersion, finalEpoch)
	}
}

func assertDeletionBlocked(t *testing.T, ctx context.Context, pool *pgxpool.Pool, videoID uuid.UUID, want string) {
	t.Helper()
	settled, reason, err := postgres.NewVideoDeletionRepository(pool).UploadsSettled(ctx,
		postgres.VideoDeletionJob{VideoID: videoID})
	if err != nil {
		t.Fatalf("check deletion ASR gate: %v", err)
	}
	if settled || reason != want {
		t.Fatalf("deletion ASR gate = settled:%t reason:%s, want blocked by %s", settled, reason, want)
	}
}

func assertDeletionSettled(t *testing.T, ctx context.Context, pool *pgxpool.Pool, videoID uuid.UUID) {
	t.Helper()
	settled, reason, err := postgres.NewVideoDeletionRepository(pool).UploadsSettled(ctx,
		postgres.VideoDeletionJob{VideoID: videoID})
	if err != nil {
		t.Fatalf("check settled deletion ASR gate: %v", err)
	}
	if !settled || reason != "" {
		t.Fatalf("deletion ASR gate after remote terminal = settled:%t reason:%s, want settled", settled, reason)
	}
}

func openIsolatedASRDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL ASR reconciliation integration test")
	}
	adminPool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := "asr_reconcile_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create isolated ASR test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse integration database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open isolated ASR test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate isolated ASR test schema: %v", err)
	}
	return pool
}

type terminalTaskQuerier struct {
	status    string
	requestID string
	taskIDs   []string
}

func (q *terminalTaskQuerier) GetTask(_ context.Context, taskID string) (funasr.Task, error) {
	q.taskIDs = append(q.taskIDs, taskID)
	if taskID == "" {
		return funasr.Task{}, errors.New("provider task ID is empty")
	}
	return funasr.Task{ID: taskID, Status: q.status, RequestID: q.requestID}, nil
}
