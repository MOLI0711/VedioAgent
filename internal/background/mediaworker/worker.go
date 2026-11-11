package mediaworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/observability"
	"mirrorcore/internal/repository/postgres"
)

const (
	workerCount = 2
	claimPoll   = time.Second
)

type Worker struct {
	tasks      *tasks.Manager
	repository *postgres.StageRepository
	processors map[string]StageProcessor
	stages     []string
	disk       *diskAdmission
	logger     *slog.Logger
	metrics    *observability.Metrics
}

type StageProcessor interface {
	Process(context.Context, tasks.Task) error
}

func NewWorker(taskManager *tasks.Manager, repository *postgres.StageRepository, processors map[string]StageProcessor,
	tempDir string, logger *slog.Logger, metrics ...*observability.Metrics) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	var workerMetrics *observability.Metrics
	if len(metrics) > 0 {
		workerMetrics = metrics[0]
	}
	stages := make([]string, 0, len(processors))
	for stage := range processors {
		stages = append(stages, stage)
	}
	sort.Strings(stages)
	return &Worker{tasks: taskManager, repository: repository, processors: processors, stages: stages,
		disk: newDiskAdmission(tempDir), logger: logger, metrics: workerMetrics}
}

func (w *Worker) Run(ctx context.Context) error {
	if len(w.stages) == 0 {
		return errors.New("media worker has no configured stages")
	}
	var group sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		group.Add(1)
		go func(slot int) {
			defer group.Done()
			w.runSlot(ctx, slot)
		}(i + 1)
	}
	<-ctx.Done()
	group.Wait()
	return ctx.Err()
}

func (w *Worker) runSlot(ctx context.Context, slot int) {
	for ctx.Err() == nil {
		task, err := w.tasks.ClaimNextStages(ctx, w.stages)
		if err != nil {
			if ctx.Err() == nil {
				w.logger.Error("claim media task failed", "slot", slot, "error", err)
			}
			if !wait(ctx, claimPoll) {
				return
			}
			continue
		}
		if task == nil {
			if !wait(ctx, claimPoll) {
				return
			}
			continue
		}
		w.logger.Info("media task claimed", "task_id", task.ID, "video_id", task.VideoID,
			"task_version", task.TaskVersion, "stage", task.Stage, "run_epoch", task.RunEpoch)
		var release func()
		if task.Stage == "PROBE" || task.Stage == "AUDIO" {
			var admitted bool
			release, admitted, err = w.disk.Reserve()
			if err != nil {
				w.logger.Error("media worker scratch capacity check failed", "slot", slot, "error", err)
				w.releaseForCapacity(*task)
				continue
			}
			if !admitted {
				w.releaseForCapacity(*task)
				continue
			}
		}
		w.process(ctx, slot, *task)
		if release != nil {
			release()
		}
	}
}

func (w *Worker) releaseForCapacity(task tasks.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.tasks.Wait(ctx, task, "SCRATCH_CAPACITY", time.Now().Add(5*time.Second)); err != nil && !errors.Is(err, tasks.ErrLeaseLost) {
		w.logger.Warn("could not release media task while waiting for scratch capacity", "task_id", task.ID, "error", err)
	}
}

func (w *Worker) process(ctx context.Context, slot int, task tasks.Task) {
	started := time.Now()
	outcome := "failure"
	if w.metrics != nil {
		w.metrics.ObserveWorkerActive("media", 1)
		defer func() {
			w.metrics.ObserveWorkerActive("media", -1)
			w.metrics.ObserveWorkerStage("media", task.Stage, outcome, time.Since(started))
		}()
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	leaseLost := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(tasks.LeaseRefresh)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				valid, err := w.tasks.RenewLease(renewCtx, task)
				cancel()
				if err != nil || !valid {
					select {
					case <-leaseLost:
					default:
						close(leaseLost)
					}
					cancelWork()
					if err != nil && ctx.Err() == nil {
						w.logger.Warn("media task lease renewal failed; cancelling stage", "task_id", task.ID,
							"run_epoch", task.RunEpoch, "error", err)
					}
					return
				}
			}
		}
	}()

	processor := w.processors[task.Stage]
	var processErr error
	if processor == nil {
		processErr = &StageFailure{Code: "UNSUPPORTED_WORKER_STAGE", Retryable: false,
			Err: fmt.Errorf("no worker processor is configured for stage %q", task.Stage)}
	} else {
		processErr = processor.Process(workCtx, task)
	}
	cancelWork()
	<-heartbeatDone
	select {
	case <-leaseLost:
		outcome = "lease_lost"
		w.logger.Warn("media stage stopped after lease loss", "task_id", task.ID, "stage", task.Stage, "run_epoch", task.RunEpoch)
		return
	default:
	}
	if processErr == nil {
		outcome = "success"
		w.logger.Info("media stage handler returned", "task_id", task.ID, "stage", task.Stage, "run_epoch", task.RunEpoch)
		return
	}
	if ctx.Err() != nil {
		outcome = "shutdown"
		w.releaseAfterShutdown(task)
		return
	}
	var stageFailure *StageFailure
	code, retryable, retryAfter := "MEDIA_STAGE_FAILED", true, time.Duration(0)
	if errors.As(processErr, &stageFailure) {
		code, retryable, retryAfter = stageFailure.Code, stageFailure.Retryable, stageFailure.RetryAfter
	}
	storeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	failures, err := w.repository.RecordStageFailure(storeCtx,
		postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID, Stage: task.Stage,
			RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}, code, processErr.Error(), retryable, retryAfter)
	cancel()
	if err != nil {
		outcome = "failure"
		w.logger.Warn("could not persist media stage failure", "task_id", task.ID, "stage", task.Stage,
			"run_epoch", task.RunEpoch, "slot", slot, "error", err)
		return
	}
	if retryable {
		outcome = "retry_wait"
	}
	w.logger.Warn("media stage failed", "task_id", task.ID, "stage", task.Stage,
		"run_epoch", task.RunEpoch, "failure_count", failures, "retryable", retryable,
		"code", code, "slot", slot, "error", processErr)
}

func (w *Worker) releaseAfterShutdown(task tasks.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lastError, _ := json.Marshal(map[string]any{"code": "WORKER_SHUTDOWN", "retryable": true})
	if err := w.tasks.Retry(ctx, task, time.Now().Add(time.Second), lastError); err != nil && !errors.Is(err, tasks.ErrLeaseLost) {
		w.logger.Warn("could not release media task during shutdown", "task_id", task.ID, "error", err)
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
