package contentworker

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/content"
	"mirrorcore/internal/observability"
	"mirrorcore/internal/repository/postgres"
)

const pollInterval = time.Second

type Worker struct {
	service *content.Service
	owner   string
	logger  *slog.Logger
	metrics *observability.Metrics
}

func NewWorker(service *content.Service, logger *slog.Logger, metrics ...*observability.Metrics) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	var workerMetrics *observability.Metrics
	if len(metrics) > 0 {
		workerMetrics = metrics[0]
	}
	return &Worker{service: service, owner: uuid.NewString(), logger: logger, metrics: workerMetrics}
}

func (w *Worker) Run(ctx context.Context) error {
	if w.service == nil {
		return errors.New("content worker requires a content service")
	}
	nextMaintenance := time.Time{}
	for ctx.Err() == nil {
		if nextMaintenance.IsZero() || !time.Now().Before(nextMaintenance) {
			if reaped, err := w.service.Maintain(ctx); err != nil {
				w.logger.Error("content job maintenance failed", "error", err)
			} else if reaped > 0 {
				w.logger.Info("content jobs maintained", "count", reaped)
			}
			nextMaintenance = time.Now().Add(15 * time.Second)
		}
		job, err := w.service.Claim(ctx, w.owner)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.logger.Error("claim content job failed", "error", err)
			if !wait(ctx, pollInterval) {
				break
			}
			continue
		}
		if job == nil {
			if !wait(ctx, pollInterval) {
				break
			}
			continue
		}
		w.runJob(ctx, *job)
	}
	return ctx.Err()
}

func (w *Worker) runJob(ctx context.Context, job postgres.ContentJob) {
	started := time.Now()
	outcome := "failure"
	if w.metrics != nil {
		w.metrics.ObserveWorkerActive("content", 1)
		defer func() {
			w.metrics.ObserveWorkerActive("content", -1)
			w.metrics.ObserveWorkerStage("content", "content_job", outcome, time.Since(started))
		}()
	}
	w.logger.Info("content job claimed", "job_id", job.ID, "result_id", job.ResultID,
		"revision", job.Revision, "run_epoch", job.RunEpoch)
	workCtx, cancelWork := context.WithCancel(ctx)
	leaseLost := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				valid, err := w.service.Renew(renewCtx, job)
				cancel()
				if err != nil || !valid {
					close(leaseLost)
					cancelWork()
					if err != nil && ctx.Err() == nil {
						w.logger.Warn("content lease renewal failed; cancelling provider work",
							"job_id", job.ID, "run_epoch", job.RunEpoch, "error", err)
					}
					return
				}
			}
		}
	}()
	err := w.service.Process(workCtx, job)
	cancelWork()
	<-heartbeatDone
	select {
	case <-leaseLost:
		w.logger.Warn("content job stopped after lease loss", "job_id", job.ID, "run_epoch", job.RunEpoch)
		return
	default:
	}
	if err == nil {
		outcome = "success"
		w.logger.Info("content job completed", "job_id", job.ID, "revision", job.Revision)
		return
	}
	if ctx.Err() != nil {
		outcome = "shutdown"
		retryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if retryErr := w.service.Retry(retryCtx, job, time.Now().Add(time.Second), "WORKER_SHUTDOWN", "后台进程正在关闭"); retryErr != nil && !errors.Is(retryErr, postgres.ErrContentLeaseLost) {
			w.logger.Warn("could not release content job during shutdown", "job_id", job.ID, "error", retryErr)
		}
		return
	}
	var runErr *content.RunError
	if errors.As(err, &runErr) && runErr.Permanent {
		failCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if failErr := w.service.Fail(failCtx, job, runErr.Code, publicMessage(runErr)); failErr != nil {
			w.logger.Warn("could not persist terminal content failure", "job_id", job.ID, "error", failErr)
		}
		w.logger.Warn("content job failed permanently", "job_id", job.ID, "code", runErr.Code)
		return
	}
	code, message := "CONTENT_PROCESSING_FAILED", "内容处理暂时失败，将在后台重试"
	if runErr != nil {
		code, message = runErr.Code, publicMessage(runErr)
	}
	backoff := time.Duration(math.Min(float64(60), math.Pow(2, float64(min(job.RunEpoch, 6))))) * time.Second
	retryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if retryErr := w.service.Retry(retryCtx, job, time.Now().Add(backoff), code, message); retryErr != nil {
		outcome = "failure"
		w.logger.Warn("could not schedule content retry", "job_id", job.ID, "error", retryErr)
		return
	}
	outcome = "retry_wait"
	w.logger.Warn("content job scheduled for retry", "job_id", job.ID, "code", code,
		"run_epoch", job.RunEpoch, "retry_after", backoff)
}

func publicMessage(runErr *content.RunError) string {
	message := strings.TrimSpace(runErr.Message)
	if message == "" {
		return "内容生成失败"
	}
	if len(message) > 300 {
		message = message[:300]
	}
	return message
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
