package asrreconcile

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/providers/funasr"
	"mirrorcore/internal/repository/postgres"
)

const (
	maintenancePeriod   = 5 * time.Second
	queryTimeout        = 10 * time.Second
	maxClaimsPerPass    = 10
	defaultPollInterval = 15 * time.Second
)

type TaskQuerier interface {
	GetTask(context.Context, string) (funasr.Task, error)
}

type Worker struct {
	repository *postgres.ASRReconciliationRepository
	provider   TaskQuerier
	region     string
	workspace  string
	owner      string
	logger     *slog.Logger
}

func NewWorker(repository *postgres.ASRReconciliationRepository, provider TaskQuerier,
	region, workspace string, logger *slog.Logger) (*Worker, error) {
	if repository == nil || provider == nil || region == "" || workspace == "" {
		return nil, errors.New("ASR reconciler requires repository, provider, region, and workspace")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{repository: repository, provider: provider, region: region, workspace: workspace,
		owner: uuid.NewString(), logger: logger}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(maintenancePeriod)
	defer ticker.Stop()
	for {
		if err := w.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("Fun-ASR reconciliation pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) ReconcileOnce(ctx context.Context) error {
	for claimed := 0; claimed < maxClaimsPerPass; claimed++ {
		job, err := w.repository.ClaimNext(ctx, w.owner, w.region, w.workspace)
		if err != nil {
			return err
		}
		if job == nil {
			return nil
		}
		if job.State == "RESULT_READY" {
			status := strings.TrimSpace(job.ProviderStatus)
			if status == "" {
				status = "SUCCEEDED"
			}
			if err := w.repository.RecordProviderStatus(ctx, *job, status, "", true, time.Time{}); err != nil {
				w.recordFailure(ctx, *job, err)
				return err
			}
			w.logger.Info("orphaned Fun-ASR task is confirmed remotely complete",
				"submission_id", job.ID, "video_id", job.VideoID, "provider_status", status)
			continue
		}
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		providerTask, queryErr := w.provider.GetTask(queryCtx, job.ProviderTaskID)
		cancel()
		if queryErr != nil {
			delay, code := retryDelay(job.ReconcileAttempts, queryErr)
			if err := w.repository.RecordQueryFailure(ctx, *job, code, time.Now().Add(delay)); err != nil {
				w.logger.Error("could not persist Fun-ASR reconciliation retry",
					"submission_id", job.ID, "error", err)
				return errors.Join(queryErr, err)
			}
			w.logger.Warn("orphaned Fun-ASR task remains unconfirmed",
				"submission_id", job.ID, "video_id", job.VideoID,
				"provider_status", job.ProviderStatus, "query_error_code", code, "retry_in", delay)
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(providerTask.Status))
		if status == "" {
			status = "UNKNOWN"
		}
		if subtaskStatus := strings.ToUpper(strings.TrimSpace(providerTask.SubtaskStatus)); subtaskStatus != "" {
			status += "/" + subtaskStatus
		}
		terminal := isTerminal(providerTask.Status)
		delay := time.Time{}
		if !terminal {
			delay = time.Now().Add(defaultPollInterval)
		}
		if err := w.repository.RecordProviderStatus(ctx, *job, status, providerTask.RequestID, terminal, delay); err != nil {
			w.recordFailure(ctx, *job, err)
			return err
		}
		if terminal {
			w.logger.Info("orphaned Fun-ASR task reached a remote terminal status",
				"submission_id", job.ID, "video_id", job.VideoID, "provider_status", status)
		} else {
			w.logger.Debug("orphaned Fun-ASR task is still active remotely",
				"submission_id", job.ID, "video_id", job.VideoID, "provider_status", status)
		}
	}
	return nil
}

func (w *Worker) recordFailure(ctx context.Context, job postgres.ASRReconciliationJob, cause error) {
	code := "ASR_RECONCILIATION_CHECKPOINT_FAILED"
	if err := w.repository.RecordQueryFailure(ctx, job, code, time.Now().Add(defaultPollInterval)); err != nil {
		w.logger.Error("could not release Fun-ASR reconciliation lease", "submission_id", job.ID,
			"cause", cause, "error", err)
	}
}

func isTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED", "FAILED", "CANCELED", "CANCELLED", "EXPIRED":
		return true
	default:
		return false
	}
}

func retryDelay(attempt int, err error) (time.Duration, string) {
	if attempt < 1 {
		attempt = 1
	}
	exponent := min(attempt-1, 6)
	delay := 5 * time.Second * time.Duration(1<<exponent)
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	code := "ASR_QUERY_TRANSPORT_ERROR"
	var apiErr *funasr.APIError
	if errors.As(err, &apiErr) {
		code = apiErr.Code
		if code == "" {
			code = "ASR_QUERY_HTTP_ERROR"
		}
		if apiErr.StatusCode == 404 && delay < 15*time.Minute {
			delay = 15 * time.Minute
		}
		if apiErr.RetryAfter > delay {
			delay = apiErr.RetryAfter
		}
	}
	if delay > 30*time.Minute {
		delay = 30 * time.Minute
	}
	if len(code) > 80 {
		code = code[:80]
	}
	return delay, code
}
