package uploads

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

const (
	maintenancePeriod = 30 * time.Second
	maxSweepPerPass   = 50
)

type Manager struct {
	repository *postgres.UploadRepository
	store      ossstore.ObjectStore
	logger     *slog.Logger
}

func NewManager(repository *postgres.UploadRepository, store ossstore.ObjectStore, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{repository: repository, store: store, logger: logger}
}

// MaintainOnce aborts cancelled and expired multipart uploads after their
// presigned URLs, request leases, and grace periods have elapsed.
func (m *Manager) MaintainOnce(ctx context.Context) error {
	for i := 0; i < maxSweepPerPass; i++ {
		session, err := m.repository.ClaimNextTermination(ctx)
		if err != nil {
			return err
		}
		if session == nil {
			return nil
		}
		operationCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
		err = m.store.AbortMultipart(operationCtx, session.ObjectKey, session.UploadID)
		cancel()
		if err != nil {
			if releaseErr := m.repository.ReleaseTermination(ctx, *session); releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
			m.logger.Warn("OSS multipart cleanup will retry", "upload_session_id", session.ID,
				"operation_epoch", session.OperationEpoch, "error", err)
			return nil
		}
		if err := m.repository.FinishTermination(ctx, *session); err != nil {
			m.logger.Warn("could not persist OSS multipart cleanup", "upload_session_id", session.ID,
				"operation_epoch", session.OperationEpoch, "error", err)
			continue
		}
		m.logger.Info("OSS multipart upload cleaned up", "upload_session_id", session.ID,
			"terminal_reason", session.TerminationReason)
	}
	return nil
}

func (m *Manager) RunMaintenance(ctx context.Context) error {
	ticker := time.NewTicker(maintenancePeriod)
	defer ticker.Stop()
	for {
		if err := m.MaintainOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.Error("upload maintenance failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
