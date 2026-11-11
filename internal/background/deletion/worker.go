package deletion

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/observability"
	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

const (
	pollInterval       = 2 * time.Second
	maxObjectsPerClaim = 25
)

type Worker struct {
	repository *postgres.VideoDeletionRepository
	store      ossstore.ObjectStore
	bucket     string
	owner      string
	logger     *slog.Logger
	metrics    *observability.Metrics
}

func NewWorker(repository *postgres.VideoDeletionRepository, store ossstore.ObjectStore, bucket string,
	logger *slog.Logger, metrics ...*observability.Metrics) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	var workerMetrics *observability.Metrics
	if len(metrics) > 0 {
		workerMetrics = metrics[0]
	}
	return &Worker{repository: repository, store: store, bucket: bucket, owner: uuid.NewString(), logger: logger, metrics: workerMetrics}
}

func (w *Worker) Run(ctx context.Context) error {
	if w.repository == nil || w.store == nil || w.bucket == "" {
		return errors.New("video deletion worker requires database, OSS, and bucket")
	}
	for ctx.Err() == nil {
		job, err := w.repository.ClaimNext(ctx, w.owner)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.logger.Error("claim video deletion failed", "error", err)
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
		w.process(ctx, *job)
	}
	return ctx.Err()
}

func (w *Worker) process(ctx context.Context, job postgres.VideoDeletionJob) {
	started := time.Now()
	outcome := "failure"
	if w.metrics != nil {
		w.metrics.ObserveWorkerActive("deletion", 1)
		defer func() {
			w.metrics.ObserveWorkerActive("deletion", -1)
			w.metrics.ObserveWorkerStage("deletion", job.Phase, outcome, time.Since(started))
		}()
	}
	w.logger.Info("video deletion claimed", "deletion_id", job.ID, "video_id", job.VideoID,
		"phase", job.Phase, "run_epoch", job.RunEpoch)
	var err error
	switch job.Phase {
	case "WAIT_UPLOADS":
		var settled bool
		var blockingReason string
		settled, blockingReason, err = w.repository.UploadsSettled(ctx, job)
		if err == nil && !settled {
			cause := errors.New("video deletion is waiting for a dependent operation to settle")
			err = w.repository.RetryWithCode(ctx, job, cause, 30*time.Second, blockingReason)
			if err == nil {
				outcome = "retry_wait"
			}
			return
		}
		if err == nil {
			err = w.repository.PrepareObjects(ctx, job, w.bucket)
			if errors.Is(err, postgres.ErrDeletionWait) {
				err = w.repository.Retry(ctx, job, err, 30*time.Second)
				if err == nil {
					outcome = "retry_wait"
				}
				return
			}
			if err == nil {
				job.Phase = "DISCOVER_OBJECTS"
				err = w.discoverObjects(ctx, job)
			}
		}
	case "DISCOVER_OBJECTS", "DISCOVER_OBJECTS_FINAL":
		err = w.discoverObjects(ctx, job)
	case "DELETE_OBJECTS":
		err = w.deleteObjects(ctx, job)
	case "DELETE_OBJECTS_FINAL":
		err = w.deleteObjects(ctx, job)
	default:
		if isPurgePhase(job.Phase) {
			err = w.purgeRows(ctx, job)
		} else {
			err = errors.New("unknown video deletion phase")
		}
	}
	if err == nil {
		outcome = "success"
		return
	}
	if ctx.Err() != nil {
		outcome = "shutdown"
		return
	}
	w.logger.Warn("video deletion will retry", "deletion_id", job.ID, "video_id", job.VideoID,
		"phase", job.Phase, "run_epoch", job.RunEpoch, "error", err)
	if retryErr := w.repository.Retry(ctx, job, err, 10*time.Second); retryErr != nil && ctx.Err() == nil {
		w.logger.Error("could not persist video deletion retry", "deletion_id", job.ID, "error", retryErr)
		return
	}
	if ctx.Err() == nil {
		outcome = "retry_wait"
	} else {
		outcome = "shutdown"
	}
}

func (w *Worker) deleteObjects(ctx context.Context, job postgres.VideoDeletionJob) error {
	for i := 0; i < maxObjectsPerClaim; i++ {
		valid, err := w.repository.Renew(ctx, job)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("video deletion lease lost before OSS delete")
		}
		object, err := w.repository.ClaimObject(ctx, job)
		if err != nil {
			return err
		}
		if object == nil {
			if job.Phase == "DELETE_OBJECTS_FINAL" {
				if err := w.repository.AdvanceAfterFinalObjectDelete(ctx, job); err != nil {
					return err
				}
				job.Phase = "PURGE_GC_OBJECTS_FINAL"
				return w.purgeRows(ctx, job)
			}
			if err := w.repository.AdvanceToPurge(ctx, job); err != nil {
				return err
			}
			job.Phase = "CLEAR_VIDEO_REFS"
			return w.purgeRows(ctx, job)
		}
		if object.Bucket != w.bucket {
			cause := errors.New("blob bucket differs from configured OSS bucket")
			if recordErr := w.repository.FailObject(ctx, job, *object, cause); recordErr != nil {
				return recordErr
			}
			return cause
		}
		operationCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
		err = w.store.DeleteObject(operationCtx, object.Key, object.Version)
		cancel()
		if err != nil {
			if recordErr := w.repository.FailObject(ctx, job, *object, err); recordErr != nil {
				return recordErr
			}
			return err
		}
		if err := w.repository.FinishObject(ctx, job, *object); err != nil {
			return err
		}
		w.logger.Info("video object deleted", "deletion_id", job.ID, "video_id", job.VideoID,
			"object_id", object.ID, "run_epoch", job.RunEpoch)
	}
	return w.repository.Yield(ctx, job, time.Second)
}

func (w *Worker) purgeRows(ctx context.Context, job postgres.VideoDeletionJob) error {
	for batch := 0; batch < postgres.VideoGCBatchesPerClaim; batch++ {
		valid, err := w.repository.Renew(ctx, job)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("video deletion lease lost during batched metadata purge")
		}
		nextPhase, complete, err := w.repository.PurgeBatch(ctx, job)
		if err != nil {
			return err
		}
		if complete {
			w.logger.Info("video deletion completed", "deletion_id", job.ID, "video_id", job.VideoID)
			return nil
		}
		job.Phase = nextPhase
		if !isPurgePhase(job.Phase) {
			return w.repository.Yield(ctx, job, time.Second)
		}
	}
	return w.repository.Yield(ctx, job, time.Second)
}

func (w *Worker) discoverObjects(ctx context.Context, job postgres.VideoDeletionJob) error {
	prefixes := []string{
		"source/" + job.VideoID.String() + "/",
		"artifacts/audio/" + job.VideoID.String() + "/",
		"artifacts/asr/" + job.VideoID.String() + "/",
	}
	finalScan := job.Phase == "DISCOVER_OBJECTS_FINAL"
	for pageNumber := 0; pageNumber < postgres.VideoGCBatchesPerClaim; pageNumber++ {
		if int(job.ObjectScanPrefixIndex) >= len(prefixes) {
			return errors.New("persisted OSS scan prefix index is out of range")
		}
		valid, err := w.repository.Renew(ctx, job)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("video deletion lease lost during orphan object scan")
		}
		prefix := prefixes[job.ObjectScanPrefixIndex]
		operationCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
		page, err := w.store.ListObjectVersions(operationCtx, prefix, job.ObjectScanKeyMarker,
			job.ObjectScanVersionMarker)
		cancel()
		if err != nil {
			return err
		}
		objects := make([]postgres.DiscoveredObject, 0, len(page.Objects))
		for _, item := range page.Objects {
			objects = append(objects, postgres.DiscoveredObject{Key: item.Key, VersionID: item.VersionID})
		}
		nextPrefixIndex := job.ObjectScanPrefixIndex
		nextKeyMarker, nextVersionMarker := page.NextKeyMarker, page.NextVersionIDMarker
		if !page.IsTruncated {
			nextPrefixIndex++
			nextKeyMarker, nextVersionMarker = "", ""
		}
		phase, err := w.repository.CommitObjectScanPage(ctx, job, w.bucket, prefix, objects,
			nextPrefixIndex, nextKeyMarker, nextVersionMarker, page.IsTruncated, finalScan)
		if err != nil {
			return err
		}
		job.Phase = phase
		if phase == "DELETE_OBJECTS" || phase == "DELETE_OBJECTS_FINAL" {
			return w.repository.Yield(ctx, job, time.Second)
		}
		job.ObjectScanPrefixIndex = nextPrefixIndex
		job.ObjectScanKeyMarker = nextKeyMarker
		job.ObjectScanVersionMarker = nextVersionMarker
	}
	return w.repository.Yield(ctx, job, time.Second)
}

func isPurgePhase(phase string) bool {
	return strings.HasPrefix(phase, "PURGE_") || strings.HasPrefix(phase, "REDACT_") ||
		phase == "CLEAR_VIDEO_REFS" || phase == "FINALIZE"
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
