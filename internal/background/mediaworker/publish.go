package mediaworker

import (
	"context"
	"errors"

	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/repository/postgres"
)

type PublishProcessor struct {
	repository  *postgres.StageRepository
	workspaceID string
}

func NewPublishProcessor(repository *postgres.StageRepository, workspaceID string) (*PublishProcessor, error) {
	if repository == nil {
		return nil, errors.New("publish processor requires a stage repository")
	}
	return &PublishProcessor{repository: repository, workspaceID: workspaceID}, nil
}

func (p *PublishProcessor) Process(ctx context.Context, task tasks.Task) error {
	stageTask := postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID,
		Stage: task.Stage, RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}
	if _, err := p.repository.PublishResult(ctx, stageTask, p.workspaceID); err != nil {
		if errors.Is(err, postgres.ErrPublishArtifactsInvalid) {
			return &StageFailure{Code: "PUBLISH_ARTIFACTS_INVALID", Retryable: false, Err: err}
		}
		return &StageFailure{Code: "PUBLISH_FAILED", Retryable: true, Err: err}
	}
	return nil
}
