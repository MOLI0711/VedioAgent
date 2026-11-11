package mediaworker

import (
	"context"
	"errors"
	"fmt"

	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/providerquota"
	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/repository/postgres"
)

const maxEmbeddingBatchAttempts = 3

type EmbeddingProvider interface {
	EmbedDocuments(context.Context, []string) (embedding.Result, error)
}

// EmbeddingIdentity is persisted with an embedding set so vectors produced by
// a local fixture cannot be mistaken for vectors returned by a cloud model.
type EmbeddingIdentity struct {
	Profile          string `json:"profile"`
	Provider         string `json:"provider"`
	Region           string `json:"region"`
	WorkspaceID      string `json:"workspace_id"`
	Model            string `json:"model"`
	Dimension        int    `json:"dimension"`
	TextType         string `json:"text_type"`
	OutputType       string `json:"output_type"`
	BatchSize        int    `json:"batch_size"`
	MaxBatchAttempts int    `json:"max_batch_attempts"`
}

type EmbeddingProcessor struct {
	repository *postgres.StageRepository
	provider   EmbeddingProvider
	identity   EmbeddingIdentity
}

func NewEmbeddingProcessor(repository *postgres.StageRepository, provider EmbeddingProvider,
	region, workspaceID string) (*EmbeddingProcessor, error) {
	return NewEmbeddingProcessorWithIdentity(repository, provider, EmbeddingIdentity{
		Profile: "bailian_embedding_v1", Provider: "bailian-dashscope", Region: region,
		WorkspaceID: workspaceID, Model: embedding.Model, Dimension: embedding.Dimension,
		TextType: "document", OutputType: "dense", BatchSize: embedding.MaxBatch,
		MaxBatchAttempts: maxEmbeddingBatchAttempts,
	})
}

func NewEmbeddingProcessorWithIdentity(repository *postgres.StageRepository, provider EmbeddingProvider,
	identity EmbeddingIdentity) (*EmbeddingProcessor, error) {
	if repository == nil || identity.Profile == "" || identity.Provider == "" || identity.Region == "" ||
		identity.Model == "" || identity.Dimension != embedding.Dimension || identity.TextType != "document" ||
		identity.OutputType != "dense" || identity.BatchSize != embedding.MaxBatch ||
		identity.MaxBatchAttempts != maxEmbeddingBatchAttempts || (provider != nil && identity.WorkspaceID == "") {
		return nil, errors.New("embedding processor requires a valid provider identity and repository")
	}
	return &EmbeddingProcessor{repository: repository, provider: provider, identity: identity}, nil
}

func (p *EmbeddingProcessor) Process(ctx context.Context, task tasks.Task) error {
	stageTask := postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID,
		Stage: task.Stage, RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}
	input, err := p.repository.LoadEmbeddingInput(ctx, stageTask)
	if err != nil {
		return &StageFailure{Code: "EMBEDDING_INPUT_NOT_READY", Retryable: false, Err: err}
	}
	inputFingerprint, err := postgres.EmbeddingInputFingerprint(input.ChunkSetID, input.Chunks)
	if err != nil {
		return &StageFailure{Code: "EMBEDDING_INPUT_INVALID", Retryable: false, Err: err}
	}
	configHash, configSnapshot, err := postgres.HashJSON(p.identity)
	if err != nil {
		return err
	}
	if _, err := p.repository.StartStage(ctx, stageTask, input.SourceBlobID, inputFingerprint, configHash); err != nil {
		return err
	}
	records := make([]postgres.EmbeddingInput, len(input.Chunks))
	for index, item := range input.Chunks {
		records[index] = item
		records[index].InputSHA256, err = postgres.EmbeddingRecordFingerprint(item.ID, item.TextSHA256, configHash)
		if err != nil {
			return &StageFailure{Code: "EMBEDDING_INPUT_INVALID", Retryable: false, Err: err}
		}
	}
	artifact, err := p.repository.PrepareEmbeddingSet(ctx, stageTask, input, inputFingerprint, configHash,
		configSnapshot, records)
	if err != nil {
		return err
	}
	if artifact.State == "BUILDING" {
		if len(records) > 0 && p.provider == nil {
			return &StageFailure{Code: "EMBEDDING_PROVIDER_NOT_CONFIGURED", Retryable: false,
				Err: errors.New("Bailian API credentials are required to create document embeddings")}
		}
		for start := 0; start < len(records); start += embedding.MaxBatch {
			end := min(start+embedding.MaxBatch, len(records))
			batchRecords := records[start:end]
			batchIndex := start / embedding.MaxBatch
			batchFingerprint, err := postgres.EmbeddingBatchFingerprint(batchIndex, batchRecords)
			if err != nil {
				return &StageFailure{Code: "EMBEDDING_BATCH_INVALID", Retryable: false, Err: err}
			}
			batch := postgres.EmbeddingBatch{Index: batchIndex, FirstSeq: batchRecords[0].Seq,
				LastSeq: batchRecords[len(batchRecords)-1].Seq, InputFingerprint: batchFingerprint, Inputs: batchRecords}
			reservation, err := p.repository.ReserveEmbeddingBatch(ctx, stageTask, input, artifact, batch, configHash)
			if err != nil {
				if errors.Is(err, postgres.ErrEmbeddingBudgetExhausted) {
					return &StageFailure{Code: "EMBEDDING_REQUEST_BUDGET_EXHAUSTED", Retryable: false, Err: err}
				}
				return err
			}
			if reservation.AlreadySucceeded {
				continue
			}
			texts := make([]string, len(batchRecords))
			for i := range batchRecords {
				texts[i] = batchRecords[i].Text
			}
			result, callErr := p.provider.EmbedDocuments(ctx, texts)
			if callErr != nil {
				failure := embeddingFailure(callErr)
				if recordErr := p.repository.RecordEmbeddingBatchFailure(ctx, stageTask, input, configHash,
					batch, failure.Code, failure.Error()); recordErr != nil {
					return errors.Join(failure, fmt.Errorf("record failed embedding request: %w", recordErr))
				}
				return failure
			}
			vectors, err := embedding.ValidateVectors(result, len(batchRecords))
			if err != nil {
				failure := &StageFailure{Code: "EMBEDDING_RESPONSE_INVALID", Retryable: true, Err: err}
				if recordErr := p.repository.RecordEmbeddingBatchFailure(ctx, stageTask, input, configHash,
					batch, failure.Code, failure.Error()); recordErr != nil {
					return errors.Join(failure, fmt.Errorf("record invalid embedding response: %w", recordErr))
				}
				return failure
			}
			if err := p.repository.SaveEmbeddingBatch(ctx, stageTask, input, artifact, configHash, batch,
				vectors, result.RequestID, result.UsageTokens); err != nil {
				return &StageFailure{Code: "EMBEDDING_VECTOR_PERSIST_FAILED", Retryable: true, Err: err}
			}
		}
	} else if artifact.State != "READY" && artifact.State != "NO_CONTENT" {
		return &StageFailure{Code: "EMBEDDING_ARTIFACT_STATE_INVALID", Retryable: false,
			Err: fmt.Errorf("reusable embedding set has state %q", artifact.State)}
	}
	if err := p.repository.CompleteEmbeddingSet(ctx, stageTask, input, artifact, inputFingerprint,
		configHash, configSnapshot, records); err != nil {
		return err
	}
	return nil
}

func embeddingFailure(err error) *StageFailure {
	if errors.Is(err, providerquota.ErrEstimateExceedsTPM) {
		return &StageFailure{Code: "EMBEDDING_QUOTA_ESTIMATE_EXCEEDS_TPM", Retryable: false, Err: err}
	}
	var apiErr *embedding.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.Code
		if code == "" {
			code = "EMBEDDING_PROVIDER_ERROR"
		}
		return &StageFailure{Code: code, Retryable: apiErr.Retryable, RetryAfter: apiErr.RetryAfter, Err: err}
	}
	return &StageFailure{Code: "EMBEDDING_PROVIDER_ERROR", Retryable: true, Err: err}
}
