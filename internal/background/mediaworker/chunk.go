package mediaworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/chunk"
	"mirrorcore/internal/repository/postgres"
)

const chunkBatchSize = 100

type ChunkProcessor struct {
	repository *postgres.StageRepository
	counter    chunk.UTF8ByteCounter
	config     chunk.Config
}

func NewChunkProcessor(repository *postgres.StageRepository) (*ChunkProcessor, error) {
	if repository == nil {
		return nil, errors.New("chunk processor requires a repository")
	}
	config := chunk.DefaultConfig()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &ChunkProcessor{repository: repository, config: config}, nil
}

func (p *ChunkProcessor) Process(ctx context.Context, task tasks.Task) error {
	stageTask := postgres.StageTask{ID: task.ID, VideoID: task.VideoID, OwnerID: task.OwnerID,
		Stage: task.Stage, RunEpoch: task.RunEpoch, LeaseOwner: task.LeaseOwner}
	input, err := p.repository.LoadChunkInput(ctx, stageTask)
	if err != nil {
		return &StageFailure{Code: "CHUNK_INPUT_NOT_READY", Retryable: false, Err: err}
	}
	inputFingerprint, _, err := postgres.HashJSON(struct {
		TranscriptID          uuid.UUID `json:"transcript_id"`
		TranscriptFingerprint string    `json:"transcript_fingerprint"`
		TranscriptConfigHash  string    `json:"transcript_config_hash"`
	}{input.TranscriptID, hex.EncodeToString(input.TranscriptFingerprint), hex.EncodeToString(input.TranscriptConfigHash)})
	if err != nil {
		return err
	}
	identity := struct {
		Profile            string `json:"profile"`
		TargetBytes        int    `json:"target_bytes"`
		MaxBytes           int    `json:"max_bytes"`
		OverlapSentences   int    `json:"overlap_sentences"`
		MaxOverlapBytes    int    `json:"max_overlap_bytes"`
		MeasurementUnit    string `json:"measurement_unit"`
		CounterFingerprint string `json:"counter_fingerprint"`
	}{chunk.ProfileVersion, p.config.TargetBytes, p.config.MaxBytes, p.config.OverlapSentences,
		p.config.MaxOverlapBytes, "UTF8_BYTES", p.counter.FingerprintHex()}
	configHash, configSnapshot, err := postgres.HashJSON(identity)
	if err != nil {
		return err
	}
	if _, err := p.repository.StartStage(ctx, stageTask, input.SourceBlobID, inputFingerprint, configHash); err != nil {
		return err
	}
	chunks, err := chunk.Build(input.Segments, p.counter, p.config)
	if err != nil {
		return &StageFailure{Code: "CHUNK_BUILD_FAILED", Retryable: false, Err: err}
	}
	records := make([]postgres.ChunkRecord, len(chunks))
	for i, item := range chunks {
		sum := sha256.Sum256([]byte(item.Text))
		sources := make([]postgres.ChunkSource, len(item.Sources))
		for j, source := range item.Sources {
			sources[j] = postgres.ChunkSource{TranscriptSegmentSeq: source.TranscriptSegmentSeq,
				StartByte: source.StartByte, EndByte: source.EndByte}
		}
		records[i] = postgres.ChunkRecord{Seq: item.Seq, Text: item.Text, TextSHA256: sum[:],
			ByteCount: item.ByteCount, StartMS: item.StartMS, EndMS: item.EndMS, Sources: sources}
	}
	artifact, err := p.repository.PrepareChunkSet(ctx, stageTask, input, inputFingerprint, configHash,
		p.counter.Fingerprint(), configSnapshot, records)
	if err != nil {
		return err
	}
	for i := range records {
		records[i].ID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("mirrorcore:chunk:%s:%d", artifact.ID, records[i].Seq)))
	}
	if artifact.State == "BUILDING" {
		for start := 0; start < len(records); start += chunkBatchSize {
			end := min(start+chunkBatchSize, len(records))
			if err := p.repository.SaveChunkBatch(ctx, stageTask, input, artifact, records[start:end]); err != nil {
				return err
			}
		}
	} else if artifact.State != "READY" && artifact.State != "NO_CONTENT" {
		return &StageFailure{Code: "CHUNK_ARTIFACT_STATE_INVALID", Retryable: false,
			Err: fmt.Errorf("reusable chunk set has state %q", artifact.State)}
	}
	if err := p.repository.CompleteChunkSet(ctx, stageTask, input, artifact, inputFingerprint, configHash,
		configSnapshot, records); err != nil {
		return err
	}
	return nil
}
