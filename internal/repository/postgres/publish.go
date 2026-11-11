package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"mirrorcore/internal/contentprofile"
)

var ErrPublishArtifactsInvalid = errors.New("parse result artifacts are incomplete or inconsistent")

type publishArtifacts struct {
	taskVersion              int32
	sourceBlobID             uuid.UUID
	transcriptStepState      string
	transcriptStepInputHash  []byte
	transcriptStepConfigHash []byte
	transcriptID             uuid.UUID
	transcriptState          string
	transcriptCount          int
	transcriptCommitted      int
	transcriptInputHash      []byte
	transcriptConfigHash     []byte
	chunkStepState           string
	chunkStepInputHash       []byte
	chunkStepConfigHash      []byte
	chunkSetID               uuid.UUID
	chunkTranscriptID        uuid.UUID
	chunkState               string
	chunkCount               int
	chunkInputHash           []byte
	chunkConfigHash          []byte
	chunkCounterFingerprint  []byte
	chunkConfigSnapshot      []byte
	embeddingStepState       string
	embeddingStepInputHash   []byte
	embeddingStepConfigHash  []byte
	embeddingSetID           uuid.UUID
	embeddingChunkSetID      uuid.UUID
	embeddingState           string
	embeddingCount           int
	embeddingInputHash       []byte
	embeddingConfigHash      []byte
	embeddingConfigSnapshot  []byte
}

type publishManifest struct {
	ID           uuid.UUID
	ResultType   string
	TranscriptID uuid.UUID
	ChunkSetID   uuid.UUID
	EmbeddingID  uuid.UUID
	TranscriptN  int
	ChunkN       int
	EmbeddingN   int
	Snapshot     []byte
}

// PublishResult persists an immutable manifest and atomically advances the
// video pointer only when this task version is greater than the active one.
func (r *StageRepository) PublishResult(ctx context.Context, task StageTask, workspaceID string) (string, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", fmt.Errorf("begin result publication: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return "", err
	}
	artifacts, err := loadPublishArtifacts(ctx, tx, task)
	if err != nil {
		return "", err
	}
	resultType, err := validatePublishArtifacts(ctx, tx, task.VideoID, artifacts)
	if err != nil {
		return "", err
	}
	snapshot, err := json.Marshal(map[string]any{
		"profile":                      "parse_result_v1",
		"transcript_id":                artifacts.transcriptID.String(),
		"transcript_input_fingerprint": HexHash(artifacts.transcriptInputHash),
		"transcript_config_hash":       HexHash(artifacts.transcriptConfigHash),
		"chunk_set_id":                 artifacts.chunkSetID.String(),
		"chunk_input_fingerprint":      HexHash(artifacts.chunkInputHash),
		"chunk_config_hash":            HexHash(artifacts.chunkConfigHash),
		"chunk_counter_fingerprint":    HexHash(artifacts.chunkCounterFingerprint),
		"chunk_config":                 json.RawMessage(artifacts.chunkConfigSnapshot),
		"embedding_set_id":             artifacts.embeddingSetID.String(),
		"embedding_input_fingerprint":  HexHash(artifacts.embeddingInputHash),
		"embedding_config_hash":        HexHash(artifacts.embeddingConfigHash),
		"embedding_config":             json.RawMessage(artifacts.embeddingConfigSnapshot),
	})
	if err != nil {
		return "", fmt.Errorf("encode result manifest configuration: %w", err)
	}
	manifest, err := loadOrCreateManifest(ctx, tx, task, artifacts, resultType, snapshot)
	if err != nil {
		return "", err
	}
	if resultType == "TEXT" {
		if err := seedInitialContentJob(ctx, tx, task, manifest, workspaceID); err != nil {
			return "", err
		}
	}
	var activeResultID *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT active_result_id FROM videos WHERE id=$1 AND owner_id=$2`,
		task.VideoID, task.OwnerID).Scan(&activeResultID); err != nil {
		return "", fmt.Errorf("read active result pointer: %w", err)
	}
	activeVersion := int32(0)
	if activeResultID != nil {
		if err := tx.QueryRow(ctx, `SELECT task_version FROM parse_results WHERE id=$1 AND video_id=$2`,
			*activeResultID, task.VideoID).Scan(&activeVersion); err != nil {
			return "", fmt.Errorf("read active result version: %w", err)
		}
	}
	outcome := "SKIPPED_OLDER"
	finalActiveVersion := activeVersion
	if activeResultID != nil && *activeResultID == manifest.ID {
		outcome = "ALREADY_ACTIVE"
	} else if artifacts.taskVersion > activeVersion {
		result, err := tx.Exec(ctx, `UPDATE videos SET active_result_id=$3
			WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE'`, task.VideoID, task.OwnerID, manifest.ID)
		if err != nil {
			return "", fmt.Errorf("advance active result pointer: %w", err)
		}
		if result.RowsAffected() != 1 {
			return "", ErrStageLeaseLost
		}
		outcome = "PUBLISHED"
		finalActiveVersion = artifacts.taskVersion
	}
	inputHash, _, err := HashJSON(struct {
		Profile      string    `json:"profile"`
		TaskVersion  int32     `json:"task_version"`
		ResultID     uuid.UUID `json:"result_id"`
		TranscriptID uuid.UUID `json:"transcript_id"`
		ChunkSetID   uuid.UUID `json:"chunk_set_id"`
		EmbeddingID  uuid.UUID `json:"embedding_set_id"`
	}{"parse_result_publish_v1", artifacts.taskVersion, manifest.ID, artifacts.transcriptID,
		artifacts.chunkSetID, artifacts.embeddingSetID})
	if err != nil {
		return "", fmt.Errorf("hash result publication input: %w", err)
	}
	configHash, _, err := HashJSON(struct {
		Profile string `json:"profile"`
		Rule    string `json:"rule"`
	}{"parse_result_publish_v1", "publish-if-task-version-exceeds-active"})
	if err != nil {
		return "", fmt.Errorf("hash result publication rule: %w", err)
	}
	stepMetadata, err := json.Marshal(map[string]any{"result_id": manifest.ID.String(), "result_type": resultType,
		"task_version": artifacts.taskVersion, "outcome": outcome, "active_task_version": finalActiveVersion})
	if err != nil {
		return "", fmt.Errorf("encode result publication checkpoint: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_result_id,output_metadata,failure_count,last_run_epoch,started_at,completed_at,updated_at)
		VALUES($1,$2,'PUBLISH','READY',$3,$4,$5,$6,$7::jsonb,0,$8,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state='READY',input_fingerprint=EXCLUDED.input_fingerprint,
		config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=NULL,
		output_asr_submission_id=NULL,output_transcript_id=NULL,output_chunk_set_id=NULL,
		output_embedding_set_id=NULL,output_result_id=EXCLUDED.output_result_id,output_metadata=EXCLUDED.output_metadata,
		last_run_epoch=EXCLUDED.last_run_epoch,completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`,
		task.ID, task.VideoID, inputHash, configHash, artifacts.sourceBlobID, manifest.ID, string(stepMetadata), task.RunEpoch)
	if err != nil {
		return "", fmt.Errorf("save PUBLISH checkpoint: %w", err)
	}
	result, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='SUCCEEDED',completed_at=clock_timestamp(),
		wait_reason=NULL,last_error=NULL,lease_owner=NULL,lease_until=NULL
		WHERE id=$1 AND video_id=$2 AND owner_id=$3 AND task_version=$4 AND current_stage='PUBLISH'
		AND status='RUNNING' AND run_epoch=$5 AND lease_owner=$6
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		task.ID, task.VideoID, task.OwnerID, artifacts.taskVersion, task.RunEpoch, task.LeaseOwner)
	if err != nil {
		return "", fmt.Errorf("complete published parse task: %w", err)
	}
	if result.RowsAffected() != 1 {
		return "", ErrStageLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit result publication: %w", err)
	}
	return outcome, nil
}

func seedInitialContentJob(ctx context.Context, tx pgx.Tx, task StageTask, manifest publishManifest, workspaceID string) error {
	var profileSnapshot []byte
	var profileHash []byte
	profileSnapshot, profileHash, err := contentprofile.NewSnapshotForWorkspace(workspaceID)
	if err != nil {
		return fmt.Errorf("build initial content profile: %w", err)
	}
	inputHash, _, err := HashJSON(struct {
		Profile     string    `json:"profile"`
		ResultID    uuid.UUID `json:"result_id"`
		Transcript  uuid.UUID `json:"transcript_id"`
		ProfileHash string    `json:"profile_hash"`
	}{contentprofile.ProfileVersion, manifest.ID, manifest.TranscriptID, HexHash(profileHash)})
	if err != nil {
		return fmt.Errorf("hash initial content input: %w", err)
	}
	jobID := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO content_jobs(id,owner_id,video_id,result_id,transcript_id,revision,
		profile_snapshot,profile_hash,input_hash,state,stage)
		VALUES($1,$2,$3,$4,$5,1,$6::jsonb,$7,$8,'READY','EXTRACT')
		ON CONFLICT(result_id,revision) DO NOTHING`, jobID, task.OwnerID, task.VideoID, manifest.ID,
		manifest.TranscriptID, string(profileSnapshot), profileHash, inputHash)
	if err != nil {
		return fmt.Errorf("seed initial content job: %w", err)
	}
	var savedJobID, savedTranscriptID uuid.UUID
	var savedProfileHash, savedInputHash []byte
	if err := tx.QueryRow(ctx, `SELECT id,transcript_id,profile_hash,input_hash FROM content_jobs
		WHERE result_id=$1 AND revision=1`, manifest.ID).Scan(&savedJobID, &savedTranscriptID,
		&savedProfileHash, &savedInputHash); err != nil {
		return fmt.Errorf("verify initial content job: %w", err)
	}
	if savedTranscriptID != manifest.TranscriptID || !bytes.Equal(savedProfileHash, profileHash) ||
		!bytes.Equal(savedInputHash, inputHash) {
		return errors.New("existing initial content job differs from the immutable result")
	}
	_, err = tx.Exec(ctx, `INSERT INTO result_content_heads(result_id,owner_id,video_id,next_revision,latest_job_id)
		VALUES($1,$2,$3,2,$4) ON CONFLICT(result_id) DO NOTHING`, manifest.ID, task.OwnerID, task.VideoID, savedJobID)
	if err != nil {
		return fmt.Errorf("create result content head: %w", err)
	}
	var savedOwnerID, savedVideoID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT owner_id,video_id FROM result_content_heads WHERE result_id=$1`, manifest.ID).
		Scan(&savedOwnerID, &savedVideoID); err != nil {
		return fmt.Errorf("verify result content head: %w", err)
	}
	if savedOwnerID != task.OwnerID || savedVideoID != task.VideoID {
		return errors.New("existing content head belongs to a different result owner")
	}
	return nil
}

func loadPublishArtifacts(ctx context.Context, tx pgx.Tx, task StageTask) (publishArtifacts, error) {
	var item publishArtifacts
	var sourceBlob *uuid.UUID
	err := tx.QueryRow(ctx, `SELECT task.task_version,ts.source_blob_id,
		ts.state,ts.input_fingerprint,ts.config_hash,tr.id,tr.state,tr.expected_segment_count,tr.last_committed_seq,tr.input_fingerprint,tr.config_hash,
		cs_step.state,cs_step.input_fingerprint,cs_step.config_hash,cs.id,cs.transcript_id,cs.state,cs.expected_chunk_count,cs.input_fingerprint,cs.config_hash,
		cs.counter_fingerprint,cs.config_snapshot,
		es_step.state,es_step.input_fingerprint,es_step.config_hash,es.id,es.chunk_set_id,es.state,es.expected_embedding_count,es.input_fingerprint,es.config_hash,
		es.config_snapshot
		FROM parse_tasks task
		JOIN task_steps ts ON ts.task_id=task.id AND ts.video_id=task.video_id AND ts.stage='TRANSCRIPT'
		JOIN transcripts tr ON tr.id=ts.output_transcript_id AND tr.video_id=task.video_id
		JOIN task_steps cs_step ON cs_step.task_id=task.id AND cs_step.video_id=task.video_id AND cs_step.stage='CHUNK'
		JOIN chunk_sets cs ON cs.id=cs_step.output_chunk_set_id AND cs.video_id=task.video_id
		JOIN task_steps es_step ON es_step.task_id=task.id AND es_step.video_id=task.video_id AND es_step.stage='EMBEDDING'
		JOIN embedding_sets es ON es.id=es_step.output_embedding_set_id AND es.video_id=task.video_id
		WHERE task.id=$1 AND task.video_id=$2 AND task.owner_id=$3 AND task.current_stage='PUBLISH'`,
		task.ID, task.VideoID, task.OwnerID).Scan(&item.taskVersion, &sourceBlob,
		&item.transcriptStepState, &item.transcriptStepInputHash, &item.transcriptStepConfigHash,
		&item.transcriptID, &item.transcriptState, &item.transcriptCount,
		&item.transcriptCommitted, &item.transcriptInputHash, &item.transcriptConfigHash,
		&item.chunkStepState, &item.chunkStepInputHash, &item.chunkStepConfigHash,
		&item.chunkSetID, &item.chunkTranscriptID, &item.chunkState,
		&item.chunkCount, &item.chunkInputHash, &item.chunkConfigHash, &item.chunkCounterFingerprint,
		&item.chunkConfigSnapshot, &item.embeddingStepState, &item.embeddingStepInputHash,
		&item.embeddingStepConfigHash, &item.embeddingSetID, &item.embeddingChunkSetID,
		&item.embeddingState, &item.embeddingCount, &item.embeddingInputHash, &item.embeddingConfigHash,
		&item.embeddingConfigSnapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return publishArtifacts{}, fmt.Errorf("%w: PUBLISH requires TRANSCRIPT, CHUNK, and EMBEDDING checkpoints", ErrPublishArtifactsInvalid)
	}
	if err != nil {
		return publishArtifacts{}, fmt.Errorf("load result publication inputs: %w", err)
	}
	if sourceBlob == nil || *sourceBlob == uuid.Nil {
		return publishArtifacts{}, fmt.Errorf("%w: result publication source blob is missing", ErrPublishArtifactsInvalid)
	}
	item.sourceBlobID = *sourceBlob
	return item, nil
}

func validatePublishArtifacts(ctx context.Context, tx pgx.Tx, videoID uuid.UUID, item publishArtifacts) (string, error) {
	if item.taskVersion <= 0 || item.transcriptID == uuid.Nil || item.chunkSetID == uuid.Nil || item.embeddingSetID == uuid.Nil ||
		len(item.transcriptStepInputHash) != sha256.Size || len(item.transcriptStepConfigHash) != sha256.Size ||
		len(item.chunkStepInputHash) != sha256.Size || len(item.chunkStepConfigHash) != sha256.Size ||
		len(item.embeddingStepInputHash) != sha256.Size || len(item.embeddingStepConfigHash) != sha256.Size ||
		len(item.transcriptInputHash) != sha256.Size || len(item.transcriptConfigHash) != sha256.Size ||
		len(item.chunkInputHash) != sha256.Size || len(item.chunkConfigHash) != sha256.Size ||
		len(item.chunkCounterFingerprint) != sha256.Size || len(item.embeddingInputHash) != sha256.Size ||
		len(item.embeddingConfigHash) != sha256.Size || !json.Valid(item.chunkConfigSnapshot) ||
		!json.Valid(item.embeddingConfigSnapshot) {
		return "", fmt.Errorf("%w: result publication artifact identity is incomplete", ErrPublishArtifactsInvalid)
	}
	if item.chunkTranscriptID != item.transcriptID || item.embeddingChunkSetID != item.chunkSetID ||
		!bytes.Equal(item.transcriptStepInputHash, item.transcriptInputHash) ||
		!bytes.Equal(item.transcriptStepConfigHash, item.transcriptConfigHash) ||
		!bytes.Equal(item.chunkStepInputHash, item.chunkInputHash) ||
		!bytes.Equal(item.chunkStepConfigHash, item.chunkConfigHash) ||
		!bytes.Equal(item.embeddingStepInputHash, item.embeddingInputHash) ||
		!bytes.Equal(item.embeddingStepConfigHash, item.embeddingConfigHash) ||
		item.transcriptStepState != item.transcriptState || item.chunkStepState != item.chunkState ||
		item.embeddingStepState != item.embeddingState || item.transcriptCommitted != item.transcriptCount {
		return "", fmt.Errorf("%w: result publication checkpoints do not match their artifacts", ErrPublishArtifactsInvalid)
	}
	var actualTranscriptCount, actualChunkCount, actualEmbeddingCount, chunksWithoutSources int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM transcript_segments WHERE transcript_id=$1 AND video_id=$2`,
		item.transcriptID, videoID).Scan(&actualTranscriptCount); err != nil {
		return "", fmt.Errorf("count result transcript segments: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM chunks WHERE chunk_set_id=$1 AND video_id=$2`,
		item.chunkSetID, videoID).Scan(&actualChunkCount); err != nil {
		return "", fmt.Errorf("count result chunks: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM chunk_embeddings WHERE embedding_set_id=$1 AND video_id=$2 AND chunk_set_id=$3`,
		item.embeddingSetID, videoID, item.chunkSetID).Scan(&actualEmbeddingCount); err != nil {
		return "", fmt.Errorf("count result embeddings: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM chunks c WHERE c.chunk_set_id=$1 AND c.video_id=$2
		AND NOT EXISTS (SELECT 1 FROM chunk_sources s WHERE s.chunk_id=c.id AND s.chunk_set_id=c.chunk_set_id
		AND s.video_id=c.video_id AND s.transcript_id=$3)`, item.chunkSetID, videoID, item.transcriptID).
		Scan(&chunksWithoutSources); err != nil {
		return "", fmt.Errorf("validate result chunk sources: %w", err)
	}
	if actualTranscriptCount != item.transcriptCount || actualChunkCount != item.chunkCount ||
		actualEmbeddingCount != item.embeddingCount || chunksWithoutSources != 0 ||
		item.chunkCount != item.embeddingCount {
		return "", fmt.Errorf("%w: result publication counts or source references are incomplete", ErrPublishArtifactsInvalid)
	}
	if item.transcriptState == "NO_SPEECH" && item.chunkState == "NO_CONTENT" && item.embeddingState == "NO_CONTENT" &&
		item.transcriptCount == 0 && item.chunkCount == 0 && item.embeddingCount == 0 {
		return "NO_SPEECH", nil
	}
	if item.transcriptState == "READY" && item.chunkState == "READY" && item.embeddingState == "READY" &&
		item.transcriptCount > 0 && item.chunkCount > 0 && item.embeddingCount == item.chunkCount {
		return "TEXT", nil
	}
	return "", fmt.Errorf("%w: result publication artifact states are incompatible", ErrPublishArtifactsInvalid)
}

func loadOrCreateManifest(ctx context.Context, tx pgx.Tx, task StageTask, artifacts publishArtifacts,
	resultType string, snapshot []byte) (publishManifest, error) {
	var manifest publishManifest
	err := tx.QueryRow(ctx, `SELECT id,result_type,transcript_id,chunk_set_id,embedding_set_id,
		transcript_segment_count,chunk_count,embedding_count,config_snapshot FROM parse_results
		WHERE task_id=$1 AND video_id=$2 FOR UPDATE`, task.ID, task.VideoID).
		Scan(&manifest.ID, &manifest.ResultType, &manifest.TranscriptID, &manifest.ChunkSetID,
			&manifest.EmbeddingID, &manifest.TranscriptN, &manifest.ChunkN, &manifest.EmbeddingN, &manifest.Snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		manifest = publishManifest{ID: uuid.New(), ResultType: resultType, TranscriptID: artifacts.transcriptID,
			ChunkSetID: artifacts.chunkSetID, EmbeddingID: artifacts.embeddingSetID,
			TranscriptN: artifacts.transcriptCount, ChunkN: artifacts.chunkCount, EmbeddingN: artifacts.embeddingCount,
			Snapshot: append([]byte(nil), snapshot...)}
		_, err = tx.Exec(ctx, `INSERT INTO parse_results(id,task_id,video_id,task_version,result_type,
			transcript_id,chunk_set_id,embedding_set_id,transcript_segment_count,chunk_count,embedding_count,config_snapshot)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)`, manifest.ID, task.ID, task.VideoID,
			artifacts.taskVersion, resultType, manifest.TranscriptID, manifest.ChunkSetID, manifest.EmbeddingID,
			manifest.TranscriptN, manifest.ChunkN, manifest.EmbeddingN, string(snapshot))
		if err != nil {
			return publishManifest{}, fmt.Errorf("insert immutable parse result manifest: %w", err)
		}
		return manifest, nil
	}
	if err != nil {
		return publishManifest{}, fmt.Errorf("load existing parse result manifest: %w", err)
	}
	if manifest.ResultType != resultType || manifest.TranscriptID != artifacts.transcriptID ||
		manifest.ChunkSetID != artifacts.chunkSetID || manifest.EmbeddingID != artifacts.embeddingSetID ||
		manifest.TranscriptN != artifacts.transcriptCount || manifest.ChunkN != artifacts.chunkCount ||
		manifest.EmbeddingN != artifacts.embeddingCount || !jsonEqual(manifest.Snapshot, snapshot) {
		return publishManifest{}, errors.New("existing parse result manifest differs from completed artifacts")
	}
	return manifest, nil
}
