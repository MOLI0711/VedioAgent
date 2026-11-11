package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const embeddingBatchSize = 10

var ErrEmbeddingBudgetExhausted = errors.New("embedding provider request budget exhausted")
var ErrEmbeddingArtifactMismatch = errors.New("embedding checkpoint does not match its inputs")

type EmbeddingInput struct {
	ID          uuid.UUID
	Seq         int
	Text        string
	TextSHA256  []byte
	InputSHA256 []byte
}

type EmbeddingInputSet struct {
	SourceBlobID          uuid.UUID
	ChunkSetID            uuid.UUID
	ChunkSetState         string
	ChunkInputFingerprint []byte
	ChunkConfigHash       []byte
	Chunks                []EmbeddingInput
}

type EmbeddingArtifact struct {
	ID            uuid.UUID
	State         string
	CreatedByTask uuid.UUID
	ExpectedCount int
}

type EmbeddingBatch struct {
	Index            int
	FirstSeq         int
	LastSeq          int
	InputFingerprint []byte
	Inputs           []EmbeddingInput
}

type EmbeddingReservation struct {
	AlreadySucceeded bool
	Attempt          int
}

func (r *StageRepository) LoadEmbeddingInput(ctx context.Context, task StageTask) (EmbeddingInputSet, error) {
	var input EmbeddingInputSet
	var sourceBlob *uuid.UUID
	var expected int
	err := r.pool.QueryRow(ctx, `SELECT step.source_blob_id,step.output_chunk_set_id,cs.state,
		cs.input_fingerprint,cs.config_hash,cs.expected_chunk_count
		FROM task_steps step JOIN chunk_sets cs ON cs.id=step.output_chunk_set_id AND cs.video_id=step.video_id
		JOIN videos v ON v.id=step.video_id AND v.owner_id=$3
		WHERE step.task_id=$1 AND step.video_id=$2 AND step.stage='CHUNK' AND step.state IN ('READY','NO_CONTENT')`,
		task.ID, task.VideoID, task.OwnerID).Scan(&sourceBlob, &input.ChunkSetID, &input.ChunkSetState,
		&input.ChunkInputFingerprint, &input.ChunkConfigHash, &expected)
	if errors.Is(err, pgx.ErrNoRows) {
		return EmbeddingInputSet{}, errors.New("EMBEDDING stage requires a committed CHUNK checkpoint")
	}
	if err != nil {
		return EmbeddingInputSet{}, fmt.Errorf("load chunk checkpoint for embedding: %w", err)
	}
	if sourceBlob == nil || *sourceBlob == uuid.Nil || input.ChunkSetID == uuid.Nil ||
		len(input.ChunkInputFingerprint) != sha256.Size || len(input.ChunkConfigHash) != sha256.Size ||
		(input.ChunkSetState != "READY" && input.ChunkSetState != "NO_CONTENT") {
		return EmbeddingInputSet{}, errors.New("chunk checkpoint identity is incomplete or not publishable")
	}
	input.SourceBlobID = *sourceBlob
	rows, err := r.pool.Query(ctx, `SELECT id,seq,text,text_sha256 FROM chunks WHERE chunk_set_id=$1 AND video_id=$2 ORDER BY seq`,
		input.ChunkSetID, task.VideoID)
	if err != nil {
		return EmbeddingInputSet{}, fmt.Errorf("read chunks for embedding: %w", err)
	}
	defer rows.Close()
	input.Chunks = make([]EmbeddingInput, 0, expected)
	for rows.Next() {
		var item EmbeddingInput
		if err := rows.Scan(&item.ID, &item.Seq, &item.Text, &item.TextSHA256); err != nil {
			return EmbeddingInputSet{}, fmt.Errorf("scan chunk for embedding: %w", err)
		}
		if item.Seq != len(input.Chunks)+1 || item.ID == uuid.Nil || item.Text == "" || len(item.TextSHA256) != sha256.Size {
			return EmbeddingInputSet{}, errors.New("chunk set sequence or content identity is invalid")
		}
		sum := sha256.Sum256([]byte(item.Text))
		if !bytes.Equal(sum[:], item.TextSHA256) {
			return EmbeddingInputSet{}, fmt.Errorf("chunk %d text checksum is invalid", item.Seq)
		}
		input.Chunks = append(input.Chunks, item)
	}
	if err := rows.Err(); err != nil {
		return EmbeddingInputSet{}, fmt.Errorf("iterate chunks for embedding: %w", err)
	}
	if len(input.Chunks) != expected || (input.ChunkSetState == "NO_CONTENT" && expected != 0) ||
		(input.ChunkSetState == "READY" && expected == 0) {
		return EmbeddingInputSet{}, errors.New("chunk set count does not match its complete checkpoint")
	}
	return input, nil
}

func EmbeddingInputFingerprint(chunkSetID uuid.UUID, records []EmbeddingInput) ([]byte, error) {
	items := make([]struct {
		ID         uuid.UUID `json:"id"`
		Seq        int       `json:"seq"`
		TextSHA256 string    `json:"text_sha256"`
	}, len(records))
	for i, record := range records {
		if record.ID == uuid.Nil || record.Seq != i+1 || len(record.TextSHA256) != sha256.Size {
			return nil, errors.New("embedding input fingerprint received an invalid chunk identity")
		}
		items[i] = struct {
			ID         uuid.UUID `json:"id"`
			Seq        int       `json:"seq"`
			TextSHA256 string    `json:"text_sha256"`
		}{record.ID, record.Seq, hex.EncodeToString(record.TextSHA256)}
	}
	hash, _, err := HashJSON(struct {
		Profile  string `json:"profile"`
		ChunkSet string `json:"chunk_set_id"`
		Chunks   any    `json:"chunks"`
	}{"embedding_input_v1", chunkSetID.String(), items})
	return hash, err
}

func EmbeddingBatchFingerprint(batchIndex int, records []EmbeddingInput) ([]byte, error) {
	if batchIndex < 0 || len(records) == 0 || len(records) > embeddingBatchSize {
		return nil, errors.New("embedding batch identity is out of bounds")
	}
	items := make([]struct {
		ID          uuid.UUID `json:"id"`
		Seq         int       `json:"seq"`
		InputSHA256 string    `json:"input_sha256"`
	}, len(records))
	for i, record := range records {
		if record.ID == uuid.Nil || record.Seq != batchIndex*embeddingBatchSize+i+1 || len(record.InputSHA256) != sha256.Size {
			return nil, errors.New("embedding batch contains an invalid input identity")
		}
		items[i] = struct {
			ID          uuid.UUID `json:"id"`
			Seq         int       `json:"seq"`
			InputSHA256 string    `json:"input_sha256"`
		}{record.ID, record.Seq, hex.EncodeToString(record.InputSHA256)}
	}
	hash, _, err := HashJSON(struct {
		Profile string `json:"profile"`
		Index   int    `json:"batch_index"`
		Inputs  any    `json:"inputs"`
	}{"embedding_batch_v1", batchIndex, items})
	return hash, err
}

func EmbeddingRecordFingerprint(chunkID uuid.UUID, textHash, configHash []byte) ([]byte, error) {
	if chunkID == uuid.Nil || len(textHash) != sha256.Size || len(configHash) != sha256.Size {
		return nil, errors.New("embedding input identity is invalid")
	}
	hash, _, err := HashJSON(struct {
		Profile    string `json:"profile"`
		ChunkID    string `json:"chunk_id"`
		TextSHA256 string `json:"text_sha256"`
		ConfigHash string `json:"config_hash"`
	}{"bailian_embedding_v1", chunkID.String(), hex.EncodeToString(textHash), hex.EncodeToString(configHash)})
	return hash, err
}

func (r *StageRepository) PrepareEmbeddingSet(ctx context.Context, task StageTask, input EmbeddingInputSet,
	inputFingerprint, configHash []byte, configSnapshot json.RawMessage, records []EmbeddingInput) (EmbeddingArtifact, error) {
	if input.ChunkSetID == uuid.Nil || len(inputFingerprint) != sha256.Size || len(configHash) != sha256.Size ||
		!json.Valid(configSnapshot) || len(records) > 1_000_000 || len(records) != len(input.Chunks) {
		return EmbeddingArtifact{}, errors.New("embedding set identity or output bounds are invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return EmbeddingArtifact{}, fmt.Errorf("begin embedding set preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return EmbeddingArtifact{}, err
	}
	rows, err := tx.Query(ctx, `SELECT id,state,created_by_task,expected_embedding_count,config_snapshot
		FROM embedding_sets WHERE video_id=$1 AND chunk_set_id=$2 AND input_fingerprint=$3 AND config_hash=$4
		AND state IN ('READY','NO_CONTENT') ORDER BY created_at,id FOR UPDATE`, task.VideoID, input.ChunkSetID,
		inputFingerprint, configHash)
	if err != nil {
		return EmbeddingArtifact{}, fmt.Errorf("find reusable embedding set: %w", err)
	}
	type candidate struct {
		artifact EmbeddingArtifact
		config   []byte
	}
	candidates := make([]candidate, 0)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.artifact.ID, &item.artifact.State, &item.artifact.CreatedByTask,
			&item.artifact.ExpectedCount, &item.config); err != nil {
			rows.Close()
			return EmbeddingArtifact{}, fmt.Errorf("scan reusable embedding set: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return EmbeddingArtifact{}, fmt.Errorf("iterate reusable embedding sets: %w", err)
	}
	rows.Close()
	for _, item := range candidates {
		if item.artifact.ExpectedCount == len(records) && jsonEqual(item.config, configSnapshot) {
			if err := verifyEmbeddingSetTx(ctx, tx, task.VideoID, input.ChunkSetID, item.artifact.ID, records); err == nil {
				if _, err := tx.Exec(ctx, `DELETE FROM chunk_embeddings WHERE embedding_set_id IN (
					SELECT id FROM embedding_sets WHERE video_id=$1 AND created_by_task=$2 AND chunk_set_id=$3
					AND input_fingerprint=$4 AND config_hash=$5 AND state='BUILDING')`, task.VideoID, task.ID,
					input.ChunkSetID, inputFingerprint, configHash); err != nil {
					return EmbeddingArtifact{}, fmt.Errorf("discard redundant building embedding rows after reuse: %w", err)
				}
				if _, err := tx.Exec(ctx, `UPDATE embedding_sets SET state='BROKEN',updated_at=clock_timestamp()
					WHERE video_id=$1 AND created_by_task=$2 AND chunk_set_id=$3 AND input_fingerprint=$4 AND config_hash=$5
					AND state='BUILDING'`, task.VideoID, task.ID, input.ChunkSetID, inputFingerprint, configHash); err != nil {
					return EmbeddingArtifact{}, fmt.Errorf("retire redundant building embedding set after reuse: %w", err)
				}
				if err := tx.Commit(ctx); err != nil {
					return EmbeddingArtifact{}, fmt.Errorf("commit reusable embedding set lookup: %w", err)
				}
				return item.artifact, nil
			} else if !errors.Is(err, ErrEmbeddingArtifactMismatch) {
				return EmbeddingArtifact{}, err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE embedding_sets SET state='BROKEN',updated_at=clock_timestamp()
			WHERE id=$1 AND state IN ('READY','NO_CONTENT')`, item.artifact.ID); err != nil {
			return EmbeddingArtifact{}, fmt.Errorf("mark inconsistent reusable embedding set broken: %w", err)
		}
	}
	var artifact EmbeddingArtifact
	err = tx.QueryRow(ctx, `SELECT id,state,created_by_task,expected_embedding_count FROM embedding_sets
		WHERE video_id=$1 AND created_by_task=$2 AND chunk_set_id=$3 AND input_fingerprint=$4 AND config_hash=$5
		AND state IN ('BUILDING','BROKEN') ORDER BY (state='BUILDING') DESC,created_at DESC LIMIT 1 FOR UPDATE`,
		task.VideoID, task.ID, input.ChunkSetID, inputFingerprint, configHash).
		Scan(&artifact.ID, &artifact.State, &artifact.CreatedByTask, &artifact.ExpectedCount)
	if errors.Is(err, pgx.ErrNoRows) {
		artifact = EmbeddingArtifact{ID: uuid.New(), State: "BUILDING", CreatedByTask: task.ID, ExpectedCount: len(records)}
		if _, err := tx.Exec(ctx, `INSERT INTO embedding_sets(id,video_id,chunk_set_id,created_by_task,input_fingerprint,
			config_hash,config_snapshot,state,expected_embedding_count) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,'BUILDING',$8)`,
			artifact.ID, task.VideoID, input.ChunkSetID, task.ID, inputFingerprint, configHash,
			string(configSnapshot), len(records)); err != nil {
			return EmbeddingArtifact{}, fmt.Errorf("create embedding set checkpoint: %w", err)
		}
	} else if err != nil {
		return EmbeddingArtifact{}, fmt.Errorf("load task-scoped embedding set checkpoint: %w", err)
	} else {
		var storedInput, storedConfig, storedSnapshot []byte
		if err := tx.QueryRow(ctx, `SELECT input_fingerprint,config_hash,config_snapshot FROM embedding_sets WHERE id=$1`, artifact.ID).
			Scan(&storedInput, &storedConfig, &storedSnapshot); err != nil {
			return EmbeddingArtifact{}, fmt.Errorf("verify task-scoped embedding configuration: %w", err)
		}
		if artifact.ExpectedCount != len(records) || !bytes.Equal(storedInput, inputFingerprint) ||
			!bytes.Equal(storedConfig, configHash) || !jsonEqual(storedSnapshot, configSnapshot) {
			return EmbeddingArtifact{}, errors.New("task-scoped embedding set differs from its immutable configuration")
		}
		if artifact.State == "BROKEN" {
			if _, err := tx.Exec(ctx, `DELETE FROM chunk_embeddings WHERE embedding_set_id=$1`, artifact.ID); err != nil {
				return EmbeddingArtifact{}, fmt.Errorf("clear broken embeddings: %w", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE embedding_sets SET state='BUILDING',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); err != nil {
				return EmbeddingArtifact{}, fmt.Errorf("reset broken embedding set: %w", err)
			}
			artifact.State = "BUILDING"
		}
		if artifact.State == "BUILDING" {
			if err := verifyEmbeddingPrefixTx(ctx, tx, task.VideoID, input.ChunkSetID, artifact.ID, records); err != nil {
				if !errors.Is(err, ErrEmbeddingArtifactMismatch) {
					return EmbeddingArtifact{}, err
				}
				if _, err := tx.Exec(ctx, `DELETE FROM chunk_embeddings WHERE embedding_set_id=$1`, artifact.ID); err != nil {
					return EmbeddingArtifact{}, fmt.Errorf("clear mismatched embedding prefix: %w", err)
				}
			}
		}
	}
	if artifact.State != "BUILDING" {
		return EmbeddingArtifact{}, errors.New("embedding set is not in a resumable state")
	}
	if err := tx.Commit(ctx); err != nil {
		return EmbeddingArtifact{}, fmt.Errorf("commit embedding set preparation: %w", err)
	}
	return artifact, nil
}

func (r *StageRepository) ReserveEmbeddingBatch(ctx context.Context, task StageTask, input EmbeddingInputSet,
	artifact EmbeddingArtifact, batch EmbeddingBatch, configHash []byte) (EmbeddingReservation, error) {
	if artifact.State != "BUILDING" || artifact.CreatedByTask != task.ID || artifact.ID == uuid.Nil ||
		batch.Index < 0 || len(batch.Inputs) == 0 || len(batch.Inputs) > embeddingBatchSize ||
		batch.FirstSeq != batch.Inputs[0].Seq || batch.LastSeq != batch.Inputs[len(batch.Inputs)-1].Seq ||
		batch.FirstSeq != batch.Index*embeddingBatchSize+1 || batch.LastSeq != batch.FirstSeq+len(batch.Inputs)-1 ||
		len(batch.InputFingerprint) != sha256.Size || len(configHash) != sha256.Size {
		return EmbeddingReservation{}, errors.New("embedding batch reservation is invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return EmbeddingReservation{}, fmt.Errorf("begin embedding batch reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return EmbeddingReservation{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO embedding_batches(task_id,video_id,chunk_set_id,config_hash,batch_index,
		first_chunk_seq,last_chunk_seq,expected_input_count,input_fingerprint,state)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDING') ON CONFLICT(task_id,chunk_set_id,config_hash,batch_index) DO NOTHING`,
		task.ID, task.VideoID, input.ChunkSetID, configHash, batch.Index, batch.FirstSeq, batch.LastSeq,
		len(batch.Inputs), batch.InputFingerprint); err != nil {
		return EmbeddingReservation{}, fmt.Errorf("create embedding batch budget record: %w", err)
	}
	var first, last, count, attempts int
	var storedFingerprint []byte
	var state string
	if err := tx.QueryRow(ctx, `SELECT first_chunk_seq,last_chunk_seq,expected_input_count,input_fingerprint,attempts,state
		FROM embedding_batches WHERE task_id=$1 AND chunk_set_id=$2 AND config_hash=$3 AND batch_index=$4 FOR UPDATE`,
		task.ID, input.ChunkSetID, configHash, batch.Index).
		Scan(&first, &last, &count, &storedFingerprint, &attempts, &state); err != nil {
		return EmbeddingReservation{}, fmt.Errorf("lock embedding request budget record: %w", err)
	}
	if first != batch.FirstSeq || last != batch.LastSeq || count != len(batch.Inputs) || !bytes.Equal(storedFingerprint, batch.InputFingerprint) {
		return EmbeddingReservation{}, errors.New("embedding batch identity changed after its first reservation")
	}
	if state == "SUCCEEDED" {
		if err := verifyEmbeddingBatchTx(ctx, tx, task.VideoID, input.ChunkSetID, artifact.ID, batch.Inputs); err != nil {
			if !errors.Is(err, ErrEmbeddingArtifactMismatch) {
				return EmbeddingReservation{}, fmt.Errorf("verify completed embedding batch: %w", err)
			}
			if attempts >= 3 {
				return EmbeddingReservation{}, ErrEmbeddingBudgetExhausted
			}
			for _, item := range batch.Inputs {
				if _, err := tx.Exec(ctx, `DELETE FROM chunk_embeddings WHERE embedding_set_id=$1 AND chunk_id=$2`, artifact.ID, item.ID); err != nil {
					return EmbeddingReservation{}, fmt.Errorf("clear invalid persisted vector for chunk %d: %w", item.Seq, err)
				}
			}
			state = "FAILED"
		} else {
			if err := tx.Commit(ctx); err != nil {
				return EmbeddingReservation{}, fmt.Errorf("commit completed embedding batch check: %w", err)
			}
			return EmbeddingReservation{AlreadySucceeded: true, Attempt: attempts}, nil
		}
	}
	if attempts >= 3 {
		return EmbeddingReservation{}, ErrEmbeddingBudgetExhausted
	}
	attempts++
	if _, err := tx.Exec(ctx, `UPDATE embedding_batches SET attempts=$5,state='IN_FLIGHT',last_error=NULL,updated_at=clock_timestamp()
		WHERE task_id=$1 AND chunk_set_id=$2 AND config_hash=$3 AND batch_index=$4`,
		task.ID, input.ChunkSetID, configHash, batch.Index, attempts); err != nil {
		return EmbeddingReservation{}, fmt.Errorf("reserve embedding provider request: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return EmbeddingReservation{}, fmt.Errorf("commit embedding provider request reservation: %w", err)
	}
	return EmbeddingReservation{Attempt: attempts}, nil
}

func (r *StageRepository) RecordEmbeddingBatchFailure(ctx context.Context, task StageTask, input EmbeddingInputSet,
	configHash []byte, batch EmbeddingBatch, code, message string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin embedding batch failure checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	encoded, _ := json.Marshal(map[string]string{"code": code, "message": boundedError(message)})
	result, err := tx.Exec(ctx, `UPDATE embedding_batches SET state='FAILED',last_error=$6::jsonb,updated_at=clock_timestamp()
		WHERE task_id=$1 AND chunk_set_id=$2 AND config_hash=$3 AND batch_index=$4
		AND input_fingerprint=$5 AND state='IN_FLIGHT'`, task.ID, input.ChunkSetID, configHash, batch.Index,
		batch.InputFingerprint, string(encoded))
	if err != nil {
		return fmt.Errorf("record embedding batch failure: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("embedding batch failure no longer matches its active reservation")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit embedding batch failure checkpoint: %w", err)
	}
	return nil
}

func (r *StageRepository) SaveEmbeddingBatch(ctx context.Context, task StageTask, input EmbeddingInputSet,
	artifact EmbeddingArtifact, configHash []byte, batch EmbeddingBatch, vectors [][]float32,
	requestID string, usageTokens int64) error {
	if artifact.State != "BUILDING" || artifact.CreatedByTask != task.ID || artifact.ID == uuid.Nil ||
		len(vectors) != len(batch.Inputs) || usageTokens < 0 || len(batch.Inputs) == 0 || len(batch.Inputs) > embeddingBatchSize {
		return errors.New("embedding batch output is invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin embedding vector save: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var setState string
	var owner uuid.UUID
	var expected int
	if err := tx.QueryRow(ctx, `SELECT state,created_by_task,expected_embedding_count FROM embedding_sets
		WHERE id=$1 AND video_id=$2 AND chunk_set_id=$3 FOR UPDATE`, artifact.ID, task.VideoID, input.ChunkSetID).
		Scan(&setState, &owner, &expected); err != nil {
		return fmt.Errorf("lock embedding set progress: %w", err)
	}
	if setState != "BUILDING" || owner != task.ID || expected != artifact.ExpectedCount {
		return errors.New("embedding batch no longer belongs to this build checkpoint")
	}
	var batchState string
	var storedFingerprint []byte
	if err := tx.QueryRow(ctx, `SELECT state,input_fingerprint FROM embedding_batches WHERE task_id=$1 AND chunk_set_id=$2
		AND config_hash=$3 AND batch_index=$4 FOR UPDATE`, task.ID, input.ChunkSetID, configHash, batch.Index).
		Scan(&batchState, &storedFingerprint); err != nil {
		return fmt.Errorf("lock reserved embedding batch: %w", err)
	}
	if batchState != "IN_FLIGHT" || !bytes.Equal(storedFingerprint, batch.InputFingerprint) {
		return errors.New("embedding result does not match its active reserved batch")
	}
	for i, record := range batch.Inputs {
		vector := vectors[i]
		if len(vector) != 1024 || !validVector(vector) || record.ID == uuid.Nil || len(record.InputSHA256) != sha256.Size {
			return fmt.Errorf("embedding result %d is invalid", i)
		}
		vectorText := formatVector(vector)
		if _, err := tx.Exec(ctx, `INSERT INTO chunk_embeddings(embedding_set_id,video_id,chunk_set_id,chunk_id,input_sha256,embedding)
			VALUES($1,$2,$3,$4,$5,$6::vector(1024)) ON CONFLICT(embedding_set_id,chunk_id) DO NOTHING`,
			artifact.ID, task.VideoID, input.ChunkSetID, record.ID, record.InputSHA256, vectorText); err != nil {
			return fmt.Errorf("persist embedding for chunk %d: %w", record.Seq, err)
		}
		var persistedHash []byte
		var persistedVector string
		if err := tx.QueryRow(ctx, `SELECT input_sha256,embedding::text FROM chunk_embeddings
			WHERE embedding_set_id=$1 AND chunk_id=$2`, artifact.ID, record.ID).Scan(&persistedHash, &persistedVector); err != nil {
			return fmt.Errorf("verify persisted embedding for chunk %d: %w", record.Seq, err)
		}
		if !bytes.Equal(persistedHash, record.InputSHA256) {
			return fmt.Errorf("persisted embedding differs for chunk %d: %w", record.Seq, ErrEmbeddingArtifactMismatch)
		}
		// A valid first committed vector is the durable artifact for this exact input/config.
		// Do not compare it numerically with a retried provider response; model output may vary.
		if _, err := parseVector(persistedVector); err != nil {
			return fmt.Errorf("persisted embedding is invalid for chunk %d: %w", record.Seq, ErrEmbeddingArtifactMismatch)
		}
	}
	result, err := tx.Exec(ctx, `UPDATE embedding_batches SET state='SUCCEEDED',provider_request_id=$5,usage_tokens=$6,
		last_error=NULL,updated_at=clock_timestamp() WHERE task_id=$1 AND chunk_set_id=$2 AND config_hash=$3
		AND batch_index=$4 AND state='IN_FLIGHT' AND input_fingerprint=$7`, task.ID, input.ChunkSetID,
		configHash, batch.Index, requestID, usageTokens, batch.InputFingerprint)
	if err != nil {
		return fmt.Errorf("complete embedding batch ledger: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("embedding batch reservation changed before commit")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit embedding vectors and batch ledger: %w", err)
	}
	return nil
}

func (r *StageRepository) CompleteEmbeddingSet(ctx context.Context, task StageTask, input EmbeddingInputSet,
	artifact EmbeddingArtifact, inputFingerprint, configHash []byte, configSnapshot json.RawMessage,
	records []EmbeddingInput) error {
	if artifact.ID == uuid.Nil || len(inputFingerprint) != sha256.Size || len(configHash) != sha256.Size ||
		!json.Valid(configSnapshot) || len(records) != artifact.ExpectedCount {
		return errors.New("embedding set completion identity is invalid")
	}
	state := "READY"
	if len(records) == 0 {
		state = "NO_CONTENT"
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin embedding set completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var currentState string
	var owner uuid.UUID
	var count int
	var storedInput, storedConfig, storedSnapshot []byte
	if err := tx.QueryRow(ctx, `SELECT state,created_by_task,expected_embedding_count,input_fingerprint,config_hash,config_snapshot FROM embedding_sets
		WHERE id=$1 AND video_id=$2 AND chunk_set_id=$3 FOR UPDATE`, artifact.ID, task.VideoID, input.ChunkSetID).
		Scan(&currentState, &owner, &count, &storedInput, &storedConfig, &storedSnapshot); err != nil {
		return fmt.Errorf("lock completed embedding set: %w", err)
	}
	if !bytes.Equal(storedInput, inputFingerprint) || !bytes.Equal(storedConfig, configHash) ||
		!jsonEqual(storedSnapshot, configSnapshot) {
		return errors.New("embedding set configuration changed before completion")
	}
	if count != len(records) || (currentState != "BUILDING" && currentState != state) ||
		(currentState == "BUILDING" && owner != task.ID) {
		return errors.New("embedding set is incomplete or has changed ownership")
	}
	if err := verifyEmbeddingSetTx(ctx, tx, task.VideoID, input.ChunkSetID, artifact.ID, records); err != nil {
		if currentState == "BUILDING" && errors.Is(err, ErrEmbeddingArtifactMismatch) {
			if _, updateErr := tx.Exec(ctx, `UPDATE embedding_sets SET state='BROKEN',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); updateErr != nil {
				return errors.Join(err, fmt.Errorf("mark incomplete embedding set broken: %w", updateErr))
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return errors.Join(err, fmt.Errorf("commit broken embedding set state: %w", commitErr))
			}
		}
		return fmt.Errorf("embedding rows do not match complete deterministic input: %w", err)
	}
	publishedSetID := artifact.ID
	if currentState == "BUILDING" {
		rows, err := tx.Query(ctx, `SELECT id,state,expected_embedding_count,config_snapshot
			FROM embedding_sets WHERE video_id=$1 AND chunk_set_id=$2 AND input_fingerprint=$3 AND config_hash=$4
			AND id<>$5 AND state IN ('READY','NO_CONTENT') ORDER BY created_at,id FOR UPDATE`, task.VideoID,
			input.ChunkSetID, inputFingerprint, configHash, artifact.ID)
		if err != nil {
			return fmt.Errorf("find concurrently completed embedding set: %w", err)
		}
		type reusableEmbedding struct {
			id     uuid.UUID
			state  string
			count  int
			config []byte
		}
		candidates := make([]reusableEmbedding, 0)
		for rows.Next() {
			var item reusableEmbedding
			if err := rows.Scan(&item.id, &item.state, &item.count, &item.config); err != nil {
				rows.Close()
				return fmt.Errorf("scan concurrently completed embedding set: %w", err)
			}
			candidates = append(candidates, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate concurrently completed embedding sets: %w", err)
		}
		rows.Close()
		for _, candidate := range candidates {
			candidateState := "READY"
			if len(records) == 0 {
				candidateState = "NO_CONTENT"
			}
			valid := candidate.state == candidateState && candidate.count == len(records) &&
				jsonEqual(candidate.config, configSnapshot)
			if valid {
				if verifyErr := verifyEmbeddingSetTx(ctx, tx, task.VideoID, input.ChunkSetID, candidate.id, records); verifyErr == nil {
					publishedSetID = candidate.id
					break
				} else if !errors.Is(verifyErr, ErrEmbeddingArtifactMismatch) {
					return verifyErr
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE embedding_sets SET state='BROKEN',updated_at=clock_timestamp()
				WHERE id=$1 AND state IN ('READY','NO_CONTENT')`, candidate.id); err != nil {
				return fmt.Errorf("mark invalid concurrent embedding set broken: %w", err)
			}
		}
		if publishedSetID != artifact.ID {
			if _, err := tx.Exec(ctx, `DELETE FROM chunk_embeddings WHERE embedding_set_id=$1`, artifact.ID); err != nil {
				return fmt.Errorf("discard duplicate building embedding rows: %w", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE embedding_sets SET state='BROKEN',updated_at=clock_timestamp()
				WHERE id=$1 AND state='BUILDING'`, artifact.ID); err != nil {
				return fmt.Errorf("retire duplicate building embedding set: %w", err)
			}
		} else if _, err := tx.Exec(ctx, `UPDATE embedding_sets SET state=$2,updated_at=clock_timestamp()
			WHERE id=$1 AND state='BUILDING'`, artifact.ID, state); err != nil {
			return fmt.Errorf("publish embedding set: %w", err)
		}
	}
	metadata, err := json.Marshal(map[string]any{"embedding_set_id": publishedSetID.String(), "embedding_count": len(records),
		"state": state, "input_fingerprint": HexHash(inputFingerprint), "config_hash": HexHash(configHash)})
	if err != nil {
		return fmt.Errorf("encode embedding checkpoint metadata: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_embedding_set_id,output_metadata,failure_count,last_run_epoch,started_at,completed_at,updated_at)
		VALUES($1,$2,'EMBEDDING',$3,$4,$5,$6,$7,$8::jsonb,0,$9,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state=EXCLUDED.state,input_fingerprint=EXCLUDED.input_fingerprint,
		config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=NULL,
		output_asr_submission_id=NULL,output_transcript_id=NULL,output_chunk_set_id=NULL,
		output_embedding_set_id=EXCLUDED.output_embedding_set_id,output_metadata=EXCLUDED.output_metadata,
		last_run_epoch=EXCLUDED.last_run_epoch,completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`,
		task.ID, task.VideoID, state, inputFingerprint, configHash, input.SourceBlobID, publishedSetID, string(metadata), task.RunEpoch)
	if err != nil {
		return fmt.Errorf("save embedding stage checkpoint: %w", err)
	}
	if err := advanceStage(ctx, tx, task, "PUBLISH"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit embedding set completion: %w", err)
	}
	return nil
}

func verifyEmbeddingSetTx(ctx context.Context, tx pgx.Tx, videoID, chunkSetID, embeddingSetID uuid.UUID,
	expected []EmbeddingInput) error {
	if err := verifyEmbeddingPrefixTx(ctx, tx, videoID, chunkSetID, embeddingSetID, expected); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM chunk_embeddings WHERE embedding_set_id=$1 AND video_id=$2`,
		embeddingSetID, videoID).Scan(&count); err != nil {
		return fmt.Errorf("count embeddings in set: %w", err)
	}
	if count != len(expected) {
		return fmt.Errorf("embedding count %d does not match expected %d: %w", count, len(expected), ErrEmbeddingArtifactMismatch)
	}
	return nil
}

func verifyEmbeddingPrefixTx(ctx context.Context, tx pgx.Tx, videoID, chunkSetID, embeddingSetID uuid.UUID,
	expected []EmbeddingInput) error {
	rows, err := tx.Query(ctx, `SELECT chunk_id,input_sha256,embedding::text FROM chunk_embeddings
		WHERE embedding_set_id=$1 AND video_id=$2 AND chunk_set_id=$3 ORDER BY chunk_id`, embeddingSetID, videoID, chunkSetID)
	if err != nil {
		return fmt.Errorf("read persisted embedding prefix: %w", err)
	}
	defer rows.Close()
	index := make(map[uuid.UUID]EmbeddingInput, len(expected))
	for _, item := range expected {
		index[item.ID] = item
	}
	for rows.Next() {
		var id uuid.UUID
		var hash []byte
		var vectorText string
		if err := rows.Scan(&id, &hash, &vectorText); err != nil {
			return fmt.Errorf("scan persisted embedding: %w", err)
		}
		item, ok := index[id]
		if !ok || !bytes.Equal(hash, item.InputSHA256) {
			return fmt.Errorf("persisted embedding input identity changed: %w", ErrEmbeddingArtifactMismatch)
		}
		if _, err := parseVector(vectorText); err != nil {
			return fmt.Errorf("persisted embedding vector is invalid: %w", ErrEmbeddingArtifactMismatch)
		}
		delete(index, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate persisted embeddings: %w", err)
	}
	return nil
}

func verifyEmbeddingBatchTx(ctx context.Context, tx pgx.Tx, videoID, chunkSetID, embeddingSetID uuid.UUID,
	expected []EmbeddingInput) error {
	for _, item := range expected {
		var hash []byte
		var vector string
		if err := tx.QueryRow(ctx, `SELECT input_sha256,embedding::text FROM chunk_embeddings
			WHERE embedding_set_id=$1 AND video_id=$2 AND chunk_set_id=$3 AND chunk_id=$4`,
			embeddingSetID, videoID, chunkSetID, item.ID).Scan(&hash, &vector); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("completed batch is missing vector for chunk %d: %w", item.Seq, ErrEmbeddingArtifactMismatch)
			}
			return fmt.Errorf("read completed batch vector for chunk %d: %w", item.Seq, err)
		}
		if !bytes.Equal(hash, item.InputSHA256) {
			return fmt.Errorf("completed batch input hash changed for chunk %d: %w", item.Seq, ErrEmbeddingArtifactMismatch)
		}
		if _, err := parseVector(vector); err != nil {
			return fmt.Errorf("completed batch vector is invalid for chunk %d: %w", item.Seq, ErrEmbeddingArtifactMismatch)
		}
	}
	return nil
}

func validVector(vector []float32) bool {
	if len(vector) != 1024 {
		return false
	}
	var norm float64
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return false
		}
		norm += float64(value) * float64(value)
	}
	return norm > 0 && !math.IsInf(norm, 0) && !math.IsNaN(norm)
}

func formatVector(vector []float32) string {
	var builder strings.Builder
	builder.Grow(len(vector) * 12)
	builder.WriteByte('[')
	for index, value := range vector {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
	}
	builder.WriteByte(']')
	return builder.String()
}

func parseVector(encoded string) ([]float32, error) {
	encoded = strings.TrimSpace(encoded)
	if len(encoded) < 2 || encoded[0] != '[' || encoded[len(encoded)-1] != ']' {
		return nil, errors.New("vector text has invalid delimiters")
	}
	parts := strings.Split(encoded[1:len(encoded)-1], ",")
	if len(parts) != 1024 {
		return nil, fmt.Errorf("vector has %d dimensions", len(parts))
	}
	values := make([]float32, len(parts))
	for i, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("vector dimension %d is not finite", i)
		}
		values[i] = float32(value)
	}
	if !validVector(values) {
		return nil, errors.New("vector is zero or invalid")
	}
	return values, nil
}
