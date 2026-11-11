package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrChunkArtifactMismatch = errors.New("chunk checkpoint does not match deterministic output")

type ChunkInput struct {
	SourceBlobID          uuid.UUID
	TranscriptID          uuid.UUID
	TranscriptState       string
	TranscriptFingerprint []byte
	TranscriptConfigHash  []byte
	Segments              []TranscriptSegment
}

type ChunkSource struct {
	TranscriptSegmentSeq int
	StartByte            int
	EndByte              int
}

type ChunkRecord struct {
	ID         uuid.UUID
	Seq        int
	Text       string
	TextSHA256 []byte
	ByteCount  int
	StartMS    int64
	EndMS      int64
	Sources    []ChunkSource
}

type ChunkArtifact struct {
	ID            uuid.UUID
	State         string
	CreatedByTask uuid.UUID
	ExpectedCount int
}

func (r *StageRepository) LoadChunkInput(ctx context.Context, task StageTask) (ChunkInput, error) {
	var input ChunkInput
	var sourceBlobID *uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT step.output_transcript_id,t.state,t.input_fingerprint,t.config_hash,v.source_blob_id
		FROM task_steps step
		JOIN transcripts t ON t.id=step.output_transcript_id AND t.video_id=step.video_id
		JOIN videos v ON v.id=step.video_id AND v.owner_id=$3
		WHERE step.task_id=$1 AND step.video_id=$2 AND step.stage='TRANSCRIPT'
		  AND step.state IN ('READY','NO_SPEECH')`, task.ID, task.VideoID, task.OwnerID).
		Scan(&input.TranscriptID, &input.TranscriptState, &input.TranscriptFingerprint,
			&input.TranscriptConfigHash, &sourceBlobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChunkInput{}, errors.New("CHUNK stage requires a committed transcript checkpoint")
	}
	if err != nil {
		return ChunkInput{}, fmt.Errorf("load transcript checkpoint for chunk stage: %w", err)
	}
	if sourceBlobID == nil || *sourceBlobID == uuid.Nil || input.TranscriptID == uuid.Nil ||
		len(input.TranscriptFingerprint) != sha256.Size || len(input.TranscriptConfigHash) != sha256.Size {
		return ChunkInput{}, errors.New("transcript checkpoint identity is incomplete")
	}
	if input.TranscriptState != "READY" && input.TranscriptState != "NO_SPEECH" {
		return ChunkInput{}, fmt.Errorf("transcript artifact is not publishable: %s", input.TranscriptState)
	}
	input.SourceBlobID = *sourceBlobID
	var expected, committed int
	if err := r.pool.QueryRow(ctx, `SELECT expected_segment_count,last_committed_seq FROM transcripts
		WHERE id=$1 AND video_id=$2`, input.TranscriptID, task.VideoID).Scan(&expected, &committed); err != nil {
		return ChunkInput{}, fmt.Errorf("load transcript completeness: %w", err)
	}
	if expected != committed || (input.TranscriptState == "NO_SPEECH" && expected != 0) ||
		(input.TranscriptState == "READY" && expected == 0) {
		return ChunkInput{}, errors.New("transcript checkpoint is not a complete publishable artifact")
	}
	rows, err := r.pool.Query(ctx, `SELECT seq,source_sentence_index,provider_start_ms,provider_end_ms,
		start_correction_ms,start_ms,end_ms,end_correction_ms,text
		FROM transcript_segments WHERE transcript_id=$1 ORDER BY seq`, input.TranscriptID)
	if err != nil {
		return ChunkInput{}, fmt.Errorf("read transcript segments for chunk stage: %w", err)
	}
	defer rows.Close()
	input.Segments = make([]TranscriptSegment, 0, expected)
	for rows.Next() {
		var item TranscriptSegment
		if err := rows.Scan(&item.Seq, &item.SourceSentenceIndex, &item.ProviderStartMS, &item.ProviderEndMS,
			&item.StartCorrectionMS, &item.StartMS, &item.EndMS, &item.EndCorrectionMS, &item.Text); err != nil {
			return ChunkInput{}, fmt.Errorf("scan transcript segment for chunk stage: %w", err)
		}
		if item.Seq != len(input.Segments)+1 {
			return ChunkInput{}, errors.New("transcript segment sequence is not contiguous")
		}
		input.Segments = append(input.Segments, item)
	}
	if err := rows.Err(); err != nil {
		return ChunkInput{}, fmt.Errorf("read transcript segments for chunk stage: %w", err)
	}
	if len(input.Segments) != expected {
		return ChunkInput{}, errors.New("transcript segment count does not match its checkpoint")
	}
	return input, nil
}

func (r *StageRepository) PrepareChunkSet(ctx context.Context, task StageTask, input ChunkInput,
	inputFingerprint, configHash, counterFingerprint []byte, configSnapshot json.RawMessage,
	expected []ChunkRecord) (ChunkArtifact, error) {
	if input.TranscriptID == uuid.Nil || len(inputFingerprint) != sha256.Size || len(configHash) != sha256.Size ||
		len(counterFingerprint) != sha256.Size || !json.Valid(configSnapshot) || len(expected) > 1_000_000 {
		return ChunkArtifact{}, errors.New("chunk set identity or output bounds are invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ChunkArtifact{}, fmt.Errorf("begin chunk set preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return ChunkArtifact{}, err
	}

	rows, err := tx.Query(ctx, `SELECT id,state,created_by_task,expected_chunk_count,counter_fingerprint,config_snapshot
		FROM chunk_sets WHERE video_id=$1 AND transcript_id=$2 AND input_fingerprint=$3 AND config_hash=$4
		AND state IN ('READY','NO_CONTENT') ORDER BY created_at,id FOR UPDATE`,
		task.VideoID, input.TranscriptID, inputFingerprint, configHash)
	if err != nil {
		return ChunkArtifact{}, fmt.Errorf("find reusable chunk set: %w", err)
	}
	type reusableCandidate struct {
		artifact ChunkArtifact
		counter  []byte
		config   []byte
	}
	candidates := make([]reusableCandidate, 0)
	for rows.Next() {
		var candidate reusableCandidate
		if err := rows.Scan(&candidate.artifact.ID, &candidate.artifact.State, &candidate.artifact.CreatedByTask,
			&candidate.artifact.ExpectedCount, &candidate.counter, &candidate.config); err != nil {
			rows.Close()
			return ChunkArtifact{}, fmt.Errorf("scan reusable chunk set: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ChunkArtifact{}, fmt.Errorf("iterate reusable chunk sets: %w", err)
	}
	rows.Close()
	var reusable *ChunkArtifact
	for _, candidate := range candidates {
		if candidate.artifact.ExpectedCount != len(expected) || !bytes.Equal(candidate.counter, counterFingerprint) ||
			!jsonEqual(candidate.config, configSnapshot) {
			if _, markErr := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp()
				WHERE id=$1 AND state IN ('READY','NO_CONTENT')`, candidate.artifact.ID); markErr != nil {
				return ChunkArtifact{}, fmt.Errorf("mark inconsistent reusable chunk set broken: %w", markErr)
			}
			continue
		}
		if err := verifyChunkSetTx(ctx, tx, task.VideoID, input.TranscriptID, candidate.artifact.ID, expected); err == nil {
			reusable = &candidate.artifact
			break
		} else if errors.Is(err, ErrChunkArtifactMismatch) {
			if _, markErr := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp()
				WHERE id=$1 AND state IN ('READY','NO_CONTENT')`, candidate.artifact.ID); markErr != nil {
				return ChunkArtifact{}, errors.Join(err, fmt.Errorf("mark inconsistent reusable chunk set broken: %w", markErr))
			}
		} else {
			return ChunkArtifact{}, err
		}
	}
	if reusable != nil {
		if _, err := tx.Exec(ctx, `DELETE FROM chunks WHERE chunk_set_id IN (
			SELECT id FROM chunk_sets WHERE video_id=$1 AND created_by_task=$2 AND transcript_id=$3
			AND input_fingerprint=$4 AND config_hash=$5 AND state='BUILDING')`, task.VideoID, task.ID,
			input.TranscriptID, inputFingerprint, configHash); err != nil {
			return ChunkArtifact{}, fmt.Errorf("discard redundant building chunk rows after reuse: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp()
			WHERE video_id=$1 AND created_by_task=$2 AND transcript_id=$3 AND input_fingerprint=$4 AND config_hash=$5
			AND state='BUILDING'`, task.VideoID, task.ID, input.TranscriptID, inputFingerprint, configHash); err != nil {
			return ChunkArtifact{}, fmt.Errorf("retire redundant building chunk set after reuse: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ChunkArtifact{}, fmt.Errorf("commit reusable chunk set lookup: %w", err)
		}
		return *reusable, nil
	}

	var artifact ChunkArtifact
	err = tx.QueryRow(ctx, `SELECT id,state,created_by_task,expected_chunk_count
		FROM chunk_sets WHERE video_id=$1 AND created_by_task=$2 AND transcript_id=$3
		AND input_fingerprint=$4 AND config_hash=$5 AND state IN ('BUILDING','BROKEN')
		ORDER BY (state='BUILDING') DESC,created_at DESC LIMIT 1 FOR UPDATE`,
		task.VideoID, task.ID, input.TranscriptID, inputFingerprint, configHash).
		Scan(&artifact.ID, &artifact.State, &artifact.CreatedByTask, &artifact.ExpectedCount)
	if errors.Is(err, pgx.ErrNoRows) {
		artifact = ChunkArtifact{ID: uuid.New(), State: "BUILDING", CreatedByTask: task.ID, ExpectedCount: len(expected)}
		_, err = tx.Exec(ctx, `INSERT INTO chunk_sets(id,video_id,transcript_id,created_by_task,input_fingerprint,
			config_hash,counter_fingerprint,measurement_unit,config_snapshot,state,expected_chunk_count)
			VALUES($1,$2,$3,$4,$5,$6,$7,'UTF8_BYTES',$8::jsonb,'BUILDING',$9)`, artifact.ID, task.VideoID,
			input.TranscriptID, task.ID, inputFingerprint, configHash, counterFingerprint, string(configSnapshot), len(expected))
		if err != nil {
			return ChunkArtifact{}, fmt.Errorf("create chunk set checkpoint: %w", err)
		}
	} else if err != nil {
		return ChunkArtifact{}, fmt.Errorf("load task-scoped chunk set checkpoint: %w", err)
	} else {
		var storedCounter, storedConfig []byte
		if err := tx.QueryRow(ctx, `SELECT counter_fingerprint,config_snapshot FROM chunk_sets WHERE id=$1`, artifact.ID).
			Scan(&storedCounter, &storedConfig); err != nil {
			return ChunkArtifact{}, fmt.Errorf("verify task-scoped chunk set configuration: %w", err)
		}
		if artifact.ExpectedCount != len(expected) || !bytes.Equal(storedCounter, counterFingerprint) ||
			!jsonEqual(storedConfig, configSnapshot) {
			return ChunkArtifact{}, errors.New("task-scoped chunk set does not match its immutable configuration")
		}
		if artifact.State == "BROKEN" {
			if _, err := tx.Exec(ctx, `DELETE FROM chunks WHERE chunk_set_id=$1`, artifact.ID); err != nil {
				return ChunkArtifact{}, fmt.Errorf("clear broken chunk set rows: %w", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE chunk_sets SET state='BUILDING',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); err != nil {
				return ChunkArtifact{}, fmt.Errorf("reset broken chunk set: %w", err)
			}
			artifact.State = "BUILDING"
		}
		if artifact.State == "BUILDING" {
			if err := verifyChunkPrefixTx(ctx, tx, task.VideoID, input.TranscriptID, artifact.ID, expected); err != nil {
				if !errors.Is(err, ErrChunkArtifactMismatch) {
					return ChunkArtifact{}, err
				}
				if _, updateErr := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); updateErr != nil {
					return ChunkArtifact{}, fmt.Errorf("mark mismatched chunk prefix broken: %w", updateErr)
				}
				if _, deleteErr := tx.Exec(ctx, `DELETE FROM chunks WHERE chunk_set_id=$1`, artifact.ID); deleteErr != nil {
					return ChunkArtifact{}, fmt.Errorf("clear mismatched chunk prefix: %w", deleteErr)
				}
				if _, updateErr := tx.Exec(ctx, `UPDATE chunk_sets SET state='BUILDING',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); updateErr != nil {
					return ChunkArtifact{}, fmt.Errorf("restart chunk prefix after mismatch: %w", updateErr)
				}
			}
		}
	}
	if artifact.State != "BUILDING" {
		return ChunkArtifact{}, errors.New("task-scoped chunk set is not in a resumable state")
	}
	if err := tx.Commit(ctx); err != nil {
		return ChunkArtifact{}, fmt.Errorf("commit chunk set preparation: %w", err)
	}
	return artifact, nil
}

func (r *StageRepository) SaveChunkBatch(ctx context.Context, task StageTask, input ChunkInput,
	artifact ChunkArtifact, records []ChunkRecord) error {
	if artifact.ID == uuid.Nil || artifact.State != "BUILDING" || artifact.CreatedByTask != task.ID ||
		len(records) == 0 || len(records) > 100 {
		return errors.New("chunk batch bounds or ownership are invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin chunk batch: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var state string
	var owner uuid.UUID
	var expected int
	if err := tx.QueryRow(ctx, `SELECT state,created_by_task,expected_chunk_count FROM chunk_sets
		WHERE id=$1 AND video_id=$2 AND transcript_id=$3 FOR UPDATE`, artifact.ID, task.VideoID, input.TranscriptID).
		Scan(&state, &owner, &expected); err != nil {
		return fmt.Errorf("lock chunk set progress: %w", err)
	}
	if state != "BUILDING" || owner != task.ID || expected != artifact.ExpectedCount {
		return errors.New("chunk batch no longer belongs to this build checkpoint")
	}
	for _, record := range records {
		if record.ID == uuid.Nil || record.Seq <= 0 || record.Seq > expected || record.Text == "" ||
			len(record.TextSHA256) != sha256.Size || record.ByteCount <= 0 || record.StartMS < 0 || record.EndMS <= record.StartMS || len(record.Sources) == 0 {
			return errors.New("chunk batch contains an invalid chunk record")
		}
		textHash := sha256.Sum256([]byte(record.Text))
		if !bytes.Equal(textHash[:], record.TextSHA256) {
			return errors.New("chunk text checksum does not match its content")
		}
		for _, source := range record.Sources {
			if source.TranscriptSegmentSeq <= 0 || source.StartByte < 0 || source.EndByte <= source.StartByte {
				return errors.New("chunk batch contains an invalid transcript source range")
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO chunks(id,chunk_set_id,video_id,seq,text,text_sha256,byte_count,start_ms,end_ms)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(chunk_set_id,seq) DO NOTHING`,
			record.ID, artifact.ID, task.VideoID, record.Seq, record.Text, record.TextSHA256,
			record.ByteCount, record.StartMS, record.EndMS); err != nil {
			return fmt.Errorf("insert chunk %d: %w", record.Seq, err)
		}
		var actual ChunkRecord
		if err := tx.QueryRow(ctx, `SELECT id,seq,text,text_sha256,byte_count,start_ms,end_ms FROM chunks
			WHERE chunk_set_id=$1 AND seq=$2`, artifact.ID, record.Seq).
			Scan(&actual.ID, &actual.Seq, &actual.Text, &actual.TextSHA256, &actual.ByteCount, &actual.StartMS, &actual.EndMS); err != nil {
			return fmt.Errorf("verify persisted chunk %d: %w", record.Seq, err)
		}
		if !sameChunk(actual, record) {
			if _, err := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); err != nil {
				return fmt.Errorf("mark conflicting chunk set broken: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit mismatched chunk checkpoint: %w", err)
			}
			return fmt.Errorf("persisted chunk %d differs from deterministic recomputation: %w", record.Seq, ErrChunkArtifactMismatch)
		}
		for _, source := range record.Sources {
			if _, err := tx.Exec(ctx, `INSERT INTO chunk_sources(chunk_id,chunk_set_id,video_id,transcript_id,
				transcript_segment_seq,start_byte,end_byte) VALUES($1,$2,$3,$4,$5,$6,$7)
				ON CONFLICT(chunk_id,transcript_segment_seq,start_byte,end_byte) DO NOTHING`, record.ID,
				artifact.ID, task.VideoID, input.TranscriptID, source.TranscriptSegmentSeq, source.StartByte, source.EndByte); err != nil {
				return fmt.Errorf("insert source range for chunk %d: %w", record.Seq, err)
			}
		}
		if err := verifyChunkSourcesTx(ctx, tx, record.ID, input.TranscriptID, record.Sources); err != nil {
			if errors.Is(err, ErrChunkArtifactMismatch) {
				if _, updateErr := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); updateErr != nil {
					return fmt.Errorf("mark conflicting chunk sources broken: %w", updateErr)
				}
				if commitErr := tx.Commit(ctx); commitErr != nil {
					return fmt.Errorf("commit mismatched source ranges: %w", commitErr)
				}
			}
			return fmt.Errorf("verify source ranges for chunk %d: %w", record.Seq, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit chunk batch: %w", err)
	}
	return nil
}

func (r *StageRepository) CompleteChunkSet(ctx context.Context, task StageTask, input ChunkInput,
	artifact ChunkArtifact, inputFingerprint, configHash []byte, configSnapshot json.RawMessage,
	records []ChunkRecord) error {
	if artifact.ID == uuid.Nil || len(inputFingerprint) != sha256.Size || len(configHash) != sha256.Size ||
		!json.Valid(configSnapshot) || len(records) != artifact.ExpectedCount {
		return errors.New("chunk set completion identity is invalid")
	}
	state := "READY"
	if len(records) == 0 {
		state = "NO_CONTENT"
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin chunk set completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var artifactState string
	var owner uuid.UUID
	var expected int
	var storedInput, storedConfig, storedCounter, storedSnapshot []byte
	if err := tx.QueryRow(ctx, `SELECT state,created_by_task,expected_chunk_count,input_fingerprint,config_hash,
		counter_fingerprint,config_snapshot FROM chunk_sets
		WHERE id=$1 AND video_id=$2 AND transcript_id=$3 FOR UPDATE`, artifact.ID, task.VideoID, input.TranscriptID).
		Scan(&artifactState, &owner, &expected, &storedInput, &storedConfig, &storedCounter, &storedSnapshot); err != nil {
		return fmt.Errorf("lock completed chunk set: %w", err)
	}
	if !bytes.Equal(storedInput, inputFingerprint) || !bytes.Equal(storedConfig, configHash) ||
		!jsonEqual(storedSnapshot, configSnapshot) {
		return errors.New("chunk set configuration changed before completion")
	}
	if expected != len(records) || (artifactState != "BUILDING" && artifactState != state) ||
		(artifactState == "BUILDING" && owner != task.ID) {
		return errors.New("chunk set is not complete or has changed ownership")
	}
	if err := verifyChunkSetTx(ctx, tx, task.VideoID, input.TranscriptID, artifact.ID, records); err != nil {
		if artifactState == "BUILDING" && errors.Is(err, ErrChunkArtifactMismatch) {
			if _, updateErr := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp() WHERE id=$1`, artifact.ID); updateErr != nil {
				return errors.Join(err, fmt.Errorf("mark incomplete chunk set broken: %w", updateErr))
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return errors.Join(err, fmt.Errorf("commit broken chunk set state: %w", commitErr))
			}
		}
		return fmt.Errorf("chunk set rows do not match complete deterministic output: %w", err)
	}
	publishedSetID := artifact.ID
	if artifactState == "BUILDING" {
		rows, err := tx.Query(ctx, `SELECT id,state,expected_chunk_count,counter_fingerprint,config_snapshot
			FROM chunk_sets WHERE video_id=$1 AND transcript_id=$2 AND input_fingerprint=$3 AND config_hash=$4
			AND id<>$5 AND state IN ('READY','NO_CONTENT') ORDER BY created_at,id FOR UPDATE`, task.VideoID,
			input.TranscriptID, inputFingerprint, configHash, artifact.ID)
		if err != nil {
			return fmt.Errorf("find concurrently completed chunk set: %w", err)
		}
		type reusableChunk struct {
			id      uuid.UUID
			state   string
			count   int
			counter []byte
			config  []byte
		}
		candidates := make([]reusableChunk, 0)
		for rows.Next() {
			var item reusableChunk
			if err := rows.Scan(&item.id, &item.state, &item.count, &item.counter, &item.config); err != nil {
				rows.Close()
				return fmt.Errorf("scan concurrently completed chunk set: %w", err)
			}
			candidates = append(candidates, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate concurrently completed chunk sets: %w", err)
		}
		rows.Close()
		for _, candidate := range candidates {
			candidateState := "READY"
			if len(records) == 0 {
				candidateState = "NO_CONTENT"
			}
			candidateRecords := make([]ChunkRecord, len(records))
			copy(candidateRecords, records)
			for i := range candidateRecords {
				candidateRecords[i].ID = uuid.NewSHA1(uuid.NameSpaceURL,
					[]byte(fmt.Sprintf("mirrorcore:chunk:%s:%d", candidate.id, candidateRecords[i].Seq)))
			}
			valid := candidate.state == candidateState && candidate.count == len(records) &&
				bytes.Equal(candidate.counter, storedCounter) && jsonEqual(candidate.config, configSnapshot)
			if valid {
				if verifyErr := verifyChunkSetTx(ctx, tx, task.VideoID, input.TranscriptID, candidate.id, candidateRecords); verifyErr == nil {
					publishedSetID = candidate.id
					break
				} else if !errors.Is(verifyErr, ErrChunkArtifactMismatch) {
					return verifyErr
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp()
				WHERE id=$1 AND state IN ('READY','NO_CONTENT')`, candidate.id); err != nil {
				return fmt.Errorf("mark invalid concurrent chunk set broken: %w", err)
			}
		}
		if publishedSetID != artifact.ID {
			if _, err := tx.Exec(ctx, `DELETE FROM chunks WHERE chunk_set_id=$1`, artifact.ID); err != nil {
				return fmt.Errorf("discard duplicate building chunk rows: %w", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE chunk_sets SET state='BROKEN',updated_at=clock_timestamp()
				WHERE id=$1 AND state='BUILDING'`, artifact.ID); err != nil {
				return fmt.Errorf("retire duplicate building chunk set: %w", err)
			}
		} else if _, err := tx.Exec(ctx, `UPDATE chunk_sets SET state=$2,updated_at=clock_timestamp()
			WHERE id=$1 AND state='BUILDING'`, artifact.ID, state); err != nil {
			return fmt.Errorf("publish chunk set: %w", err)
		}
	}
	metadata, err := json.Marshal(map[string]any{"chunk_set_id": publishedSetID.String(), "chunk_count": len(records),
		"state": state, "input_fingerprint": HexHash(inputFingerprint), "config_hash": HexHash(configHash)})
	if err != nil {
		return fmt.Errorf("encode chunk checkpoint metadata: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_chunk_set_id,output_metadata,failure_count,last_run_epoch,started_at,completed_at,updated_at)
		VALUES($1,$2,'CHUNK',$3,$4,$5,$6,$7,$8::jsonb,0,$9,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state=EXCLUDED.state,input_fingerprint=EXCLUDED.input_fingerprint,
		config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=NULL,
		output_asr_submission_id=NULL,output_transcript_id=NULL,output_chunk_set_id=EXCLUDED.output_chunk_set_id,
		output_embedding_set_id=NULL,output_metadata=EXCLUDED.output_metadata,last_run_epoch=EXCLUDED.last_run_epoch,
		completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`, task.ID, task.VideoID,
		state, inputFingerprint, configHash, input.SourceBlobID, publishedSetID, string(metadata), task.RunEpoch)
	if err != nil {
		return fmt.Errorf("save chunk stage checkpoint: %w", err)
	}
	if err := advanceStage(ctx, tx, task, "EMBEDDING"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit chunk set completion: %w", err)
	}
	return nil
}

func verifyChunkSetTx(ctx context.Context, tx pgx.Tx, videoID, transcriptID, chunkSetID uuid.UUID,
	expected []ChunkRecord) error {
	rows, err := tx.Query(ctx, `SELECT id,seq,text,text_sha256,byte_count,start_ms,end_ms FROM chunks
		WHERE chunk_set_id=$1 AND video_id=$2 ORDER BY seq`, chunkSetID, videoID)
	if err != nil {
		return fmt.Errorf("read chunks for verification: %w", err)
	}
	actual := make([]ChunkRecord, 0, len(expected))
	for rows.Next() {
		var item ChunkRecord
		if err := rows.Scan(&item.ID, &item.Seq, &item.Text, &item.TextSHA256, &item.ByteCount, &item.StartMS, &item.EndMS); err != nil {
			rows.Close()
			return fmt.Errorf("scan chunk for verification: %w", err)
		}
		actual = append(actual, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate chunks for verification: %w", err)
	}
	rows.Close()
	if len(actual) != len(expected) {
		return fmt.Errorf("chunk count %d does not match expected %d: %w", len(actual), len(expected), ErrChunkArtifactMismatch)
	}
	for i := range actual {
		if !sameChunk(actual[i], expected[i]) {
			return fmt.Errorf("chunk %d differs from deterministic output: %w", i+1, ErrChunkArtifactMismatch)
		}
		if err := loadChunkSourcesTx(ctx, tx, &actual[i], transcriptID); err != nil {
			return err
		}
		if !reflect.DeepEqual(actual[i].Sources, expected[i].Sources) {
			return fmt.Errorf("chunk %d source ranges differ from deterministic output: %w", i+1, ErrChunkArtifactMismatch)
		}
	}
	return nil
}

func verifyChunkSourcesTx(ctx context.Context, tx pgx.Tx, chunkID, transcriptID uuid.UUID, expected []ChunkSource) error {
	var actual ChunkRecord
	actual.ID = chunkID
	if err := loadChunkSourcesTx(ctx, tx, &actual, transcriptID); err != nil {
		return err
	}
	if !reflect.DeepEqual(actual.Sources, expected) {
		return fmt.Errorf("persisted source ranges differ from deterministic recomputation: %w", ErrChunkArtifactMismatch)
	}
	return nil
}

func verifyChunkPrefixTx(ctx context.Context, tx pgx.Tx, videoID, transcriptID, chunkSetID uuid.UUID,
	expected []ChunkRecord) error {
	rows, err := tx.Query(ctx, `SELECT id,seq,text,text_sha256,byte_count,start_ms,end_ms FROM chunks
		WHERE chunk_set_id=$1 AND video_id=$2 ORDER BY seq`, chunkSetID, videoID)
	if err != nil {
		return fmt.Errorf("read chunk prefix for recovery: %w", err)
	}
	defer rows.Close()
	type persistedChunk struct{ record ChunkRecord }
	prefix := make([]persistedChunk, 0, len(expected))
	for rows.Next() {
		var actual ChunkRecord
		if err := rows.Scan(&actual.ID, &actual.Seq, &actual.Text, &actual.TextSHA256,
			&actual.ByteCount, &actual.StartMS, &actual.EndMS); err != nil {
			return fmt.Errorf("scan chunk prefix for recovery: %w", err)
		}
		prefix = append(prefix, persistedChunk{record: actual})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate chunk prefix for recovery: %w", err)
	}
	rows.Close()
	if len(prefix) > len(expected) {
		return fmt.Errorf("persisted chunk prefix exceeds deterministic output: %w", ErrChunkArtifactMismatch)
	}
	for index := range prefix {
		actual := &prefix[index].record
		if !sameChunk(*actual, expected[index]) {
			return fmt.Errorf("persisted chunk prefix differs at sequence %d: %w", index+1, ErrChunkArtifactMismatch)
		}
		if err := loadChunkSourcesTx(ctx, tx, actual, transcriptID); err != nil {
			return err
		}
		if !reflect.DeepEqual(actual.Sources, expected[index].Sources) {
			return fmt.Errorf("persisted chunk source prefix differs at sequence %d: %w", index+1, ErrChunkArtifactMismatch)
		}
	}
	return nil
}

func loadChunkSourcesTx(ctx context.Context, tx pgx.Tx, record *ChunkRecord, transcriptID uuid.UUID) error {
	rows, err := tx.Query(ctx, `SELECT transcript_segment_seq,start_byte,end_byte FROM chunk_sources
		WHERE chunk_id=$1 AND transcript_id=$2 ORDER BY transcript_segment_seq,start_byte,end_byte`, record.ID, transcriptID)
	if err != nil {
		return fmt.Errorf("read chunk source ranges: %w", err)
	}
	defer rows.Close()
	record.Sources = make([]ChunkSource, 0)
	for rows.Next() {
		var source ChunkSource
		if err := rows.Scan(&source.TranscriptSegmentSeq, &source.StartByte, &source.EndByte); err != nil {
			return fmt.Errorf("scan chunk source range: %w", err)
		}
		record.Sources = append(record.Sources, source)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate chunk source ranges: %w", err)
	}
	return nil
}

func sameChunk(actual, expected ChunkRecord) bool {
	return (expected.ID == uuid.Nil || actual.ID == expected.ID) && actual.Seq == expected.Seq && actual.Text == expected.Text &&
		bytes.Equal(actual.TextSHA256, expected.TextSHA256) && actual.ByteCount == expected.ByteCount &&
		actual.StartMS == expected.StartMS && actual.EndMS == expected.EndMS
}

func jsonEqual(a, b []byte) bool {
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}
