package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type TranscriptInput struct {
	ASRState            string
	ASRInputFingerprint []byte
	ASRProfileHash      []byte
	SourceBlobID        uuid.UUID
	RawResult           AudioBlob
	AudioBlobID         *uuid.UUID
	AudioOriginMS       int64
	MediaDurationMS     int64
}

type TranscriptArtifact struct {
	ID            uuid.UUID
	State         string
	ExpectedCount int
	LastCommitted int
}

type TranscriptSegment struct {
	Seq                 int
	SourceSentenceIndex int
	ProviderStartMS     int64
	ProviderEndMS       int64
	StartCorrectionMS   int32
	StartMS             int64
	EndMS               int64
	EndCorrectionMS     int32
	Text                string
}

func (r *StageRepository) LoadTranscriptInput(ctx context.Context, task StageTask) (TranscriptInput, error) {
	var input TranscriptInput
	var outputRaw, audioID *uuid.UUID
	var rawState, rawBucket, rawKey, rawVersion string
	var rawSize int64
	var rawSHA, rawFingerprint, rawProfile []byte
	var rawMetadata json.RawMessage
	var audioOrigin int64
	err := r.pool.QueryRow(ctx, `SELECT asr.state,asr.input_fingerprint,asr.config_hash,asr.source_blob_id,
		asr.output_blob_id,COALESCE(raw.id,'00000000-0000-0000-0000-000000000000'::uuid),
		COALESCE(raw.state,''),COALESCE(raw.bucket,''),COALESCE(raw.object_key,''),COALESCE(raw.object_version,''),
		COALESCE(raw.size_bytes,0),COALESCE(raw.sha256,decode('', 'hex')),
		COALESCE(raw.artifact_fingerprint,decode('', 'hex')),COALESCE(raw.profile_hash,decode('', 'hex')),
		COALESCE(raw.metadata,'{}'::jsonb),COALESCE((raw.metadata->>'audio_origin_ms')::bigint,0),
		audio.output_blob_id,v.media_duration_ms
		FROM task_steps asr JOIN videos v ON v.id=asr.video_id
		LEFT JOIN blobs raw ON raw.id=asr.output_blob_id AND raw.video_id=asr.video_id
		LEFT JOIN task_steps audio ON audio.task_id=asr.task_id AND audio.video_id=asr.video_id AND audio.stage='AUDIO'
		WHERE asr.task_id=$1 AND asr.video_id=$2 AND asr.stage='ASR'`, task.ID, task.VideoID).
		Scan(&input.ASRState, &input.ASRInputFingerprint, &input.ASRProfileHash, &input.SourceBlobID,
			&outputRaw, &input.RawResult.ID, &rawState, &rawBucket, &rawKey, &rawVersion, &rawSize,
			&rawSHA, &rawFingerprint, &rawProfile, &rawMetadata, &audioOrigin, &audioID, &input.MediaDurationMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return TranscriptInput{}, errors.New("TRANSCRIPT stage requires a committed ASR checkpoint")
	}
	if err != nil {
		return TranscriptInput{}, fmt.Errorf("load ASR result for transcript stage: %w", err)
	}
	if input.SourceBlobID == uuid.Nil || len(input.ASRInputFingerprint) != 32 || len(input.ASRProfileHash) != 32 || input.MediaDurationMS <= 0 {
		return TranscriptInput{}, errors.New("ASR checkpoint identity or source media duration is invalid")
	}
	if input.ASRState == "NO_AUDIO" {
		if outputRaw != nil {
			return TranscriptInput{}, errors.New("NO_AUDIO checkpoint unexpectedly references a raw ASR result")
		}
		return input, nil
	}
	if input.ASRState != "READY" || outputRaw == nil || input.RawResult.ID == uuid.Nil || rawState != "READY" ||
		rawBucket == "" || rawKey == "" || rawSize <= 0 || len(rawSHA) != 32 || len(rawFingerprint) != 32 || len(rawProfile) != 32 {
		return TranscriptInput{}, errors.New("ASR checkpoint does not reference a valid durable raw result")
	}
	input.AudioOriginMS = audioOrigin
	input.RawResult.Bucket, input.RawResult.ObjectKey, input.RawResult.ObjectVersion = rawBucket, rawKey, rawVersion
	input.RawResult.SizeBytes, input.RawResult.State, input.RawResult.SHA256 = rawSize, rawState, rawSHA
	input.RawResult.ArtifactFingerprint, input.RawResult.ProfileHash, input.RawResult.Metadata = rawFingerprint, rawProfile, rawMetadata
	if audioID != nil {
		input.AudioBlobID = audioID
	}
	if input.AudioOriginMS < -input.MediaDurationMS || input.AudioOriginMS > input.MediaDurationMS {
		return TranscriptInput{}, errors.New("audio timeline origin is outside the source video range")
	}
	return input, nil
}

// PrepareTranscript creates or loads the immutable transcript identity and its
// continuous-prefix checkpoint. The source payload and normalization version
// determine reuse; run_epoch only fences writes, not the artifact identity.
func (r *StageRepository) PrepareTranscript(ctx context.Context, task StageTask, input TranscriptInput,
	inputFingerprint, configHash []byte, expectedCount int) (TranscriptArtifact, error) {
	if len(inputFingerprint) != 32 || len(configHash) != 32 || expectedCount < 0 || expectedCount > 1_000_000 {
		return TranscriptArtifact{}, errors.New("transcript identity or expected segment count is invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return TranscriptArtifact{}, fmt.Errorf("begin transcript preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return TranscriptArtifact{}, err
	}
	id := uuid.New()
	var audioBlob any
	if input.AudioBlobID != nil {
		audioBlob = *input.AudioBlobID
	}
	_, err = tx.Exec(ctx, `INSERT INTO transcripts(id,video_id,source_asr_blob_id,source_audio_blob_id,created_by_task,
		input_fingerprint,config_hash,state,audio_origin_ms,media_duration_ms,expected_segment_count,last_committed_seq)
		VALUES($1,$2,NULLIF($3,'00000000-0000-0000-0000-000000000000'::uuid),$4,$5,$6,$7,'BUILDING',$8,$9,$10,0)
		ON CONFLICT(video_id,input_fingerprint,config_hash) DO NOTHING`, id, task.VideoID, input.RawResult.ID,
		audioBlob, task.ID, inputFingerprint, configHash, input.AudioOriginMS, input.MediaDurationMS, expectedCount)
	if err != nil {
		return TranscriptArtifact{}, fmt.Errorf("create transcript checkpoint: %w", err)
	}
	var artifact TranscriptArtifact
	var sourceASR uuid.UUID
	var sourceAudio *uuid.UUID
	var storedInput, storedConfig []byte
	var storedOrigin, storedDuration int64
	err = tx.QueryRow(ctx, `SELECT id,state,expected_segment_count,last_committed_seq,
		COALESCE(source_asr_blob_id,'00000000-0000-0000-0000-000000000000'::uuid),source_audio_blob_id,
		input_fingerprint,config_hash,audio_origin_ms,media_duration_ms
		FROM transcripts WHERE video_id=$1 AND input_fingerprint=$2 AND config_hash=$3 FOR UPDATE`,
		task.VideoID, inputFingerprint, configHash).
		Scan(&artifact.ID, &artifact.State, &artifact.ExpectedCount, &artifact.LastCommitted,
			&sourceASR, &sourceAudio, &storedInput, &storedConfig, &storedOrigin, &storedDuration)
	if err != nil {
		return TranscriptArtifact{}, fmt.Errorf("load transcript checkpoint: %w", err)
	}
	if sourceASR != input.RawResult.ID || !sameOptionalUUID(sourceAudio, input.AudioBlobID) ||
		artifact.ExpectedCount != expectedCount || len(storedInput) != 32 || len(storedConfig) != 32 ||
		storedOrigin != input.AudioOriginMS || storedDuration != input.MediaDurationMS {
		return TranscriptArtifact{}, errors.New("existing transcript checkpoint does not match its immutable input")
	}
	if artifact.State == "BROKEN" {
		if _, err := tx.Exec(ctx, `DELETE FROM transcript_segments WHERE transcript_id=$1`, artifact.ID); err != nil {
			return TranscriptArtifact{}, fmt.Errorf("clear broken transcript segments: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE transcripts SET state='BUILDING',last_committed_seq=0,updated_at=clock_timestamp()
			WHERE id=$1`, artifact.ID); err != nil {
			return TranscriptArtifact{}, fmt.Errorf("reset broken transcript checkpoint: %w", err)
		}
		artifact.State, artifact.LastCommitted = "BUILDING", 0
	}
	if err := tx.Commit(ctx); err != nil {
		return TranscriptArtifact{}, fmt.Errorf("commit transcript preparation: %w", err)
	}
	return artifact, nil
}

func (r *StageRepository) SaveTranscriptBatch(ctx context.Context, task StageTask, transcriptID uuid.UUID,
	startSeq, expectedCount int, segments []TranscriptSegment) error {
	if transcriptID == uuid.Nil || startSeq <= 0 || len(segments) == 0 || len(segments) > 200 {
		return errors.New("transcript batch bounds are invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transcript segment batch: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var state string
	var expected, last int
	if err := tx.QueryRow(ctx, `SELECT state,expected_segment_count,last_committed_seq FROM transcripts
		WHERE id=$1 AND video_id=$2 FOR UPDATE`, transcriptID, task.VideoID).Scan(&state, &expected, &last); err != nil {
		return fmt.Errorf("lock transcript progress: %w", err)
	}
	if state != "BUILDING" || expected != expectedCount || startSeq != last+1 || len(segments) > expected-last {
		return errors.New("transcript batch no longer follows the continuous committed prefix")
	}
	for i, segment := range segments {
		if segment.Seq != startSeq+i || segment.SourceSentenceIndex <= 0 || segment.StartMS < 0 || segment.EndMS <= segment.StartMS ||
			segment.ProviderStartMS < 0 || segment.ProviderEndMS <= segment.ProviderStartMS ||
			segment.StartCorrectionMS < 0 || segment.EndCorrectionMS > 0 || segment.Text == "" {
			return errors.New("transcript batch contains an invalid normalized segment")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO transcript_segments(transcript_id,video_id,seq,source_sentence_index,
			provider_start_ms,provider_end_ms,start_correction_ms,start_ms,end_ms,end_correction_ms,text)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, transcriptID, task.VideoID, segment.Seq,
			segment.SourceSentenceIndex, segment.ProviderStartMS, segment.ProviderEndMS, segment.StartCorrectionMS,
			segment.StartMS, segment.EndMS, segment.EndCorrectionMS, segment.Text); err != nil {
			return fmt.Errorf("insert transcript segment %d: %w", segment.Seq, err)
		}
	}
	newLast := startSeq + len(segments) - 1
	result, err := tx.Exec(ctx, `UPDATE transcripts SET last_committed_seq=$2,updated_at=clock_timestamp()
		WHERE id=$1 AND state='BUILDING' AND last_committed_seq=$3`, transcriptID, newLast, last)
	if err != nil {
		return fmt.Errorf("advance transcript continuous prefix: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("transcript progress changed while committing a batch")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transcript segment batch: %w", err)
	}
	return nil
}

func (r *StageRepository) VerifyTranscriptPrefix(ctx context.Context, transcriptID uuid.UUID,
	expected []TranscriptSegment, committed int) error {
	if committed < 0 || committed > len(expected) {
		return errors.New("transcript committed prefix is outside the normalized input")
	}
	rows, err := r.pool.Query(ctx, `SELECT seq,source_sentence_index,provider_start_ms,provider_end_ms,
		start_correction_ms,start_ms,end_ms,end_correction_ms,text FROM transcript_segments
		WHERE transcript_id=$1 ORDER BY seq`, transcriptID)
	if err != nil {
		return fmt.Errorf("read transcript committed prefix: %w", err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var actual TranscriptSegment
		if err := rows.Scan(&actual.Seq, &actual.SourceSentenceIndex, &actual.ProviderStartMS, &actual.ProviderEndMS,
			&actual.StartCorrectionMS, &actual.StartMS, &actual.EndMS, &actual.EndCorrectionMS, &actual.Text); err != nil {
			return fmt.Errorf("scan transcript committed prefix: %w", err)
		}
		if index >= committed || !reflect.DeepEqual(actual, expected[index]) {
			return errors.New("persisted transcript rows do not match the immutable normalized input")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read transcript committed prefix: %w", err)
	}
	if index != committed {
		return errors.New("transcript committed prefix has a gap or missing row")
	}
	return nil
}

func (r *StageRepository) MarkTranscriptBroken(ctx context.Context, task StageTask, transcriptID uuid.UUID) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin mark transcript broken: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE transcripts SET state='BROKEN',updated_at=clock_timestamp()
		WHERE id=$1 AND video_id=$2 AND state='BUILDING'`, transcriptID, task.VideoID)
	if err != nil {
		return fmt.Errorf("mark transcript checkpoint broken: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("transcript checkpoint is no longer buildable")
	}
	return tx.Commit(ctx)
}

func (r *StageRepository) CompleteTranscript(ctx context.Context, task StageTask, input TranscriptInput,
	transcriptID uuid.UUID, inputFingerprint, configHash []byte, state string, metadata json.RawMessage) error {
	if transcriptID == uuid.Nil || len(inputFingerprint) != 32 || len(configHash) != 32 || !json.Valid(metadata) ||
		(state != "READY" && state != "NO_SPEECH") {
		return errors.New("transcript completion checkpoint is invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transcript completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockStageTask(ctx, tx, task); err != nil {
		return err
	}
	var artifactState string
	var expected, last, segmentCount int
	if err := tx.QueryRow(ctx, `SELECT t.state,t.expected_segment_count,t.last_committed_seq,
		(SELECT count(*) FROM transcript_segments s WHERE s.transcript_id=t.id)
		FROM transcripts t WHERE t.id=$1 AND t.video_id=$2 FOR UPDATE`, transcriptID, task.VideoID).
		Scan(&artifactState, &expected, &last, &segmentCount); err != nil {
		return fmt.Errorf("verify complete transcript: %w", err)
	}
	if expected != last || expected != segmentCount || (state == "NO_SPEECH" && expected != 0) ||
		(state == "READY" && expected == 0) || (artifactState != "BUILDING" && artifactState != state) {
		return errors.New("transcript segments are not a complete continuous artifact")
	}
	if artifactState == "BUILDING" {
		if _, err := tx.Exec(ctx, `UPDATE transcripts SET state=$2,updated_at=clock_timestamp() WHERE id=$1`, transcriptID, state); err != nil {
			return fmt.Errorf("publish transcript artifact: %w", err)
		}
	}
	stepState := "READY"
	if state == "NO_SPEECH" {
		stepState = "NO_SPEECH"
	}
	_, err = tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_transcript_id,output_metadata,failure_count,last_run_epoch,started_at,completed_at,updated_at)
		VALUES($1,$2,'TRANSCRIPT',$3,$4,$5,$6,$7,$8::jsonb,0,$9,clock_timestamp(),clock_timestamp(),clock_timestamp())
		ON CONFLICT(task_id,stage) DO UPDATE SET state=EXCLUDED.state,input_fingerprint=EXCLUDED.input_fingerprint,
		config_hash=EXCLUDED.config_hash,source_blob_id=EXCLUDED.source_blob_id,output_blob_id=NULL,
		output_asr_submission_id=NULL,output_transcript_id=EXCLUDED.output_transcript_id,
		output_metadata=EXCLUDED.output_metadata,last_run_epoch=EXCLUDED.last_run_epoch,
		completed_at=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp()`, task.ID, task.VideoID,
		stepState, inputFingerprint, configHash, input.SourceBlobID, transcriptID, string(metadata), task.RunEpoch)
	if err != nil {
		return fmt.Errorf("save transcript stage checkpoint: %w", err)
	}
	if err := advanceStage(ctx, tx, task, "CHUNK"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func sameOptionalUUID(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (a TranscriptArtifact) Complete() bool {
	return (a.State == "READY" || a.State == "NO_SPEECH") && a.LastCommitted == a.ExpectedCount
}
