package localdemo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/queue/kafka"
)

const FixtureEmail = "demo@mirrorcore.local"

var fixtureSentences = []string{
	"示例视频介绍了 Kafka 在视频解析系统中的作用：通过消息队列传递解析任务，解耦 API 与后台 Worker。",
	"Worker 从 PostgreSQL 领取任务并持有租约；每次接管都会递增执行 epoch，旧执行者提交时会被拒绝。",
}

// SeedFixture creates an immutable, pre-parsed video fixture. The fixture has no
// source media object and must not be presented as a real upload or ASR result.
func SeedFixture(ctx context.Context, pool *pgxpool.Pool, ownerEmail string) (uuid.UUID, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin local demo fixture seed: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('mirrorcore/local-demo-fixture/v1'))`); err != nil {
		return uuid.Nil, fmt.Errorf("lock local demo fixture seed: %w", err)
	}
	var ownerID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE lower(email)=$1`, strings.ToLower(strings.TrimSpace(ownerEmail))).Scan(&ownerID); err != nil {
		return uuid.Nil, fmt.Errorf("load local demo owner: %w", err)
	}

	videoID := fixtureID("video")
	var existingOwner uuid.UUID
	err = tx.QueryRow(ctx, `SELECT owner_id FROM videos WHERE id=$1`, videoID).Scan(&existingOwner)
	if err == nil {
		if existingOwner != ownerID {
			return uuid.Nil, errors.New("local demo fixture already belongs to a different account")
		}
		if err := verifyFixture(ctx, tx, videoID); err != nil {
			return uuid.Nil, err
		}
		if err := ensurePipelineTask(ctx, tx, ownerID, videoID); err != nil {
			return uuid.Nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, fmt.Errorf("commit existing local demo fixture: %w", err)
		}
		return videoID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, fmt.Errorf("check local demo fixture: %w", err)
	}

	taskID := fixtureID("parse-task-v1")
	transcriptID := fixtureID("transcript-v1")
	chunkSetID := fixtureID("chunk-set-v1")
	embeddingSetID := fixtureID("embedding-set-v1")
	resultID := fixtureID("parse-result-v1")
	taskHash := digest("local-demo-task-v2")
	transcriptInput := digest("local-demo-transcript-input-v2")
	transcriptConfig := digest("local-demo-transcript-config-v2")
	chunkInput := digest("local-demo-chunk-input-v2")
	chunkConfig := digest("local-demo-chunk-config-v2")
	chunkCounter := digest("local-demo-chunk-counter-v2")
	embeddingInput := digest("local-demo-embedding-input-v2")
	embeddingConfig := digest("local-demo-embedding-config-v2")

	if _, err := tx.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,lifecycle,next_task_version,latest_task_id,
		source_name,source_size_bytes,media_duration_ms)
		VALUES($1,$2,'VALID','ACTIVE',2,NULL,'local-demo-fixture.mp4',1,60000)`, videoID, ownerID); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo video: %w", err)
	}
	taskConfig, _ := json.Marshal(map[string]any{"profile": "local_demo_fixture_v2", "media_processed": false})
	if _, err := tx.Exec(ctx, `INSERT INTO parse_tasks(id,video_id,owner_id,task_version,request_hash,config_snapshot,
		status,current_stage,run_epoch,next_run_at,deadline_at,completed_at)
		VALUES($1,$2,$3,1,$4,$5::jsonb,'SUCCEEDED','PUBLISH',1,clock_timestamp(),
		clock_timestamp()+interval '1 day',clock_timestamp())`, taskID, videoID, ownerID, taskHash[:], string(taskConfig)); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo parse task: %w", err)
	}

	if _, err := tx.Exec(ctx, `INSERT INTO transcripts(id,video_id,created_by_task,input_fingerprint,config_hash,state,
		media_duration_ms,audio_origin_ms,expected_segment_count,last_committed_seq)
		VALUES($1,$2,$3,$4,$5,'READY',60000,0,2,2)`, transcriptID, videoID, taskID, transcriptInput[:], transcriptConfig[:]); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo transcript: %w", err)
	}
	segmentTimes := [][2]int64{{1000, 8000}, {9000, 18000}}
	for i, sentence := range fixtureSentences {
		seq := i + 1
		times := segmentTimes[i]
		if _, err := tx.Exec(ctx, `INSERT INTO transcript_segments(transcript_id,video_id,seq,source_sentence_index,
			provider_start_ms,provider_end_ms,start_ms,end_ms,text)
			VALUES($1,$2,$3,$3,$4,$5,$4,$5,$6)`, transcriptID, videoID, seq, times[0], times[1], sentence); err != nil {
			return uuid.Nil, fmt.Errorf("insert local demo transcript segment %d: %w", seq, err)
		}
	}

	chunkConfigSnapshot, _ := json.Marshal(map[string]any{"profile": "fixture_chunk_v2", "measurement_unit": "UTF8_BYTES"})
	if _, err := tx.Exec(ctx, `INSERT INTO chunk_sets(id,video_id,transcript_id,created_by_task,input_fingerprint,
		config_hash,counter_fingerprint,measurement_unit,config_snapshot,state,expected_chunk_count)
		VALUES($1,$2,$3,$4,$5,$6,$7,'UTF8_BYTES',$8::jsonb,'READY',2)`, chunkSetID, videoID, transcriptID,
		taskID, chunkInput[:], chunkConfig[:], chunkCounter[:], string(chunkConfigSnapshot)); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo chunk set: %w", err)
	}
	for i, sentence := range fixtureSentences {
		seq := i + 1
		chunkID := fixtureID(fmt.Sprintf("chunk-%d", seq))
		textHash := sha256.Sum256([]byte(sentence))
		times := segmentTimes[i]
		if _, err := tx.Exec(ctx, `INSERT INTO chunks(id,chunk_set_id,video_id,seq,text,text_sha256,byte_count,start_ms,end_ms)
			VALUES($1,$2,$3,$4,$5,$6,octet_length($5),$7,$8)`, chunkID, chunkSetID, videoID, seq,
			sentence, textHash[:], times[0], times[1]); err != nil {
			return uuid.Nil, fmt.Errorf("insert local demo chunk %d: %w", seq, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO chunk_sources(chunk_id,chunk_set_id,video_id,transcript_id,
			transcript_segment_seq,start_byte,end_byte) VALUES($1,$2,$3,$4,$5,0,octet_length($6))`,
			chunkID, chunkSetID, videoID, transcriptID, seq, sentence); err != nil {
			return uuid.Nil, fmt.Errorf("insert local demo chunk source %d: %w", seq, err)
		}
	}

	embeddingConfigSnapshot, _ := json.Marshal(map[string]any{"profile": "fixture_unit_vector_v2", "dimension": 1024})
	if _, err := tx.Exec(ctx, `INSERT INTO embedding_sets(id,video_id,chunk_set_id,created_by_task,input_fingerprint,
		config_hash,config_snapshot,state,expected_embedding_count)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,'READY',2)`, embeddingSetID, videoID, chunkSetID, taskID,
		embeddingInput[:], embeddingConfig[:], string(embeddingConfigSnapshot)); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo embedding set: %w", err)
	}
	for i, sentence := range fixtureSentences {
		chunkID := fixtureID(fmt.Sprintf("chunk-%d", i+1))
		textHash := sha256.Sum256([]byte(sentence))
		if _, err := tx.Exec(ctx, `INSERT INTO chunk_embeddings(embedding_set_id,video_id,chunk_set_id,chunk_id,
			input_sha256,embedding) VALUES($1,$2,$3,$4,$5,$6::vector(1024))`,
			embeddingSetID, videoID, chunkSetID, chunkID, textHash[:], unitVectorText(i)); err != nil {
			return uuid.Nil, fmt.Errorf("insert local demo embedding %d: %w", i+1, err)
		}
	}

	resultConfig, _ := json.Marshal(map[string]any{
		"profile": "local_demo_fixture_v2", "transcript_id": transcriptID.String(),
		"chunk_set_id": chunkSetID.String(), "embedding_set_id": embeddingSetID.String(),
	})
	if _, err := tx.Exec(ctx, `INSERT INTO parse_results(id,task_id,video_id,task_version,result_type,transcript_id,
		chunk_set_id,embedding_set_id,transcript_segment_count,chunk_count,embedding_count,config_snapshot)
		VALUES($1,$2,$3,1,'TEXT',$4,$5,$6,2,2,2,$7::jsonb)`, resultID, taskID, videoID,
		transcriptID, chunkSetID, embeddingSetID, string(resultConfig)); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo parse result: %w", err)
	}
	stepInput := digest("local-demo-publish-step-v2")
	stepConfig := digest("local-demo-publish-config-v2")
	stepMetadata, _ := json.Marshal(map[string]any{"result_id": resultID.String(), "outcome": "FIXTURE"})
	if _, err := tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		output_result_id,output_metadata,last_run_epoch,started_at,completed_at)
		VALUES($1,$2,'PUBLISH','READY',$3,$4,$5,$6::jsonb,1,clock_timestamp(),clock_timestamp())`,
		taskID, videoID, stepInput[:], stepConfig[:], resultID, string(stepMetadata)); err != nil {
		return uuid.Nil, fmt.Errorf("insert local demo publish checkpoint: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE videos SET latest_task_id=$2,active_result_id=$3 WHERE id=$1 AND owner_id=$4`,
		videoID, taskID, resultID, ownerID); err != nil {
		return uuid.Nil, fmt.Errorf("publish local demo fixture: %w", err)
	}
	if err := ensurePipelineTask(ctx, tx, ownerID, videoID); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit local demo fixture: %w", err)
	}
	return videoID, nil
}

func verifyFixture(ctx context.Context, tx pgx.Tx, videoID uuid.UUID) error {
	var activeID *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT active_result_id FROM videos WHERE id=$1`, videoID).Scan(&activeID); err != nil {
		return fmt.Errorf("verify local demo video: %w", err)
	}
	if activeID == nil {
		return errors.New("existing local demo fixture has an unexpected active result")
	}
	var activeTaskID uuid.UUID
	var activeVersion int32
	if err := tx.QueryRow(ctx, `SELECT task_id,task_version FROM parse_results WHERE id=$1 AND video_id=$2`,
		*activeID, videoID).Scan(&activeTaskID, &activeVersion); err != nil {
		return fmt.Errorf("verify active local demo result: %w", err)
	}
	if !(*activeID == fixtureID("parse-result-v1") && activeTaskID == fixtureID("parse-task-v1") && activeVersion == 1) &&
		!(activeTaskID == fixtureID("pipeline-task-v1") && activeVersion == 2) {
		return errors.New("existing local demo fixture has an unexpected active result")
	}
	var segmentCount, chunkCount, embeddingCount int
	if err := tx.QueryRow(ctx, `SELECT pr.transcript_segment_count,pr.chunk_count,pr.embedding_count
		FROM parse_results pr WHERE pr.id=$1 AND pr.video_id=$2 AND pr.task_version=1 AND pr.result_type='TEXT'`,
		fixtureID("parse-result-v1"), videoID).Scan(&segmentCount, &chunkCount, &embeddingCount); err != nil {
		return fmt.Errorf("verify local demo parse result: %w", err)
	}
	if segmentCount != 2 || chunkCount != 2 || embeddingCount != 2 {
		return errors.New("existing local demo fixture has incomplete artifact counts")
	}
	var actualSegments, actualChunks, actualSources, actualEmbeddings, validDimensions int
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM transcript_segments WHERE transcript_id=$1),
		(SELECT count(*) FROM chunks WHERE chunk_set_id=$2),
		(SELECT count(*) FROM chunk_sources WHERE chunk_set_id=$2),
		(SELECT count(*) FROM chunk_embeddings WHERE embedding_set_id=$3),
		(SELECT count(*) FROM chunk_embeddings WHERE embedding_set_id=$3 AND vector_dims(embedding)=1024)`,
		fixtureID("transcript-v1"), fixtureID("chunk-set-v1"), fixtureID("embedding-set-v1")).
		Scan(&actualSegments, &actualChunks, &actualSources, &actualEmbeddings, &validDimensions); err != nil {
		return fmt.Errorf("verify local demo artifact rows: %w", err)
	}
	if actualSegments != 2 || actualChunks != 2 || actualSources != 2 || actualEmbeddings != 2 || validDimensions != 2 {
		return errors.New("existing local demo fixture has missing or invalid artifact rows")
	}
	rows, err := tx.Query(ctx, `SELECT seq,text FROM transcript_segments WHERE transcript_id=$1 ORDER BY seq`, fixtureID("transcript-v1"))
	if err != nil {
		return fmt.Errorf("verify local demo transcript: %w", err)
	}
	defer rows.Close()
	for i, expected := range fixtureSentences {
		if !rows.Next() {
			return errors.New("existing local demo fixture has incomplete transcript segments")
		}
		var seq int
		var text string
		if err := rows.Scan(&seq, &text); err != nil {
			return fmt.Errorf("scan local demo transcript: %w", err)
		}
		if seq != i+1 || text != expected {
			return errors.New("existing local demo fixture transcript content does not match this version")
		}
	}
	if rows.Next() {
		return errors.New("existing local demo fixture has unexpected transcript segments")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate local demo transcript: %w", err)
	}
	return nil
}

func ensurePipelineTask(ctx context.Context, tx pgx.Tx, ownerID, videoID uuid.UUID) error {
	taskID := fixtureID("pipeline-task-v1")
	var taskVersion int32
	err := tx.QueryRow(ctx, `SELECT task_version FROM parse_tasks WHERE id=$1 AND video_id=$2 AND owner_id=$3`,
		taskID, videoID, ownerID).Scan(&taskVersion)
	if err == nil {
		if taskVersion != 2 {
			return errors.New("local demo pipeline task has an unexpected task version")
		}
		return verifyPipelineTask(ctx, tx, ownerID, videoID, taskID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check local demo pipeline task: %w", err)
	}

	requestHash := digest("local-demo-pipeline-request-v1")
	taskConfig, err := json.Marshal(map[string]any{
		"profile": "local_demo_pipeline_v1", "transcript_source": fixtureID("transcript-v1").String(),
		"media_processing": "bypassed_synthetic_source", "model_calls": "deterministic_local_embedding",
	})
	if err != nil {
		return fmt.Errorf("encode local demo pipeline config: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO parse_tasks(id,video_id,owner_id,task_version,request_hash,config_snapshot,
		status,current_stage,run_epoch,next_run_at,deadline_at)
		VALUES($1,$2,$3,2,$4,$5::jsonb,'WAIT_DISPATCH','CHUNK',0,clock_timestamp(),
		clock_timestamp()+interval '6 hours')`, taskID, videoID, ownerID, requestHash[:], string(taskConfig)); err != nil {
		return fmt.Errorf("insert local demo pipeline task: %w", err)
	}

	sourceBlobID := fixtureID("pipeline-source-blob-v1")
	syntheticSourceHash := digest("mirrorcore-local-demo-placeholder-byte-v1")
	if _, err := tx.Exec(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,object_version,sha256,size_bytes,state,created_by_task)
		VALUES($1,$2,'SOURCE_VIDEO','local-demo','local-demo/pipeline/source.mp4','local-demo-v1',$3,1,'READY',$4)`,
		sourceBlobID, videoID, syntheticSourceHash[:], taskID); err != nil {
		return fmt.Errorf("insert local demo synthetic source reference: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE videos SET source_blob_id=$2,next_task_version=3,latest_task_id=$3
		WHERE id=$1 AND owner_id=$4`, videoID, sourceBlobID, taskID, ownerID); err != nil {
		return fmt.Errorf("attach local demo synthetic source: %w", err)
	}
	transcriptInput := digest("local-demo-transcript-input-v2")
	transcriptConfig := digest("local-demo-transcript-config-v2")
	if _, err := tx.Exec(ctx, `INSERT INTO task_steps(task_id,video_id,stage,state,input_fingerprint,config_hash,
		source_blob_id,output_transcript_id,last_run_epoch,started_at,completed_at)
		VALUES($1,$2,'TRANSCRIPT','READY',$3,$4,$5,$6,0,clock_timestamp(),clock_timestamp())`,
		taskID, videoID, transcriptInput[:], transcriptConfig[:], sourceBlobID, fixtureID("transcript-v1")); err != nil {
		return fmt.Errorf("seed local demo transcript checkpoint: %w", err)
	}

	eventID := fixtureID("pipeline-outbox-event-v1")
	traceID := fixtureID("pipeline-trace-v1")
	payload, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": "parse.requested", "schema_version": 1,
		"task_id": taskID, "video_id": videoID, "task_version": 2,
		"occurred_at": time.Now().UTC().Format(time.RFC3339Nano), "trace_id": traceID,
	})
	if err != nil {
		return fmt.Errorf("encode local demo pipeline event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_id,event_type,schema_version,topic,event_key,payload,status)
		VALUES($1,$2,'parse.requested',1,$3,$4,$5::jsonb,'PENDING')`,
		eventID, taskID, kafka.ParseRequestedTopic, videoID.String(), string(payload)); err != nil {
		return fmt.Errorf("insert local demo pipeline outbox: %w", err)
	}
	return nil
}

func verifyPipelineTask(ctx context.Context, tx pgx.Tx, ownerID, videoID, taskID uuid.UUID) error {
	requestHash := digest("local-demo-pipeline-request-v1")
	var savedHash []byte
	var profile, transcriptSource, mediaProcessing, modelCalls string
	if err := tx.QueryRow(ctx, `SELECT request_hash,config_snapshot->>'profile',config_snapshot->>'transcript_source',
		config_snapshot->>'media_processing',config_snapshot->>'model_calls' FROM parse_tasks
		WHERE id=$1 AND video_id=$2 AND owner_id=$3 AND task_version=2`, taskID, videoID, ownerID).
		Scan(&savedHash, &profile, &transcriptSource, &mediaProcessing, &modelCalls); err != nil {
		return fmt.Errorf("verify local demo pipeline task: %w", err)
	}
	if !bytes.Equal(savedHash, requestHash[:]) || profile != "local_demo_pipeline_v1" ||
		transcriptSource != fixtureID("transcript-v1").String() || mediaProcessing != "bypassed_synthetic_source" ||
		modelCalls != "deterministic_local_embedding" {
		return errors.New("local demo pipeline task differs from its fixed configuration")
	}

	expectedSourceID := fixtureID("pipeline-source-blob-v1")
	var sourceID, latestTaskID uuid.UUID
	var nextVersion int32
	if err := tx.QueryRow(ctx, `SELECT source_blob_id,latest_task_id,next_task_version FROM videos
		WHERE id=$1 AND owner_id=$2 AND lifecycle='ACTIVE'`, videoID, ownerID).
		Scan(&sourceID, &latestTaskID, &nextVersion); err != nil {
		return fmt.Errorf("verify local demo pipeline video: %w", err)
	}
	if sourceID != expectedSourceID || latestTaskID != taskID || nextVersion != 3 {
		return errors.New("local demo pipeline video references differ from its fixed task")
	}
	syntheticHash := digest("mirrorcore-local-demo-placeholder-byte-v1")
	var kind, bucket, objectKey, objectVersion, state string
	var savedSourceHash []byte
	var size int64
	var createdBy uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT kind,bucket,object_key,COALESCE(object_version,''),sha256,size_bytes,state,created_by_task
		FROM blobs WHERE id=$1 AND video_id=$2`, expectedSourceID, videoID).
		Scan(&kind, &bucket, &objectKey, &objectVersion, &savedSourceHash, &size, &state, &createdBy); err != nil {
		return fmt.Errorf("verify local demo placeholder source: %w", err)
	}
	if kind != "SOURCE_VIDEO" || bucket != "local-demo" || objectKey != "local-demo/pipeline/source.mp4" ||
		objectVersion != "local-demo-v1" || !bytes.Equal(savedSourceHash, syntheticHash[:]) || size != 1 ||
		state != "READY" || createdBy != taskID {
		return errors.New("local demo placeholder source differs from its fixed identity")
	}

	transcriptInput := digest("local-demo-transcript-input-v2")
	transcriptConfig := digest("local-demo-transcript-config-v2")
	var stepState string
	var stepInput, stepConfig []byte
	var stepSource, transcriptID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT state,input_fingerprint,config_hash,source_blob_id,output_transcript_id
		FROM task_steps WHERE task_id=$1 AND video_id=$2 AND stage='TRANSCRIPT'`, taskID, videoID).
		Scan(&stepState, &stepInput, &stepConfig, &stepSource, &transcriptID); err != nil {
		return fmt.Errorf("verify local demo transcript checkpoint: %w", err)
	}
	if stepState != "READY" || !bytes.Equal(stepInput, transcriptInput[:]) ||
		!bytes.Equal(stepConfig, transcriptConfig[:]) || stepSource != expectedSourceID ||
		transcriptID != fixtureID("transcript-v1") {
		return errors.New("local demo transcript checkpoint differs from its fixed identity")
	}

	eventID := fixtureID("pipeline-outbox-event-v1")
	var aggregateID uuid.UUID
	var eventType, topic, eventKey, payloadEvent, payloadTask, payloadVideo, payloadVersion string
	var schemaVersion int
	err := tx.QueryRow(ctx, `SELECT aggregate_id,event_type,schema_version,topic,event_key,
		payload->>'event_id',payload->>'task_id',payload->>'video_id',payload->>'task_version'
		FROM outbox_events WHERE event_id=$1`, eventID).Scan(&aggregateID, &eventType, &schemaVersion, &topic,
		&eventKey, &payloadEvent, &payloadTask, &payloadVideo, &payloadVersion)
	if err != nil {
		return fmt.Errorf("verify local demo pipeline outbox: %w", err)
	}
	if aggregateID != taskID || eventType != "parse.requested" || schemaVersion != 1 ||
		topic != kafka.ParseRequestedTopic || eventKey != videoID.String() || payloadEvent != eventID.String() ||
		payloadTask != taskID.String() || payloadVideo != videoID.String() || payloadVersion != "2" {
		return errors.New("local demo pipeline outbox differs from its fixed task")
	}
	return nil
}

func fixtureID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("mirrorcore/local-demo/v2/"+name))
}

func digest(value string) [32]byte { return sha256.Sum256([]byte(value)) }

func unitVectorText(activeIndex int) string {
	var builder strings.Builder
	builder.Grow(2 + 1024*2)
	builder.WriteByte('[')
	for i := 0; i < 1024; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		if i == activeIndex {
			builder.WriteByte('1')
		} else {
			builder.WriteByte('0')
		}
	}
	builder.WriteByte(']')
	return builder.String()
}
