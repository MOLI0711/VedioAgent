package localdemo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/queue/kafka"
	"mirrorcore/internal/storage/ossstore"
)

// SeedMediaFixture writes a real, synthetic MP4 into the local object store and
// creates the first PROBE task. Repeated calls verify the existing identity and
// never rewind a task that has already progressed.
func SeedMediaFixture(ctx context.Context, pool *pgxpool.Pool, store *FileObjectStore,
	ownerEmail, sourcePath string) (uuid.UUID, uuid.UUID, error) {
	return SeedMediaFixtureWithKey(ctx, pool, store, ownerEmail, sourcePath, "media-v1")
}

// SeedMediaFixtureWithKey creates or verifies an independently identified synthetic
// media task. Reusing a key is idempotent; a new key creates a new video and task.
func SeedMediaFixtureWithKey(ctx context.Context, pool *pgxpool.Pool, store *FileObjectStore,
	ownerEmail, sourcePath, fixtureKey string) (uuid.UUID, uuid.UUID, error) {
	if pool == nil || store == nil || strings.TrimSpace(ownerEmail) == "" || strings.TrimSpace(sourcePath) == "" {
		return uuid.Nil, uuid.Nil, errors.New("local media fixture requires a database, object store, owner, and MP4 path")
	}
	if !validMediaFixtureKey(fixtureKey) {
		return uuid.Nil, uuid.Nil, errors.New("local media fixture key must be 1-64 ASCII letters, digits, hyphens, or underscores")
	}
	file, err := os.Open(sourcePath)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("open local media fixture: %w", err)
	}
	hash := sha256.New()
	size, err := io.Copy(hash, io.LimitReader(file, application.UploadMaxBytes+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return uuid.Nil, uuid.Nil, errors.Join(err, closeErr)
	}
	if size <= 0 || size > application.UploadMaxBytes {
		return uuid.Nil, uuid.Nil, errors.New("local media fixture is empty or exceeds the upload size limit")
	}
	sha := hash.Sum(nil)
	shaText := hex.EncodeToString(sha)
	videoID := mediaFixtureID("video", fixtureKey)
	taskID := mediaFixtureID("task", fixtureKey)
	blobID := mediaFixtureID("source-blob", fixtureKey)
	objectKey := "local-demo-media/" + shaText + "/voice-demo.mp4"
	metadata := map[string]string{"video-id": videoID.String(), "fixture": "synthetic-voice-demo-v1"}
	if fixtureKey != "media-v1" {
		objectKey = "local-demo-media/" + fixtureKey + "/" + shaText + "/voice-demo.mp4"
		metadata["fixture-key"] = fixtureKey
	}
	info, err := store.PutObjectFile(ctx, objectKey, sourcePath, "video/mp4", metadata)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("store synthetic local media: %w", err)
	}
	if info.Size != size || info.VersionID != "sha256:"+shaText {
		return uuid.Nil, uuid.Nil, errors.New("stored synthetic media does not match its source checksum")
	}
	ownerEmail = strings.ToLower(strings.TrimSpace(ownerEmail))
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("begin local media fixture seed: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('mirrorcore/local-demo-media-fixture/v1'))`); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("lock local media fixture seed: %w", err)
	}
	var ownerID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE lower(email)=$1`, ownerEmail).Scan(&ownerID); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("load local media fixture owner: %w", err)
	}
	var currentVideoID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM videos WHERE id=$1`, videoID).Scan(&currentVideoID)
	if err == nil {
		if err := verifyMediaFixture(ctx, tx, ownerID, videoID, taskID, blobID, fixtureKey,
			objectKey, info, sha, size); err != nil {
			return uuid.Nil, uuid.Nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, uuid.Nil, fmt.Errorf("commit existing local media fixture check: %w", err)
		}
		return videoID, taskID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, fmt.Errorf("check local media fixture: %w", err)
	}

	requestHash := sha256.Sum256([]byte("local-demo-media-pipeline-v1:" + fixtureKey + ":" + shaText))
	configSnapshot, err := json.Marshal(map[string]any{
		"profile": "local_demo_media_pipeline_v1", "fixture_id": fixtureKey, "source_sha256": shaText,
		"media_profile": "probe-v2/audio-v2", "asr_profile": "local_demo_asr_v1",
		"asr_provider": "local-demo", "asr_model": "deterministic-asr-v1",
		"note": "synthetic MP4; deterministic transcript adapter; no cloud calls",
	})
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("encode local media task configuration: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,lifecycle,next_task_version,latest_task_id,
		source_blob_id,source_name,source_size_bytes)
		VALUES($1,$2,'UPLOADED','ACTIVE',2,$3,$4,'voice-demo.mp4',$5)`,
		videoID, ownerID, taskID, blobID, size); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("insert local media video: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO parse_tasks(id,video_id,owner_id,task_version,request_hash,config_snapshot,
		status,current_stage,run_epoch,next_run_at,deadline_at)
		VALUES($1,$2,$3,1,$4,$5::jsonb,'WAIT_DISPATCH','PROBE',0,clock_timestamp(),
		clock_timestamp()+interval '6 hours')`, taskID, videoID, ownerID, requestHash[:], string(configSnapshot)); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("insert local media parse task: %w", err)
	}
	blobMetadata := map[string]string{"fixture": "synthetic-voice-demo-v1", "asr": "deterministic-local"}
	if fixtureKey != "media-v1" {
		blobMetadata["fixture_id"] = fixtureKey
	}
	blobMetadataJSON, err := json.Marshal(blobMetadata)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("encode local media blob metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,object_version,sha256,size_bytes,state,created_by_task,metadata)
		VALUES($1,$2,'SOURCE_VIDEO',$3,$4,$5,$6,$7,'STORED',$8,$9::jsonb)`, blobID, videoID, LocalDemoBucket,
		objectKey, info.VersionID, sha, size, taskID, string(blobMetadataJSON)); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("insert local media source blob: %w", err)
	}
	eventID := mediaFixtureID("outbox-event", fixtureKey)
	traceID := mediaFixtureID("trace", fixtureKey)
	payload, err := json.Marshal(map[string]any{"event_id": eventID, "event_type": "parse.requested",
		"schema_version": 1, "task_id": taskID, "video_id": videoID, "task_version": 1,
		"occurred_at": time.Now().UTC().Format(time.RFC3339Nano), "trace_id": traceID})
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("encode local media pipeline event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_id,event_type,schema_version,topic,event_key,payload,status)
		VALUES($1,$2,'parse.requested',1,$3,$4,$5::jsonb,'PENDING')`, eventID, taskID,
		kafka.ParseRequestedTopic, videoID.String(), string(payload)); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("insert local media outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("commit local media fixture: %w", err)
	}
	return videoID, taskID, nil
}

func verifyMediaFixture(ctx context.Context, tx pgx.Tx, ownerID, videoID, taskID, blobID uuid.UUID,
	fixtureKey, objectKey string, info ossstore.ObjectInfo, sha []byte, size int64) error {
	var actualOwner, actualTask, actualBlob uuid.UUID
	var fileState, lifecycle, sourceName string
	var savedSize int64
	var nextVersion int32
	if err := tx.QueryRow(ctx, `SELECT owner_id,latest_task_id,source_blob_id,file_state,lifecycle,next_task_version,
		source_name,source_size_bytes FROM videos WHERE id=$1`, videoID).
		Scan(&actualOwner, &actualTask, &actualBlob, &fileState, &lifecycle, &nextVersion, &sourceName, &savedSize); err != nil {
		return fmt.Errorf("verify local media fixture video: %w", err)
	}
	if actualOwner != ownerID || actualTask != taskID || actualBlob != blobID || lifecycle != "ACTIVE" ||
		(fileState != "UPLOADED" && fileState != "VALID") || nextVersion != 2 || sourceName != "voice-demo.mp4" || savedSize != size {
		return errors.New("existing local media fixture video identity differs from the configured fixture")
	}
	var savedKey, savedVersion, state string
	var savedHash []byte
	var savedObjectSize int64
	if err := tx.QueryRow(ctx, `SELECT object_key,COALESCE(object_version,''),sha256,size_bytes,state FROM blobs
		WHERE id=$1 AND video_id=$2 AND kind='SOURCE_VIDEO' AND bucket=$3`, blobID, videoID, LocalDemoBucket).
		Scan(&savedKey, &savedVersion, &savedHash, &savedObjectSize, &state); err != nil {
		return fmt.Errorf("verify local media source blob: %w", err)
	}
	if savedKey != objectKey || savedVersion != info.VersionID || !equalHash(savedHash, sha) || savedObjectSize != size ||
		(state != "STORED" && state != "READY") {
		return errors.New("existing local media source blob does not match the synthetic fixture")
	}
	var taskStatus, stage, profile, storedFixtureKey string
	if err := tx.QueryRow(ctx, `SELECT status,current_stage,config_snapshot->>'profile',
		COALESCE(config_snapshot->>'fixture_id','media-v1') FROM parse_tasks
		WHERE id=$1 AND video_id=$2 AND owner_id=$3 AND task_version=1`, taskID, videoID, ownerID).
		Scan(&taskStatus, &stage, &profile, &storedFixtureKey); err != nil {
		return fmt.Errorf("verify local media parse task: %w", err)
	}
	if taskStatus == "CANCELLED" || profile != "local_demo_media_pipeline_v1" || storedFixtureKey != fixtureKey || stage == "" {
		return errors.New("existing local media task has an unexpected identity")
	}
	return nil
}

func validMediaFixtureKey(key string) bool {
	if len(key) == 0 || len(key) > 64 || strings.TrimSpace(key) != key {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
			!(c >= '0' && c <= '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func mediaFixtureID(kind, key string) uuid.UUID {
	if key == "media-v1" {
		switch kind {
		case "video":
			return fixtureID("media-video-v1")
		case "task":
			return fixtureID("media-task-v1")
		case "source-blob":
			return fixtureID("media-source-blob-v1")
		case "outbox-event":
			return fixtureID("media-outbox-event-v1")
		case "trace":
			return fixtureID("media-trace-v1")
		}
	}
	return fixtureID("media-" + kind + "-" + key)
}

func equalHash(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
