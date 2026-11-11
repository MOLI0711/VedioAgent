package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/contentprofile"
	"mirrorcore/internal/database"
)

func TestSeedInitialContentJobPersistsWorkspaceProfile(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	defer pool.Close()
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate integration database: %v", err)
	}

	for _, test := range []struct {
		name        string
		workspaceID string
		provider    string
		model       string
		profile     string
	}{
		{name: "local demo", workspaceID: contentprofile.LocalDemoWorkspace,
			provider: "local-demo", model: contentprofile.LocalDemoModel, profile: contentprofile.LocalDemoProfile},
		{name: "Bailian", workspaceID: "integration-workspace",
			provider: "aliyun-bailian", model: contentprofile.Model, profile: contentprofile.ProfileVersion},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin test fixture transaction: %v", err)
			}
			t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

			ownerID, videoID, taskID := uuid.New(), uuid.New(), uuid.New()
			transcriptID, chunkSetID, embeddingSetID, resultID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			fingerprint := sha256.Sum256([]byte(uuid.NewString()))
			configHash := sha256.Sum256([]byte(uuid.NewString()))
			counterHash := sha256.Sum256([]byte(uuid.NewString()))

			if _, err := tx.Exec(ctx, `INSERT INTO users(id,email,password_hash)
				VALUES($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test"); err != nil {
				t.Fatalf("insert owner: %v", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
				VALUES($1,$2,'VALID','content-profile-test.mp4',1)`, videoID, ownerID); err != nil {
				t.Fatalf("insert video: %v", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO parse_tasks(id,video_id,owner_id,task_version,request_hash,
				config_snapshot,status,current_stage,deadline_at)
				VALUES($1,$2,$3,1,$4,'{}','SUCCEEDED','DONE',clock_timestamp()+interval '1 hour')`,
				taskID, videoID, ownerID, fingerprint[:]); err != nil {
				t.Fatalf("insert parse task: %v", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO transcripts(id,video_id,created_by_task,input_fingerprint,
				config_hash,state,media_duration_ms,audio_origin_ms,expected_segment_count,last_committed_seq)
				VALUES($1,$2,$3,$4,$5,'READY',1000,0,1,1)`, transcriptID, videoID, taskID,
				fingerprint[:], configHash[:]); err != nil {
				t.Fatalf("insert transcript: %v", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO chunk_sets(id,video_id,transcript_id,created_by_task,
				input_fingerprint,config_hash,counter_fingerprint,measurement_unit,config_snapshot,state,expected_chunk_count)
				VALUES($1,$2,$3,$4,$5,$6,$7,'UTF8_BYTES','{}','READY',1)`, chunkSetID, videoID,
				transcriptID, taskID, fingerprint[:], configHash[:], counterHash[:]); err != nil {
				t.Fatalf("insert chunk set: %v", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO embedding_sets(id,video_id,chunk_set_id,created_by_task,
				input_fingerprint,config_hash,config_snapshot,state,expected_embedding_count)
				VALUES($1,$2,$3,$4,$5,$6,'{}','READY',1)`, embeddingSetID, videoID, chunkSetID,
				taskID, fingerprint[:], configHash[:]); err != nil {
				t.Fatalf("insert embedding set: %v", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO parse_results(id,task_id,video_id,task_version,result_type,
				transcript_id,chunk_set_id,embedding_set_id,transcript_segment_count,chunk_count,embedding_count,config_snapshot)
				VALUES($1,$2,$3,1,'TEXT',$4,$5,$6,1,1,1,'{}')`, resultID, taskID, videoID,
				transcriptID, chunkSetID, embeddingSetID); err != nil {
				t.Fatalf("insert parse result: %v", err)
			}

			task := StageTask{ID: taskID, VideoID: videoID, OwnerID: ownerID}
			manifest := publishManifest{ID: resultID, TranscriptID: transcriptID}
			if err := seedInitialContentJob(ctx, tx, task, manifest, test.workspaceID); err != nil {
				t.Fatalf("seed initial content job: %v", err)
			}
			if err := seedInitialContentJob(ctx, tx, task, manifest, test.workspaceID); err != nil {
				t.Fatalf("idempotent content job seed: %v", err)
			}

			var jobCount int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM content_jobs WHERE result_id=$1 AND revision=1`,
				resultID).Scan(&jobCount); err != nil {
				t.Fatalf("read initial content job: %v", err)
			}
			if jobCount != 1 {
				t.Fatalf("initial content jobs=%d, want exactly one", jobCount)
			}
			var savedSnapshot, savedHash []byte
			if err := tx.QueryRow(ctx, `SELECT profile_snapshot,profile_hash FROM content_jobs
				WHERE result_id=$1 AND revision=1`, resultID).Scan(&savedSnapshot, &savedHash); err != nil {
				t.Fatalf("read persisted profile: %v", err)
			}
			var snapshot contentprofile.Snapshot
			if err := json.Unmarshal(savedSnapshot, &snapshot); err != nil {
				t.Fatalf("decode persisted profile snapshot: %v", err)
			}
			if snapshot.Provider != test.provider || snapshot.WorkspaceID != test.workspaceID ||
				snapshot.Model != test.model || snapshot.Profile != test.profile {
				t.Fatalf("persisted profile = %+v, want provider=%s model=%s profile=%s workspace=%s",
					snapshot, test.provider, test.model, test.profile, test.workspaceID)
			}
			_, expectedHash, err := contentprofile.NewSnapshotForWorkspace(test.workspaceID)
			if err != nil {
				t.Fatalf("build expected profile: %v", err)
			}
			if !bytes.Equal(savedHash, expectedHash) {
				t.Fatal("persisted profile hash does not match workspace profile")
			}
		})
	}
}
