package deletion

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/database"
	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

func TestWorkerDeletesOSSObjectsAndFinalizesVideoTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool := openIsolatedDeletionDatabase(t, ctx)

	ownerID, videoID, blobID := uuid.New(), uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO users(id,email,password_hash)
		VALUES($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test"); err != nil {
		t.Fatalf("insert deletion owner: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES($1,$2,'UPLOADED','private-recording.mp4',1024)`, videoID, ownerID); err != nil {
		t.Fatalf("insert deletion video: %v", err)
	}
	blobKey := "source/" + videoID.String() + "/original.mp4"
	if _, err := pool.Exec(ctx, `INSERT INTO blobs(id,video_id,kind,bucket,object_key,object_version,size_bytes,state)
		VALUES($1,$2,'SOURCE_VIDEO','integration-bucket',$3,'source-v1',1024,'STORED')`, blobID, videoID, blobKey); err != nil {
		t.Fatalf("insert source blob: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE videos SET source_blob_id=$2 WHERE id=$1`, videoID, blobID); err != nil {
		t.Fatalf("link source blob: %v", err)
	}

	deletion, err := application.NewResourceService(postgres.NewResourceRepository(pool)).DeleteVideo(ctx, ownerID, videoID)
	if err != nil {
		t.Fatalf("request video deletion: %v", err)
	}
	if deletion.State != "PENDING" || deletion.Phase != "WAIT_UPLOADS" {
		t.Fatalf("initial deletion state = %s/%s, want PENDING/WAIT_UPLOADS", deletion.State, deletion.Phase)
	}
	if _, err := pool.Exec(ctx, `UPDATE video_deletion_jobs SET next_run_at=clock_timestamp() WHERE id=$1`, deletion.DeletionID); err != nil {
		t.Fatalf("make deletion job due for integration test: %v", err)
	}

	orphanKey := "artifacts/audio/" + videoID.String() + "/orphan.wav"
	store := &memoryDeletionObjectStore{objects: map[string]string{
		blobKey:   "source-v1",
		orphanKey: "orphan-v1",
	}}
	repository := postgres.NewVideoDeletionRepository(pool)
	worker := NewWorker(repository, store, "integration-bucket", slog.New(slog.NewTextHandler(io.Discard, nil)))
	completed := false
	for attempt := 0; attempt < 10; attempt++ {
		var state string
		if err := pool.QueryRow(ctx, `SELECT state FROM video_deletion_jobs WHERE id=$1`, deletion.DeletionID).Scan(&state); err != nil {
			t.Fatalf("read deletion job before claim: %v", err)
		}
		if state == "SUCCEEDED" {
			completed = true
			break
		}
		if _, err := pool.Exec(ctx, `UPDATE video_deletion_jobs SET next_run_at=clock_timestamp()
			WHERE id=$1 AND state IN ('PENDING','RETRY_WAIT')`, deletion.DeletionID); err != nil {
			t.Fatalf("make yielded deletion phase due: %v", err)
		}
		job, err := repository.ClaimNext(ctx, uuid.NewString())
		if err != nil {
			t.Fatalf("claim deletion phase %d: %v", attempt+1, err)
		}
		if job == nil {
			t.Fatalf("deletion phase %d was not claimable", attempt+1)
		}
		worker.process(ctx, *job)
		var nextState, phase string
		if err := pool.QueryRow(ctx, `SELECT state,phase FROM video_deletion_jobs WHERE id=$1`, deletion.DeletionID).
			Scan(&nextState, &phase); err != nil {
			t.Fatalf("read deletion job after phase %s: %v", job.Phase, err)
		}
		if nextState == "RETRY_WAIT" {
			t.Fatalf("deletion entered retry after phase %s; current phase=%s", job.Phase, phase)
		}
		if nextState == "SUCCEEDED" {
			completed = true
			break
		}
	}
	if !completed {
		var phase, state string
		_ = pool.QueryRow(ctx, `SELECT state,phase FROM video_deletion_jobs WHERE id=$1`, deletion.DeletionID).
			Scan(&state, &phase)
		t.Fatalf("deletion did not finish within bounded claims: state=%s phase=%s", state, phase)
	}

	var lifecycle, sourceName, jobState, jobPhase string
	if err := pool.QueryRow(ctx, `SELECT lifecycle,source_name FROM videos WHERE id=$1`, videoID).
		Scan(&lifecycle, &sourceName); err != nil {
		t.Fatalf("read deleted video tombstone: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT state,phase FROM video_deletion_jobs WHERE id=$1`, deletion.DeletionID).
		Scan(&jobState, &jobPhase); err != nil {
		t.Fatalf("read completed deletion job: %v", err)
	}
	var blobCount, ledgerCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM blobs WHERE video_id=$1`, videoID).Scan(&blobCount); err != nil {
		t.Fatalf("count retained blobs: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_gc_objects WHERE video_id=$1`, videoID).Scan(&ledgerCount); err != nil {
		t.Fatalf("count retained GC ledger rows: %v", err)
	}
	if lifecycle != "DELETED" || sourceName != "deleted" || jobState != "SUCCEEDED" || jobPhase != "DONE" || blobCount != 0 || ledgerCount != 0 {
		t.Fatalf("final state video=%s/%s job=%s/%s blobs=%d ledger=%d", lifecycle, sourceName, jobState, jobPhase, blobCount, ledgerCount)
	}
	if len(store.deleted) != 2 || store.objects[blobKey] != "" || store.objects[orphanKey] != "" {
		t.Fatalf("OSS deletions = %v, remaining objects = %v; want source and discovered orphan removed", store.deleted, store.objects)
	}
	if store.listCalls < 6 {
		t.Fatalf("OSS version-list calls=%d, want initial and final scans of all three prefixes", store.listCalls)
	}
}

func openIsolatedDeletionDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL video deletion integration test")
	}
	adminPool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := "video_deletion_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create isolated deletion test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse integration database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open isolated deletion test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate isolated deletion test schema: %v", err)
	}
	return pool
}

type memoryDeletionObjectStore struct {
	objects   map[string]string
	deleted   []string
	listCalls int
}

func (*memoryDeletionObjectStore) InitiateMultipart(context.Context, string, string, string) (string, error) {
	return "", errors.New("unexpected multipart initiation")
}
func (*memoryDeletionObjectStore) PresignPart(context.Context, string, string, int32, int64, time.Duration) (ossstore.SignedRequest, error) {
	return ossstore.SignedRequest{}, errors.New("unexpected part presign")
}
func (*memoryDeletionObjectStore) PresignGet(context.Context, string, string, time.Duration) (ossstore.SignedRequest, error) {
	return ossstore.SignedRequest{}, errors.New("unexpected GET presign")
}
func (*memoryDeletionObjectStore) ListParts(context.Context, string, string) ([]ossstore.UploadedPart, error) {
	return nil, errors.New("unexpected multipart part listing")
}
func (*memoryDeletionObjectStore) CompleteMultipart(context.Context, string, string, []ossstore.UploadedPart) error {
	return errors.New("unexpected multipart completion")
}
func (*memoryDeletionObjectStore) HeadObject(context.Context, string) (ossstore.ObjectInfo, bool, error) {
	return ossstore.ObjectInfo{}, false, errors.New("unexpected object HEAD")
}
func (*memoryDeletionObjectStore) AbortMultipart(context.Context, string, string) error {
	return errors.New("unexpected multipart abort")
}
func (*memoryDeletionObjectStore) HeadObjectVersion(context.Context, string, string) (ossstore.ObjectInfo, bool, error) {
	return ossstore.ObjectInfo{}, false, errors.New("unexpected version HEAD")
}
func (s *memoryDeletionObjectStore) ListObjectVersions(_ context.Context, prefix, _, _ string) (ossstore.ObjectVersionPage, error) {
	s.listCalls++
	keys := make([]string, 0)
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	page := ossstore.ObjectVersionPage{Objects: make([]ossstore.ListedObjectVersion, 0, len(keys))}
	for _, key := range keys {
		page.Objects = append(page.Objects, ossstore.ListedObjectVersion{Key: key, VersionID: s.objects[key]})
	}
	return page, nil
}
func (s *memoryDeletionObjectStore) DeleteObject(_ context.Context, key, version string) error {
	if actual, ok := s.objects[key]; !ok || actual != version {
		return errors.New("requested object version was not found")
	}
	s.deleted = append(s.deleted, key+"#"+version)
	delete(s.objects, key)
	return nil
}
func (*memoryDeletionObjectStore) DownloadObject(context.Context, string, string, io.Writer, int64) (ossstore.ObjectInfo, error) {
	return ossstore.ObjectInfo{}, errors.New("unexpected object download")
}
func (*memoryDeletionObjectStore) PutObjectFile(context.Context, string, string, string, map[string]string) (ossstore.ObjectInfo, error) {
	return ossstore.ObjectInfo{}, errors.New("unexpected object upload")
}
