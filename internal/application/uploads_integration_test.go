package application_test

import (
	"context"
	"errors"
	"io"
	"os"
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

var (
	errOSSCompletionAckLost  = errors.New("injected lost OSS completion acknowledgement")
	errDBFinalizationAckLost = errors.New("injected lost database finalization acknowledgement")
)

func TestUploadCompletionRecoversAfterOSSAndDatabaseAcknowledgementLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool := openIsolatedUploadDatabase(t, ctx)

	ownerID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO users(id,email,password_hash)
		VALUES($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test"); err != nil {
		t.Fatalf("insert upload owner: %v", err)
	}

	store := &faultingUploadObjectStore{expectedSize: 3, loseCompletionAcknowledgement: true}
	repository := &loseFinalizeAcknowledgementRepository{
		UploadRepository: postgres.NewUploadRepository(pool, "integration-bucket"),
		loseNext:         true,
	}
	service := application.NewUploadService(repository, store)
	session, _, err := service.Create(ctx, application.UploadInput{
		OwnerID: ownerID, RequestKey: "upload-recovery-" + uuid.NewString(),
		SourceName: "recovery.mp4", SizeBytes: 3, ContentType: "video/mp4",
	}, "integration-request")
	if err != nil {
		t.Fatalf("create upload session: %v", err)
	}
	if session.Status != "UPLOADING" || session.UploadID != "multipart-1" {
		t.Fatalf("created upload session = %+v, want UPLOADING with persisted multipart ID", session)
	}

	input := application.CompleteUploadInput{Parts: []application.CompletePart{{PartNumber: 1, ETag: "etag-1"}}}
	_, err = service.Complete(ctx, ownerID, session.ID, input)
	var serviceErr *application.ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.Code != "STORAGE_VERIFY_FAILED" {
		t.Fatalf("first Complete() error = %v, want recoverable storage verification error", err)
	}
	if !store.objectExists || store.completeCalls != 1 {
		t.Fatalf("OSS side effect not retained after lost acknowledgement: exists=%t complete_calls=%d",
			store.objectExists, store.completeCalls)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM upload_sessions WHERE id=$1`, session.ID).Scan(&status); err != nil {
		t.Fatalf("read status after uncertain OSS completion: %v", err)
	}
	if status != "VERIFYING" {
		t.Fatalf("upload status after uncertain OSS completion = %s, want VERIFYING", status)
	}

	_, err = service.Complete(ctx, ownerID, session.ID, input)
	if !errors.Is(err, errDBFinalizationAckLost) {
		t.Fatalf("second Complete() error = %v, want lost DB acknowledgement after committed finalize", err)
	}
	if repository.finalizeCalls != 1 {
		t.Fatalf("FinalizeUpload() calls = %d, want 1", repository.finalizeCalls)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM upload_sessions WHERE id=$1`, session.ID).Scan(&status); err != nil {
		t.Fatalf("read status after committed finalization: %v", err)
	}
	if status != "COMPLETE" {
		t.Fatalf("upload status after committed finalization = %s, want COMPLETE", status)
	}

	result, err := service.Complete(ctx, ownerID, session.ID, input)
	if err != nil {
		t.Fatalf("replay Complete() after lost DB acknowledgement: %v", err)
	}
	if result.Status != "COMPLETE" || result.Task == nil || result.Task.TaskVersion != 1 ||
		result.Task.VideoID != session.VideoID || result.Task.ID == uuid.Nil {
		t.Fatalf("replayed completion result = %+v", result)
	}
	if repository.finalizeCalls != 1 || store.completeCalls != 1 {
		t.Fatalf("replay repeated external or DB finalization: finalize_calls=%d oss_complete_calls=%d",
			repository.finalizeCalls, store.completeCalls)
	}

	var sessionTaskID *uuid.UUID
	var parseSlotReserved bool
	if err := pool.QueryRow(ctx, `SELECT initial_task_id,parse_slot_reserved FROM upload_sessions WHERE id=$1`, session.ID).
		Scan(&sessionTaskID, &parseSlotReserved); err != nil {
		t.Fatalf("read finalized upload identity: %v", err)
	}
	if sessionTaskID == nil || *sessionTaskID != result.Task.ID || parseSlotReserved {
		t.Fatalf("finalized upload initial_task_id=%v reserved=%t", sessionTaskID, parseSlotReserved)
	}
	var taskCount, outboxCount, sourceBlobCount int
	var nextVersion int32
	var fileState string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM parse_tasks WHERE video_id=$1`, session.VideoID).Scan(&taskCount); err != nil {
		t.Fatalf("count initial parse tasks: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1`, result.Task.ID).Scan(&outboxCount); err != nil {
		t.Fatalf("count initial outbox events: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM blobs WHERE video_id=$1 AND kind='SOURCE_VIDEO'`, session.VideoID).Scan(&sourceBlobCount); err != nil {
		t.Fatalf("count source blobs: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT next_task_version,file_state FROM videos WHERE id=$1`, session.VideoID).
		Scan(&nextVersion, &fileState); err != nil {
		t.Fatalf("read finalized video state: %v", err)
	}
	if taskCount != 1 || outboxCount != 1 || sourceBlobCount != 1 || nextVersion != 2 || fileState != "UPLOADED" {
		t.Fatalf("finalized counts task=%d outbox=%d source_blob=%d next_version=%d file_state=%s",
			taskCount, outboxCount, sourceBlobCount, nextVersion, fileState)
	}
}

func openIsolatedUploadDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL upload recovery integration test")
	}
	adminPool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := "upload_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create isolated upload test schema: %v", err)
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
		t.Fatalf("open isolated upload test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate isolated upload test schema: %v", err)
	}
	return pool
}

type loseFinalizeAcknowledgementRepository struct {
	application.UploadRepository
	loseNext      bool
	finalizeCalls int
}

func (r *loseFinalizeAcknowledgementRepository) FinalizeUpload(ctx context.Context, sessionID, ownerID uuid.UUID,
	epoch int64, info ossstore.ObjectInfo) (application.UploadTask, error) {
	r.finalizeCalls++
	task, err := r.UploadRepository.FinalizeUpload(ctx, sessionID, ownerID, epoch, info)
	if err != nil {
		return application.UploadTask{}, err
	}
	if r.loseNext {
		r.loseNext = false
		return application.UploadTask{}, errDBFinalizationAckLost
	}
	return task, nil
}

type faultingUploadObjectStore struct {
	expectedSize                  int64
	uploadSessionID               string
	videoID                       string
	objectInfo                    ossstore.ObjectInfo
	objectExists                  bool
	loseCompletionAcknowledgement bool
	completeCalls                 int
}

func (s *faultingUploadObjectStore) InitiateMultipart(_ context.Context, _, sessionID, videoID string) (string, error) {
	s.uploadSessionID, s.videoID = sessionID, videoID
	return "multipart-1", nil
}

func (*faultingUploadObjectStore) PresignPart(context.Context, string, string, int32, int64, time.Duration) (ossstore.SignedRequest, error) {
	return ossstore.SignedRequest{}, errors.New("unexpected PresignPart call")
}

func (*faultingUploadObjectStore) PresignGet(context.Context, string, string, time.Duration) (ossstore.SignedRequest, error) {
	return ossstore.SignedRequest{}, errors.New("unexpected PresignGet call")
}

func (*faultingUploadObjectStore) ListParts(context.Context, string, string) ([]ossstore.UploadedPart, error) {
	return []ossstore.UploadedPart{{Number: 1, Size: 3, ETag: "etag-1"}}, nil
}

func (s *faultingUploadObjectStore) CompleteMultipart(context.Context, string, string, []ossstore.UploadedPart) error {
	s.completeCalls++
	s.objectExists = true
	s.objectInfo = ossstore.ObjectInfo{Size: s.expectedSize, VersionID: "object-version-1",
		Metadata: map[string]string{"upload-session-id": s.uploadSessionID, "video-id": s.videoID}}
	if s.loseCompletionAcknowledgement {
		s.loseCompletionAcknowledgement = false
		return errOSSCompletionAckLost
	}
	return nil
}

func (s *faultingUploadObjectStore) HeadObject(context.Context, string) (ossstore.ObjectInfo, bool, error) {
	return s.objectInfo, s.objectExists, nil
}

func (*faultingUploadObjectStore) AbortMultipart(context.Context, string, string) error {
	return errors.New("unexpected AbortMultipart call")
}

func (s *faultingUploadObjectStore) HeadObjectVersion(ctx context.Context, key, _ string) (ossstore.ObjectInfo, bool, error) {
	return s.HeadObject(ctx, key)
}

func (*faultingUploadObjectStore) ListObjectVersions(context.Context, string, string, string) (ossstore.ObjectVersionPage, error) {
	return ossstore.ObjectVersionPage{}, errors.New("unexpected ListObjectVersions call")
}

func (*faultingUploadObjectStore) DeleteObject(context.Context, string, string) error {
	return errors.New("unexpected DeleteObject call")
}

func (*faultingUploadObjectStore) DownloadObject(context.Context, string, string, io.Writer, int64) (ossstore.ObjectInfo, error) {
	return ossstore.ObjectInfo{}, errors.New("unexpected DownloadObject call")
}

func (*faultingUploadObjectStore) PutObjectFile(context.Context, string, string, string, map[string]string) (ossstore.ObjectInfo, error) {
	return ossstore.ObjectInfo{}, errors.New("unexpected PutObjectFile call")
}
