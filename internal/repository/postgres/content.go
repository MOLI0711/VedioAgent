package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrContentJobNotFound = errors.New("content job not found")
	ErrContentNotFound    = errors.New("content result not found")
	ErrContentNoSpeech    = errors.New("content is unavailable for a no-speech result")
	ErrContentInProgress  = errors.New("a content revision is already in progress")
	ErrContentIdempotency = errors.New("content idempotency key was reused for a different request")
	ErrContentLeaseLost   = errors.New("content job lease is no longer valid")
)

type ContentInProgressError struct{ JobID uuid.UUID }

func (e *ContentInProgressError) Error() string        { return ErrContentInProgress.Error() }
func (e *ContentInProgressError) Is(target error) bool { return target == ErrContentInProgress }

type ContentRepository struct{ pool *pgxpool.Pool }

func NewContentRepository(pool *pgxpool.Pool) *ContentRepository {
	return &ContentRepository{pool: pool}
}

type ContentJob struct {
	ID             uuid.UUID       `json:"id"`
	OwnerID        uuid.UUID       `json:"-"`
	VideoID        uuid.UUID       `json:"video_id"`
	ResultID       uuid.UUID       `json:"result_id"`
	TranscriptID   uuid.UUID       `json:"transcript_id"`
	Revision       int             `json:"revision"`
	State          string          `json:"state"`
	Stage          string          `json:"stage"`
	RunEpoch       int64           `json:"run_epoch,omitempty"`
	LeaseOwner     string          `json:"-"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	NextRunAt      time.Time       `json:"next_run_at"`
	FirstStartedAt *time.Time      `json:"first_started_at,omitempty"`
	DeadlineAt     *time.Time      `json:"deadline_at,omitempty"`
	ProfileHash    []byte          `json:"-"`
	Profile        json.RawMessage `json:"profile_snapshot,omitempty"`
	InputHash      []byte          `json:"-"`
	Error          json.RawMessage `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

type ContentSegment struct {
	Seq     int    `json:"seq"`
	Text    string `json:"text"`
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
}

type ContentInput struct {
	TotalSegments int
	Segments      []ContentSegment
}

type ContentSourceRange struct {
	SourceID string `json:"source_id"`
	Seq      int    `json:"seq"`
	Start    int    `json:"start_offset"`
	End      int    `json:"end_offset"`
	TextHash string `json:"text_hash"`
}

type ContentBatchPlan struct {
	BatchNo    int                  `json:"batch_no"`
	Ranges     []ContentSourceRange `json:"source_ranges"`
	InputHash  []byte               `json:"-"`
	InputBytes int                  `json:"input_bytes"`
}

type ContentBatch struct {
	ContentBatchPlan
	State             string
	Attempts          int
	Output            json.RawMessage
	ProviderRequestID string
	PromptTokens      int64
	CompletionTokens  int64
	OutcomeCode       string
}

type ContentSourceRecord struct {
	RefID        string
	TranscriptID uuid.UUID
	SentenceSeq  int
	StartOffset  int
	EndOffset    int
	Text         string
	TextHash     []byte
}

type ContentArtifactInsert struct {
	ID       uuid.UUID
	Job      ContentJob
	Payload  json.RawMessage
	Sources  []ContentSourceRecord
	Coverage json.RawMessage
}

type ContentResultView struct {
	State            string          `json:"state"`
	ResultID         uuid.UUID       `json:"result_id"`
	LatestJobID      *uuid.UUID      `json:"latest_job_id,omitempty"`
	LatestJobState   *string         `json:"latest_job_state,omitempty"`
	ActiveArtifactID *uuid.UUID      `json:"active_artifact_id,omitempty"`
	Artifact         json.RawMessage `json:"artifact,omitempty"`
}

func (r *ContentRepository) CreateContentJob(ctx context.Context, ownerID, resultID uuid.UUID,
	requestKey string, requestHash, profileSnapshot, profileHash []byte) (ContentJob, bool, error) {
	if ownerID == uuid.Nil || resultID == uuid.Nil || requestKey == "" || len(requestKey) > 128 ||
		len(requestHash) != sha256.Size || !json.Valid(profileSnapshot) || len(profileHash) != sha256.Size {
		return ContentJob{}, false, errors.New("content job request identity is invalid")
	}
	var result struct {
		VideoID      uuid.UUID
		TranscriptID uuid.UUID
		Type         string
	}
	if err := r.pool.QueryRow(ctx, `SELECT r.video_id,r.transcript_id,r.result_type FROM parse_results r
		JOIN videos v ON v.id=r.video_id AND v.owner_id=$2 AND v.lifecycle='ACTIVE'
		WHERE r.id=$1`, resultID, ownerID).Scan(&result.VideoID, &result.TranscriptID, &result.Type); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ContentJob{}, false, ErrContentNotFound
		}
		return ContentJob{}, false, fmt.Errorf("read content result identity: %w", err)
	}
	if result.Type == "NO_SPEECH" {
		return ContentJob{}, false, ErrContentNoSpeech
	}
	if result.Type != "TEXT" {
		return ContentJob{}, false, errors.New("unsupported content result type")
	}
	inputHash, _, err := HashJSON(struct {
		ProfileHash string    `json:"profile_hash"`
		ResultID    uuid.UUID `json:"result_id"`
		Transcript  uuid.UUID `json:"transcript_id"`
	}{HexHash(profileHash), resultID, result.TranscriptID})
	if err != nil {
		return ContentJob{}, false, fmt.Errorf("hash content job input: %w", err)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ContentJob{}, false, fmt.Errorf("begin content job admission: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT lifecycle FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`,
		result.VideoID, ownerID).Scan(&lifecycle); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ContentJob{}, false, ErrContentNotFound
		}
		return ContentJob{}, false, fmt.Errorf("lock content video: %w", err)
	}
	if lifecycle != "ACTIVE" {
		return ContentJob{}, false, ErrContentNotFound
	}
	var headOwner, headVideo uuid.UUID
	var nextRevision int
	var latestJob *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT owner_id,video_id,next_revision,latest_job_id FROM result_content_heads
		WHERE result_id=$1 FOR UPDATE`, resultID).Scan(&headOwner, &headVideo, &nextRevision, &latestJob); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ContentJob{}, false, ErrContentNotFound
		}
		return ContentJob{}, false, fmt.Errorf("lock result content head: %w", err)
	}
	if headOwner != ownerID || headVideo != result.VideoID {
		return ContentJob{}, false, ErrContentNotFound
	}
	if existing, err := findContentJobByKey(ctx, tx, ownerID, requestKey); err == nil {
		if !bytes.Equal(existing.RequestHash, requestHash) {
			return ContentJob{}, false, ErrContentIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return ContentJob{}, false, fmt.Errorf("commit idempotent content job lookup: %w", err)
		}
		return existing.ContentJob, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return ContentJob{}, false, fmt.Errorf("check content job idempotency key: %w", err)
	}
	if latestJob != nil {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM content_jobs WHERE id=$1`, *latestJob).Scan(&state); err != nil {
			return ContentJob{}, false, fmt.Errorf("read latest content job state: %w", err)
		}
		if state == "READY" || state == "RUNNING" || state == "RETRY_WAIT" {
			return ContentJob{}, false, &ContentInProgressError{JobID: *latestJob}
		}
	}
	job := ContentJob{ID: uuid.New(), OwnerID: ownerID, VideoID: result.VideoID, ResultID: resultID,
		TranscriptID: result.TranscriptID, Revision: nextRevision, State: "READY", Stage: "EXTRACT",
		ProfileHash: append([]byte(nil), profileHash...), Profile: append([]byte(nil), profileSnapshot...),
		InputHash: inputHash}
	_, err = tx.Exec(ctx, `INSERT INTO content_jobs(id,owner_id,video_id,result_id,transcript_id,revision,
		create_request_key,request_hash,profile_snapshot,profile_hash,input_hash,state,stage)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,'READY','EXTRACT')
		ON CONFLICT DO NOTHING`, job.ID, ownerID, result.VideoID, resultID, result.TranscriptID,
		job.Revision, requestKey, requestHash, string(profileSnapshot), profileHash, inputHash)
	if err != nil {
		return ContentJob{}, false, fmt.Errorf("insert content revision job: %w", err)
	}
	var saved bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM content_jobs WHERE id=$1)`, job.ID).Scan(&saved); err != nil {
		return ContentJob{}, false, fmt.Errorf("check inserted content job: %w", err)
	}
	if !saved {
		existing, lookupErr := findContentJobByKey(ctx, tx, ownerID, requestKey)
		if lookupErr == nil {
			if !bytes.Equal(existing.RequestHash, requestHash) {
				return ContentJob{}, false, ErrContentIdempotency
			}
			if err := tx.Commit(ctx); err != nil {
				return ContentJob{}, false, fmt.Errorf("commit concurrent content job replay: %w", err)
			}
			return existing.ContentJob, false, nil
		}
		return ContentJob{}, false, errors.New("content revision allocation conflicted without an idempotent request")
	}
	if _, err := tx.Exec(ctx, `UPDATE result_content_heads SET next_revision=$2,latest_job_id=$3,updated_at=clock_timestamp()
		WHERE result_id=$1`, resultID, nextRevision+1, job.ID); err != nil {
		return ContentJob{}, false, fmt.Errorf("advance content revision head: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ContentJob{}, false, fmt.Errorf("commit content job admission: %w", err)
	}
	return job, true, nil
}

type contentJobWithHash struct {
	ContentJob
	RequestHash []byte
}

func findContentJobByKey(ctx context.Context, tx pgx.Tx, ownerID uuid.UUID, requestKey string) (contentJobWithHash, error) {
	var job contentJobWithHash
	err := tx.QueryRow(ctx, `SELECT id,owner_id,video_id,result_id,transcript_id,revision,state,stage,
		run_epoch,COALESCE(lease_owner,''),lease_until,next_run_at,first_started_at,deadline_at,
		profile_hash,profile_snapshot,input_hash,error,created_at,completed_at,request_hash
		FROM content_jobs WHERE owner_id=$1 AND create_request_key=$2`, ownerID, requestKey).
		Scan(&job.ID, &job.OwnerID, &job.VideoID, &job.ResultID, &job.TranscriptID, &job.Revision,
			&job.State, &job.Stage, &job.RunEpoch, &job.LeaseOwner, &job.LeaseUntil, &job.NextRunAt,
			&job.FirstStartedAt, &job.DeadlineAt, &job.ProfileHash, &job.Profile, &job.InputHash,
			&job.Error, &job.CreatedAt, &job.CompletedAt, &job.RequestHash)
	return job, err
}

func (r *ContentRepository) FindContentJobByKey(ctx context.Context, ownerID uuid.UUID, requestKey string) (ContentJob, error) {
	var job ContentJob
	err := r.pool.QueryRow(ctx, `SELECT id,owner_id,video_id,result_id,transcript_id,revision,state,stage,
		run_epoch,COALESCE(lease_owner,''),lease_until,next_run_at,first_started_at,deadline_at,
		profile_hash,profile_snapshot,input_hash,error,created_at,completed_at
		FROM content_jobs WHERE owner_id=$1 AND create_request_key=$2`, ownerID, requestKey).
		Scan(&job.ID, &job.OwnerID, &job.VideoID, &job.ResultID, &job.TranscriptID, &job.Revision,
			&job.State, &job.Stage, &job.RunEpoch, &job.LeaseOwner, &job.LeaseUntil, &job.NextRunAt,
			&job.FirstStartedAt, &job.DeadlineAt, &job.ProfileHash, &job.Profile, &job.InputHash,
			&job.Error, &job.CreatedAt, &job.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContentJob{}, ErrContentJobNotFound
	}
	if err != nil {
		return ContentJob{}, fmt.Errorf("find content job by key: %w", err)
	}
	return job, nil
}

func (r *ContentRepository) GetContentJob(ctx context.Context, ownerID, jobID uuid.UUID) (ContentJob, error) {
	var job ContentJob
	err := r.pool.QueryRow(ctx, `SELECT id,owner_id,video_id,result_id,transcript_id,revision,state,stage,
		run_epoch,COALESCE(lease_owner,''),lease_until,next_run_at,first_started_at,deadline_at,
		profile_hash,profile_snapshot,input_hash,error,created_at,completed_at
		FROM content_jobs WHERE owner_id=$1 AND id=$2`, ownerID, jobID).
		Scan(&job.ID, &job.OwnerID, &job.VideoID, &job.ResultID, &job.TranscriptID, &job.Revision,
			&job.State, &job.Stage, &job.RunEpoch, &job.LeaseOwner, &job.LeaseUntil, &job.NextRunAt,
			&job.FirstStartedAt, &job.DeadlineAt, &job.ProfileHash, &job.Profile, &job.InputHash,
			&job.Error, &job.CreatedAt, &job.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContentJob{}, ErrContentJobNotFound
	}
	if err != nil {
		return ContentJob{}, fmt.Errorf("get content job: %w", err)
	}
	return job, nil
}

func (r *ContentRepository) GetResultContent(ctx context.Context, ownerID, resultID uuid.UUID) (ContentResultView, error) {
	var view ContentResultView
	var resultType string
	err := r.pool.QueryRow(ctx, `SELECT r.result_type,h.latest_job_id,h.active_artifact_id,a.validated_payload,j.state
		FROM parse_results r JOIN videos v ON v.id=r.video_id AND v.owner_id=$1
		LEFT JOIN result_content_heads h ON h.result_id=r.id
		LEFT JOIN content_artifacts a ON a.id=h.active_artifact_id AND a.result_id=r.id
		LEFT JOIN content_jobs j ON j.id=h.latest_job_id
		WHERE r.id=$2 AND v.lifecycle='ACTIVE'`, ownerID, resultID).
		Scan(&resultType, &view.LatestJobID, &view.ActiveArtifactID, &view.Artifact, &view.LatestJobState)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContentResultView{}, ErrContentNotFound
	}
	if err != nil {
		return ContentResultView{}, fmt.Errorf("get result content head: %w", err)
	}
	view.ResultID = resultID
	if resultType == "NO_SPEECH" {
		view.State = "NO_CONTENT"
	} else if view.ActiveArtifactID != nil {
		view.State = "READY"
	} else if view.LatestJobState != nil && *view.LatestJobState == "FAILED" {
		view.State = "FAILED"
	} else if view.LatestJobID != nil {
		view.State = "PENDING"
	} else {
		view.State = "UNAVAILABLE"
	}
	return view, nil
}

func (r *ContentRepository) GetContentArtifact(ctx context.Context, ownerID, artifactID uuid.UUID) (json.RawMessage, error) {
	var payload []byte
	err := r.pool.QueryRow(ctx, `SELECT a.validated_payload FROM content_artifacts a
		JOIN videos v ON v.id=a.video_id AND v.owner_id=a.owner_id
		WHERE a.id=$1 AND a.owner_id=$2 AND a.state='READY' AND v.lifecycle='ACTIVE'`, artifactID, ownerID).
		Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrContentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get content artifact: %w", err)
	}
	return payload, nil
}

func (r *ContentRepository) ClaimContentJob(ctx context.Context, owner, workspaceID string) (*ContentJob, error) {
	if owner == "" || workspaceID == "" {
		return nil, errors.New("content worker owner and workspace identity are required")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin content job claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var job ContentJob
	err = tx.QueryRow(ctx, `WITH candidate AS (
		SELECT j.id FROM content_jobs j JOIN videos v ON v.id=j.video_id AND v.owner_id=j.owner_id
		WHERE j.state IN ('READY','RETRY_WAIT') AND j.next_run_at<=clock_timestamp()
		  AND v.lifecycle='ACTIVE' AND (j.deadline_at IS NULL OR j.deadline_at>clock_timestamp())
		  AND j.profile_snapshot->>'workspace_id'=$2
		ORDER BY j.next_run_at,j.created_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT 1
	)
	UPDATE content_jobs j SET state='RUNNING',run_epoch=j.run_epoch+1,lease_owner=$1,
		lease_until=clock_timestamp()+interval '90 seconds',first_started_at=COALESCE(j.first_started_at,clock_timestamp()),
		deadline_at=COALESCE(j.deadline_at,clock_timestamp()+interval '30 minutes'),error=NULL
	FROM candidate c WHERE j.id=c.id
	RETURNING j.id,j.owner_id,j.video_id,j.result_id,j.transcript_id,j.revision,j.state,j.stage,j.run_epoch,
		j.lease_owner,j.lease_until,j.next_run_at,j.first_started_at,j.deadline_at,j.profile_hash,
		j.profile_snapshot,j.input_hash,j.error,j.created_at,j.completed_at`, owner, workspaceID).
		Scan(&job.ID, &job.OwnerID, &job.VideoID, &job.ResultID, &job.TranscriptID, &job.Revision,
			&job.State, &job.Stage, &job.RunEpoch, &job.LeaseOwner, &job.LeaseUntil, &job.NextRunAt,
			&job.FirstStartedAt, &job.DeadlineAt, &job.ProfileHash, &job.Profile, &job.InputHash,
			&job.Error, &job.CreatedAt, &job.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty content claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim content job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit content job claim: %w", err)
	}
	return &job, nil
}

func (r *ContentRepository) RenewContentLease(ctx context.Context, job ContentJob) (bool, error) {
	result, err := r.pool.Exec(ctx, `UPDATE content_jobs SET lease_until=clock_timestamp()+interval '90 seconds'
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$2 AND lease_owner=$3
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return false, fmt.Errorf("renew content job lease: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func (r *ContentRepository) MaintainContentJobs(ctx context.Context) (int64, error) {
	var affected int64
	result, err := r.pool.Exec(ctx, `UPDATE content_jobs SET state='FAILED',stage='DONE',
		error=jsonb_build_object('code','CONTENT_DEADLINE_EXCEEDED','retryable',false),
		completed_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL
		WHERE state IN ('READY','RUNNING','RETRY_WAIT') AND deadline_at IS NOT NULL AND deadline_at<=clock_timestamp()`)
	if err != nil {
		return 0, fmt.Errorf("fail expired content jobs: %w", err)
	}
	affected += result.RowsAffected()
	result, err = r.pool.Exec(ctx, `UPDATE content_jobs SET state='FAILED',stage='DONE',
		error=jsonb_build_object('code','CONTENT_NOT_STARTED_WITHIN_24H','retryable',false),
		completed_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL
		WHERE state IN ('READY','RETRY_WAIT') AND first_started_at IS NULL
		AND created_at<=clock_timestamp()-interval '24 hours'`)
	if err != nil {
		return affected, fmt.Errorf("fail never-started content jobs: %w", err)
	}
	affected += result.RowsAffected()
	result, err = r.pool.Exec(ctx, `UPDATE content_jobs SET state='RETRY_WAIT',next_run_at=clock_timestamp(),
		error=jsonb_build_object('code','CONTENT_LEASE_EXPIRED','retryable',true),lease_owner=NULL,lease_until=NULL
		WHERE state='RUNNING' AND deadline_at>clock_timestamp() AND lease_until<=clock_timestamp()`)
	if err != nil {
		return affected, fmt.Errorf("recover expired content leases: %w", err)
	}
	affected += result.RowsAffected()
	return affected, nil
}

func lockContentJob(ctx context.Context, tx pgx.Tx, job ContentJob) error {
	var state, owner string
	var epoch int64
	var leaseValid, deadlineValid bool
	err := tx.QueryRow(ctx, `SELECT state,run_epoch,COALESCE(lease_owner,''),
		lease_until>clock_timestamp(),deadline_at>clock_timestamp()
		FROM content_jobs WHERE id=$1 FOR UPDATE`, job.ID).Scan(&state, &epoch, &owner, &leaseValid, &deadlineValid)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrContentLeaseLost
	}
	if err != nil {
		return fmt.Errorf("lock content job execution: %w", err)
	}
	if state != "RUNNING" || epoch != job.RunEpoch || owner != job.LeaseOwner || !leaseValid || !deadlineValid {
		return ErrContentLeaseLost
	}
	return nil
}

func (r *ContentRepository) LoadContentInput(ctx context.Context, job ContentJob) (ContentInput, error) {
	var input ContentInput
	err := r.pool.QueryRow(ctx, `SELECT r.transcript_segment_count FROM content_jobs j
		JOIN videos v ON v.id=j.video_id AND v.owner_id=j.owner_id AND v.lifecycle='ACTIVE'
		JOIN parse_results r ON r.id=j.result_id AND r.video_id=j.video_id AND r.transcript_id=j.transcript_id
		JOIN transcripts t ON t.id=j.transcript_id AND t.video_id=j.video_id
		WHERE j.id=$1 AND j.state='RUNNING' AND j.run_epoch=$2 AND j.lease_owner=$3
		AND j.lease_until>clock_timestamp() AND j.deadline_at>clock_timestamp()
		AND r.result_type='TEXT' AND t.state='READY'`, job.ID, job.RunEpoch, job.LeaseOwner).Scan(&input.TotalSegments)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContentInput{}, ErrContentLeaseLost
	}
	if err != nil {
		return ContentInput{}, fmt.Errorf("load content transcript identity: %w", err)
	}
	rows, err := r.pool.Query(ctx, `SELECT seq,text,start_ms,end_ms FROM transcript_segments
		WHERE transcript_id=$1 ORDER BY seq LIMIT 1201`, job.TranscriptID)
	if err != nil {
		return ContentInput{}, fmt.Errorf("load bounded content transcript: %w", err)
	}
	defer rows.Close()
	input.Segments = make([]ContentSegment, 0, 1201)
	for rows.Next() {
		var segment ContentSegment
		if err := rows.Scan(&segment.Seq, &segment.Text, &segment.StartMS, &segment.EndMS); err != nil {
			return ContentInput{}, fmt.Errorf("scan content transcript segment: %w", err)
		}
		input.Segments = append(input.Segments, segment)
	}
	if err := rows.Err(); err != nil {
		return ContentInput{}, fmt.Errorf("iterate content transcript: %w", err)
	}
	if len(input.Segments) == 0 || input.TotalSegments <= 0 || input.Segments[0].Seq != 1 {
		return ContentInput{}, errors.New("content transcript is empty or not sequential")
	}
	return input, nil
}

func (r *ContentRepository) EnsureContentPlan(ctx context.Context, job ContentJob,
	batches []ContentBatchPlan, coverage json.RawMessage) error {
	if len(batches) == 0 || len(batches) > 12 || !json.Valid(coverage) {
		return errors.New("content plan is empty or outside its batch budget")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin content plan commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockContentJob(ctx, tx, job); err != nil {
		return err
	}
	var existing int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM content_job_batches WHERE job_id=$1`, job.ID).Scan(&existing); err != nil {
		return fmt.Errorf("count persisted content batches: %w", err)
	}
	if existing == 0 {
		for index, batch := range batches {
			if batch.BatchNo != index+1 || len(batch.Ranges) == 0 || len(batch.Ranges) > 100 ||
				batch.InputBytes < 1 || batch.InputBytes > 16*1024 || len(batch.InputHash) != sha256.Size {
				return errors.New("content batch plan contains invalid source bounds")
			}
			ranges, err := json.Marshal(batch.Ranges)
			if err != nil {
				return fmt.Errorf("encode fixed content source ranges: %w", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO content_job_batches(job_id,batch_no,source_ranges,input_hash,input_bytes,state)
				VALUES($1,$2,$3::jsonb,$4,$5,'PLANNED')`, job.ID, batch.BatchNo, string(ranges),
				batch.InputHash, batch.InputBytes); err != nil {
				return fmt.Errorf("persist content batch plan: %w", err)
			}
		}
		updated, err := tx.Exec(ctx, `UPDATE content_jobs SET coverage_json=$2::jsonb,stage='EXTRACT'
			WHERE id=$1 AND state='RUNNING' AND run_epoch=$3 AND lease_owner=$4
			AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`, job.ID, string(coverage), job.RunEpoch, job.LeaseOwner)
		if err != nil {
			return fmt.Errorf("persist initial content coverage: %w", err)
		}
		if updated.RowsAffected() != 1 {
			return ErrContentLeaseLost
		}
	} else {
		rows, err := tx.Query(ctx, `SELECT batch_no,source_ranges,input_hash,input_bytes FROM content_job_batches
			WHERE job_id=$1 ORDER BY batch_no`, job.ID)
		if err != nil {
			return fmt.Errorf("read immutable content plan: %w", err)
		}
		index := 0
		for rows.Next() {
			var batch ContentBatchPlan
			var ranges []byte
			if err := rows.Scan(&batch.BatchNo, &ranges, &batch.InputHash, &batch.InputBytes); err != nil {
				rows.Close()
				return fmt.Errorf("scan immutable content plan: %w", err)
			}
			if index >= len(batches) {
				rows.Close()
				return errors.New("persisted content plan has extra batches")
			}
			if err := json.Unmarshal(ranges, &batch.Ranges); err != nil {
				rows.Close()
				return fmt.Errorf("decode immutable content ranges: %w", err)
			}
			expectedRanges, _ := json.Marshal(batches[index].Ranges)
			if batch.BatchNo != batches[index].BatchNo || batch.InputBytes != batches[index].InputBytes ||
				!bytes.Equal(batch.InputHash, batches[index].InputHash) || !jsonEqual(ranges, expectedRanges) {
				rows.Close()
				return errors.New("recomputed content plan differs from its persisted immutable ranges")
			}
			index++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate immutable content plan: %w", err)
		}
		rows.Close()
		var savedCoverage []byte
		if err := tx.QueryRow(ctx, `SELECT coverage_json FROM content_jobs WHERE id=$1`, job.ID).Scan(&savedCoverage); err != nil {
			return fmt.Errorf("read persisted content coverage: %w", err)
		}
		if index != len(batches) || !jsonEqual(savedCoverage, coverage) {
			return errors.New("recomputed content coverage differs from the persisted plan")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit content batch plan: %w", err)
	}
	return nil
}

func (r *ContentRepository) LoadContentBatches(ctx context.Context, job ContentJob) ([]ContentBatch, error) {
	rows, err := r.pool.Query(ctx, `SELECT batch_no,source_ranges,input_hash,input_bytes,state,attempts,
		validated_output,COALESCE(provider_request_id,''),COALESCE(prompt_tokens,0),COALESCE(completion_tokens,0),
		COALESCE(outcome_code,'') FROM content_job_batches WHERE job_id=$1 ORDER BY batch_no`, job.ID)
	if err != nil {
		return nil, fmt.Errorf("load content batches: %w", err)
	}
	defer rows.Close()
	items := make([]ContentBatch, 0, 12)
	for rows.Next() {
		var item ContentBatch
		var ranges, output []byte
		if err := rows.Scan(&item.BatchNo, &ranges, &item.InputHash, &item.InputBytes, &item.State,
			&item.Attempts, &output, &item.ProviderRequestID, &item.PromptTokens, &item.CompletionTokens,
			&item.OutcomeCode); err != nil {
			return nil, fmt.Errorf("scan content batch: %w", err)
		}
		if err := json.Unmarshal(ranges, &item.Ranges); err != nil {
			return nil, fmt.Errorf("decode content batch ranges: %w", err)
		}
		item.Output = append([]byte(nil), output...)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate content batches: %w", err)
	}
	return items, nil
}

func (r *ContentRepository) ReserveContentBatchAttempt(ctx context.Context, job ContentJob, batchNo int) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin content batch attempt reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockContentJob(ctx, tx, job); err != nil {
		return 0, err
	}
	var attempts int
	err = tx.QueryRow(ctx, `UPDATE content_job_batches b SET state='CALLING',attempts=b.attempts+1,
		provider_request_id=NULL,prompt_tokens=NULL,completion_tokens=NULL,outcome_code=NULL,updated_at=clock_timestamp()
		FROM content_jobs j WHERE b.job_id=$1 AND b.batch_no=$2 AND b.state IN ('PLANNED','CALLING')
		AND b.attempts<2 AND j.id=b.job_id AND j.total_model_attempts<26 AND j.state='RUNNING'
		AND j.run_epoch=$3 AND j.lease_owner=$4 AND j.lease_until>clock_timestamp() AND j.deadline_at>clock_timestamp()
		RETURNING b.attempts`, job.ID, batchNo, job.RunEpoch, job.LeaseOwner).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, errors.New("content batch attempt budget is exhausted or batch is not runnable")
	}
	if err != nil {
		return 0, fmt.Errorf("reserve content batch attempt: %w", err)
	}
	updated, err := tx.Exec(ctx, `UPDATE content_jobs SET total_model_attempts=total_model_attempts+1,stage='EXTRACT'
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$2 AND lease_owner=$3
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`, job.ID, job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return 0, fmt.Errorf("increment content model budget: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return 0, ErrContentLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit content batch attempt reservation: %w", err)
	}
	return attempts, nil
}

func (r *ContentRepository) RecordContentBatchFailure(ctx context.Context, job ContentJob, batchNo int, code string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin content batch failure update: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockContentJob(ctx, tx, job); err != nil {
		return err
	}
	updated, err := tx.Exec(ctx, `UPDATE content_job_batches b SET outcome_code=$3,
		state=CASE WHEN b.attempts>=2 THEN 'FAILED' ELSE 'CALLING' END,updated_at=clock_timestamp()
		FROM content_jobs j WHERE b.job_id=$1 AND b.batch_no=$2 AND b.state='CALLING' AND j.id=b.job_id
		AND j.state='RUNNING' AND j.run_epoch=$4 AND j.lease_owner=$5
		AND j.lease_until>clock_timestamp() AND j.deadline_at>clock_timestamp()`,
		job.ID, batchNo, safeContentCode(code), job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return fmt.Errorf("record content batch failure: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return ErrContentLeaseLost
	}
	return tx.Commit(ctx)
}

func (r *ContentRepository) SaveContentBatch(ctx context.Context, job ContentJob, batch ContentBatch,
	output json.RawMessage, providerID string, promptTokens, completionTokens int64) error {
	if !json.Valid(output) || promptTokens < 0 || completionTokens < 0 || len(providerID) > 256 {
		return errors.New("validated content batch output metadata is invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin content batch commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockContentJob(ctx, tx, job); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE content_job_batches b SET state='READY',validated_output=$3::jsonb,
		provider_request_id=NULLIF($4,''),prompt_tokens=$5,completion_tokens=$6,outcome_code='OK',updated_at=clock_timestamp()
		FROM content_jobs j WHERE b.job_id=$1 AND b.batch_no=$2 AND b.state='CALLING' AND b.attempts BETWEEN 1 AND 2
		AND j.id=b.job_id AND j.state='RUNNING' AND j.run_epoch=$7 AND j.lease_owner=$8
		AND j.lease_until>clock_timestamp() AND j.deadline_at>clock_timestamp()`,
		job.ID, batch.BatchNo, string(output), providerID, promptTokens, completionTokens, job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return fmt.Errorf("commit validated content batch: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("content batch is not awaiting a validated output")
	}
	return tx.Commit(ctx)
}

func (r *ContentRepository) ReserveContentMergeAttempt(ctx context.Context, job ContentJob) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin content merge attempt reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockContentJob(ctx, tx, job); err != nil {
		return 0, err
	}
	var ready, attempts int
	if err := tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='READY'),
		(SELECT merge_attempts FROM content_jobs WHERE id=$1) FROM content_job_batches WHERE job_id=$1`, job.ID).
		Scan(&ready, &attempts); err != nil {
		return 0, fmt.Errorf("verify content batches before merge: %w", err)
	}
	if ready == 0 || attempts >= 2 {
		return 0, errors.New("content merge is not ready or has exhausted its attempt budget")
	}
	result, err := tx.Exec(ctx, `UPDATE content_jobs SET merge_attempts=merge_attempts+1,
		total_model_attempts=total_model_attempts+1,stage='MERGE'
		WHERE id=$1 AND total_model_attempts<26 AND merge_attempts<2 AND state='RUNNING'
		AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return 0, fmt.Errorf("reserve content merge attempt: %w", err)
	}
	if result.RowsAffected() != 1 {
		return 0, errors.New("content model attempt budget is exhausted")
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit content merge attempt: %w", err)
	}
	return attempts + 1, nil
}

func (r *ContentRepository) SaveContentMerge(ctx context.Context, job ContentJob, output json.RawMessage) error {
	if !json.Valid(output) {
		return errors.New("content merge candidate must be valid JSON")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin content merge checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockContentJob(ctx, tx, job); err != nil {
		return err
	}
	updated, err := tx.Exec(ctx, `UPDATE content_jobs SET merge_output=$2::jsonb,stage='PUBLISH'
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$3 AND lease_owner=$4
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`, job.ID, string(output), job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return fmt.Errorf("persist validated content merge checkpoint: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return ErrContentLeaseLost
	}
	return tx.Commit(ctx)
}

func (r *ContentRepository) LoadContentMerge(ctx context.Context, job ContentJob) (json.RawMessage, error) {
	var output []byte
	err := r.pool.QueryRow(ctx, `SELECT merge_output FROM content_jobs WHERE id=$1 AND state='RUNNING'
		AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner).Scan(&output)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrContentLeaseLost
	}
	if err != nil {
		return nil, fmt.Errorf("load content merge checkpoint: %w", err)
	}
	return output, nil
}

func (r *ContentRepository) PublishContent(ctx context.Context, artifact ContentArtifactInsert) (bool, error) {
	job := artifact.Job
	if artifact.ID == uuid.Nil || !json.Valid(artifact.Payload) || !json.Valid(artifact.Coverage) ||
		len(artifact.Sources) == 0 || len(artifact.Sources) > 1200 || len(job.ProfileHash) != sha256.Size || len(job.InputHash) != sha256.Size {
		return false, errors.New("content artifact identity or bounds are invalid")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin content artifact publication: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var lifecycle string
	err = tx.QueryRow(ctx, `SELECT lifecycle FROM videos WHERE id=$1 AND owner_id=$2 FOR UPDATE`, job.VideoID, job.OwnerID).Scan(&lifecycle)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrContentLeaseLost
	}
	if err != nil {
		return false, fmt.Errorf("lock content publication video: %w", err)
	}
	if lifecycle != "ACTIVE" {
		return false, ErrContentLeaseLost
	}
	if err := lockContentJob(ctx, tx, job); err != nil {
		return false, err
	}
	var savedResultID, savedTranscriptID uuid.UUID
	var savedRevision int
	var savedProfileHash, savedInputHash []byte
	if err := tx.QueryRow(ctx, `SELECT result_id,transcript_id,revision,profile_hash,input_hash FROM content_jobs WHERE id=$1`, job.ID).
		Scan(&savedResultID, &savedTranscriptID, &savedRevision, &savedProfileHash, &savedInputHash); err != nil {
		return false, fmt.Errorf("verify content publication identity: %w", err)
	}
	if savedResultID != job.ResultID || savedTranscriptID != job.TranscriptID || savedRevision != job.Revision ||
		!bytes.Equal(savedProfileHash, job.ProfileHash) || !bytes.Equal(savedInputHash, job.InputHash) {
		return false, errors.New("content publication differs from its immutable job snapshot")
	}
	_, err = tx.Exec(ctx, `INSERT INTO content_artifacts(id,owner_id,video_id,result_id,transcript_id,revision,
		job_id,profile_hash,input_hash,validated_payload,state)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'READY')`, artifact.ID, job.OwnerID, job.VideoID,
		job.ResultID, job.TranscriptID, job.Revision, job.ID, job.ProfileHash, job.InputHash, string(artifact.Payload))
	if err != nil {
		return false, fmt.Errorf("insert immutable content artifact: %w", err)
	}
	type sourcePayload struct {
		RefID        string `json:"ref_id"`
		SentenceSeq  int    `json:"sentence_seq"`
		StartOffset  int    `json:"start_offset"`
		EndOffset    int    `json:"end_offset"`
		TextSnapshot string `json:"text_snapshot"`
		TextHash     string `json:"text_hash"`
	}
	sourceRows := make([]sourcePayload, 0, len(artifact.Sources))
	for _, source := range artifact.Sources {
		sum := sha256.Sum256([]byte(source.Text))
		if source.RefID == "" || source.TranscriptID != job.TranscriptID || source.SentenceSeq <= 0 ||
			source.StartOffset < 0 || source.EndOffset <= source.StartOffset || source.EndOffset-source.StartOffset != len(source.Text) ||
			len(source.TextHash) != sha256.Size || !bytes.Equal(sum[:], source.TextHash) {
			return false, errors.New("content source reference is invalid")
		}
		sourceRows = append(sourceRows, sourcePayload{RefID: source.RefID, SentenceSeq: source.SentenceSeq,
			StartOffset: source.StartOffset, EndOffset: source.EndOffset, TextSnapshot: source.Text,
			TextHash: hex.EncodeToString(source.TextHash)})
	}
	encodedSources, err := json.Marshal(sourceRows)
	if err != nil {
		return false, fmt.Errorf("encode content source references: %w", err)
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO content_source_refs(artifact_id,owner_id,video_id,result_id,transcript_id,
		ref_id,sentence_seq,start_offset,end_offset,text_snapshot,text_hash)
		SELECT $1,$2,$3,$4,$5,src.ref_id,src.sentence_seq,src.start_offset,src.end_offset,
			src.text_snapshot,decode(src.text_hash,'hex')
		FROM jsonb_to_recordset($6::jsonb) AS src(ref_id text,sentence_seq integer,start_offset integer,
			end_offset integer,text_snapshot text,text_hash text)
		JOIN transcript_segments ts ON ts.transcript_id=$5 AND ts.seq=src.sentence_seq
		WHERE src.start_offset>=0 AND src.end_offset>src.start_offset
		  AND src.end_offset<=octet_length(ts.text)
		  AND convert_from(substring(convert_to(ts.text,'UTF8') FROM src.start_offset+1
			FOR src.end_offset-src.start_offset),'UTF8')=src.text_snapshot`, artifact.ID, job.OwnerID,
		job.VideoID, job.ResultID, job.TranscriptID, string(encodedSources))
	if err != nil {
		return false, fmt.Errorf("insert verified content source references: %w", err)
	}
	if inserted.RowsAffected() != int64(len(sourceRows)) {
		return false, errors.New("content source range does not match the fixed transcript bytes")
	}
	result, err := tx.Exec(ctx, `UPDATE result_content_heads SET active_artifact_id=$2,updated_at=clock_timestamp()
		WHERE result_id=$1 AND owner_id=$3 AND video_id=$4 AND next_revision>$5
		AND (active_artifact_id IS NULL OR EXISTS (
			SELECT 1 FROM content_artifacts active WHERE active.id=result_content_heads.active_artifact_id
			AND active.revision<$5))`, job.ResultID, artifact.ID, job.OwnerID, job.VideoID, job.Revision)
	if err != nil {
		return false, fmt.Errorf("advance active content artifact: %w", err)
	}
	if result.RowsAffected() != 1 {
		return false, errors.New("content head rejected an older or mismatched revision")
	}
	completed, err := tx.Exec(ctx, `UPDATE content_jobs SET state='SUCCEEDED',stage='DONE',coverage_json=$2::jsonb,
		completed_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,error=NULL
		WHERE id=$1 AND state='RUNNING' AND run_epoch=$3 AND lease_owner=$4
		AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		job.ID, string(artifact.Coverage), job.RunEpoch, job.LeaseOwner)
	if err != nil {
		return false, fmt.Errorf("complete content job: %w", err)
	}
	if completed.RowsAffected() != 1 {
		return false, ErrContentLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit content artifact: %w", err)
	}
	return true, nil
}

func (r *ContentRepository) RetryContentJob(ctx context.Context, job ContentJob, nextRun time.Time, code, message string) error {
	encoded, _ := json.Marshal(map[string]any{"code": safeContentCode(code), "message": message, "retryable": true})
	result, err := r.pool.Exec(ctx, `UPDATE content_jobs SET state='RETRY_WAIT',next_run_at=LEAST($4,deadline_at),
		error=$5::jsonb,lease_owner=NULL,lease_until=NULL WHERE id=$1 AND state='RUNNING'
		AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()`,
		job.ID, job.RunEpoch, job.LeaseOwner, nextRun, string(encoded))
	if err != nil {
		return fmt.Errorf("retry content job: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrContentLeaseLost
	}
	return nil
}

func (r *ContentRepository) FailContentJob(ctx context.Context, job ContentJob, code, message string) error {
	encoded, _ := json.Marshal(map[string]any{"code": safeContentCode(code), "message": message, "retryable": false})
	result, err := r.pool.Exec(ctx, `UPDATE content_jobs SET state='FAILED',stage='DONE',error=$4::jsonb,
		completed_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL WHERE id=$1 AND state='RUNNING'
		AND run_epoch=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`, job.ID, job.RunEpoch, job.LeaseOwner, string(encoded))
	if err != nil {
		return fmt.Errorf("fail content job: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrContentLeaseLost
	}
	return nil
}

func safeContentCode(value string) string {
	if value == "" || len(value) > 80 {
		return "CONTENT_JOB_FAILED"
	}
	for _, r := range value {
		if !((r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return "CONTENT_JOB_FAILED"
		}
	}
	return value
}
