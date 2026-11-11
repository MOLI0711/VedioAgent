package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
)

type PlaybackRepository struct{ pool *pgxpool.Pool }

func NewPlaybackRepository(pool *pgxpool.Pool) *PlaybackRepository {
	return &PlaybackRepository{pool: pool}
}

func (r *PlaybackRepository) GetPlaybackSource(ctx context.Context, ownerID, videoID uuid.UUID,
	questionID *uuid.UUID, evidenceID string, contentArtifactID *uuid.UUID, contentRefID string) (application.PlaybackSource, error) {
	var source application.PlaybackSource
	var err error
	if questionID == nil && contentArtifactID == nil {
		err = r.pool.QueryRow(ctx, `SELECT v.id,b.bucket,b.object_key,COALESCE(b.object_version,''),
			0::bigint,0::bigint,v.media_duration_ms
			FROM videos v JOIN blobs b ON b.id=v.source_blob_id AND b.video_id=v.id
			AND b.state='READY' AND b.kind='SOURCE_VIDEO'
			WHERE v.id=$1 AND v.owner_id=$2 AND v.lifecycle='ACTIVE' AND v.file_state='VALID'`,
			videoID, ownerID).Scan(&source.VideoID, &source.Bucket, &source.ObjectKey, &source.VersionID,
			&source.StartMS, &source.EndMS, &source.Duration)
	} else if questionID != nil {
		err = r.pool.QueryRow(ctx, `SELECT q.video_id,b.bucket,b.object_key,COALESCE(b.object_version,''),
			qe.start_ms,qe.end_ms,v.media_duration_ms
			FROM questions q
			JOIN videos v ON v.id=q.video_id AND v.owner_id=q.owner_id AND v.lifecycle='ACTIVE' AND v.file_state='VALID'
			JOIN question_evidence qe ON qe.question_id=q.id AND qe.video_id=q.video_id
			JOIN blobs b ON b.id=v.source_blob_id AND b.video_id=v.id AND b.state='READY' AND b.kind='SOURCE_VIDEO'
			WHERE v.id=$1 AND q.id=$2 AND q.owner_id=$3 AND q.status='SUCCEEDED' AND q.purged_at IS NULL
			  AND qe.evidence_id=$4`, videoID, *questionID, ownerID, evidenceID).
			Scan(&source.VideoID, &source.Bucket, &source.ObjectKey, &source.VersionID,
				&source.StartMS, &source.EndMS, &source.Duration)
	} else {
		err = r.pool.QueryRow(ctx, `SELECT a.video_id,b.bucket,b.object_key,COALESCE(b.object_version,''),
			ts.start_ms,ts.end_ms,v.media_duration_ms
			FROM content_artifacts a
			JOIN result_content_heads h ON h.result_id=a.result_id AND h.video_id=a.video_id AND h.active_artifact_id=a.id
			JOIN content_source_refs sr ON sr.artifact_id=a.id AND sr.owner_id=a.owner_id AND sr.video_id=a.video_id
			JOIN transcript_segments ts ON ts.transcript_id=sr.transcript_id AND ts.video_id=sr.video_id AND ts.seq=sr.sentence_seq
			JOIN videos v ON v.id=a.video_id AND v.owner_id=a.owner_id AND v.lifecycle='ACTIVE' AND v.file_state='VALID'
			JOIN blobs b ON b.id=v.source_blob_id AND b.video_id=v.id AND b.state='READY' AND b.kind='SOURCE_VIDEO'
			WHERE a.id=$1 AND a.owner_id=$2 AND a.video_id=$3 AND a.state='READY' AND sr.ref_id=$4`,
			*contentArtifactID, ownerID, videoID, contentRefID).
			Scan(&source.VideoID, &source.Bucket, &source.ObjectKey, &source.VersionID,
				&source.StartMS, &source.EndMS, &source.Duration)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return application.PlaybackSource{}, &application.ServiceError{Status: 404, Code: "NOT_FOUND", Message: "引用不存在"}
	}
	if err != nil {
		return application.PlaybackSource{}, fmt.Errorf("load evidence playback source: %w", err)
	}
	return source, nil
}
