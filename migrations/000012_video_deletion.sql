CREATE TABLE video_deletion_jobs (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL UNIQUE,
    owner_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED')),
    phase text NOT NULL CHECK (phase IN ('WAIT_UPLOADS', 'DELETE_OBJECTS', 'PURGE_ROWS', 'DONE')),
    run_epoch bigint NOT NULL DEFAULT 0 CHECK (run_epoch >= 0),
    lease_owner text,
    lease_until timestamptz,
    next_run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    FOREIGN KEY (owner_id, video_id) REFERENCES videos(owner_id, id),
    CHECK ((state = 'RUNNING' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'RUNNING' AND lease_owner IS NULL AND lease_until IS NULL)),
    CHECK ((state = 'SUCCEEDED' AND phase = 'DONE' AND completed_at IS NOT NULL)
        OR (state <> 'SUCCEEDED' AND phase <> 'DONE' AND completed_at IS NULL))
);
CREATE INDEX video_deletion_jobs_due_idx ON video_deletion_jobs(state, next_run_at, created_at)
    WHERE state IN ('PENDING', 'RETRY_WAIT');
CREATE INDEX video_deletion_jobs_lease_idx ON video_deletion_jobs(lease_until) WHERE state = 'RUNNING';

CREATE TABLE video_gc_objects (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL REFERENCES videos(id),
    bucket text NOT NULL,
    object_key text NOT NULL,
    object_version text,
    state text NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'DELETED')),
    lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    CHECK ((state = 'DELETED' AND deleted_at IS NOT NULL) OR (state <> 'DELETED' AND deleted_at IS NULL))
);
CREATE UNIQUE INDEX video_gc_objects_identity_idx
    ON video_gc_objects(video_id, bucket, object_key, COALESCE(object_version, ''));
CREATE INDEX video_gc_objects_pending_idx ON video_gc_objects(video_id, state, created_at);
