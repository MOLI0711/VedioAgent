ALTER TABLE parse_tasks
    ADD CONSTRAINT parse_tasks_id_video_unique UNIQUE (id, video_id);

ALTER TABLE videos
    ADD COLUMN probe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE blobs
    ADD COLUMN artifact_fingerprint bytea CHECK (artifact_fingerprint IS NULL OR octet_length(artifact_fingerprint) = 32),
    ADD COLUMN profile_hash bytea CHECK (profile_hash IS NULL OR octet_length(profile_hash) = 32),
    ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE UNIQUE INDEX blobs_reusable_artifact_idx
    ON blobs(video_id, kind, artifact_fingerprint, profile_hash)
    WHERE state = 'READY' AND artifact_fingerprint IS NOT NULL AND profile_hash IS NOT NULL;

CREATE TABLE task_steps (
    task_id uuid NOT NULL,
    video_id uuid NOT NULL,
    stage text NOT NULL CHECK (stage IN ('PROBE', 'AUDIO', 'ASR', 'TRANSCRIPT', 'CHUNK', 'EMBEDDING', 'PUBLISH')),
    state text NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'READY', 'NO_AUDIO', 'FAILED')),
    input_fingerprint bytea NOT NULL CHECK (octet_length(input_fingerprint) = 32),
    config_hash bytea NOT NULL CHECK (octet_length(config_hash) = 32),
    source_blob_id uuid,
    output_blob_id uuid,
    output_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    failure_count integer NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    last_run_epoch bigint NOT NULL DEFAULT 0 CHECK (last_run_epoch >= 0),
    last_error jsonb,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (task_id, stage),
    FOREIGN KEY (task_id, video_id) REFERENCES parse_tasks(id, video_id),
    FOREIGN KEY (source_blob_id, video_id) REFERENCES blobs(id, video_id),
    FOREIGN KEY (output_blob_id, video_id) REFERENCES blobs(id, video_id),
    CHECK (state NOT IN ('READY', 'NO_AUDIO') OR completed_at IS NOT NULL)
);

CREATE INDEX task_steps_blob_ref_idx ON task_steps(output_blob_id) WHERE output_blob_id IS NOT NULL;
