CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE users (
    id uuid PRIMARY KEY,
    email text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE auth_sessions (
    token_hash bytea PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    csrf_hash bytea NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX auth_sessions_user_expiry_idx ON auth_sessions(user_id, expires_at);
CREATE INDEX auth_sessions_expiry_idx ON auth_sessions(expires_at);

CREATE TABLE videos (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES users(id),
    source_blob_id uuid,
    file_state text NOT NULL DEFAULT 'UPLOADING'
        CHECK (file_state IN ('UPLOADING', 'UPLOADED', 'VALID', 'REJECTED', 'EXPIRED')),
    lifecycle text NOT NULL DEFAULT 'ACTIVE'
        CHECK (lifecycle IN ('ACTIVE', 'DELETING', 'DELETED')),
    next_task_version integer NOT NULL DEFAULT 1 CHECK (next_task_version > 0),
    latest_task_id uuid,
    active_result_id uuid,
    source_name text NOT NULL,
    source_size_bytes bigint NOT NULL CHECK (source_size_bytes > 0),
    media_duration_ms bigint CHECK (media_duration_ms IS NULL OR media_duration_ms > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(owner_id, id)
);
CREATE INDEX videos_owner_created_idx ON videos(owner_id, created_at DESC, id DESC);

CREATE TABLE blobs (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL REFERENCES videos(id),
    kind text NOT NULL CHECK (kind IN ('SOURCE_VIDEO', 'AUDIO', 'ASR_RAW')),
    bucket text NOT NULL,
    object_key text NOT NULL UNIQUE,
    object_version text,
    sha256 bytea,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    state text NOT NULL CHECK (state IN ('STORED', 'READY', 'BROKEN', 'DELETING', 'DELETED')),
    created_by_task uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(id, video_id)
);
CREATE INDEX blobs_video_state_idx ON blobs(video_id, state);

ALTER TABLE videos
    ADD CONSTRAINT videos_source_blob_fk
    FOREIGN KEY(source_blob_id, id) REFERENCES blobs(id, video_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE parse_tasks (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    task_version integer NOT NULL CHECK (task_version > 0),
    create_request_key text,
    request_hash bytea NOT NULL,
    config_snapshot jsonb NOT NULL,
    status text NOT NULL CHECK (status IN (
        'WAIT_DISPATCH', 'READY', 'RUNNING', 'WAITING', 'RETRY_WAIT',
        'SUCCEEDED', 'FAILED', 'CANCELLED'
    )),
    current_stage text NOT NULL DEFAULT 'PROBE',
    wait_reason text,
    run_epoch bigint NOT NULL DEFAULT 0 CHECK (run_epoch >= 0),
    lease_owner text,
    lease_until timestamptz,
    next_run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    admitted_at timestamptz,
    deadline_at timestamptz NOT NULL,
    manual_retry_count integer NOT NULL DEFAULT 0 CHECK (manual_retry_count >= 0),
    last_error jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    UNIQUE(video_id, task_version),
    UNIQUE(id, video_id, owner_id)
);
CREATE UNIQUE INDEX parse_tasks_owner_create_key_idx
    ON parse_tasks(owner_id, create_request_key) WHERE create_request_key IS NOT NULL;
CREATE INDEX parse_tasks_due_idx ON parse_tasks(status, next_run_at, admitted_at, id);
CREATE INDEX parse_tasks_lease_idx ON parse_tasks(status, lease_until) WHERE status = 'RUNNING';

ALTER TABLE videos
    ADD CONSTRAINT videos_latest_task_fk
    FOREIGN KEY(latest_task_id, id, owner_id)
    REFERENCES parse_tasks(id, video_id, owner_id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE blobs
    ADD CONSTRAINT blobs_created_by_task_fk
    FOREIGN KEY(created_by_task) REFERENCES parse_tasks(id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE upload_sessions (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL UNIQUE REFERENCES videos(id),
    owner_id uuid NOT NULL REFERENCES users(id),
    create_request_key text NOT NULL,
    request_hash bytea NOT NULL,
    object_key text NOT NULL UNIQUE,
    upload_id text,
    status text NOT NULL CHECK (status IN (
        'INITIATING', 'UPLOADING', 'COMPLETING', 'VERIFYING', 'COMPLETE',
        'ABORTING', 'ABORTED', 'EXPIRED'
    )),
    parse_slot_reserved boolean NOT NULL DEFAULT true,
    operation_epoch bigint NOT NULL DEFAULT 0 CHECK (operation_epoch >= 0),
    operation_lease_until timestamptz,
    expected_size_bytes bigint NOT NULL CHECK (expected_size_bytes > 0),
    expires_at timestamptz NOT NULL,
    initial_task_id uuid REFERENCES parse_tasks(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(owner_id, create_request_key)
);
CREATE INDEX upload_sessions_expiry_idx ON upload_sessions(status, expires_at);

CREATE TABLE runtime_gates (
    name text PRIMARY KEY,
    config_version integer NOT NULL DEFAULT 1 CHECK (config_version > 0),
    max_inflight integer NOT NULL CHECK (max_inflight > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO runtime_gates(name, max_inflight) VALUES ('fun_asr', 2);

CREATE TABLE outbox_events (
    event_id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    topic text NOT NULL,
    event_key text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'FAILED')),
    send_epoch bigint NOT NULL DEFAULT 0 CHECK (send_epoch >= 0),
    lease_owner text,
    lease_until timestamptz,
    next_send_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX outbox_due_idx ON outbox_events(status, next_send_at, created_at);

CREATE TABLE inbox_events (
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    payload_hash bytea NOT NULL,
    task_id uuid REFERENCES parse_tasks(id),
    topic text NOT NULL,
    partition integer NOT NULL,
    offset_id bigint NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(consumer_name, event_id),
    UNIQUE(topic, partition, offset_id)
);

CREATE TABLE kafka_rejections (
    topic text NOT NULL,
    partition integer NOT NULL,
    offset_id bigint NOT NULL,
    payload_hash bytea NOT NULL,
    payload_prefix bytea NOT NULL,
    reason text NOT NULL,
    dlq_event_id uuid NOT NULL REFERENCES outbox_events(event_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(topic, partition, offset_id)
);

CREATE TABLE conversations (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES users(id),
    video_id uuid NOT NULL,
    title text,
    create_request_key text NOT NULL,
    request_hash bytea NOT NULL,
    next_turn_no bigint NOT NULL DEFAULT 1 CHECK (next_turn_no > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    archived_at timestamptz,
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    UNIQUE(owner_id, create_request_key),
    UNIQUE(id, video_id, owner_id)
);
CREATE INDEX conversations_owner_video_updated_idx
    ON conversations(owner_id, video_id, updated_at DESC, id DESC);

CREATE TABLE questions (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES users(id),
    video_id uuid NOT NULL,
    conversation_id uuid,
    turn_no bigint,
    question_text text NOT NULL,
    retrieval_query text,
    context_question_ids uuid[] NOT NULL DEFAULT '{}',
    context_snapshot jsonb NOT NULL DEFAULT '[]'::jsonb,
    client_request_key text NOT NULL,
    request_hash bytea NOT NULL,
    result_id uuid,
    generation_profile jsonb,
    retrieval_profile jsonb,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'INTERRUPTED')),
    owner_instance text,
    execution_id uuid,
    lease_until timestamptz,
    deadline_at timestamptz NOT NULL,
    answer_json jsonb,
    error jsonb,
    completed_at timestamptz,
    purged_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    FOREIGN KEY(conversation_id, video_id, owner_id) REFERENCES conversations(id, video_id, owner_id),
    UNIQUE(owner_id, client_request_key),
    UNIQUE(conversation_id, turn_no),
    CHECK ((status = 'RUNNING' AND completed_at IS NULL) OR status <> 'RUNNING'),
    CHECK (status <> 'SUCCEEDED' OR purged_at IS NOT NULL OR (answer_json IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE INDEX questions_owner_video_created_idx
    ON questions(owner_id, video_id, created_at DESC, id DESC);
CREATE INDEX questions_lease_idx ON questions(lease_until) WHERE status = 'RUNNING';
