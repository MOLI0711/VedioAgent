CREATE TABLE embedding_sets (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL,
    chunk_set_id uuid NOT NULL,
    created_by_task uuid NOT NULL,
    input_fingerprint bytea NOT NULL CHECK (octet_length(input_fingerprint) = 32),
    config_hash bytea NOT NULL CHECK (octet_length(config_hash) = 32),
    config_snapshot jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('BUILDING', 'READY', 'NO_CONTENT', 'BROKEN')),
    expected_embedding_count integer NOT NULL CHECK (expected_embedding_count BETWEEN 0 AND 1000000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(id, video_id),
    UNIQUE(id, video_id, chunk_set_id),
    UNIQUE(id, video_id, created_by_task),
    FOREIGN KEY(chunk_set_id, video_id) REFERENCES chunk_sets(id, video_id),
    FOREIGN KEY(created_by_task, video_id) REFERENCES parse_tasks(id, video_id),
    CHECK (state <> 'READY' OR expected_embedding_count > 0),
    CHECK (state <> 'NO_CONTENT' OR expected_embedding_count = 0)
);
CREATE UNIQUE INDEX embedding_sets_building_task_idx
    ON embedding_sets(video_id, created_by_task, chunk_set_id, input_fingerprint, config_hash)
    WHERE state = 'BUILDING';
CREATE UNIQUE INDEX embedding_sets_reusable_idx
    ON embedding_sets(video_id, chunk_set_id, input_fingerprint, config_hash)
    WHERE state IN ('READY', 'NO_CONTENT');
CREATE INDEX embedding_sets_chunk_set_idx ON embedding_sets(chunk_set_id, state);

CREATE TABLE chunk_embeddings (
    embedding_set_id uuid NOT NULL,
    video_id uuid NOT NULL,
    chunk_set_id uuid NOT NULL,
    chunk_id uuid NOT NULL,
    input_sha256 bytea NOT NULL CHECK (octet_length(input_sha256) = 32),
    embedding vector(1024) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(embedding_set_id, chunk_id),
    FOREIGN KEY(embedding_set_id, video_id, chunk_set_id)
        REFERENCES embedding_sets(id, video_id, chunk_set_id) ON DELETE CASCADE,
    FOREIGN KEY(chunk_id, chunk_set_id, video_id)
        REFERENCES chunks(id, chunk_set_id, video_id)
);
CREATE INDEX chunk_embeddings_video_set_idx ON chunk_embeddings(video_id, embedding_set_id, chunk_id);

CREATE TABLE embedding_batches (
    task_id uuid NOT NULL,
    video_id uuid NOT NULL,
    chunk_set_id uuid NOT NULL,
    config_hash bytea NOT NULL CHECK (octet_length(config_hash) = 32),
    batch_index integer NOT NULL CHECK (batch_index >= 0),
    first_chunk_seq integer NOT NULL CHECK (first_chunk_seq > 0),
    last_chunk_seq integer NOT NULL CHECK (last_chunk_seq >= first_chunk_seq),
    expected_input_count integer NOT NULL CHECK (expected_input_count BETWEEN 1 AND 10),
    input_fingerprint bytea NOT NULL CHECK (octet_length(input_fingerprint) = 32),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    state text NOT NULL CHECK (state IN ('PENDING', 'IN_FLIGHT', 'SUCCEEDED', 'FAILED')),
    provider_request_id text,
    usage_tokens bigint CHECK (usage_tokens IS NULL OR usage_tokens >= 0),
    last_error jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(task_id, chunk_set_id, config_hash, batch_index),
    FOREIGN KEY(task_id, video_id) REFERENCES parse_tasks(id, video_id),
    FOREIGN KEY(chunk_set_id, video_id) REFERENCES chunk_sets(id, video_id),
    CHECK (last_chunk_seq - first_chunk_seq + 1 = expected_input_count)
);
CREATE INDEX embedding_batches_state_idx ON embedding_batches(task_id, state, batch_index);

ALTER TABLE task_steps ADD COLUMN output_embedding_set_id uuid;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_embedding_set_fk
    FOREIGN KEY(output_embedding_set_id, video_id) REFERENCES embedding_sets(id, video_id);
CREATE INDEX task_steps_embedding_set_idx ON task_steps(output_embedding_set_id)
    WHERE output_embedding_set_id IS NOT NULL;
ALTER TABLE task_steps DROP CONSTRAINT task_steps_artifact_stage_check;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_artifact_stage_check
    CHECK ((output_transcript_id IS NULL OR stage = 'TRANSCRIPT')
       AND (output_chunk_set_id IS NULL OR stage = 'CHUNK')
       AND (output_embedding_set_id IS NULL OR stage = 'EMBEDDING'));
