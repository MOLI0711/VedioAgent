CREATE TABLE content_jobs (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES users(id),
    video_id uuid NOT NULL,
    result_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    create_request_key text,
    request_hash bytea,
    profile_snapshot jsonb NOT NULL,
    profile_hash bytea NOT NULL CHECK (octet_length(profile_hash) = 32),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    state text NOT NULL CHECK (state IN ('READY', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    stage text NOT NULL CHECK (stage IN ('EXTRACT', 'MERGE', 'PUBLISH', 'DONE')),
    run_epoch bigint NOT NULL DEFAULT 0 CHECK (run_epoch >= 0),
    lease_owner text,
    lease_until timestamptz,
    next_run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    first_started_at timestamptz,
    deadline_at timestamptz,
    total_model_attempts integer NOT NULL DEFAULT 0 CHECK (total_model_attempts BETWEEN 0 AND 26),
    merge_attempts integer NOT NULL DEFAULT 0 CHECK (merge_attempts BETWEEN 0 AND 2),
    merge_output jsonb,
    coverage_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    error jsonb,
    completed_at timestamptz,
    UNIQUE(result_id, revision),
    UNIQUE(owner_id, create_request_key),
    UNIQUE(id, result_id, video_id),
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    FOREIGN KEY(result_id, video_id, transcript_id) REFERENCES parse_results(id, video_id, transcript_id),
    CHECK ((create_request_key IS NULL AND request_hash IS NULL)
        OR (create_request_key IS NOT NULL AND request_hash IS NOT NULL AND octet_length(request_hash) = 32)),
    CHECK ((state = 'RUNNING' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'RUNNING' AND lease_owner IS NULL AND lease_until IS NULL)),
    CHECK ((state IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND completed_at IS NOT NULL)
        OR (state IN ('READY', 'RUNNING', 'RETRY_WAIT') AND completed_at IS NULL)),
    CHECK ((first_started_at IS NULL AND deadline_at IS NULL)
        OR (first_started_at IS NOT NULL AND deadline_at > first_started_at)),
    CHECK (jsonb_typeof(profile_snapshot) = 'object' AND jsonb_typeof(coverage_json) = 'object'),
    CHECK ((state IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND stage = 'DONE')
        OR (state IN ('READY', 'RUNNING', 'RETRY_WAIT') AND stage <> 'DONE'))
);
CREATE INDEX content_jobs_due_idx ON content_jobs(state, next_run_at, created_at)
    WHERE state IN ('READY', 'RETRY_WAIT');
CREATE INDEX content_jobs_lease_idx ON content_jobs(lease_until) WHERE state = 'RUNNING';
CREATE INDEX content_jobs_video_result_idx ON content_jobs(owner_id, video_id, result_id, revision DESC);

CREATE TABLE content_job_batches (
    job_id uuid NOT NULL REFERENCES content_jobs(id) ON DELETE CASCADE,
    batch_no integer NOT NULL CHECK (batch_no BETWEEN 1 AND 12),
    source_ranges jsonb NOT NULL CHECK (jsonb_typeof(source_ranges) = 'array'),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    input_bytes integer NOT NULL CHECK (input_bytes BETWEEN 0 AND 16384),
    state text NOT NULL CHECK (state IN ('PLANNED', 'CALLING', 'READY', 'FAILED')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 2),
    validated_output jsonb,
    provider_request_id text,
    prompt_tokens bigint CHECK (prompt_tokens IS NULL OR prompt_tokens >= 0),
    completion_tokens bigint CHECK (completion_tokens IS NULL OR completion_tokens >= 0),
    outcome_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id, batch_no),
    CHECK ((state = 'READY' AND validated_output IS NOT NULL AND attempts BETWEEN 1 AND 2)
        OR (state <> 'READY' AND validated_output IS NULL)),
    CHECK ((state = 'PLANNED' AND attempts = 0)
        OR (state IN ('CALLING', 'READY') AND attempts BETWEEN 1 AND 2)
        OR (state = 'FAILED' AND attempts = 2))
);

CREATE TABLE content_artifacts (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES users(id),
    video_id uuid NOT NULL,
    result_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    job_id uuid NOT NULL,
    profile_hash bytea NOT NULL CHECK (octet_length(profile_hash) = 32),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    validated_payload jsonb NOT NULL CHECK (jsonb_typeof(validated_payload) = 'object'),
    state text NOT NULL CHECK (state IN ('READY')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(result_id, revision),
    UNIQUE(id, result_id, video_id),
    UNIQUE(id, result_id, video_id, transcript_id),
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    FOREIGN KEY(result_id, video_id, transcript_id) REFERENCES parse_results(id, video_id, transcript_id),
    FOREIGN KEY(job_id, result_id, video_id) REFERENCES content_jobs(id, result_id, video_id),
    FOREIGN KEY(result_id, revision) REFERENCES content_jobs(result_id, revision)
);
CREATE INDEX content_artifacts_result_idx ON content_artifacts(result_id, revision DESC);

CREATE TABLE content_source_refs (
    artifact_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    video_id uuid NOT NULL,
    result_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    ref_id text NOT NULL CHECK (ref_id ~ '^CREF[1-9][0-9]*$'),
    sentence_seq integer NOT NULL CHECK (sentence_seq > 0),
    start_offset integer NOT NULL CHECK (start_offset >= 0),
    end_offset integer NOT NULL CHECK (end_offset > start_offset),
    text_snapshot text NOT NULL CHECK (length(text_snapshot) > 0),
    text_hash bytea NOT NULL CHECK (octet_length(text_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(artifact_id, ref_id),
    UNIQUE(artifact_id, transcript_id, sentence_seq, start_offset, end_offset),
    FOREIGN KEY(artifact_id, result_id, video_id, transcript_id)
        REFERENCES content_artifacts(id, result_id, video_id, transcript_id) ON DELETE CASCADE,
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    FOREIGN KEY(transcript_id, video_id, sentence_seq)
        REFERENCES transcript_segments(transcript_id, video_id, seq),
    CHECK (end_offset - start_offset = octet_length(text_snapshot))
);
CREATE INDEX content_source_refs_transcript_idx ON content_source_refs(transcript_id, sentence_seq);

CREATE TABLE result_content_heads (
    result_id uuid PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES users(id),
    video_id uuid NOT NULL,
    next_revision integer NOT NULL CHECK (next_revision > 0),
    latest_job_id uuid,
    active_artifact_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(result_id, video_id),
    FOREIGN KEY(result_id, video_id) REFERENCES parse_results(id, video_id) ON DELETE CASCADE,
    FOREIGN KEY(owner_id, video_id) REFERENCES videos(owner_id, id),
    FOREIGN KEY(latest_job_id, result_id, video_id) REFERENCES content_jobs(id, result_id, video_id),
    FOREIGN KEY(active_artifact_id, result_id, video_id) REFERENCES content_artifacts(id, result_id, video_id)
);
CREATE INDEX result_content_heads_video_idx ON result_content_heads(owner_id, video_id);
