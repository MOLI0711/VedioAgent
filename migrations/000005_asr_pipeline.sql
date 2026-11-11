CREATE TABLE asr_submissions (
    id uuid PRIMARY KEY,
    task_id uuid NOT NULL,
    video_id uuid NOT NULL,
    asr_input_fingerprint bytea NOT NULL CHECK (octet_length(asr_input_fingerprint) = 32),
    profile_hash bytea NOT NULL CHECK (octet_length(profile_hash) = 32),
    audio_blob_id uuid NOT NULL,
    submit_seq smallint NOT NULL CHECK (submit_seq BETWEEN 1 AND 2),
    operation_id uuid NOT NULL UNIQUE,
    state text NOT NULL CHECK (state IN (
        'SUBMITTING', 'UNKNOWN', 'ACCEPTED', 'RUNNING', 'RESULT_READY', 'SUCCEEDED', 'FAILED', 'REJECTED'
    )),
    region text NOT NULL,
    workspace_id text NOT NULL,
    model text NOT NULL,
    provider_task_id text,
    provider_request_id text,
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    submitted_at timestamptz,
    unknown_since timestamptz,
    provider_deadline_at timestamptz NOT NULL,
    next_poll_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    poll_count integer NOT NULL DEFAULT 0 CHECK (poll_count >= 0),
    result_download_attempts integer NOT NULL DEFAULT 0 CHECK (result_download_attempts BETWEEN 0 AND 3),
    provider_status text,
    raw_result_blob_id uuid,
    last_error jsonb,
    last_run_epoch bigint NOT NULL CHECK (last_run_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(task_id, submit_seq),
    UNIQUE(id, video_id),
    UNIQUE(id, task_id, video_id),
    FOREIGN KEY(task_id, video_id) REFERENCES parse_tasks(id, video_id),
    FOREIGN KEY(audio_blob_id, video_id) REFERENCES blobs(id, video_id),
    FOREIGN KEY(raw_result_blob_id, video_id) REFERENCES blobs(id, video_id),
    CHECK (state NOT IN ('ACCEPTED', 'RUNNING', 'RESULT_READY', 'SUCCEEDED') OR provider_task_id IS NOT NULL),
    CHECK (state <> 'SUCCEEDED' OR raw_result_blob_id IS NOT NULL)
);

CREATE UNIQUE INDEX asr_submissions_provider_task_idx
    ON asr_submissions(region, workspace_id, provider_task_id)
    WHERE provider_task_id IS NOT NULL;
CREATE INDEX asr_submissions_gate_idx
    ON asr_submissions(state, provider_deadline_at)
    WHERE state IN ('SUBMITTING', 'UNKNOWN', 'ACCEPTED', 'RUNNING');
CREATE INDEX asr_submissions_due_idx
    ON asr_submissions(task_id, next_poll_at DESC);

ALTER TABLE task_steps
    ADD COLUMN output_asr_submission_id uuid;
ALTER TABLE task_steps
    ADD CONSTRAINT task_steps_asr_submission_fk
    FOREIGN KEY (output_asr_submission_id, task_id, video_id)
    REFERENCES asr_submissions(id, task_id, video_id);
CREATE INDEX task_steps_asr_submission_idx
    ON task_steps(output_asr_submission_id) WHERE output_asr_submission_id IS NOT NULL;
