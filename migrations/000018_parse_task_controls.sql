ALTER TABLE parse_tasks
    ADD COLUMN retry_request_key text,
    ADD CONSTRAINT parse_tasks_manual_retry_limit_check
        CHECK (manual_retry_count BETWEEN 0 AND 1),
    ADD CONSTRAINT parse_tasks_retry_request_key_check
        CHECK ((manual_retry_count = 0 AND retry_request_key IS NULL)
            OR (manual_retry_count = 1 AND retry_request_key IS NOT NULL
                AND length(retry_request_key) BETWEEN 1 AND 128));

CREATE TABLE parse_task_retry_history (
    task_id uuid NOT NULL REFERENCES parse_tasks(id) ON DELETE CASCADE,
    retry_no smallint NOT NULL CHECK (retry_no = 1),
    request_key text NOT NULL CHECK (length(request_key) BETWEEN 1 AND 128),
    previous_stage text NOT NULL,
    previous_deadline_at timestamptz NOT NULL,
    previous_error jsonb NOT NULL,
    stage_failure_snapshot jsonb NOT NULL CHECK (jsonb_typeof(stage_failure_snapshot) = 'object'),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (task_id, retry_no),
    UNIQUE (task_id, request_key)
);
