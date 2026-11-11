CREATE TABLE asr_manual_resolutions (
    submission_id uuid PRIMARY KEY REFERENCES asr_submissions(id),
    operator_id text NOT NULL CHECK (length(operator_id) BETWEEN 1 AND 128),
    terminal_status text NOT NULL CHECK (terminal_status IN (
        'SUCCEEDED', 'FAILED', 'REJECTED', 'CANCELED', 'CANCELLED', 'EXPIRED', 'NOT_ACCEPTED'
    )),
    evidence_ref text NOT NULL CHECK (length(evidence_ref) BETWEEN 1 AND 512),
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 1000),
    superseded_at timestamptz,
    superseded_reason text,
    superseded_by_provider_task_id text,
    CHECK ((superseded_at IS NULL AND superseded_reason IS NULL AND superseded_by_provider_task_id IS NULL)
        OR (superseded_at IS NOT NULL AND superseded_reason IS NOT NULL)),
    resolved_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
