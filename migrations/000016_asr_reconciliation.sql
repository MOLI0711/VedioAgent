ALTER TABLE asr_submissions DROP CONSTRAINT asr_submissions_state_check;
ALTER TABLE asr_submissions ADD CONSTRAINT asr_submissions_state_check CHECK (state IN (
    'SUBMITTING', 'UNKNOWN', 'ACCEPTED', 'RUNNING', 'RESULT_READY',
    'SUCCEEDED', 'FAILED', 'REJECTED', 'REMOTE_TERMINAL'
));

ALTER TABLE asr_submissions
    ADD COLUMN reconcile_epoch bigint NOT NULL DEFAULT 0 CHECK (reconcile_epoch >= 0),
    ADD COLUMN reconcile_owner text,
    ADD COLUMN reconcile_lease_until timestamptz,
    ADD COLUMN reconcile_attempts integer NOT NULL DEFAULT 0 CHECK (reconcile_attempts >= 0),
    ADD COLUMN remote_terminal_at timestamptz,
    ADD CONSTRAINT asr_submissions_reconcile_lease_check CHECK (
        (reconcile_owner IS NULL AND reconcile_lease_until IS NULL)
        OR (reconcile_owner IS NOT NULL AND reconcile_lease_until IS NOT NULL)
    ),
    ADD CONSTRAINT asr_submissions_remote_terminal_check CHECK (
        (state = 'REMOTE_TERMINAL' AND remote_terminal_at IS NOT NULL)
        OR (state <> 'REMOTE_TERMINAL' AND remote_terminal_at IS NULL)
    );

CREATE INDEX asr_submissions_reconcile_due_idx
    ON asr_submissions(region, workspace_id, next_poll_at, prepared_at)
    WHERE state IN ('SUBMITTING', 'UNKNOWN', 'ACCEPTED', 'RUNNING', 'RESULT_READY')
      AND provider_task_id IS NOT NULL;
