ALTER TABLE videos
    DROP CONSTRAINT videos_file_state_check;

ALTER TABLE videos
    ADD CONSTRAINT videos_file_state_check
    CHECK (file_state IN ('UPLOADING', 'UPLOADED', 'VALID', 'REJECTED', 'EXPIRED', 'CANCELLED'));

ALTER TABLE upload_sessions
    ADD COLUMN termination_reason text CHECK (termination_reason IN ('CANCEL', 'EXPIRED')),
    ADD COLUMN termination_after timestamptz,
    ADD CONSTRAINT upload_sessions_aborting_reason_check
        CHECK (status <> 'ABORTING' OR termination_reason IS NOT NULL);

CREATE INDEX upload_sessions_termination_idx
    ON upload_sessions(status, termination_after, operation_lease_until)
    WHERE status IN ('INITIATING', 'UPLOADING', 'ABORTING');
