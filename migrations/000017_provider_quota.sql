CREATE TABLE provider_quota_gates (
    workspace_id text NOT NULL,
    model text NOT NULL,
    rpm_limit integer NOT NULL CHECK (rpm_limit > 0),
    tpm_limit bigint NOT NULL CHECK (tpm_limit > 0),
    cooldown_until timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, model)
);

CREATE TABLE provider_call_reservations (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL,
    model text NOT NULL,
    reserved_tokens bigint NOT NULL CHECK (reserved_tokens > 0),
    actual_tokens bigint CHECK (actual_tokens >= 0),
    outcome text NOT NULL DEFAULT 'RESERVED' CHECK (outcome IN ('RESERVED', 'SUCCEEDED', 'FAILED')),
    provider_request_id text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    FOREIGN KEY (workspace_id, model) REFERENCES provider_quota_gates(workspace_id, model),
    CHECK ((outcome = 'RESERVED' AND completed_at IS NULL)
        OR (outcome <> 'RESERVED' AND completed_at IS NOT NULL))
);

CREATE INDEX provider_call_reservations_window_idx
    ON provider_call_reservations(workspace_id, model, created_at);
