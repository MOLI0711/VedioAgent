ALTER TABLE conversations ADD CONSTRAINT conversations_title_bytes_check
    CHECK (title IS NULL OR octet_length(title) <= 512) NOT VALID;
ALTER TABLE conversations ADD CONSTRAINT conversations_request_hash_length_check
    CHECK (octet_length(request_hash) = 32) NOT VALID;

ALTER TABLE parse_results ADD CONSTRAINT parse_results_id_video_transcript_unique
    UNIQUE(id, video_id, transcript_id);
ALTER TABLE questions ADD CONSTRAINT questions_id_video_result_unique
    UNIQUE(id, video_id, result_id);
ALTER TABLE questions ADD CONSTRAINT questions_turn_pair_check
    CHECK ((conversation_id IS NULL AND turn_no IS NULL) OR (conversation_id IS NOT NULL AND turn_no > 0)) NOT VALID;
CREATE INDEX questions_conversation_history_idx
    ON questions(conversation_id, turn_no DESC) WHERE conversation_id IS NOT NULL;

CREATE TABLE question_steps (
    question_id uuid NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
    seq integer NOT NULL CHECK (seq > 0),
    kind text NOT NULL CHECK (kind IN ('RETRIEVAL', 'GENERATION', 'VALIDATION', 'TOOL')),
    input_hash bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    provider_request_id text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    prompt_tokens bigint CHECK (prompt_tokens IS NULL OR prompt_tokens >= 0),
    completion_tokens bigint CHECK (completion_tokens IS NULL OR completion_tokens >= 0),
    usage_tokens bigint CHECK (usage_tokens IS NULL OR usage_tokens >= 0),
    outcome_code text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY(question_id, seq),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);
CREATE INDEX question_steps_question_time_idx ON question_steps(question_id, started_at);

CREATE TABLE question_evidence (
    question_id uuid NOT NULL,
    video_id uuid NOT NULL,
    result_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    evidence_id text NOT NULL CHECK (evidence_id ~ '^E[1-9][0-9]*$'),
    sentence_seq integer NOT NULL CHECK (sentence_seq > 0),
    start_byte integer NOT NULL CHECK (start_byte = 0),
    end_byte integer NOT NULL CHECK (end_byte > 0),
    original_text text NOT NULL CHECK (length(original_text) > 0),
    start_ms bigint NOT NULL CHECK (start_ms >= 0),
    end_ms bigint NOT NULL CHECK (end_ms > start_ms),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(question_id, evidence_id),
    UNIQUE(question_id, transcript_id, sentence_seq),
    FOREIGN KEY(question_id, video_id, result_id) REFERENCES questions(id, video_id, result_id) ON DELETE CASCADE,
    FOREIGN KEY(result_id, video_id, transcript_id)
        REFERENCES parse_results(id, video_id, transcript_id),
    FOREIGN KEY(transcript_id, video_id, sentence_seq)
        REFERENCES transcript_segments(transcript_id, video_id, seq),
    CHECK (end_byte = octet_length(original_text))
);
CREATE INDEX question_evidence_result_idx ON question_evidence(result_id, transcript_id, sentence_seq);

