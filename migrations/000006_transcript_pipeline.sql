CREATE TABLE transcripts (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL REFERENCES videos(id),
    source_asr_blob_id uuid,
    source_audio_blob_id uuid,
    created_by_task uuid NOT NULL REFERENCES parse_tasks(id),
    input_fingerprint bytea NOT NULL CHECK (octet_length(input_fingerprint) = 32),
    config_hash bytea NOT NULL CHECK (octet_length(config_hash) = 32),
    state text NOT NULL CHECK (state IN ('BUILDING', 'READY', 'NO_SPEECH', 'BROKEN')),
    media_duration_ms bigint NOT NULL CHECK (media_duration_ms > 0),
    audio_origin_ms bigint NOT NULL,
    expected_segment_count integer NOT NULL CHECK (expected_segment_count BETWEEN 0 AND 1000000),
    last_committed_seq integer NOT NULL DEFAULT 0 CHECK (last_committed_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(video_id, input_fingerprint, config_hash),
    UNIQUE(id, video_id),
    FOREIGN KEY(source_asr_blob_id, video_id) REFERENCES blobs(id, video_id),
    FOREIGN KEY(source_audio_blob_id, video_id) REFERENCES blobs(id, video_id),
    CHECK (last_committed_seq <= expected_segment_count),
    CHECK (audio_origin_ms BETWEEN -media_duration_ms AND media_duration_ms),
    CHECK (state <> 'READY' OR (expected_segment_count > 0 AND last_committed_seq = expected_segment_count)),
    CHECK (state <> 'NO_SPEECH' OR (expected_segment_count = 0 AND last_committed_seq = 0))
);
CREATE INDEX transcripts_reuse_idx ON transcripts(video_id, input_fingerprint, config_hash, state);

CREATE TABLE transcript_segments (
    transcript_id uuid NOT NULL,
    video_id uuid NOT NULL,
    seq integer NOT NULL CHECK (seq > 0),
    source_sentence_index integer NOT NULL CHECK (source_sentence_index > 0),
    provider_start_ms bigint NOT NULL CHECK (provider_start_ms >= 0),
    provider_end_ms bigint NOT NULL CHECK (provider_end_ms > provider_start_ms),
    start_correction_ms integer NOT NULL DEFAULT 0 CHECK (start_correction_ms >= 0),
    start_ms bigint NOT NULL CHECK (start_ms >= 0),
    end_ms bigint NOT NULL CHECK (end_ms > start_ms),
    end_correction_ms integer NOT NULL DEFAULT 0 CHECK (end_correction_ms <= 0),
    text text NOT NULL CHECK (length(btrim(text)) > 0),
    PRIMARY KEY(transcript_id, seq),
    FOREIGN KEY(transcript_id, video_id) REFERENCES transcripts(id, video_id) ON DELETE CASCADE
);
CREATE INDEX transcript_segments_video_time_idx ON transcript_segments(video_id, transcript_id, start_ms, seq);

ALTER TABLE task_steps ADD COLUMN output_transcript_id uuid;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_transcript_fk
    FOREIGN KEY(output_transcript_id, video_id) REFERENCES transcripts(id, video_id);
CREATE INDEX task_steps_transcript_idx ON task_steps(output_transcript_id)
    WHERE output_transcript_id IS NOT NULL;

ALTER TABLE task_steps DROP CONSTRAINT task_steps_state_check;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_state_check
    CHECK (state IN ('PENDING', 'RUNNING', 'READY', 'NO_AUDIO', 'NO_SPEECH', 'FAILED'));
ALTER TABLE task_steps ADD CONSTRAINT task_steps_transcript_completed_check
    CHECK (state NOT IN ('READY', 'NO_AUDIO', 'NO_SPEECH') OR completed_at IS NOT NULL);
