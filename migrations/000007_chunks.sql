ALTER TABLE transcript_segments
    ADD CONSTRAINT transcript_segments_transcript_video_seq_unique UNIQUE(transcript_id, video_id, seq);

CREATE TABLE chunk_sets (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    created_by_task uuid NOT NULL,
    input_fingerprint bytea NOT NULL CHECK (octet_length(input_fingerprint) = 32),
    config_hash bytea NOT NULL CHECK (octet_length(config_hash) = 32),
    counter_fingerprint bytea NOT NULL CHECK (octet_length(counter_fingerprint) = 32),
    measurement_unit text NOT NULL CHECK (measurement_unit = 'UTF8_BYTES'),
    config_snapshot jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('BUILDING', 'READY', 'NO_CONTENT', 'BROKEN')),
    expected_chunk_count integer NOT NULL CHECK (expected_chunk_count BETWEEN 0 AND 1000000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(id, video_id),
    UNIQUE(id, video_id, transcript_id),
    FOREIGN KEY(transcript_id, video_id) REFERENCES transcripts(id, video_id),
    FOREIGN KEY(created_by_task, video_id) REFERENCES parse_tasks(id, video_id),
    CHECK (state <> 'READY' OR expected_chunk_count > 0),
    CHECK (state <> 'NO_CONTENT' OR expected_chunk_count = 0)
);
CREATE UNIQUE INDEX chunk_sets_building_task_idx
    ON chunk_sets(video_id, created_by_task, transcript_id, input_fingerprint, config_hash)
    WHERE state = 'BUILDING';
CREATE UNIQUE INDEX chunk_sets_reusable_idx
    ON chunk_sets(video_id, transcript_id, input_fingerprint, config_hash)
    WHERE state IN ('READY', 'NO_CONTENT');
CREATE INDEX chunk_sets_transcript_idx ON chunk_sets(transcript_id, state);

CREATE TABLE chunks (
    id uuid PRIMARY KEY,
    chunk_set_id uuid NOT NULL,
    video_id uuid NOT NULL,
    seq integer NOT NULL CHECK (seq > 0),
    text text NOT NULL CHECK (length(text) > 0),
    text_sha256 bytea NOT NULL CHECK (octet_length(text_sha256) = 32),
    byte_count integer NOT NULL CHECK (byte_count = octet_length(text) AND byte_count BETWEEN 1 AND 768),
    start_ms bigint NOT NULL CHECK (start_ms >= 0),
    end_ms bigint NOT NULL CHECK (end_ms > start_ms),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(chunk_set_id, seq),
    UNIQUE(id, chunk_set_id, video_id),
    FOREIGN KEY(chunk_set_id, video_id) REFERENCES chunk_sets(id, video_id) ON DELETE CASCADE
);
CREATE INDEX chunks_video_time_idx ON chunks(video_id, chunk_set_id, start_ms, seq);

CREATE TABLE chunk_sources (
    chunk_id uuid NOT NULL,
    chunk_set_id uuid NOT NULL,
    video_id uuid NOT NULL,
    transcript_id uuid NOT NULL,
    transcript_segment_seq integer NOT NULL CHECK (transcript_segment_seq > 0),
    start_byte integer NOT NULL CHECK (start_byte >= 0),
    end_byte integer NOT NULL CHECK (end_byte > start_byte),
    PRIMARY KEY(chunk_id, transcript_segment_seq, start_byte, end_byte),
    FOREIGN KEY(chunk_id, chunk_set_id, video_id) REFERENCES chunks(id, chunk_set_id, video_id) ON DELETE CASCADE,
    FOREIGN KEY(chunk_set_id, video_id, transcript_id) REFERENCES chunk_sets(id, video_id, transcript_id) ON DELETE CASCADE,
    FOREIGN KEY(transcript_id, video_id, transcript_segment_seq)
        REFERENCES transcript_segments(transcript_id, video_id, seq)
);
CREATE INDEX chunk_sources_transcript_idx ON chunk_sources(transcript_id, transcript_segment_seq);

ALTER TABLE task_steps ADD COLUMN output_chunk_set_id uuid;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_chunk_set_fk
    FOREIGN KEY(output_chunk_set_id, video_id) REFERENCES chunk_sets(id, video_id);
CREATE INDEX task_steps_chunk_set_idx ON task_steps(output_chunk_set_id)
    WHERE output_chunk_set_id IS NOT NULL;
ALTER TABLE task_steps DROP CONSTRAINT task_steps_transcript_completed_check;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_completed_artifact_check
    CHECK (state NOT IN ('READY', 'NO_AUDIO', 'NO_SPEECH', 'NO_CONTENT') OR completed_at IS NOT NULL);
ALTER TABLE task_steps ADD CONSTRAINT task_steps_artifact_stage_check
    CHECK ((output_transcript_id IS NULL OR stage = 'TRANSCRIPT')
       AND (output_chunk_set_id IS NULL OR stage = 'CHUNK'));
ALTER TABLE task_steps DROP CONSTRAINT task_steps_state_check;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_state_check
    CHECK (state IN ('PENDING', 'RUNNING', 'READY', 'NO_AUDIO', 'NO_SPEECH', 'NO_CONTENT', 'FAILED'));
