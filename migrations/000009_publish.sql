ALTER TABLE parse_tasks ADD CONSTRAINT parse_tasks_id_video_version_unique
    UNIQUE(id, video_id, task_version);

CREATE TABLE parse_results (
    id uuid PRIMARY KEY,
    task_id uuid NOT NULL,
    video_id uuid NOT NULL,
    task_version integer NOT NULL CHECK (task_version > 0),
    result_type text NOT NULL CHECK (result_type IN ('TEXT', 'NO_SPEECH')),
    transcript_id uuid NOT NULL,
    chunk_set_id uuid NOT NULL,
    embedding_set_id uuid NOT NULL,
    transcript_segment_count integer NOT NULL CHECK (transcript_segment_count >= 0),
    chunk_count integer NOT NULL CHECK (chunk_count >= 0),
    embedding_count integer NOT NULL CHECK (embedding_count >= 0),
    config_snapshot jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(task_id),
    UNIQUE(video_id, task_version),
    UNIQUE(id, video_id),
    FOREIGN KEY(task_id, video_id, task_version) REFERENCES parse_tasks(id, video_id, task_version),
    FOREIGN KEY(transcript_id, video_id) REFERENCES transcripts(id, video_id),
    FOREIGN KEY(chunk_set_id, video_id, transcript_id) REFERENCES chunk_sets(id, video_id, transcript_id),
    FOREIGN KEY(embedding_set_id, video_id, chunk_set_id) REFERENCES embedding_sets(id, video_id, chunk_set_id),
    CHECK ((result_type = 'TEXT' AND transcript_segment_count > 0 AND chunk_count > 0
        AND embedding_count = chunk_count)
       OR (result_type = 'NO_SPEECH' AND transcript_segment_count = 0 AND chunk_count = 0
        AND embedding_count = 0))
);
CREATE INDEX parse_results_video_version_idx ON parse_results(video_id, task_version DESC);

ALTER TABLE videos ADD CONSTRAINT videos_active_result_fk
    FOREIGN KEY(active_result_id, id) REFERENCES parse_results(id, video_id);
ALTER TABLE questions ADD CONSTRAINT questions_result_fk
    FOREIGN KEY(result_id, video_id) REFERENCES parse_results(id, video_id);

ALTER TABLE task_steps ADD COLUMN output_result_id uuid;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_result_fk
    FOREIGN KEY(output_result_id, video_id) REFERENCES parse_results(id, video_id);
CREATE INDEX task_steps_result_idx ON task_steps(output_result_id)
    WHERE output_result_id IS NOT NULL;
ALTER TABLE task_steps DROP CONSTRAINT task_steps_artifact_stage_check;
ALTER TABLE task_steps ADD CONSTRAINT task_steps_artifact_stage_check
    CHECK ((output_transcript_id IS NULL OR stage = 'TRANSCRIPT')
       AND (output_chunk_set_id IS NULL OR stage = 'CHUNK')
       AND (output_embedding_set_id IS NULL OR stage = 'EMBEDDING')
       AND (output_result_id IS NULL OR stage = 'PUBLISH'));
