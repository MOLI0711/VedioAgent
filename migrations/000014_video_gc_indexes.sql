ALTER TABLE video_deletion_jobs DROP CONSTRAINT video_deletion_jobs_phase_check;
ALTER TABLE video_deletion_jobs ADD CONSTRAINT video_deletion_jobs_phase_check
    CHECK (phase IN (
        'WAIT_UPLOADS', 'DELETE_OBJECTS', 'CLEAR_VIDEO_REFS',
        'PURGE_QUESTION_EVIDENCE', 'REDACT_QUESTIONS', 'PURGE_QUESTION_STEPS',
        'REDACT_CONVERSATIONS', 'REDACT_UPLOADS', 'REDACT_PARSE_TASKS',
        'PURGE_CONTENT_HEADS', 'PURGE_CONTENT_SOURCE_REFS', 'PURGE_CONTENT_ARTIFACTS',
        'PURGE_CONTENT_JOB_BATCHES', 'PURGE_CONTENT_JOBS', 'PURGE_TASK_STEPS',
        'PURGE_EMBEDDING_BATCHES', 'PURGE_ASR_SUBMISSIONS', 'PURGE_KAFKA_REJECTIONS',
        'PURGE_INBOX_EVENTS', 'PURGE_OUTBOX_EVENTS', 'PURGE_PARSE_RESULTS',
        'PURGE_CHUNK_EMBEDDINGS', 'PURGE_EMBEDDING_SETS', 'PURGE_CHUNK_SOURCES',
        'PURGE_CHUNKS', 'PURGE_CHUNK_SETS', 'PURGE_TRANSCRIPT_SEGMENTS',
        'PURGE_TRANSCRIPTS', 'PURGE_BLOBS', 'PURGE_GC_OBJECTS', 'FINALIZE', 'DONE'
    ));

CREATE INDEX questions_video_gc_idx ON questions(video_id,id);
CREATE INDEX question_evidence_video_gc_idx ON question_evidence(video_id,question_id,evidence_id);
CREATE INDEX conversations_video_gc_idx ON conversations(video_id,id);
CREATE INDEX result_content_heads_video_gc_idx ON result_content_heads(video_id,result_id);
CREATE INDEX content_source_refs_video_gc_idx ON content_source_refs(video_id,artifact_id,ref_id);
CREATE INDEX content_artifacts_video_gc_idx ON content_artifacts(video_id,id);
CREATE INDEX content_jobs_video_gc_idx ON content_jobs(video_id,id);
CREATE INDEX task_steps_video_gc_idx ON task_steps(video_id,task_id,stage);
CREATE INDEX embedding_batches_video_gc_idx ON embedding_batches(video_id,task_id,chunk_set_id,config_hash,batch_index);
CREATE INDEX asr_submissions_video_gc_idx ON asr_submissions(video_id,id);
CREATE INDEX kafka_rejections_dlq_event_idx ON kafka_rejections(dlq_event_id);
CREATE INDEX inbox_events_task_idx ON inbox_events(task_id) WHERE task_id IS NOT NULL;
CREATE INDEX outbox_events_aggregate_idx ON outbox_events(aggregate_id,event_id);
CREATE INDEX embedding_sets_video_gc_idx ON embedding_sets(video_id,id);
CREATE INDEX chunk_sources_video_gc_idx ON chunk_sources(video_id,chunk_id,transcript_segment_seq);
CREATE INDEX chunk_sets_video_gc_idx ON chunk_sets(video_id,id);
