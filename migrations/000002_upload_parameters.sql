ALTER TABLE upload_sessions
    ADD COLUMN part_size_bytes bigint NOT NULL DEFAULT 8388608 CHECK (part_size_bytes > 0);

ALTER TABLE upload_sessions
    ADD COLUMN declared_content_type text NOT NULL DEFAULT 'video/mp4';
