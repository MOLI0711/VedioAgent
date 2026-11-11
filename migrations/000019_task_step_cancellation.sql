ALTER TABLE task_steps
    DROP CONSTRAINT task_steps_state_check,
    ADD CONSTRAINT task_steps_state_check
        CHECK (state IN ('PENDING', 'RUNNING', 'READY', 'NO_AUDIO', 'NO_SPEECH', 'NO_CONTENT', 'FAILED', 'CANCELLED'));
