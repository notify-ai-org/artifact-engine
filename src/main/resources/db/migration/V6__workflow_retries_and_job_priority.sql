-- Queue priority: jobs resubmitted by the workflow retry scheduler yield to fresh work.
-- The default keeps rows written by older application versions valid.
ALTER TABLE artifact_job
    ADD COLUMN priority VARCHAR(16) NOT NULL DEFAULT 'NORMAL';

-- Workflow retries: CRASHED workflows are resumed from their failed steps with backoff until the
-- retry policy is exhausted, then marked DEAD_LETTERED and pushed to the dead-letter queue.
ALTER TABLE artifact_workflow
    ADD COLUMN retry_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_retry_at TIMESTAMPTZ;

CREATE INDEX ix_artifact_workflow_retry
    ON artifact_workflow (status, next_retry_at);
