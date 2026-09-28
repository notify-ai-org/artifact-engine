-- Staged workflows: steps that share a stage run in parallel. Existing workflows are linear,
-- so each existing step becomes its own stage.
ALTER TABLE artifact_workflow_step ADD COLUMN stage INTEGER;
UPDATE artifact_workflow_step SET stage = step_order;
ALTER TABLE artifact_workflow_step ALTER COLUMN stage SET NOT NULL;

CREATE INDEX ix_artifact_workflow_step_stage
    ON artifact_workflow_step (workflow_id, stage);

-- One in-flight S3 multipart upload per artifact version. STORE_INIT inserts the row; parts and
-- completion read the upload id from it; completion or workflow crash deletes it.
CREATE TABLE artifact_multipart_upload (
    tenant_id VARCHAR(128) NOT NULL,
    artifact_id VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    storage_key VARCHAR(1024) NOT NULL,
    upload_id VARCHAR(1024) NOT NULL,
    part_size BIGINT NOT NULL,
    part_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    composite_sha256 VARCHAR(64),
    PRIMARY KEY (tenant_id, artifact_id, version)
);
