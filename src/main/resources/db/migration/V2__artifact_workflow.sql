CREATE TABLE artifact_workflow (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    process_start_at TIMESTAMPTZ,
    process_end_at TIMESTAMPTZ,
    workflow_json TEXT NOT NULL
);

CREATE INDEX ix_artifact_workflow_status_created
    ON artifact_workflow (status, created_at);

CREATE TABLE artifact_workflow_step (
    id VARCHAR(36) PRIMARY KEY,
    workflow_id VARCHAR(36) NOT NULL REFERENCES artifact_workflow(id) ON DELETE CASCADE,
    job_record_id VARCHAR(64) NOT NULL UNIQUE,
    step_order INTEGER NOT NULL,
    CONSTRAINT uk_artifact_workflow_step_order UNIQUE (workflow_id, step_order)
);

CREATE INDEX ix_artifact_workflow_step_workflow
    ON artifact_workflow_step (workflow_id, step_order);
