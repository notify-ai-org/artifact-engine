ALTER TABLE artifact_workflow
    ADD COLUMN attributes_json TEXT,
    ADD COLUMN failure_message TEXT;

CREATE TABLE artifact_job (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(128),
    artifact_id VARCHAR(64),
    job_type VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempts INTEGER NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    lease_owner VARCHAR(256),
    lease_expires_at TIMESTAMPTZ,
    attributes_json TEXT NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_artifact_job_status_next_attempt
    ON artifact_job (status, next_attempt_at);
CREATE INDEX ix_artifact_job_tenant_artifact
    ON artifact_job (tenant_id, artifact_id);

UPDATE artifact_workflow
SET attributes_json = (workflow_json::jsonb -> 'attributes')::TEXT,
    failure_message = workflow_json::jsonb ->> 'failureMessage';

INSERT INTO artifact_job
    (id, tenant_id, artifact_id, job_type, status, attempts, next_attempt_at,
     lease_owner, lease_expires_at, attributes_json, last_error, created_at, updated_at)
SELECT step.job_record_id,
       step.job_record_json::jsonb ->> 'tenantId',
       step.job_record_json::jsonb ->> 'artifactId',
       step.job_record_json::jsonb ->> 'type',
       step.job_record_json::jsonb ->> 'status',
       (step.job_record_json::jsonb ->> 'attempts')::INTEGER,
       (step.job_record_json::jsonb ->> 'nextAttemptAt')::TIMESTAMPTZ,
       step.job_record_json::jsonb ->> 'leaseOwner',
       (step.job_record_json::jsonb ->> 'leaseExpiresAt')::TIMESTAMPTZ,
       (step.job_record_json::jsonb -> 'attributes')::TEXT,
       step.job_record_json::jsonb ->> 'lastError',
       (step.job_record_json::jsonb ->> 'createdAt')::TIMESTAMPTZ,
       (step.job_record_json::jsonb ->> 'updatedAt')::TIMESTAMPTZ
FROM artifact_workflow_step AS step;

ALTER TABLE artifact_workflow
    ALTER COLUMN attributes_json SET NOT NULL,
    DROP COLUMN workflow_json;

ALTER TABLE artifact_workflow_step
    DROP COLUMN job_record_json,
    ADD CONSTRAINT fk_artifact_workflow_step_job
        FOREIGN KEY (job_record_id) REFERENCES artifact_job(id);
