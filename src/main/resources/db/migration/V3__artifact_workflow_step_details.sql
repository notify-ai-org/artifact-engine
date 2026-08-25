ALTER TABLE artifact_workflow_step
    ADD COLUMN created_at TIMESTAMPTZ,
    ADD COLUMN updated_at TIMESTAMPTZ,
    ADD COLUMN job_record_json TEXT,
    ADD COLUMN status VARCHAR(32),
    ADD COLUMN process_start_at TIMESTAMPTZ,
    ADD COLUMN process_end_at TIMESTAMPTZ,
    ADD COLUMN prev_step_id VARCHAR(36),
    ADD COLUMN next_step_id VARCHAR(36),
    ADD COLUMN attributes_json TEXT,
    ADD COLUMN failure_message TEXT;

-- Backfill installations created with V2 from the canonical workflow JSON snapshot.
UPDATE artifact_workflow_step AS stored
SET created_at = (step.value ->> 'createdAt')::TIMESTAMPTZ,
    updated_at = (step.value ->> 'updatedAt')::TIMESTAMPTZ,
    job_record_json = (step.value -> 'jobRecord')::TEXT,
    status = step.value ->> 'status',
    process_start_at = (step.value ->> 'processStartAt')::TIMESTAMPTZ,
    process_end_at = (step.value ->> 'processEndAt')::TIMESTAMPTZ,
    prev_step_id = step.value ->> 'prevStepId',
    next_step_id = step.value ->> 'nextStepId',
    attributes_json = (step.value -> 'attributes')::TEXT,
    failure_message = step.value ->> 'failureMessage'
FROM artifact_workflow AS workflow
CROSS JOIN LATERAL jsonb_array_elements(
    workflow.workflow_json::jsonb -> 'workflowSteps') AS step(value)
WHERE stored.workflow_id = workflow.id
  AND stored.id = step.value ->> 'id';

ALTER TABLE artifact_workflow_step
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN job_record_json SET NOT NULL,
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN attributes_json SET NOT NULL;
