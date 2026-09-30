-- Dependencies between steps of the same stage (a small DAG per stage). Existing steps have none.
ALTER TABLE artifact_workflow_step
    ADD COLUMN depends_on_json TEXT NOT NULL DEFAULT '[]';
