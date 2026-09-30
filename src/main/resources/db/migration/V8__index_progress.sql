-- State carried between the per-chunk index jobs of a chunked source ingest (chunker window,
-- UTF-8 carry, counters). One row per artifact version; deleted when indexing finishes.
CREATE TABLE artifact_index_progress (
    tenant_id VARCHAR(128) NOT NULL,
    artifact_id VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    last_chunk INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, artifact_id, version)
);
