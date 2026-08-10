CREATE TABLE artifact (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(256) NOT NULL,
    idempotency_fingerprint VARCHAR(64) NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    spool_path VARCHAR(2048),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    original_name VARCHAR(255) NOT NULL,
    media_type VARCHAR(160) NOT NULL,
    size_bytes BIGINT NOT NULL,
    storage_key VARCHAR(2048),
    storage_status VARCHAR(32) NOT NULL,
    index_status VARCHAR(32) NOT NULL,
    artifact_version BIGINT NOT NULL,
    tags_csv VARCHAR(4096),
    payload_json TEXT NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uk_artifact_tenant_idempotency UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT uk_artifact_tenant_id UNIQUE (tenant_id, id)
);

CREATE INDEX ix_artifact_tenant_checksum ON artifact (tenant_id, sha256);
CREATE INDEX ix_artifact_spool_path ON artifact (spool_path);
CREATE INDEX ix_artifact_tenant_media_status
    ON artifact (tenant_id, media_type, index_status);

CREATE TABLE artifact_outbox (
    id VARCHAR(64) PRIMARY KEY,
    job_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_artifact_outbox_created ON artifact_outbox (created_at);

CREATE TABLE artifact_audit_log (
    id VARCHAR(36) PRIMARY KEY,
    operation VARCHAR(80) NOT NULL,
    principal_id VARCHAR(256) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    artifact_id VARCHAR(64),
    outcome VARCHAR(32) NOT NULL,
    latency_millis BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    safe_details_json TEXT NOT NULL
);

CREATE INDEX ix_audit_tenant_time ON artifact_audit_log (tenant_id, occurred_at);
CREATE INDEX ix_audit_artifact_time ON artifact_audit_log (artifact_id, occurred_at);

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE artifact_chunk (
    id VARCHAR(64) NOT NULL,
    artifact_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    token_count INTEGER NOT NULL,
    page_number INTEGER,
    section VARCHAR(1024),
    coordinates_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_sha256 VARCHAR(64) NOT NULL,
    embedding_model VARCHAR(256) NOT NULL,
    embedding_version VARCHAR(128) NOT NULL,
    -- Keep this dimension aligned with artifact.vector.postgres.dimensions.
    embedding vector(1536) NOT NULL,
    text_search tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_chunk_embedding_identity
        UNIQUE (tenant_id, artifact_id, chunk_index, embedding_model, embedding_version),
    CONSTRAINT fk_chunk_artifact
        FOREIGN KEY (tenant_id, artifact_id)
        REFERENCES artifact (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX ix_chunk_tenant_artifact_index
    ON artifact_chunk (tenant_id, artifact_id, chunk_index);
CREATE INDEX ix_chunk_text_search ON artifact_chunk USING GIN (text_search);
CREATE INDEX ix_chunk_embedding_cosine
    ON artifact_chunk USING hnsw (embedding vector_cosine_ops);
