- Images: PNG, JPEG, and WebP.
- Documents: PDF, TXT, Markdown, DOCX, and HTML.
- Streaming upload and ingestion-by-URL commands over TCP.
- Local durable spool with asynchronous S3 upload.
- Metadata and processing-state persistence.
- Text extraction, OCR, chunking, and embedding generation.
- Vector similarity search with metadata filtering.
- A versioned, multiplexed application protocol over TLS/TCP for ingestion, status, search, and retrieval.
- MCP tools and resources for artifact discovery and retrieval.
- Tenant isolation, checksums, idempotency, retry, and observability.

## 3. Success Criteria

- A 100 MB artifact can be ingested using streaming I/O with bounded heap usage.
- Repeating the same request with the same idempotency key creates one logical artifact.
- Re-ingesting identical bytes can be detected using SHA-256.
- An S3 outage causes artifacts to remain recoverable in the local spool.
- Pending artifacts upload automatically after S3 recovers.
- A process restart resumes incomplete extraction, embedding, and upload work.
- Semantic search returns relevant chunks and their source artifact references.
- MCP retrieval enforces the same tenant and authorization rules as native TCP retrieval.
- Deleting an artifact removes or tombstones its object, chunks, and embeddings.

## 7. Lifecycle and State Machines

Storage lifecycle:

```text
RECEIVING -> SPOOLED -> UPLOADING -> STORED
                         |             |
                         v             v
                   RETRY_PENDING     DELETED
                         |
                         v
                    DEAD_LETTER
```

Index lifecycle:

```text
PENDING -> EXTRACTING -> CHUNKING -> EMBEDDING -> READY
              |             |           |
              +-------------+-----------+
                            v
                      RETRY_PENDING
                            |
                            v
                       DEAD_LETTER
```

Workers must claim an operation with a unique owner token and expiring lease. Completion or release must update only records still owned by that token. Expired leases make crashed work recoverable.


### 5.2 Durable Local Spool

The spool is the recovery boundary between intake and S3.

```text
{spoolRoot}/{tenantHash}/{artifactId}/
  content.pending
  metadata.json
```

Rules:

- Write to a random `.tmp` path first.
- Flush and close the file before atomic rename.
- Never derive filesystem paths directly from user filenames.
- Enforce global and per-tenant byte/file quotas.
- Delete the local copy only after S3 upload is verified and the retention delay expires.
- Reconcile spool files against metadata records during startup and periodically.


### 5.3 Object Storage

Suggested S3 key:

```text
{environment}/{tenantId}/{artifactType}/{yyyy}/{MM}/{artifactId}/{version}/content
```

Store original names and content types as metadata, not as trusted key segments. Enable private buckets, versioning for the POC bucket, server-side encryption, lifecycle expiration, and blocked public access.


### Index Job

Processing stages are independently retryable:

1. Extract native text and document metadata.
2. Run OCR for images or scanned PDF pages.
3. Normalize text while retaining page/section coordinates.
4. Split text into overlapping chunks.
5. Generate embeddings in bounded batches.
6. Persist chunks and vectors idempotently.
7. Mark the artifact `READY`.

An artifact can be downloadable once object upload succeeds, while semantic retrieval becomes available only after indexing succeeds.

### Retrieval Job

Retrieval modes:

- Exact metadata lookup by artifact ID.
- Filtered listing by tenant, type, tags, source, status, or creation time.
- Keyword search over extracted text.
- Semantic vector search.
- Hybrid search combining keyword and vector scores.
- Extracted-text or chunk retrieval with source/page references.


## 6. Data Model

### `artifact`

| Field | Purpose |
|---|---|
| `id` | Stable generated artifact ID |
| `tenant_id` | Mandatory ownership boundary |
| `source_type` | `UPLOAD`, `URL`, `SCREENSHOT`, or `SYSTEM` |
| `source_uri` | Optional sanitized source reference |
| `original_name` | Display name only |
| `media_type` | Detected MIME type |
| `size_bytes` | Validated byte size |
| `sha256` | Integrity and deduplication fingerprint |
| `storage_key` | S3 key after upload |
| `spool_path` | Internal local recovery path |
| `storage_status` | Storage lifecycle state |
| `index_status` | Extraction/embedding lifecycle state |
| `version` | Artifact version |
| `metadata_json` | User and extractor metadata |
| `failure_code/message` | Last terminal or retryable failure |
| `created_at/updated_at` | Audit timestamps |

### `artifact_chunk`

| Field | Purpose |
|---|---|
| `id` | Stable chunk ID |
| `artifact_id` | Parent artifact |
| `tenant_id` | Explicit tenant filter |
| `chunk_index` | Deterministic position |
| `text` | Extracted normalized content |
| `token_count` | Retrieval budgeting |
| `page_number` | Optional source page |
| `section` | Optional heading/section |
| `coordinates_json` | Optional OCR bounding boxes |
| `content_sha256` | Chunk-level idempotency |
| `embedding_model/version` | Vector compatibility |
| `embedding` | Vector column or external vector ID |

### `artifact_operation`

Tracks asynchronous work and leases:

- `artifact_id`, `operation_type`, `status`
- `attempt_count`, `next_attempt_at`
- `lease_owner`, `lease_expires_at`
- `last_error`, `created_at`, `updated_at`

Recommended unique constraints:

```text
(tenant_id, idempotency_key)
(tenant_id, artifact_id, version)
(tenant_id, artifact_id, chunk_index, embedding_model, embedding_version)
```

Content deduplication by `(tenant_id, sha256)` should be configurable because callers may want two logical artifact records referencing the same stored blob.

## 8. Idempotency and Consistency

- Require or generate an idempotency key for intake requests.
- Store a fingerprint of tenant, operation, relevant metadata, and content checksum.
- The same key and fingerprint returns the existing artifact/result.
- The same key with a different fingerprint returns `IDEMPOTENCY_CONFLICT`.
- Generate deterministic chunk IDs from artifact version, chunk index, and chunk hash.
- Upsert embeddings using the deterministic chunk identity and model version.
- Use a transactional outbox so committed artifacts cannot be lost before processing is scheduled.
- Treat S3 upload as at-least-once; stable object keys make repeated puts safe.


## 10. MCP Interface

Expose MCP through a gateway that translates MCP operations into NAAP commands. For broad client compatibility, the gateway supports standard MCP stdio and Streamable HTTP transports. The core artifact engine itself remains TCP-only.

```text
MCP client → stdio or Streamable HTTP → MCP gateway → NAAP/TCP → artifact engine
```

Authentication is performed when the MCP connection/request is established. The gateway derives tenant and permissions from the authenticated principal and opens a tenant-bound NAAP connection. It does not expose `tenantId` as a caller-controlled tool argument.

MCP permits pluggable custom transports, so a native MCP-over-NAAP adapter is possible for controlled clients. It must preserve MCP's JSON-RPC message format and lifecycle. It is optional because ordinary MCP clients will not understand the custom TCP transport without a plugin; the gateway is the interoperability boundary.

### Tools

#### `search_artifacts`

Input:

```json
{
  "query": "checkout error shown in the payment screenshot",
  "limit": 5,
  "mediaTypes": ["image/png", "application/pdf"],
  "tags": ["production"],
  "createdAfter": "2026-08-01T00:00:00Z"
}
```

Output includes artifact metadata, relevance score, matched excerpts, page/section references, and MCP resource URIs.

#### `get_artifact_metadata`

Input: `artifactId`. Returns safe metadata and processing status.

#### `get_artifact_text`

Input: `artifactId`, optional page/section and maximum character count. Returns extracted text with citations.

#### `get_artifact_content`

Input: `artifactId`. For small artifacts, returns an MCP blob through the gateway. For large artifacts, returns an authorized resource reference. An optional edge component may issue a short-lived presigned URL; deployments that prohibit HTTP download instead return a one-time NAAP retrieval token for a native client.

#### `ingest_artifact_url` (optional)

Restricted write tool for trusted principals. Returns an artifact ID and asynchronous status.

### Resources

```text
artifact://{artifactId}/metadata
artifact://{artifactId}/text
artifact://{artifactId}/chunks/{chunkId}
artifact://{artifactId}/content
```

Binary content should be returned as an MCP blob only below a configured size. Larger content should use an authorized, short-lived download link. Search results should return resource links instead of embedding entire documents in tool output.

### MCP response safety

- Cap result count and total returned characters/tokens.
- Return source references for every excerpt.
- Mark extracted artifact text as untrusted content to reduce prompt-injection risk.
- Require explicit authorization for write, retry, delete, and download operations.
- Record tool name, principal, artifact IDs, latency, and outcome in an audit log without recording secrets or full document content.

## 11. RAG Retrieval Strategy

POC defaults:

- Chunk by headings/pages, then by approximately 500–800 tokens.
- Use 10–20% overlap when semantic continuity requires it.
- Retain page, section, and OCR-coordinate metadata.
- Retrieve a wider candidate set using both full-text and vector search.
- Apply metadata filters before or during vector search.
- Fuse scores with reciprocal rank fusion.
- Optionally rerank the top candidates.
- Apply a relevance threshold and return “no relevant context” below it.
- Enforce a final token budget before returning MCP content.

Images should be indexed from OCR text plus safe machine-generated descriptions when available. The original image remains the authoritative source.

Embedding model upgrades must not overwrite old vectors in place. Store model and version, re-index in parallel, then switch the active version.

## 12. Failure Handling

| Failure | Expected behavior |
|---|---|
| Client disconnect during intake | Remove incomplete temporary file; no committed artifact |
| Database commit failure | Keep no visible artifact; reconciliation quarantines orphan spool file |
| S3 unavailable | Keep spool file and schedule exponential retry with jitter |
| Extractor failure | Preserve original artifact; retry indexing independently |
| Embedding provider throttling | Batch retry using provider retry hints and backpressure |
| Worker crash | Lease expires and another worker resumes the operation |
| Local disk near capacity | Stop or throttle intake and emit a critical alert |
| Poison document | Move operation to dead letter after configured attempts |
| Delete during processing | Set tombstone; workers stop and cleanup all derived data |


## 14. Backpressure and Capacity Controls

Configuration should include:

- Maximum artifact size.
- Maximum spool bytes and file count.
- Per-tenant storage and request quotas.
- Intake concurrency.
- S3 upload concurrency and multipart thresholds.
- Extraction/OCR concurrency.
- Embedding batch size and requests per second.
- Maximum retry attempts and retention periods.
- MCP result count, response bytes, and token limits.

When thresholds are crossed, return `429 Too Many Requests` or `507 Insufficient Storage` with a retry hint rather than accepting data that cannot be durably retained.

## 15. Observability

Metrics:

- Intake count, bytes, latency, and rejection reason.
- Spool bytes, file count, and oldest pending artifact.
- S3 upload latency, throughput, retries, and failures.
- Extraction/OCR/embedding latency and failure rates.
- Processing queue depth and lease age.
- Chunk and vector counts per tenant/model.
- Search latency, result count, and no-result rate.
- MCP calls, errors, response size, and authorization failures.

Use artifact ID, operation ID, tenant-safe identifier, and trace ID for correlation. Provide readiness checks that fail when intake cannot safely spool, while temporary S3 or embedding outages should normally degrade readiness only according to configured policy.

