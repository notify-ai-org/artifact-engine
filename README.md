# Artifact Engine

`artifact-engine` is a Java 17 library for durable artifact intake, asynchronous storage and RAG
indexing, tenant-scoped retrieval, and Model Context Protocol exposure. The core remains independent
of any S3 SDK, vector database, OCR engine, or embedding vendor.

## Intake and recovery flow

```text
API / MCP request
   │
   ▼
ArtifactEngine ── create typed Job ── JobDispatcher
                                      │
                                      ▼
                                  IngestJob
                                      │ bounded 64 KiB copies, quota checks, SHA-256
                                      ▼
DurableSpool ── atomic content + metadata publication
   │
   ▼
MetadataStore.register(artifact, STORE + INDEX operations)
   │ single transaction
   ▼
Transactional outbox ── QueueManager ── RedisJobQueue
                                          │ leased claims + heartbeat
                         ┌────────────────┴───────────────┐
                         ▼                                ▼
                     StoreJob                         IndexJob
                  put + verification        extract/OCR → chunk → embed
                         │                                │
                         ▼                                ▼
                    ObjectStore                       VectorStore
```

An accepted artifact is recoverable from the spool before object storage is contacted. Stable
operation ids, object keys, chunk ids, and vector identities make every asynchronous step safe to
repeat. Queue completion and retry transitions succeed only for the current, unexpired lease owner.

## Important invariants

- Tenant id is mandatory at every store boundary and comes from the authenticated MCP session.
- User filenames never become filesystem paths or object-key segments.
- MIME type is detected from content; PNG, JPEG, WebP, PDF, DOCX, UTF-8 text, Markdown, and HTML are
  supported.
- Idempotency keys are compared with a fingerprint of tenant, checksum, media type, name, and intake
  metadata. Reuse with different content raises `IdempotencyConflictException`.
- Artifact creation and initial `STORE`/`INDEX` operations share a transactional-outbox commit.
- Storage and indexing state updates are atomic and cannot overwrite each other's concurrent state.
- Search performs keyword and vector candidate retrieval, reciprocal-rank fusion, metadata filters,
  and a final result cap.
- MCP tool inputs contain no caller-controlled tenant id; extracted text is marked as untrusted and
  binary responses are capped before Base64 materialization.
- Delete tombstones metadata first, blocks subsequent reads, then idempotently removes vectors,
  object content, and spool content.

## Packages

- `spool`: durable publication, global/per-tenant quotas, accounting rebuild, and reconciliation.
- `store`: metadata/outbox, object, vector, and audit contracts plus development adapters.
- `spring.jpa`: JPA metadata, transactional-outbox, and append-only audit adapters.
- `store.s3`: exact-length S3 streaming with tenant-key, checksum, owner, and SSE-KMS checks.
- `store.postgres`: tenant-scoped pgvector upserts, cosine search, and PostgreSQL full-text search.
- `queue`: in-memory and Redis leased queues plus transactional-outbox dispatch.
- `worker`: bounded local workers, durable queue workers, lease heartbeat, state machines, snapshots.
- `job`: all intake, metadata, fetch, text, search, delete, storage, and indexing workflows plus
  typed factory/dispatcher boundaries.
- `connector`: connector lifecycle, pooling, filesystem input, and SDK-neutral S3 output.
- `embed` / `extract`: batched embeddings, bounded LRU cache, native extraction, and OCR contracts.
- `auth`: authorization contract and content-based data verification.
- `mcp`: typed gateway plus a Java MCP SDK 2.x stdio server, tools, and resource templates.
- `spring`: overridable Spring bean configuration and spool policy defaults.

## Authentication and verification

`ArtifactAccessVerifier` is the single security boundary used by artifact jobs. It:

1. requires non-empty trusted principal and tenant identifiers;
2. delegates permission checks to `AuthorizationService`;
3. verifies uploaded bytes against the declared media type with `DataVerifier`;
4. normalizes untrusted filenames;
5. verifies that loaded artifacts belong to the requested tenant; and
6. converts extracted HTML to inert plain text before indexing or returning it.

Spring requires the normal `AuthorizationService` and `DataVerifier` beans and wires this layer
directly into the job factory. There is no configurable or bypassable filter chain.

## Infrastructure adapters

The module includes JPA metadata, transactional-outbox, and append-only audit stores; an atomic
Redis queue; an AWS SDK v2 S3 object store; and a PostgreSQL/pgvector vector store. Applications
still supply their configured `DataSource`, `S3Client`, `EmbeddingProvider`, document extractors/OCR,
and `AuthorizationService`.

`DefaultArtifactEngine` is intentionally a thin facade: each API method asks `ArtifactJobFactory`
for one typed job and passes it to `JobDispatcher`. Authorization, security filtering, spooling,
metadata transitions, retrieval fusion, downloads, and deletion all execute inside job
implementations. Spring supplies `DirectJobDispatcher` for synchronous calls; applications may
replace the `JobDispatcher` bean with `QueuingJobDispatcher`. Jobs implementing `QueueableJob`
provide a restart-safe `JobRecord`; the dispatcher passes that record to the queue owned by
`QueueManager` and returns the job's enqueue acknowledgement without executing the workflow.
Non-queueable request/response jobs use the configured inline dispatcher. Durable workers rebuild
`StoreJob` or `IndexJob` from the claimed record instead of deserializing process-local Java jobs.

The S3 and PostgreSQL adapters are opt-in Spring beans:

```properties
artifact.jpa.enabled=true

artifact.s3.enabled=true
artifact.s3.bucket=private-artifacts
artifact.s3.environment=production
artifact.s3.kms-key-id=alias/artifact-engine
# artifact.s3.expected-bucket-owner=123456789012
# artifact.s3.bucket-key-enabled=true

artifact.vector.postgres.enabled=true
artifact.vector.postgres.dimensions=1536
```

Import `ArtifactProductionStoreConfiguration` to register these opt-in adapters. Enabling JPA also
registers the library entities and Spring Data repositories explicitly, so the application does not
need to widen its component-scan package.

Amazon Textract OCR can be enabled for supported image uploads with:

```properties
artifact.ocr.textract.enabled=true
artifact.ocr.textract.max-input-bytes=10485760
artifact.ocr.max-output-characters=1000000
```

The default `TextractClient` uses the AWS SDK region and credentials provider chains. Applications
can instead provide their own `TextractClient` bean. OCR input and output are bounded locally before
and after the synchronous `DetectDocumentText` request.

S3 uploads stream from the durable spool with an exact content length and never buffer an entire
artifact. Every operation validates the environment and hashed-tenant key prefix. Upload and read
verification require SSE-KMS, tenant metadata, length, and SHA-256 checksums.

Vector identities are unique across tenant, artifact, chunk index, embedding model, and embedding
version. Reprocessing therefore updates one row rather than creating duplicate vectors. Search SQL
binds the authenticated tenant before applying media/tag filters and only returns artifacts whose
index state is `READY`.

## MCP stdio server

`ArtifactMcpStdioServer` exposes the native facade through the official Java MCP SDK. It provides
the following read-only tools:

- `artifact_search`
- `artifact_metadata`
- `artifact_text`
- `artifact_read_content`

It also publishes `artifact://{artifactId}/metadata`, `/text`, and `/content` resource templates.
Original bytes are returned in bounded Base64 segments with `nextOffset` and `endOfFile`; the hard
library ceiling is 4 MiB per segment. Search excerpts are capped independently, and extracted text
has a 1,000,000-character absolute ceiling.

Each stdio process is bound at startup to one authenticated principal, tenant, and scope set. Tool
schemas intentionally do not accept those fields. The launcher reserves stdout solely for MCP
JSON-RPC and redirects ordinary `System.out` output to stderr.

The reusable server can be constructed directly with an `ArtifactEngine`. To use the standalone
`ArtifactMcpStdioMain`, a deployment jar must register exactly one
`dev.notify.artifact.mcp.stdio.ArtifactMcpEngineProvider` in:

```text
META-INF/services/dev.notify.artifact.mcp.stdio.ArtifactMcpEngineProvider
```

The ACP client supplies these process-bound environment values automatically:

```text
ARTIFACT_MCP_PRINCIPAL_ID
ARTIFACT_MCP_TENANT_ID
ARTIFACT_MCP_SCOPES
```

Optional safe response controls are `ARTIFACT_MCP_MAX_TEXT_CHARACTERS`,
`ARTIFACT_MCP_MAX_CONTENT_BYTES`, and `ARTIFACT_MCP_REQUEST_TIMEOUT_SECONDS`. The deployment provider
is responsible for constructing the same production stores, workers, and authorization service used
by the native facade. Do not write logs to the preserved protocol output stream.

Production schema bootstrap is available at
`src/main/resources/db/migration/V1__artifact_engine.sql`. It targets PostgreSQL, installs the
`vector` extension, and declares `vector(1536)`. If the embedding model has another supported
dimension, change both the migration column and `artifact.vector.postgres.dimensions` before
deployment. Run extension creation with a suitably privileged migration role, configure a private
versioned S3 bucket, and keep the spool on durable local storage with sufficient per-tenant quota.

## Build

```bash
mvn -pl artifact-engine -am test
```
