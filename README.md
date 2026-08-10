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

## Security filter chains

Security checks are explicit `SecurityFilter` implementations and stop at the first rejection.
`SecurityFilterException` contains only a safe code, filter name, and non-sensitive message.

Ingestion order:

1. `AuthenticationFilter` authenticates the opaque connection/session handle and permission.
2. `TenantIsolationFilter` matches authenticated, requested, artifact, and hashed storage-key tenants.
3. `EncryptionPolicyFilter` requires TLS 1.2/1.3 and encrypted, optionally KMS-backed storage.
4. `ProtocolAbuseFilter` checks frame size, stream transitions, transfer rate, and compression ratio.
5. `UrlIngestionPolicyFilter` permits only configured schemes/hosts and public resolved addresses.
6. `ContentSignatureFilter` detects MIME type from bytes.
7. `ArchiveSafetyFilter` bounds entries, expanded bytes, entry paths, and compression ratios.
8. `MalwareScanFilter` requires a clean scanner verdict.
9. `FilenameSanitizationFilter` removes path/control/bidirectional filename content.

Retrieval order is authentication, tenant/storage-key isolation, TLS, protocol checks, clean-scan
gating, and `ExtractedHtmlSanitizationFilter`. HTML is also reduced to inert text before indexing.

Spring activates the complete layer when the application provides `AuthenticationService`,
`MalwareScanner`, and `SecurityContextFactory`. The context factory must use trusted transport/session
facts from the server edge—not headers or fields supplied directly by a remote caller. Configure the
URL host allowlist with `artifact.security.url.allowed-hosts`; an empty allowlist rejects URL intake.
The URL connector must connect to one of the IP addresses approved by the filter to prevent DNS
rebinding. Invoke `ProtocolAbuseFilter` incrementally while receiving frames so slowloris rejection
does not wait for upload completion.

Spring fails startup when those adapters are missing. Local tests may opt out explicitly with
`-Dartifact.security.allow-insecure-local=true`; this switch must not be used in a deployed service.

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
