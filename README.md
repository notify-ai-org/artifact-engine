# Artifact Engine

`artifact-engine` is a Java 17 library for durable, tenant-scoped artifact ingestion and retrieval.
It safely spools uploaded bytes, stores originals in S3, extracts and embeds text, performs hybrid
RAG search, and exposes metadata and content through a read-only Model Context Protocol (MCP)
server.

The core uses interfaces for metadata, objects, vectors, queues, authentication, and embeddings.
Production adapters are included for PostgreSQL/pgvector, S3, Redis, and OpenAI-compatible embedding
endpoints; in-memory adapters support tests and local development.

Ingestion authorizes the principal, copies input to a bounded durable spool, detects the media type,
sanitizes the filename, calculates a SHA-256 checksum, and registers metadata. Storage and indexing
are restart-safe jobs with stable operation, object, chunk, and vector identities. Optional
workflows persist ordered job steps and resume incomplete work after a restart.

## Public API

The `ArtifactEngine` facade provides:

- `ingest` to accept and spool an artifact;
- `metadata` and `listMetadata` to inspect tenant-owned artifacts;
- `content` to stream original bytes;
- `extractedText` to return bounded extracted text;
- `search` to perform filtered hybrid retrieval; and
- `delete` to tombstone metadata and remove stored content and vectors.

Every request carries a trusted principal ID and tenant ID. Applications normally construct a
`DefaultArtifactJobFactory`, select direct and queued dispatchers, and pass both to
`DefaultArtifactEngine`. `EngineOptions` controls content deduplication and the retrieval candidate
multiplier.

## Processing guarantees

- Tenant identity is required at every storage and retrieval boundary.
- User filenames never become filesystem paths or S3 key segments.
- Content type is detected from uploaded bytes rather than trusted request metadata.
- Idempotency keys are bound to a fingerprint of tenant, checksum, media type, filename, and intake
  metadata. Reusing a key for different content raises `IdempotencyConflictException`.
- The spool publishes content and metadata atomically and enforces a maximum artifact size.
- Queue claims are leased; completion, retry, heartbeat, and dead-letter transitions require the
  current lease owner.
- Storage and indexing update independent artifact states and use optimistic versions.
- Search combines keyword and vector candidates with reciprocal-rank fusion, tenant-scoped filters,
  and a final result limit.
- HTML becomes inert plain text. MCP responses identify extracted text as untrusted and bound text
  and binary response sizes.
- Deletion tombstones metadata first, blocks later reads, and then idempotently removes vectors,
  object content, and spool content.

## Main packages

- `job`, `factory`, and `dispatcher`: typed operations and direct/queued execution routing.
- `spool`: durable intake publication and recovery.
- `queue` and `worker`: in-memory or Redis queues, leased claims, batching, retries, and snapshots.
- `workflow`: persisted ordered job workflows and restart recovery.
- `store`: metadata, object, vector, job, and log contracts plus development adapters.
- `jdbc`: Jdbi PostgreSQL metadata, vector, job, and workflow stores.
- `extract` and `embed`: PDF, DOCX, HTML, Markdown, text, OCR, batching, and embedding cache support.
- `auth` and `security`: authorization, content verification, and tenant-safe storage identities.
- `mcp`: the tenant-bound gateway and Java MCP SDK stdio server.
- `environment`: command-line, environment-variable, and properties-file configuration resolution.

## Production storage

The standalone provider uses PostgreSQL through HikariCP and Jdbi when a JDBC URL is set, and
process-local metadata and vector stores otherwise. Original content uses S3 with SSE-KMS, intake
uses a durable local spool, and embeddings use an OpenAI-compatible HTTP endpoint.

S3 writes stream from the spool with an exact content length. Reads and writes validate the
environment-specific hashed tenant prefix, tenant metadata, byte length, SHA-256, expected bucket
owner (when configured), and SSE-KMS settings.

Apply the PostgreSQL migrations in order before starting a JDBC-backed deployment:

```text
src/main/resources/db/migration/V1__artifact_engine.sql
src/main/resources/db/migration/V2__artifact_workflow.sql
src/main/resources/db/migration/V3__artifact_workflow_step_details.sql
src/main/resources/db/migration/V4__normalize_workflow_records.sql
```

`V1` installs the `vector` extension and declares `vector(1536)`. If a different embedding size is
used, update the migration and `ARTIFACT_VECTOR_DIMENSIONS` together. Run extension creation with a
suitably privileged migration role.

## Standalone MCP server

`ArtifactMcpStdioServer` exposes four read-only tools:

- `artifact_search`
- `artifact_metadata`
- `artifact_text`
- `artifact_read_content`

It also publishes:

```text
artifact://{artifactId}/metadata
artifact://{artifactId}/text
artifact://{artifactId}/content
```

Each stdio process is bound to one principal, tenant, and scope set. Those trusted fields are not
accepted in tool inputs. Original content is returned as bounded Base64 chunks with `nextOffset`
and `endOfFile`; the library enforces a 4 MiB hard ceiling per chunk. Standard output is reserved
for MCP JSON-RPC and ordinary process output is redirected to standard error.

The launcher resolves configuration, in descending precedence, from command-line arguments, OS
environment variables, and `src/main/resources/artifact-mcp.properties`. Arguments support both
`--KEY=value` and `--KEY value`:

```bash
java -jar artifact-engine.jar \
  --ARTIFACT_MCP_PRINCIPAL_ID=principal-a \
  --ARTIFACT_MCP_TENANT_ID=tenant-a \
  --ARTIFACT_MCP_SCOPES=artifact.search,artifact.metadata,artifact.text,artifact.content
```

### Configuration

Identity and response limits:

```text
ARTIFACT_MCP_PRINCIPAL_ID
ARTIFACT_MCP_TENANT_ID
ARTIFACT_MCP_SCOPES
ARTIFACT_MCP_MAX_TEXT_CHARACTERS
ARTIFACT_MCP_MAX_CONTENT_BYTES
ARTIFACT_MCP_REQUEST_TIMEOUT_SECONDS
```

PostgreSQL and pgvector:

```text
ARTIFACT_JDBC_URL                 # JDBC_DATABASE_URL fallback
ARTIFACT_JDBC_USER                # DB_USER fallback
ARTIFACT_JDBC_PASSWORD            # DB_PASSWORD fallback
ARTIFACT_JDBC_MAX_POOL_SIZE       # default: 8
ARTIFACT_JDBC_MIN_IDLE            # default: 1
ARTIFACT_VECTOR_DIMENSIONS        # default: 1536
```

S3 and the local spool:

```text
ARTIFACT_S3_BUCKET                # required
ARTIFACT_S3_KMS_KEY_ID            # required
ARTIFACT_S3_REGION                # default: ap-south-1
ARTIFACT_S3_ENVIRONMENT           # default: default
ARTIFACT_S3_EXPECTED_BUCKET_OWNER # optional
ARTIFACT_S3_BUCKET_KEY_ENABLED    # default: true
ARTIFACT_SPOOL_ROOT               # default: ./data/artifact-spool
ARTIFACT_SPOOL_MAX_ARTIFACT_BYTES # default: 134217728
```

Embeddings:

```text
EMBEDDING_BASE_URL                # default: https://api.openai.com/v1
EMBEDDING_API_PATH                # default: /embeddings
EMBEDDING_API_KEY                 # OPENAI_API_KEY fallback
EMBEDDING_QUERY_MODEL             # default: text-embedding-3-small
EMBEDDING_MODELS                  # optional comma-separated models
EMBEDDING_MODEL_VERSION
EMBEDDING_MAX_BATCH_SIZE          # default: 32
EMBEDDING_MAX_WAIT_MILLIS         # default: 25
EMBEDDING_CACHE_MAX_ENTRIES       # default: 10000
EMBEDDING_CACHE_TTL_SECONDS       # default: 3600
EMBEDDING_CONNECT_TIMEOUT_SECONDS # default: 10
EMBEDDING_TIMEOUT_SECONDS         # default: 30
```

Embedding requests sharing a model are coalesced until the batch is full or the maximum wait
elapses. Successful vectors are cached with a bounded TTL, provider failures use the artifact retry
policy, and returned vector dimensions are checked before storage.

## Build and test

From the repository root:

```bash
mvn -pl artifact-engine -am test
```
