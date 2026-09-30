package dev.notify.artifact.job;

import dev.notify.artifact.chunk.ChunkEmbeddingWriter;
import dev.notify.artifact.chunk.ChunkLimitReached;
import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.IndexProgressStore.IndexProgress;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.ResumableSha256;
import dev.notify.artifact.util.StructuredLog;
import dev.notify.artifact.util.Utf8ChunkDecoder;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

/**
 * Indexes one buffered chunk of a text/plain source: resumes the chunker and UTF-8 decoder from
 * the previous chunk's saved state, embeds and commits the complete chunks, and saves the new
 * state. It also advances the artifact's running SHA-256: this is the one per-chunk step that runs
 * strictly in order with persisted progress, and a spool-free text ingest has no other full pass
 * over the content. Chunks run in order (each depends on the previous chunk's INDEX_CHUNK step); a retried job replays from the previous
 * state and deterministic chunk ids make the replay free.
 */
public final class IndexChunkJob extends AbstractJob<Integer> {
  private static final StructuredLog LOG = StructuredLog.of(IndexChunkJob.class);

  private final JobRecord record;
  private final EmbeddingService embeddings;
  private final VectorStore vectors;
  private final Chunker chunker;
  private final IndexJob.Options options;
  private final ChunkedIngestSupport support;

  public IndexChunkJob(
      JobRecord record,
      MetadataStore metadataStore,
      EmbeddingService embeddings,
      VectorStore vectors,
      Chunker chunker,
      IndexJob.Options options,
      ChunkedIngestSupport support) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.embeddings = Objects.requireNonNull(embeddings, "embeddings");
    this.vectors = Objects.requireNonNull(vectors, "vectors");
    this.chunker = Objects.requireNonNull(chunker, "chunker");
    this.options = Objects.requireNonNull(options, "options");
    this.support = Objects.requireNonNull(support, "support");
  }

  @Override
  public Integer execute() throws Exception {
    ChunkRef chunk = ChunkRef.of(record);
    Artifact artifact = artifactAtVersion(this, record, chunk.version());
    IndexProgress progress = support.progress()
        .find(artifact.tenantId(), artifact.id(), chunk.version())
        .orElse(IndexProgress.start(artifact.tenantId(), artifact.id(), chunk.version()));
    if (progress.lastChunk() >= chunk.chunk()) {
      LOG.debug("index_chunk_already_done", "artifact", artifact.id(), "chunk", chunk.chunk());
      return 0;
    }
    if (progress.lastChunk() != chunk.chunk() - 1) {
      throw new IllegalStateException("Chunk " + chunk.chunk() + " indexed before chunk "
          + (progress.lastChunk() + 1));
    }
    if (progress.truncated()) {
      // Indexing stopped at the budget, but the content digest must still cover every byte.
      ResumableSha256 digest = ResumableSha256.restore(progress.contentDigest());
      try (var lease = chunk.lease(support, artifact)) {
        digest.update(lease.buffer());
      }
      support.progress().save(advance(progress, chunk.chunk(), progress.chunker(),
          progress.utf8Carry(), true, 0, 0, digest.state()));
      return 0;
    }
    if (chunk.chunk() == 1) updateIndex(artifact, ArtifactStatus.Index.EXTRACTING);

    Instant started = Instant.now();
    ChunkEmbeddingWriter writer = new ChunkEmbeddingWriter(
        artifact, embeddings, vectors, options.commitBatchSize(), options.maxChunks(),
        () -> updateIndex(artifact, ArtifactStatus.Index.EMBEDDING));
    Chunker.Session session = chunker.resume(progress.chunker(), writer);
    ResumableSha256 digest = ResumableSha256.restore(progress.contentDigest());
    try {
      byte[] carry;
      try (var lease = chunk.lease(support, artifact)) {
        digest.update(lease.buffer());
        carry = Utf8ChunkDecoder.decode(
            Base64.getDecoder().decode(progress.utf8Carry()), lease.buffer(), session::accept);
      } catch (ChunkLimitReached limit) {
        LOG.warn("index_chunk_budget_reached", "artifact", artifact.id(), "chunk", chunk.chunk(),
            "maxChunks", options.maxChunks(), "policy", options.chunkLimitPolicy());
        if (options.chunkLimitPolicy() == IndexJob.ChunkLimitPolicy.FAIL) {
          throw new IndexJob.ChunkLimitExceededException(
              "Document exceeds the per-artifact budget of " + options.maxChunks() + " chunks");
        }
        writer.flush();
        support.progress().save(advance(progress, chunk.chunk(), progress.chunker(), "", true,
            writer.embedded(), writer.reused(), digest.state()));
        return 0;
      }
      writer.flush();
      support.progress().save(advance(progress, chunk.chunk(), session.snapshot(),
          Base64.getEncoder().encodeToString(carry), false, writer.embedded(), writer.reused(),
          digest.state()));
      LOG.info("index_chunk_committed", "artifact", artifact.id(), "chunk", chunk.chunk(),
          "bytes", chunk.length(), "embedded", writer.embedded(), "reused", writer.reused(),
          "nextChunkIndex", session.snapshot().nextIndex(),
          "duration", Duration.between(started, Instant.now()));
      return writer.embedded() + writer.reused();
    } catch (Exception failure) {
      markFailed(artifact, failure);
      throw failure;
    }
  }

  private static IndexProgress advance(
      IndexProgress progress, int chunk, Chunker.State state, String carry, boolean truncated,
      int embedded, int reused, String contentDigest) {
    return new IndexProgress(progress.tenantId(), progress.artifactId(), progress.version(), chunk,
        state, carry, truncated, progress.embedded() + embedded, progress.reused() + reused,
        contentDigest);
  }

  private void updateIndex(Artifact artifact, ArtifactStatus.Index status) {
    metadataStore.update(artifact.tenantId(), artifact.id(), current -> current.withIndex(status));
  }

  private void markFailed(Artifact artifact, Exception failure) {
    String code = failure instanceof IndexJob.ChunkLimitExceededException
        ? "INDEX_CHUNK_LIMIT_EXCEEDED"
        : "INDEXING_FAILED";
    String message = failure.getMessage() == null
        ? failure.getClass().getSimpleName()
        : failure.getMessage().substring(0, Math.min(500, failure.getMessage().length()));
    LOG.warn("index_chunk_failed", "artifact", artifact.id(), "code", code, "reason", message);
    metadataStore.update(artifact.tenantId(), artifact.id(), current -> current.withFailure(
        current.storageStatus(), ArtifactStatus.Index.RETRY_PENDING, code, message));
  }
}
