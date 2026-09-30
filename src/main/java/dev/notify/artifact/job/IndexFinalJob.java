package dev.notify.artifact.job;

import dev.notify.artifact.chunk.ChunkEmbeddingWriter;
import dev.notify.artifact.chunk.ChunkLimitReached;
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
import java.util.Base64;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Last indexing step of a chunked ingest. For text/plain indexed chunk by chunk, it flushes the
 * carried tail (the final partial chunk), removes stale chunks, and marks the artifact READY. For
 * every other format, which cannot be parsed in byte chunks, it indexes the committed spool file
 * with the regular whole-file {@link IndexJob}.
 */
public final class IndexFinalJob extends AbstractJob<Integer> {
  private static final StructuredLog LOG = StructuredLog.of(IndexFinalJob.class);
  /** Planner attribute: {@code text-chunks} when INDEX_CHUNK jobs indexed the content. */
  static final String MODE = "indexMode";
  static final String TEXT_CHUNKS = "text-chunks";

  private final JobRecord record;
  private final EmbeddingService embeddings;
  private final VectorStore vectors;
  private final Chunker chunker;
  private final IndexJob.Options options;
  private final ChunkedIngestSupport support;
  private final Supplier<IndexJob> wholeFileIndex;

  public IndexFinalJob(
      JobRecord record,
      MetadataStore metadataStore,
      EmbeddingService embeddings,
      VectorStore vectors,
      Chunker chunker,
      IndexJob.Options options,
      ChunkedIngestSupport support,
      Supplier<IndexJob> wholeFileIndex) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.embeddings = Objects.requireNonNull(embeddings, "embeddings");
    this.vectors = Objects.requireNonNull(vectors, "vectors");
    this.chunker = Objects.requireNonNull(chunker, "chunker");
    this.options = Objects.requireNonNull(options, "options");
    this.support = Objects.requireNonNull(support, "support");
    this.wholeFileIndex = Objects.requireNonNull(wholeFileIndex, "wholeFileIndex");
  }

  @Override
  public Integer execute() throws Exception {
    long version = version(record);
    Artifact artifact = artifactAtVersion(this, record, version);
    if (!TEXT_CHUNKS.equals(record.attributes().get(MODE))) {
      LOG.info("index_final_whole_file", "artifact", artifact.id(), "type", artifact.mediaType());
      return wholeFileIndex.get().execute();
    }
    IndexProgress progress = support.progress()
        .find(artifact.tenantId(), artifact.id(), version)
        .orElse(IndexProgress.start(artifact.tenantId(), artifact.id(), version));
    try {
      boolean truncated = progress.truncated();
      int chunkCount;
      ChunkEmbeddingWriter writer = new ChunkEmbeddingWriter(
          artifact, embeddings, vectors, options.commitBatchSize(), options.maxChunks(), null);
      if (truncated) {
        chunkCount = options.maxChunks();
      } else {
        Chunker.Session session = chunker.resume(progress.chunker(), writer);
        try {
          Utf8ChunkDecoder.finish(Base64.getDecoder().decode(progress.utf8Carry()), session::accept);
          chunkCount = session.finish();
        } catch (ChunkLimitReached limit) {
          if (options.chunkLimitPolicy() == IndexJob.ChunkLimitPolicy.FAIL) {
            throw new IndexJob.ChunkLimitExceededException(
                "Document exceeds the per-artifact budget of " + options.maxChunks() + " chunks");
          }
          chunkCount = options.maxChunks();
          truncated = true;
        }
        writer.flush();
      }
      if (chunkCount == 0) {
        throw new IllegalStateException("No usable text extractor for " + artifact.mediaType());
      }
      vectors.deleteChunksFrom(
          artifact.tenantId(), artifact.id(), embeddings.model(), embeddings.version(), chunkCount);
      String code = truncated ? "INDEX_TRUNCATED" : null;
      String message = truncated
          ? "Document exceeds the per-artifact budget of " + options.maxChunks() + " chunks"
          : null;
      String contentSha256 = ResumableSha256.restore(progress.contentDigest()).hexDigest();
      metadataStore.update(artifact.tenantId(), artifact.id(), current -> {
        Artifact ready = current.withFailure(
            current.storageStatus(), ArtifactStatus.Index.READY, code, message);
        // A spool-free text ingest learns its content digest only once every chunk was hashed.
        return ready.hasContentDigest() ? ready : ready.withContentDigest(contentSha256);
      });
      support.progress().delete(artifact.tenantId(), artifact.id(), version);
      LOG.info("index_completed", "artifact", artifact.id(), "mode", TEXT_CHUNKS,
          "chunks", chunkCount, "embedded", progress.embedded() + writer.embedded(),
          "reused", progress.reused() + writer.reused(), "sourceChunks", progress.lastChunk(),
          "truncated", truncated);
      return chunkCount;
    } catch (Exception failure) {
      String code = failure instanceof IndexJob.ChunkLimitExceededException
          ? "INDEX_CHUNK_LIMIT_EXCEEDED"
          : "INDEXING_FAILED";
      String message = String.valueOf(failure.getMessage());
      LOG.warn("index_failed", "artifact", artifact.id(), "code", code, "reason", message);
      metadataStore.update(artifact.tenantId(), artifact.id(), current -> current.withFailure(
          current.storageStatus(), ArtifactStatus.Index.RETRY_PENDING, code,
          message.substring(0, Math.min(500, message.length()))));
      throw failure;
    }
  }
}
