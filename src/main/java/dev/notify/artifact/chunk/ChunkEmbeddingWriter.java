package dev.notify.artifact.chunk;

import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StructuredLog;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Buffers chunks, skips ones an earlier attempt already committed, and embeds and upserts the rest
 * in batches. Shared by whole-file indexing and chunked indexing so both produce identical chunk
 * ids for identical text.
 *
 * <p>Not thread-safe: one writer per job. {@link #flush()} must be called before the job ends;
 * nothing is carried between jobs except what the chunker's state describes.
 */
public final class ChunkEmbeddingWriter implements Chunker.ChunkConsumer {
  private static final StructuredLog LOG = StructuredLog.of(ChunkEmbeddingWriter.class);

  private final Artifact artifact;
  private final EmbeddingService embeddingService;
  private final VectorStore vectorStore;
  private final int commitBatchSize;
  private final int maxChunks;
  private final Runnable beforeFirstBatch;
  private final List<String> texts;
  private int firstIndex;
  private boolean embeddingStarted;
  private int embedded;
  private int reused;
  private int batches;

  /**
   * @param beforeFirstBatch run once before the first embedding call, e.g. to mark the artifact as
   *     EMBEDDING
   */
  public ChunkEmbeddingWriter(
      Artifact artifact,
      EmbeddingService embeddingService,
      VectorStore vectorStore,
      int commitBatchSize,
      int maxChunks,
      Runnable beforeFirstBatch) {
    this.artifact = Objects.requireNonNull(artifact, "artifact");
    this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService");
    this.vectorStore = Objects.requireNonNull(vectorStore, "vectorStore");
    this.commitBatchSize = commitBatchSize;
    this.maxChunks = maxChunks;
    this.beforeFirstBatch = beforeFirstBatch == null ? () -> {} : beforeFirstBatch;
    this.texts = new ArrayList<>(commitBatchSize);
  }

  @Override
  public void accept(int index, String text) throws ChunkLimitReached {
    // Thrown from inside the extractor callback, so reading stops at the budget.
    if (index >= maxChunks) throw new ChunkLimitReached();
    if (texts.isEmpty()) firstIndex = index;
    texts.add(text);
    if (texts.size() >= commitBatchSize) flush();
  }

  public void flush() {
    if (texts.isEmpty()) return;
    if (!embeddingStarted) {
      beforeFirstBatch.run();
      embeddingStarted = true;
    }
    String model = embeddingService.model();
    String version = embeddingService.version();
    Map<Integer, String> committed =
        vectorStore.chunkIds(
            artifact.tenantId(), artifact.id(), model, version, firstIndex,
            firstIndex + texts.size());

    List<Integer> pendingIndexes = new ArrayList<>();
    List<String> pendingTexts = new ArrayList<>();
    List<String> pendingHashes = new ArrayList<>();
    for (int offset = 0; offset < texts.size(); offset++) {
      int index = firstIndex + offset;
      String text = texts.get(offset);
      String contentHash = Checksum.sha256(text);
      // The id covers artifact version, index, and content, so a match means nothing changed.
      if (!chunkId(index, contentHash).equals(committed.get(index))) {
        pendingIndexes.add(index);
        pendingTexts.add(text);
        pendingHashes.add(contentHash);
      }
    }

    if (!pendingTexts.isEmpty()) {
      List<float[]> embeddings = embeddingService.embedDocuments(pendingTexts);
      List<ArtifactChunk> chunks = new ArrayList<>(pendingTexts.size());
      for (int i = 0; i < pendingTexts.size(); i++) {
        String text = pendingTexts.get(i);
        chunks.add(
            new ArtifactChunk(
                chunkId(pendingIndexes.get(i), pendingHashes.get(i)),
                artifact.id(),
                artifact.tenantId(),
                pendingIndexes.get(i),
                text,
                estimateTokens(text),
                null,
                null,
                Map.of(),
                pendingHashes.get(i),
                model,
                version,
                embeddings.get(i)));
      }
      vectorStore.upsertAll(chunks);
    }
    batches++;
    embedded += pendingTexts.size();
    reused += texts.size() - pendingTexts.size();
    LOG.debug("index_batch_committed", "artifact", artifact.id(), "batch", batches,
        "firstChunk", firstIndex, "chunks", texts.size(), "embedded", pendingTexts.size(),
        "reused", texts.size() - pendingTexts.size());
    texts.clear();
  }

  public int embedded() {
    return embedded;
  }

  public int reused() {
    return reused;
  }

  public int batches() {
    return batches;
  }

  private String chunkId(int index, String contentHash) {
    return Checksum.sha256(
        artifact.id() + ":" + artifact.version() + ":" + index + ":" + contentHash);
  }

  private static int estimateTokens(String text) {
    return Math.max(1, (int) Math.ceil(text.length() / 4.0));
  }
}
