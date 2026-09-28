package dev.notify.artifact.job;

import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.TextExtractor;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.ocr.Ocr;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Restart-safe indexing pipeline with deterministic chunk/vector identities. */
public final class IndexJob extends AbstractJob<Integer> implements QueueableJob<Integer> {
  private static final StructuredLog LOG = StructuredLog.of(IndexJob.class);

  /** What to do when a document produces more chunks than {@link Options#maxChunks()}. */
  public enum ChunkLimitPolicy {
    /** Stop extracting and fail the job; nothing becomes searchable. */
    FAIL,
    /** Keep the first {@code maxChunks} chunks, mark READY, and flag the artifact as truncated. */
    TRUNCATE
  }

  /**
   * @param commitBatchSize chunks embedded and upserted per round trip; also the granularity at
   *     which a retried job resumes without re-embedding
   * @param maxChunks per-artifact chunk budget, bounding embedding cost and vector storage
   * @param chunkLimitPolicy behaviour once the budget is reached
   */
  public record Options(int commitBatchSize, int maxChunks, ChunkLimitPolicy chunkLimitPolicy) {
    public static final int DEFAULT_COMMIT_BATCH_SIZE = 64;
    public static final int DEFAULT_MAX_CHUNKS = 10_000;

    public Options {
      if (commitBatchSize < 1) throw new IllegalArgumentException("commitBatchSize must be positive");
      if (maxChunks < 1) throw new IllegalArgumentException("maxChunks must be positive");
      java.util.Objects.requireNonNull(chunkLimitPolicy, "chunkLimitPolicy");
    }

    public static Options defaults() {
      return new Options(DEFAULT_COMMIT_BATCH_SIZE, DEFAULT_MAX_CHUNKS, ChunkLimitPolicy.FAIL);
    }
  }

  private final String tenantId;
  private final String artifactId;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final TextExtractorFactory extractorFactory;
  private final Ocr ocr;
  private final Chunker chunker;
  private final EmbeddingService embeddingService;
  private final VectorStore vectorStore;
  private final Options options;

  public IndexJob(
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      List<TextExtractor> extractors,
      Ocr ocr,
      Chunker chunker,
      EmbeddingService embeddingService,
      VectorStore vectorStore) {
    this(
        tenantId,
        artifactId,
        metadataStore,
        objectStore,
        durableSpool,
        new TextExtractorFactory(extractors),
        ocr,
        chunker,
        embeddingService,
        vectorStore);
  }

  public IndexJob(
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      TextExtractorFactory extractorFactory,
      Ocr ocr,
      Chunker chunker,
      EmbeddingService embeddingService,
      VectorStore vectorStore) {
    this(
        tenantId,
        artifactId,
        metadataStore,
        objectStore,
        durableSpool,
        extractorFactory,
        ocr,
        chunker,
        embeddingService,
        vectorStore,
        Options.defaults());
  }

  public IndexJob(
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      TextExtractorFactory extractorFactory,
      Ocr ocr,
      Chunker chunker,
      EmbeddingService embeddingService,
      VectorStore vectorStore,
      Options options) {
    super(null, metadataStore);
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.objectStore = objectStore;
    this.durableSpool = durableSpool;
    this.extractorFactory = java.util.Objects.requireNonNull(extractorFactory, "extractorFactory");
    this.ocr = ocr;
    this.chunker = chunker;
    this.embeddingService = embeddingService;
    this.vectorStore = vectorStore;
    this.options = java.util.Objects.requireNonNull(options, "options");
  }

  @Override
  public Integer execute() throws Exception {
    Artifact artifact = requiredArtifact();
    Instant started = Instant.now();
    LOG.info("index_started", "artifact", artifactId, "version", artifact.version(),
        "type", artifact.mediaType(), "bytes", artifact.sizeBytes(),
        "model", embeddingService.model(), "maxChunks", options.maxChunks(),
        "policy", options.chunkLimitPolicy());
    ChunkWriter writer = new ChunkWriter(artifact);
    try {
      updateIndex(ArtifactStatus.Index.EXTRACTING);

      // Extraction, chunking, and embedding run as one pipeline: text flows from the extractor
      // into the chunker, and chunks are embedded and committed in bounded batches. Partial
      // results stay invisible to search until the artifact is marked READY.
      int chunkCount;
      boolean truncated = false;
      try (LocalContent content = localContent(artifact)) {
        chunkCount = extractInto(artifact, content.path(), writer);
      } catch (ChunkLimitReached limit) {
        LOG.warn("index_chunk_budget_reached", "artifact", artifactId,
            "maxChunks", options.maxChunks(), "policy", options.chunkLimitPolicy());
        if (options.chunkLimitPolicy() == ChunkLimitPolicy.FAIL) {
          throw new ChunkLimitExceededException(limitMessage());
        }
        chunkCount = options.maxChunks();
        truncated = true;
      }
      writer.flush();
      vectorStore.deleteChunksFrom(
          tenantId, artifactId, embeddingService.model(), embeddingService.version(), chunkCount);
      LOG.info("index_completed", "artifact", artifactId, "chunks", chunkCount,
          "embedded", writer.embedded, "reused", writer.reused, "batches", writer.batches,
          "truncated", truncated, "duration", Duration.between(started, Instant.now()));

      // Clearing the failure fields also drops a stale truncation flag from an earlier version.
      String code = truncated ? "INDEX_TRUNCATED" : null;
      String message = truncated ? limitMessage() : null;
      metadataStore.update(
          tenantId,
          artifactId,
          current ->
              current.withFailure(
                  current.storageStatus(), ArtifactStatus.Index.READY, code, message));
      return chunkCount;
    } catch (Exception failure) {
      String code = failure instanceof ChunkLimitExceededException
          ? "INDEX_CHUNK_LIMIT_EXCEEDED"
          : "INDEXING_FAILED";
      LOG.warn("index_failed", "artifact", artifactId, "code", code,
          "embedded", writer.embedded, "reused", writer.reused,
          "duration", Duration.between(started, Instant.now()),
          "error", failure.getClass().getSimpleName(), "reason", safeMessage(failure));
      metadataStore.update(
          tenantId,
          artifactId,
          current ->
              current.withFailure(
                  current.storageStatus(),
                  ArtifactStatus.Index.RETRY_PENDING,
                  code,
                  safeMessage(failure)));
      throw failure;
    }
  }

  @Override
  public dev.notify.artifact.model.JobRecord queueRecord() {
    Artifact artifact = requiredArtifact();
    return dev.notify.artifact.model.JobRecord.pending(
        Checksum.sha256(tenantId + ":" + artifactId + ":" + artifact.version() + ":INDEX"),
        tenantId, artifactId, dev.notify.artifact.model.JobRecord.JobType.INDEX,
        Map.of("version", Long.toString(artifact.version())));
  }

  @Override
  public Integer queuedResult() {
    return 0;
  }

  private int extractInto(Artifact artifact, Path file, ChunkWriter writer) throws IOException {
    var nativeExtractor = extractorFactory.find(artifact.mediaType());
    if (nativeExtractor.isPresent()) {
      LOG.debug("index_extracting", "artifact", artifactId,
          "extractor", nativeExtractor.get().getClass().getSimpleName());
      Chunker.Session session = chunker.stream(writer);
      nativeExtractor.get().extract(file, session::accept);
      int chunkCount = session.finish();
      if (chunkCount > 0) {
        return chunkCount;
      }
      LOG.info("index_extracted_no_text", "artifact", artifactId,
          "extractor", nativeExtractor.get().getClass().getSimpleName(),
          "ocrAvailable", ocr != null);
    }

    if (ocr != null && artifact.mediaType().startsWith("image/")) {
      LOG.info("index_ocr_started", "artifact", artifactId, "type", artifact.mediaType());
      Chunker.Session session = chunker.stream(writer);
      try (InputStream image = Files.newInputStream(file)) {
        session.accept(ocr.recognize(image, artifact.mediaType()));
      }
      return session.finish();
    }
    throw new IllegalStateException("No usable text extractor for " + artifact.mediaType());
  }

  /**
   * Extractors need a random-access local file. The spool copy is used while it exists; otherwise
   * the stored object is staged to a scratch file on the spool volume and deleted afterwards.
   */
  private LocalContent localContent(Artifact artifact) throws IOException {
    if (artifact.spoolPath() != null) {
      Optional<Path> spooled = durableSpool.existingContent(artifact.spoolPath());
      if (spooled.isPresent()) {
        LOG.debug("index_source", "artifact", artifactId, "source", "spool");
        return new LocalContent(spooled.get(), false);
      }
    }
    if (artifact.storageKey() == null
        || artifact.storageStatus() != ArtifactStatus.Storage.STORED) {
      LOG.warn("index_content_unavailable", "artifact", artifactId,
          "storage", artifact.storageStatus(), "spoolRecorded", artifact.spoolPath() != null);
      throw new NoSuchFileException("Artifact content is not available for indexing");
    }
    Path scratch = durableSpool.scratchFile();
    Instant downloadStarted = Instant.now();
    try (InputStream stored = objectStore.get(tenantId, artifact.storageKey())) {
      Files.copy(stored, scratch, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException | RuntimeException downloadFailure) {
      Files.deleteIfExists(scratch);
      throw downloadFailure;
    }
    Duration downloaded = Duration.between(downloadStarted, Instant.now());
    LOG.info("index_source", "artifact", artifactId, "source", "object_store",
        "bytes", artifact.sizeBytes(), "duration", downloaded,
        "mibPerSecond", StructuredLog.mibPerSecond(artifact.sizeBytes(), downloaded));
    return new LocalContent(scratch, true);
  }

  private record LocalContent(Path path, boolean temporary) implements AutoCloseable {
    @Override
    public void close() throws IOException {
      if (temporary) Files.deleteIfExists(path);
    }
  }

  /** Buffers chunks, skips ones an earlier attempt already committed, and embeds the rest. */
  private final class ChunkWriter implements Chunker.ChunkConsumer {
    private final Artifact artifact;
    private final List<String> texts = new ArrayList<>(options.commitBatchSize());
    private int firstIndex;
    private boolean embeddingStarted;
    // Totals for the job summary; the writer runs on a single thread.
    private int embedded;
    private int reused;
    private int batches;

    private ChunkWriter(Artifact artifact) {
      this.artifact = artifact;
    }

    @Override
    public void accept(int index, String text) throws ChunkLimitReached {
      // Thrown from inside the extractor callback, so reading stops at the budget.
      if (index >= options.maxChunks()) throw new ChunkLimitReached();
      if (texts.isEmpty()) firstIndex = index;
      texts.add(text);
      if (texts.size() >= options.commitBatchSize()) flush();
    }

    void flush() {
      if (texts.isEmpty()) return;
      if (!embeddingStarted) {
        updateIndex(ArtifactStatus.Index.EMBEDDING);
        embeddingStarted = true;
      }
      String model = embeddingService.model();
      String version = embeddingService.version();
      Map<Integer, String> committed =
          vectorStore.chunkIds(
              tenantId, artifactId, model, version, firstIndex, firstIndex + texts.size());

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
        List<float[]> embeddings = embeddingService.embed(pendingTexts);
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
      LOG.debug("index_batch_committed", "artifact", artifactId, "batch", batches,
          "firstChunk", firstIndex, "chunks", texts.size(), "embedded", pendingTexts.size(),
          "reused", texts.size() - pendingTexts.size());
      texts.clear();
    }

    private String chunkId(int index, String contentHash) {
      return Checksum.sha256(
          artifact.id() + ":" + artifact.version() + ":" + index + ":" + contentHash);
    }
  }

  private String limitMessage() {
    return "Document exceeds the per-artifact budget of " + options.maxChunks() + " chunks";
  }

  /**
   * Internal stop signal. An IOException so it passes unchanged through extractors, including
   * SAX-based ones that only tunnel checked sink failures.
   */
  private static final class ChunkLimitReached extends IOException {
    private ChunkLimitReached() {
      super("chunk budget reached");
    }
  }

  /** The document produced more chunks than allowed under {@link ChunkLimitPolicy#FAIL}. */
  public static final class ChunkLimitExceededException extends IllegalStateException {
    public ChunkLimitExceededException(String message) {
      super(message);
    }
  }

  private Artifact requiredArtifact() {
    return metadataStore
        .find(tenantId, artifactId)
        .orElseThrow(() -> new NoSuchElementException("Artifact not found: " + artifactId));
  }

  private void updateIndex(ArtifactStatus.Index status) {
    metadataStore.update(tenantId, artifactId, current -> current.withIndex(status));
  }

  private static int estimateTokens(String text) {
    return Math.max(1, (int) Math.ceil(text.length() / 4.0));
  }

  private static String safeMessage(Exception failure) {
    String message = failure.getMessage();
    if (message == null) {
      return failure.getClass().getSimpleName();
    }
    return message.substring(0, Math.min(500, message.length()));
  }
}
