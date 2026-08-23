package dev.notify.artifact.job;

import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.Ocr;
import dev.notify.artifact.extract.TextExtractor;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Chunker;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/** Restart-safe indexing pipeline with deterministic chunk/vector identities. */
public final class IndexJob extends AbstractJob<Integer> implements QueueableJob<Integer> {
  private final String tenantId;
  private final String artifactId;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final TextExtractorFactory extractorFactory;
  private final Ocr ocr;
  private final Chunker chunker;
  private final EmbeddingService embeddingService;
  private final VectorStore vectorStore;

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
  }

  @Override
  public Integer execute() throws Exception {
    Artifact artifact = requiredArtifact();
    try {
      updateIndex(ArtifactStatus.Index.EXTRACTING);
      String extractedText = extract(artifact);
      updateIndex(ArtifactStatus.Index.CHUNKING);
      List<String> chunkTexts = chunker.chunk(extractedText);

      updateIndex(ArtifactStatus.Index.EMBEDDING);
      List<float[]> embeddings = embeddingService.embed(chunkTexts);
      upsertChunks(artifact, chunkTexts, embeddings);

      updateIndex(ArtifactStatus.Index.READY);
      return chunkTexts.size();
    } catch (Exception failure) {
      metadataStore.update(
          tenantId,
          artifactId,
          current ->
              current.withFailure(
                  current.storageStatus(),
                  ArtifactStatus.Index.RETRY_PENDING,
                  "INDEXING_FAILED",
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

  private String extract(Artifact artifact) throws IOException {
    try (InputStream content = openContent(artifact)) {
      var nativeExtractor =
          extractorFactory.find(artifact.mediaType());
      if (nativeExtractor.isPresent()) {
        String text = nativeExtractor.get().extract(content);
        if (!text.isBlank()) {
          return text;
        }
      }
    }

    if (ocr != null && artifact.mediaType().startsWith("image/")) {
      try (InputStream content = openContent(artifact)) {
        return ocr.recognize(content, artifact.mediaType());
      }
    }
    throw new IllegalStateException("No usable text extractor for " + artifact.mediaType());
  }

  private InputStream openContent(Artifact artifact) throws IOException {
    if (artifact.storageKey() != null
        && artifact.storageStatus() == ArtifactStatus.Storage.STORED) {
      return objectStore.get(tenantId, artifact.storageKey());
    }
    return durableSpool.open(artifact.spoolPath());
  }

  private void upsertChunks(Artifact artifact, List<String> chunkTexts, List<float[]> embeddings) {
    for (int index = 0; index < chunkTexts.size(); index++) {
      String text = chunkTexts.get(index);
      String contentHash = Checksum.sha256(text);
      String chunkId =
          Checksum.sha256(
              artifact.id() + ":" + artifact.version() + ":" + index + ":" + contentHash);
      vectorStore.upsert(
          new ArtifactChunk(
              chunkId,
              artifact.id(),
              artifact.tenantId(),
              index,
              text,
              estimateTokens(text),
              null,
              null,
              Map.of(),
              contentHash,
              embeddingService.model(),
              embeddingService.version(),
              embeddings.get(index)));
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
