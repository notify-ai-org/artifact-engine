package dev.notify.artifact.job;

import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.Ocr;
import dev.notify.artifact.extract.TextExtractor;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.security.ExtractedHtmlSanitizationFilter;
import dev.notify.artifact.security.IndexingContentContext;
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
public record IndexJob(
    String tenantId,
    String artifactId,
    MetadataStore metadataStore,
    ObjectStore objectStore,
    DurableSpool durableSpool,
    List<TextExtractor> extractors,
    Ocr ocr,
    Chunker chunker,
    EmbeddingService embeddingService,
    VectorStore vectorStore)
    implements Job<Integer> {

  public IndexJob {
    extractors = List.copyOf(extractors);
  }

  @Override
  public Integer execute() throws Exception {
    Artifact artifact = requiredArtifact();
    try {
      updateIndex(ArtifactStatus.Index.EXTRACTING);
      String extractedText = extract(artifact);
      IndexingContentContext contentSecurity =
          new IndexingContentContext(artifact.mediaType(), extractedText);
      new ExtractedHtmlSanitizationFilter<IndexingContentContext>().verify(contentSecurity);
      extractedText = contentSecurity.extractedContent();

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

  private String extract(Artifact artifact) throws IOException {
    try (InputStream content = openContent(artifact)) {
      var nativeExtractor =
          extractors.stream()
              .filter(extractor -> extractor.supports(artifact.mediaType()))
              .findFirst();
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
