package dev.notify.artifact.job;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.VectorStore;

/** Retrieves bounded extracted text and applies the retrieval content-security chain. */
public final class ExtractedTextJob extends AbstractJob<String> {
  private final String principalId;
  private final String tenantId;
  private final String artifactId;
  private final int maxCharacters;
  private final VectorStore vectorStore;
  private final ArtifactAccessVerifier accessVerifier;

  public ExtractedTextJob(
      String principalId,
      String tenantId,
      String artifactId,
      int maxCharacters,
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ArtifactAccessVerifier accessVerifier) {
    super(accessVerifier, metadataStore);
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.maxCharacters = maxCharacters;
    this.vectorStore = vectorStore;
    this.accessVerifier = accessVerifier;
  }

  @Override
  public String execute() {
    Artifact artifact = requiredReadable(tenantId, artifactId);
    int limit = Math.max(0, maxCharacters);
    StringBuilder text = new StringBuilder(Math.min(limit, 16 * 1024));
    for (ArtifactChunk chunk : vectorStore.chunks(tenantId, artifactId)) {
      if (text.length() >= limit) {
        break;
      }
      if (!text.isEmpty()) {
        text.append('\n');
      }
      int remaining = limit - text.length();
      text.append(chunk.text(), 0, Math.min(remaining, chunk.text().length()));
    }
    return verify(
            principalId,
            tenantId,
            AuthorizationService.Permission.READ_TEXT,
            artifact,
            text.toString());
  }
}
