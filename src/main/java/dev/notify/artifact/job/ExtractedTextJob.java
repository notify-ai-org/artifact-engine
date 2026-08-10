package dev.notify.artifact.job;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.VectorStore;

/** Retrieves bounded extracted text and applies the retrieval content-security chain. */
public record ExtractedTextJob(
    String principalId,
    String tenantId,
    String artifactId,
    int maxCharacters,
    MetadataStore metadataStore,
    VectorStore vectorStore,
    AuthorizationService authorizationService,
    SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity,
    SecurityContextFactory securityContextFactory)
    implements Job<String> {

  @Override
  public String execute() {
    authorizationService.require(principalId, tenantId, AuthorizationService.Permission.READ_TEXT);
    Artifact artifact = JobRetrievalAccess.requiredReadable(metadataStore, tenantId, artifactId);
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
    return JobRetrievalAccess.verify(
            principalId,
            tenantId,
            AuthorizationService.Permission.READ_TEXT,
            artifact,
            text.toString(),
            retrievalSecurity,
            securityContextFactory)
        .extractedContent();
  }
}
