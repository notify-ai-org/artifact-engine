package dev.notify.artifact.job;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.security.TransportFacts;
import dev.notify.artifact.store.MetadataStore;
import java.util.NoSuchElementException;

/** Shared tenant-bound access primitives for retrieval jobs. */
final class JobRetrievalAccess {
  private JobRetrievalAccess() {}

  static Artifact required(MetadataStore metadataStore, String tenantId, String artifactId) {
    return metadataStore
        .find(tenantId, artifactId)
        .orElseThrow(() -> new NoSuchElementException("Artifact not found"));
  }

  static Artifact requiredReadable(
      MetadataStore metadataStore, String tenantId, String artifactId) {
    Artifact artifact = required(metadataStore, tenantId, artifactId);
    if (artifact.storageStatus() == ArtifactStatus.Storage.DELETED
        || artifact.indexStatus() == ArtifactStatus.Index.DELETED) {
      throw new NoSuchElementException("Artifact not found");
    }
    return artifact;
  }

  static RetrievalSecurityContext verify(
      String principalId,
      String tenantId,
      AuthorizationService.Permission permission,
      Artifact artifact,
      String extractedContent,
      SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity,
      SecurityContextFactory contextFactory) {
    if (retrievalSecurity == null) {
      return new RetrievalSecurityContext(
          principalId,
          tenantId,
          null,
          permission.name(),
          artifact,
          TransportFacts.internal(),
          extractedContent);
    }
    RetrievalSecurityContext context =
        contextFactory.retrieval(
            principalId, tenantId, permission.name(), artifact, extractedContent);
    return retrievalSecurity.verify(context);
  }
}
