package dev.notify.artifact.job;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.store.MetadataStore;

/** Authorizes and retrieves tenant-scoped artifact metadata. */
public record MetadataJob(
    String principalId,
    String tenantId,
    String artifactId,
    MetadataStore metadataStore,
    AuthorizationService authorizationService,
    SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity,
    SecurityContextFactory securityContextFactory)
    implements Job<Artifact> {

  @Override
  public Artifact execute() {
    authorizationService.require(
        principalId, tenantId, AuthorizationService.Permission.READ_METADATA);
    Artifact artifact = JobRetrievalAccess.required(metadataStore, tenantId, artifactId);
    JobRetrievalAccess.verify(
        principalId,
        tenantId,
        AuthorizationService.Permission.READ_METADATA,
        artifact,
        null,
        retrievalSecurity,
        securityContextFactory);
    return artifact;
  }
}
