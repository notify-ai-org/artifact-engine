package dev.notify.artifact.job;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import java.io.IOException;
import java.io.InputStream;

/** Authorizes and opens an original artifact from object storage or the durable spool. */
public record FetchJob(
    String principalId,
    String tenantId,
    String artifactId,
    MetadataStore metadataStore,
    ObjectStore objectStore,
    DurableSpool durableSpool,
    AuthorizationService authorizationService,
    SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity,
    SecurityContextFactory securityContextFactory)
    implements Job<InputStream> {

  @Override
  public InputStream execute() throws IOException {
    authorizationService.require(principalId, tenantId, AuthorizationService.Permission.DOWNLOAD);
    Artifact artifact = JobRetrievalAccess.requiredReadable(metadataStore, tenantId, artifactId);
    JobRetrievalAccess.verify(
        principalId,
        tenantId,
        AuthorizationService.Permission.DOWNLOAD,
        artifact,
        null,
        retrievalSecurity,
        securityContextFactory);
    return artifact.storageKey() != null
            && artifact.storageStatus() == ArtifactStatus.Storage.STORED
        ? objectStore.get(tenantId, artifact.storageKey())
        : durableSpool.open(artifact.spoolPath());
  }
}
