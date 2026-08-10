package dev.notify.artifact.job;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.store.MetadataStore;
import java.util.List;

/** Authorizes and lists live artifact metadata through the tenant-scoped repository boundary. */
public record ListMetadataJob(
    String principalId,
    String tenantId,
    int limit,
    MetadataStore metadataStore,
    AuthorizationService authorizationService)
    implements Job<List<Artifact>> {
  private static final int MAX_LIMIT = 10_000;

  @Override
  public List<Artifact> execute() {
    authorizationService.require(
        principalId, tenantId, AuthorizationService.Permission.READ_METADATA);
    int boundedLimit = Math.max(1, Math.min(limit, MAX_LIMIT));
    return metadataStore.list(tenantId, boundedLimit).stream()
        .filter(
            artifact ->
                artifact.storageStatus() != ArtifactStatus.Storage.DELETED
                    && artifact.indexStatus() != ArtifactStatus.Index.DELETED)
        .toList();
  }
}
