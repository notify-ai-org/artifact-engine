package dev.notify.artifact.job;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.store.MetadataStore;
import java.util.List;

/** Authorizes and lists live artifact metadata through the tenant-scoped repository boundary. */
public final class ListMetadataJob extends AbstractJob<List<Artifact>> {
  private static final int MAX_LIMIT = 10_000;
  private final String principalId;
  private final String tenantId;
  private final int limit;
  private final ArtifactAccessVerifier accessVerifier;

  public ListMetadataJob(
      String principalId,
      String tenantId,
      int limit,
      MetadataStore metadataStore,
      ArtifactAccessVerifier accessVerifier) {
    super(accessVerifier, metadataStore);
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.limit = limit;
    this.accessVerifier = accessVerifier;
  }

  @Override
  public List<Artifact> execute() {
    accessVerifier.authenticate(
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
