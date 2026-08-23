package dev.notify.artifact.job;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.store.MetadataStore;

/** Authorizes and retrieves tenant-scoped artifact metadata. */
public final class MetadataJob extends AbstractJob<Artifact> implements DirectJob<Artifact> {
  private final String principalId;
  private final String tenantId;
  private final String artifactId;
  private final ArtifactAccessVerifier accessVerifier;

  public MetadataJob(
      String principalId,
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      ArtifactAccessVerifier accessVerifier) {
    super(accessVerifier, metadataStore);
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.accessVerifier = accessVerifier;
  }

  @Override
  public Artifact execute() {
    Artifact artifact = required(tenantId, artifactId);
    verify(
        principalId,
        tenantId,
        AuthorizationService.Permission.READ_METADATA,
        artifact,
        null);
    return artifact;
  }
}
