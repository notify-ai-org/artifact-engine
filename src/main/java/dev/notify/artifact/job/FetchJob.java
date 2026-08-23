package dev.notify.artifact.job;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import java.io.IOException;
import java.io.InputStream;

/** Authorizes and opens an original artifact from object storage or the durable spool. */
public final class FetchJob extends AbstractJob<InputStream> implements DirectJob<InputStream> {
  private final String principalId;
  private final String tenantId;
  private final String artifactId;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final ArtifactAccessVerifier accessVerifier;

  public FetchJob(
      String principalId,
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      ArtifactAccessVerifier accessVerifier) {
    super(accessVerifier, metadataStore);
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.objectStore = objectStore;
    this.durableSpool = durableSpool;
    this.accessVerifier = accessVerifier;
  }

  @Override
  public InputStream execute() throws IOException {
    Artifact artifact = requiredReadable(tenantId, artifactId);
    verify(
        principalId,
        tenantId,
        AuthorizationService.Permission.DOWNLOAD,
        artifact,
        null);
    return artifact.storageKey() != null
            && artifact.storageStatus() == ArtifactStatus.Storage.STORED
        ? objectStore.get(tenantId, artifact.storageKey())
        : durableSpool.open(artifact.spoolPath());
  }
}
