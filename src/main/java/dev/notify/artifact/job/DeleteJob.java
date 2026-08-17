package dev.notify.artifact.job;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.IOException;

/** Tombstones metadata before idempotently deleting derived and original artifact data. */
public final class DeleteJob extends AbstractJob<Void> {
  private final String principalId;
  private final String tenantId;
  private final String artifactId;
  private final VectorStore vectorStore;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final ArtifactAccessVerifier accessVerifier;

  public DeleteJob(
      String principalId,
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      ArtifactAccessVerifier accessVerifier) {
    super(accessVerifier, metadataStore);
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.vectorStore = vectorStore;
    this.objectStore = objectStore;
    this.durableSpool = durableSpool;
    this.accessVerifier = accessVerifier;
  }

  @Override
  public Void execute() throws IOException {
    accessVerifier.authenticate(principalId, tenantId, AuthorizationService.Permission.DELETE);
    Artifact artifact = required(tenantId, artifactId);
    accessVerifier.verifyArtifact(tenantId, artifact);
    metadataStore.update(
        tenantId,
        artifactId,
        current ->
            current
                .withStorage(ArtifactStatus.Storage.DELETED, current.storageKey())
                .withIndex(ArtifactStatus.Index.DELETED));
    vectorStore.deleteArtifact(tenantId, artifactId);
    if (artifact.storageKey() != null) {
      objectStore.delete(tenantId, artifact.storageKey());
    }
    if (artifact.spoolPath() != null) {
      durableSpool.discard(artifact.spoolPath());
    }
    return null;
  }
}
