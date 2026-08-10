package dev.notify.artifact.job;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.IOException;

/** Tombstones metadata before idempotently deleting derived and original artifact data. */
public record DeleteJob(
    String principalId,
    String tenantId,
    String artifactId,
    MetadataStore metadataStore,
    VectorStore vectorStore,
    ObjectStore objectStore,
    DurableSpool durableSpool,
    AuthorizationService authorizationService)
    implements Job<Void> {

  @Override
  public Void execute() throws IOException {
    authorizationService.require(principalId, tenantId, AuthorizationService.Permission.DELETE);
    Artifact artifact = JobRetrievalAccess.required(metadataStore, tenantId, artifactId);
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
