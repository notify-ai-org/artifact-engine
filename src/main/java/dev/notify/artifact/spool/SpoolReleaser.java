package dev.notify.artifact.spool;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.workflow.Workflow;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Frees an artifact's local spool copy once the object store holds a verified copy.
 *
 * <p>Both store paths verify the uploaded object before marking an artifact STORED, so STORED is
 * the only precondition; readers (fetch, re-indexing) fall back to the object store when the spool
 * copy is gone. The file is deleted before the metadata is cleared: if the process dies in between,
 * a retry finds the path still recorded, deletes nothing further, and clears it.
 */
public final class SpoolReleaser {
  private final MetadataStore metadataStore;
  private final DurableSpool durableSpool;

  public SpoolReleaser(MetadataStore metadataStore, DurableSpool durableSpool) {
    this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
    this.durableSpool = Objects.requireNonNull(durableSpool, "durableSpool");
  }

  /**
   * Releases the spool copy of a stored artifact.
   *
   * @return true if a spool copy was released, false if there was none left to release
   * @throws IllegalStateException if the artifact is not stored yet (its spool copy is the only one)
   */
  public boolean release(String tenantId, String artifactId) throws IOException {
    Artifact artifact =
        metadataStore
            .find(tenantId, artifactId)
            .orElseThrow(() -> new java.util.NoSuchElementException("Artifact not found"));
    if (artifact.spoolPath() == null) return false;
    if (artifact.storageStatus() != ArtifactStatus.Storage.STORED
        || artifact.storageKey() == null) {
      throw new IllegalStateException(
          "Artifact " + artifactId + " is not stored; its spool copy is the only copy");
    }
    durableSpool.discard(artifact.spoolPath());
    metadataStore.update(tenantId, artifactId, Artifact::withoutSpool);
    return true;
  }

  /**
   * Crash listener: a workflow that crashed after storing (for example, indexing dead-lettered)
   * no longer needs the local copy. A crash before storing keeps it, since it is the only copy.
   */
  public Consumer<Workflow> onWorkflowCrashed() {
    return workflow -> {
      String tenantId = workflow.attributes().get("tenantId");
      String artifactId = workflow.attributes().get("artifactId");
      if (tenantId == null || artifactId == null) return;
      Optional<Artifact> artifact = metadataStore.find(tenantId, artifactId);
      if (artifact.isEmpty()
          || artifact.get().storageStatus() != ArtifactStatus.Storage.STORED) {
        return;
      }
      try {
        release(tenantId, artifactId);
      } catch (IOException failure) {
        throw new IllegalStateException("Failed to release spool copy of " + artifactId, failure);
      }
    };
  }

  /**
   * Releases spool copies left by workflows that predate the release stage: artifacts that are
   * stored and fully indexed. Anything still in flight is left alone.
   *
   * @return the number of spool copies released
   */
  public int sweep() throws IOException {
    int released = 0;
    for (Path entry : durableSpool.entries()) {
      Optional<Artifact> artifact = metadataStore.findBySpoolPath(entry);
      if (artifact.isPresent()
          && artifact.get().storageStatus() == ArtifactStatus.Storage.STORED
          && artifact.get().indexStatus() == ArtifactStatus.Index.READY
          && release(artifact.get().tenantId(), artifact.get().id())) {
        released++;
      }
    }
    return released;
  }
}
