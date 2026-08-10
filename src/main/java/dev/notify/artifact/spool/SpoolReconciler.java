package dev.notify.artifact.spool;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.store.MetadataStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Compares durable spool entries with metadata without deleting ambiguous recovery data. */
public final class SpoolReconciler {
  private final DurableSpool spool;
  private final MetadataStore metadataStore;

  public SpoolReconciler(DurableSpool spool, MetadataStore metadataStore) {
    this.spool = spool;
    this.metadataStore = metadataStore;
  }

  public Report reconcile(List<Artifact> incompleteArtifacts) throws IOException {
    List<Path> orphanedEntries = new ArrayList<>();
    for (Path entry : spool.entries()) {
      if (metadataStore.findBySpoolPath(entry).isEmpty()) {
        orphanedEntries.add(entry);
      }
    }

    List<String> missingEntries =
        incompleteArtifacts.stream()
            .filter(artifact -> artifact.spoolPath() != null)
            .filter(artifact -> !Files.exists(artifact.spoolPath()))
            .map(Artifact::id)
            .toList();
    return new Report(List.copyOf(orphanedEntries), missingEntries);
  }

  public record Report(List<Path> orphanedEntries, List<String> artifactIdsMissingContent) {
    public boolean healthy() {
      return orphanedEntries.isEmpty() && artifactIdsMissingContent.isEmpty();
    }
  }
}
