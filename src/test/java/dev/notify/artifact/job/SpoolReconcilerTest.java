package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReconciler;
import dev.notify.artifact.store.InMemoryStores;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpoolReconcilerTest {
  @TempDir Path spoolRoot;

  private InMemoryStores.Metadata metadata;
  private DurableSpool spool;
  private SpoolReconciler reconciler;

  @BeforeEach
  void setUp() throws IOException {
    metadata = new InMemoryStores.Metadata();
    spool = new DurableSpool(spoolRoot, 1 << 20, new ObjectMapper());
    reconciler = new SpoolReconciler(spool, metadata);
  }

  @Test
  void aSpoolThatMatchesMetadataIsHealthy() throws Exception {
    artifact("a1", ArtifactStatus.Storage.SPOOLED);
    artifact("a2", ArtifactStatus.Storage.STORED);

    assertTrue(reconciler.reconcile(metadata.awaitingStorage(10)).healthy());
  }

  @Test
  void reportsUnstoredArtifactsWhoseSpoolContentIsGone() throws Exception {
    artifact("a1", ArtifactStatus.Storage.SPOOLED);
    Files.delete(artifact("a2", ArtifactStatus.Storage.UPLOADING).spoolPath());
    Files.delete(artifact("a3", ArtifactStatus.Storage.RETRY_PENDING).spoolPath());

    SpoolReconciler.Report report = reconciler.reconcile(metadata.awaitingStorage(10));

    assertEquals(List.of("a2", "a3"), report.artifactIdsMissingContent());
    assertTrue(report.orphanedEntries().isEmpty());
  }

  @Test
  void ignoresMissingContentOnceTheObjectStoreHoldsACopy() throws Exception {
    Files.delete(artifact("a1", ArtifactStatus.Storage.STORED).spoolPath());
    // A chunked upload has no published content file until it commits.
    Files.delete(artifact("a2", ArtifactStatus.Storage.RECEIVING).spoolPath());

    assertTrue(reconciler.reconcile(metadata.awaitingStorage(10)).healthy());
  }

  @Test
  void reportsSpoolEntriesWithoutMetadata() throws Exception {
    Path orphan =
        spool
            .write("tenant", "orphan", new ByteArrayInputStream("content".getBytes()), Map.of())
            .contentPath();

    SpoolReconciler.Report report = reconciler.reconcile(metadata.awaitingStorage(10));

    assertEquals(List.of(orphan), report.orphanedEntries());
    assertTrue(report.artifactIdsMissingContent().isEmpty());
  }

  private Artifact artifact(String id, ArtifactStatus.Storage storage) throws IOException {
    Path path =
        spool
            .write("tenant", id, new ByteArrayInputStream("content".getBytes()), Map.of())
            .contentPath();
    Instant now = Instant.now();
    return metadata.save(
        new Artifact(
            id, "tenant", "key-" + id, "fp-" + id, "UPLOAD", null, "doc.txt", "text/plain", 7,
            "sha", storage == ArtifactStatus.Storage.STORED ? "key/" + id : null, path, storage,
            index(storage), 1, Map.of(), null, null, now, now));
  }

  private static ArtifactStatus.Index index(ArtifactStatus.Storage storage) {
    return storage == ArtifactStatus.Storage.STORED
        ? ArtifactStatus.Index.READY
        : ArtifactStatus.Index.PENDING;
  }
}
