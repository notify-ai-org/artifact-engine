package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReleaser;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.workflow.Workflow;
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

class SpoolReleaserTest {
  @TempDir Path spoolRoot;

  private InMemoryStores.Metadata metadata;
  private DurableSpool spool;
  private SpoolReleaser releaser;

  @BeforeEach
  void setUp() throws IOException {
    metadata = new InMemoryStores.Metadata();
    spool = new DurableSpool(spoolRoot, 1 << 20, new ObjectMapper());
    releaser = new SpoolReleaser(metadata, spool);
  }

  @Test
  void releasesAStoredArtifactsSpoolCopyAndItsQuotaOnce() throws Exception {
    Artifact artifact = artifact("a1", ArtifactStatus.Storage.STORED, ArtifactStatus.Index.READY);
    Path file = artifact.spoolPath();

    assertTrue(releaser.release("tenant", "a1"));

    assertFalse(Files.exists(file));
    assertEquals(0, spool.usage().bytes());
    assertNull(current("a1").spoolPath());
    assertFalse(releaser.release("tenant", "a1"));
  }

  @Test
  void keepsTheSpoolCopyOfAnArtifactThatIsNotStoredYet() throws Exception {
    Artifact artifact =
        artifact("a1", ArtifactStatus.Storage.UPLOADING, ArtifactStatus.Index.PENDING);

    assertThrows(IllegalStateException.class, () -> releaser.release("tenant", "a1"));

    assertTrue(Files.exists(artifact.spoolPath()));
    assertEquals(artifact.spoolPath(), current("a1").spoolPath());
  }

  @Test
  void aRetryAfterDyingBetweenDeleteAndMetadataUpdateFinishesTheRelease() throws Exception {
    Artifact artifact = artifact("a1", ArtifactStatus.Storage.STORED, ArtifactStatus.Index.READY);
    spool.discard(artifact.spoolPath()); // file gone, metadata still points at it

    assertTrue(releaser.release("tenant", "a1"));

    assertNull(current("a1").spoolPath());
  }

  @Test
  void crashedWorkflowsReleaseOnlyStoredArtifacts() throws Exception {
    Artifact stored = artifact("a1", ArtifactStatus.Storage.STORED, ArtifactStatus.Index.DEAD_LETTER);
    Artifact unstored =
        artifact("a2", ArtifactStatus.Storage.RETRY_PENDING, ArtifactStatus.Index.PENDING);

    releaser.onWorkflowCrashed().accept(crashed("a1"));
    releaser.onWorkflowCrashed().accept(crashed("a2"));

    assertFalse(Files.exists(stored.spoolPath()));
    assertTrue(Files.exists(unstored.spoolPath()));
  }

  @Test
  void sweepReleasesOnlyStoredAndIndexedArtifacts() throws Exception {
    Artifact done = artifact("a1", ArtifactStatus.Storage.STORED, ArtifactStatus.Index.READY);
    Artifact indexing =
        artifact("a2", ArtifactStatus.Storage.STORED, ArtifactStatus.Index.EMBEDDING);
    Artifact storing =
        artifact("a3", ArtifactStatus.Storage.UPLOADING, ArtifactStatus.Index.PENDING);

    assertEquals(1, releaser.sweep());

    assertFalse(Files.exists(done.spoolPath()));
    assertTrue(Files.exists(indexing.spoolPath()));
    assertTrue(Files.exists(storing.spoolPath()));
    assertEquals(0, releaser.sweep());
  }

  private Artifact artifact(
      String id, ArtifactStatus.Storage storage, ArtifactStatus.Index index) throws IOException {
    Path path =
        spool
            .write("tenant", id, new ByteArrayInputStream("content".getBytes()), Map.of())
            .contentPath();
    Instant now = Instant.now();
    return metadata.save(
        new Artifact(
            id, "tenant", "key-" + id, "fp-" + id, "UPLOAD", null, "doc.txt", "text/plain", 7,
            "sha", storage == ArtifactStatus.Storage.STORED ? "key/" + id : null, path, storage,
            index, 1, Map.of(), null, null, now, now));
  }

  private Artifact current(String id) {
    return metadata.find("tenant", id).orElseThrow();
  }

  private static Workflow crashed(String artifactId) {
    return new Workflow("wf-" + artifactId, "ingest-store-index", Instant.now(), Instant.now(),
        Workflow.WorkflowStatus.CRASHED, null, null, List.of(),
        Map.of("tenantId", "tenant", "artifactId", artifactId, "version", "1"), "boom");
  }
}
