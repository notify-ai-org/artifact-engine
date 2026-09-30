package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.multipart.MultipartPlan;
import dev.notify.artifact.workflow.WorkflowManager.PlannedStep;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SourceIngestPlanTest {
  private static final long MIB = 1024 * 1024;
  private static final MultipartPlan FIVE_CHUNKS = new MultipartPlan(25 * MIB, 5 * MIB, 5);

  @Test
  void pipelinesEveryChunkInOneStageBehindABufferWindow() {
    List<List<PlannedStep>> stages =
        SourceIngestJob.stages(artifact(), FIVE_CHUNKS, "\"v1\"", false, 2);

    assertEquals(2, stages.size(), "one stage for every chunk, then the final stage");
    Map<String, String> ids = names(stages.get(0));
    assertEquals(Set.of(), dependencies(stages.get(0), ids, "BUFFER_CHUNK:1"));
    assertEquals(Set.of(), dependencies(stages.get(0), ids, "BUFFER_CHUNK:2"));
    assertEquals(Set.of("RELEASE_BUFFER:1"), dependencies(stages.get(0), ids, "BUFFER_CHUNK:3"));
    assertEquals(Set.of("RELEASE_BUFFER:3"), dependencies(stages.get(0), ids, "BUFFER_CHUNK:5"));
    assertEquals(Set.of("BUFFER_CHUNK:4", "STORE_INIT:0"),
        dependencies(stages.get(0), ids, "STORE_CHUNK:4"));
    assertEquals(Set.of("BUFFER_CHUNK:4"), dependencies(stages.get(0), ids, "SPOOL_CHUNK:4"),
        "spooling and uploading a chunk run side by side");
    assertEquals(Set.of("STORE_CHUNK:4", "SPOOL_CHUNK:4"),
        dependencies(stages.get(0), ids, "RELEASE_BUFFER:4"));
  }

  @Test
  void indexesTextChunksInOrderWhileStoringThemInParallel() {
    List<List<PlannedStep>> stages =
        SourceIngestJob.stages(artifact(), FIVE_CHUNKS, null, true, 5);

    Map<String, String> ids = names(stages.get(0));
    assertEquals(Set.of("BUFFER_CHUNK:1"), dependencies(stages.get(0), ids, "INDEX_CHUNK:1"));
    assertEquals(Set.of("BUFFER_CHUNK:3", "INDEX_CHUNK:2"),
        dependencies(stages.get(0), ids, "INDEX_CHUNK:3"));
    assertEquals(Set.of("BUFFER_CHUNK:3", "STORE_INIT:0"),
        dependencies(stages.get(0), ids, "STORE_CHUNK:3"));
    assertTrue(stages.get(0).stream()
        .filter(step -> step.job().type() == JobRecord.JobType.BUFFER_CHUNK)
        .allMatch(step -> step.dependsOnJobIds().isEmpty()),
        "a window as large as the plan never holds a download back");
  }

  @Test
  void refusesAnEmptyWindow() {
    assertThrows(IllegalArgumentException.class,
        () -> SourceIngestJob.stages(artifact(), FIVE_CHUNKS, null, false, 0));
  }

  /** Job id to "TYPE:chunk". */
  private static Map<String, String> names(List<PlannedStep> stage) {
    Map<String, String> names = new HashMap<>();
    for (PlannedStep step : stage) {
      names.put(step.job().id(),
          step.job().type() + ":" + step.job().attributes().getOrDefault(ChunkRef.CHUNK, "0"));
    }
    return names;
  }

  private static Set<String> dependencies(
      List<PlannedStep> stage, Map<String, String> names, String name) {
    return stage.stream()
        .filter(step -> name.equals(names.get(step.job().id())))
        .findFirst()
        .orElseThrow()
        .dependsOnJobIds().stream()
        .map(names::get)
        .collect(Collectors.toSet());
  }

  private static Artifact artifact() {
    Instant now = Instant.now();
    return new Artifact("plan-1", "tenant", "k", "f", "URL", "https://example.com/a.bin",
        "a.bin", "application/pdf", 25 * MIB, Artifact.PENDING_DIGEST_PREFIX + "0".repeat(56),
        null, null, ArtifactStatus.Storage.RECEIVING, ArtifactStatus.Index.PENDING, 1, Map.of(),
        null, null, now, now);
  }
}
