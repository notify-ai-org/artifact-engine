package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.worker.WorkerManager;
import dev.notify.artifact.worker.WorkerSnapshotStore;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerManagerTest {

  @Test
  void addUsesAndSnapshotsExplicitWorkerConfiguration() {
    try (WorkerManager manager = new WorkerManager()) {
      manager.add("configured", 32, 8, 4_096, Duration.ofSeconds(2));

      WorkerManager.WorkerSnapshot snapshot = manager.snapshots().get("configured");
      assertEquals(32, snapshot.capacity());
      assertEquals(8, snapshot.batchSize());
      assertEquals(4_096, snapshot.batchBytes());
      assertEquals(Duration.ofSeconds(2), snapshot.flushInterval());
    }
  }

  @Test
  void constructorInitializesPoolAndAddExpandsOnlyToConfiguredLimit() {
    List<WorkerManager.WorkerConfiguration> initialWorkers =
        List.of(
            new WorkerManager.WorkerConfiguration("initial-1", 8),
            new WorkerManager.WorkerConfiguration("initial-2", 8));

    try (WorkerManager manager = new WorkerManager(initialWorkers, 3)) {
      assertEquals(2, manager.size());
      assertEquals(3, manager.maxWorkers());
      manager.add("expanded", 8);
      assertEquals(3, manager.size());

      assertThrows(IllegalStateException.class, () -> manager.add("too-many", 8));
    }
  }

  @Test
  void rejectsInvalidInitialPoolLimits() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new WorkerManager(List.of(new WorkerManager.WorkerConfiguration("worker", 8)), 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkerManager(
                List.of(
                    new WorkerManager.WorkerConfiguration("worker-1", 8),
                    new WorkerManager.WorkerConfiguration("worker-2", 8)),
                1));
  }

  @Test
  void restoreRecreatesWorkerWithSnapshottedConfiguration() throws IOException {
    Instant lastUsed = Instant.parse("2026-08-10T10:15:30Z");
    WorkerManager.WorkerSnapshot saved =
        new WorkerManager.WorkerSnapshot(
            "restored", 24, 0, lastUsed, 6, 2_048, Duration.ofMillis(750));
    RecordingSnapshotStore store = new RecordingSnapshotStore(List.of(saved));

    try (WorkerManager manager = new WorkerManager(failure -> {}, store)) {
      assertEquals(1, manager.restore());

      WorkerManager.WorkerSnapshot restored = manager.snapshots().get("restored");
      assertEquals(24, restored.capacity());
      assertEquals(6, restored.batchSize());
      assertEquals(2_048, restored.batchBytes());
      assertEquals(Duration.ofMillis(750), restored.flushInterval());
      assertEquals(lastUsed, restored.lastUsed());

      manager.flushSnapshots();
      assertEquals(List.of(restored), store.saved);
    }
  }

  @Test
  void legacyAddAndSnapshotsUseStableDefaults() {
    try (WorkerManager manager = new WorkerManager()) {
      manager.add("legacy", 10);

      WorkerManager.WorkerSnapshot snapshot = manager.snapshots().get("legacy");
      assertEquals(WorkerManager.DEFAULT_BATCH_SIZE, snapshot.batchSize());
      assertEquals(WorkerManager.DEFAULT_BATCH_BYTES, snapshot.batchBytes());
      assertEquals(WorkerManager.DEFAULT_FLUSH_INTERVAL, snapshot.flushInterval());
    }
  }

  @Test
  void rejectsInvalidWorkerConfigurationBeforeStartingWorker() {
    try (WorkerManager manager = new WorkerManager()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.add("invalid", 0, 8, 1_024, Duration.ofSeconds(1)));
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.add("invalid", 8, 0, 1_024, Duration.ofSeconds(1)));
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.add("invalid", 8, 8, 0, Duration.ofSeconds(1)));
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.add("invalid", 8, 8, 1_024, Duration.ZERO));
    }
  }

  private static final class RecordingSnapshotStore implements WorkerSnapshotStore {
    private final List<WorkerManager.WorkerSnapshot> loaded;
    private List<WorkerManager.WorkerSnapshot> saved = new ArrayList<>();

    private RecordingSnapshotStore(List<WorkerManager.WorkerSnapshot> loaded) {
      this.loaded = List.copyOf(loaded);
    }

    @Override
    public List<WorkerManager.WorkerSnapshot> load() {
      return loaded;
    }

    @Override
    public void save(List<WorkerManager.WorkerSnapshot> snapshots) {
      saved = List.copyOf(snapshots);
    }
  }
}
