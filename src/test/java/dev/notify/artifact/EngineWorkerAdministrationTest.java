package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.factory.ArtifactJobFactory;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.worker.Worker;
import dev.notify.artifact.worker.WorkerManager;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EngineWorkerAdministrationTest {
  private final ArtifactJobFactory jobs = Mockito.mock(ArtifactJobFactory.class);
  private final JobDispatcher dispatcher = Mockito.mock(JobDispatcher.class);

  @Test
  void delegatesWorkerAdministrationToTheWorkerManager() throws Exception {
    try (QueueManager queues = new QueueManager();
        WorkerManager manager =
            new WorkerManager(failure -> {}, null, List.of(), 3, queues, record -> () -> null)) {
      ArtifactEngine engine = new DefaultArtifactEngine(jobs, dispatcher, manager);

      WorkerManager.WorkerSnapshot added =
          engine.addWorker(new WorkerManager.WorkerConfiguration(
              "part-1", 8, 2, 2, Duration.ofMillis(100), JobRecord.JobType.STORE_PART));
      engine.addWorker(new WorkerManager.WorkerConfiguration("index-1", 8));

      assertEquals("part-1", added.id());
      assertEquals(JobRecord.JobType.STORE_PART, added.type());
      assertEquals(2, added.batchSize());
      assertEquals(2, engine.workers().size());
      assertEquals(3, engine.maxWorkers());
      assertEquals(0, engine.removeIdleWorkers(Duration.ofHours(1)));
      assertEquals(0, engine.restoreWorkers(), "no snapshot store configured");
      engine.saveWorkerSnapshots();

      List<Worker.StateChange> seen = new CopyOnWriteArrayList<>();
      Consumer<Worker.StateChange> listener = seen::add;
      engine.addJobStateListener(listener);
      engine.removeJobStateListener(listener);
      assertTrue(engine.jobStates().isEmpty());

      engine.removeWorker("part-1");
      assertEquals(List.of("index-1"), List.copyOf(engine.workers().keySet()));
    }
  }

  @Test
  void enginesWithoutAWorkerManagerRejectWorkerAdministration() {
    ArtifactEngine engine = new DefaultArtifactEngine(jobs, dispatcher);

    assertThrows(UnsupportedOperationException.class, engine::workers);
    assertThrows(UnsupportedOperationException.class, () -> engine.removeWorker("any"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> engine.addWorker(new WorkerManager.WorkerConfiguration("w", 1)));
  }
}
