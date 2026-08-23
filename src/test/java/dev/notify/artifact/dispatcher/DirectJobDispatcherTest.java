package dev.notify.artifact.dispatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.job.DirectJob;
import dev.notify.artifact.worker.DirectJobWorker;
import org.junit.jupiter.api.Test;

class DirectJobDispatcherTest {
  @Test
  void executesDirectJobOnDedicatedThread() throws Exception {
    try (DirectJobWorker worker = new DirectJobWorker(1, 4)) {
      DirectJobDispatcher dispatcher = new DirectJobDispatcher(worker);
      String thread = dispatcher.dispatch((DirectJob<String>) () -> Thread.currentThread().getName());
      assertEquals("artifact-direct-worker", thread);
    }
  }

  @Test
  void rejectsUnclassifiedJob() {
    try (DirectJobWorker worker = new DirectJobWorker(1, 4)) {
      DirectJobDispatcher dispatcher = new DirectJobDispatcher(worker);
      assertThrows(IllegalArgumentException.class, () -> dispatcher.dispatch(() -> "invalid"));
    }
  }
}
