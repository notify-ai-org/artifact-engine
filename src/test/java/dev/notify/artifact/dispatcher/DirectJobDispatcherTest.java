package dev.notify.artifact.dispatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.job.DirectJob;
import dev.notify.artifact.worker.DirectJobWorker;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
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

  @Test
  void preparesOnTheCallingThreadAndExecutesOnThePool() throws Exception {
    try (DirectJobWorker worker = new DirectJobWorker(1, 4)) {
      List<String> threads = new CopyOnWriteArrayList<>();
      DirectJob<String> job =
          new DirectJob<>() {
            @Override
            public void prepare() {
              threads.add(Thread.currentThread().getName());
            }

            @Override
            public String execute() {
              return Thread.currentThread().getName();
            }
          };

      String executedOn = new DirectJobDispatcher(worker).dispatch(job);

      assertEquals(List.of(Thread.currentThread().getName()), threads);
      assertEquals("artifact-direct-worker", executedOn);
    }
  }

  @Test
  void abandonsAPreparedJobThePoolRejects() {
    DirectJobWorker worker = new DirectJobWorker(1, 1);
    worker.close(); // A shut-down pool rejects every hand-off, deterministically.
    List<String> events = new CopyOnWriteArrayList<>();
    DirectJob<Void> rejected =
        new DirectJob<>() {
          @Override
          public void prepare() {
            events.add("prepare");
          }

          @Override
          public Void execute() {
            events.add("execute");
            return null;
          }

          @Override
          public void abandon() {
            events.add("abandon");
          }
        };

    assertThrows(RejectedExecutionException.class, () -> worker.execute(rejected));
    assertEquals(List.of("prepare", "abandon"), events);
  }
}
