package dev.notify.artifact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.model.JobRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class QueueManagerTest {
  @Test
  void createsAndRoutesToSeparateJobTypeQueues() {
    try (QueueManager manager = new QueueManager(type -> new InMemoryJobQueue())) {
      manager.enqueue(job("store-1", "tenant-a", JobRecord.JobType.STORE));
      manager.enqueue(job("index-1", "tenant-a", JobRecord.JobType.INDEX));

      JobQueue store = manager.queue(JobRecord.JobType.STORE).orElseThrow();
      JobQueue index = manager.queue(JobRecord.JobType.INDEX).orElseThrow();
      assertNotSame(store, index);
      assertEquals("store-1", claim(store, JobRecord.JobType.STORE));
      assertEquals("index-1", claim(index, JobRecord.JobType.INDEX));
    }
  }

  @Test
  void addsAndRemovesQueuesWithoutAllowingSharingOrReplacement() {
    InMemoryJobQueue store = new InMemoryJobQueue();
    InMemoryJobQueue index = new InMemoryJobQueue();
    try (QueueManager manager = new QueueManager(type -> new InMemoryJobQueue())) {
      manager.addQueue(JobRecord.JobType.STORE, store);
      manager.addQueue(JobRecord.JobType.INDEX, index);

      assertThrows(
          IllegalStateException.class,
          () -> manager.addQueue(JobRecord.JobType.STORE, index));
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.addQueue(JobRecord.JobType.FETCH, index));
      assertEquals(store, manager.removeQueue(JobRecord.JobType.STORE).orElseThrow());
      assertTrue(manager.queue(JobRecord.JobType.STORE).isEmpty());
      assertTrue(manager.removeQueue(JobRecord.JobType.STORE).isEmpty());
    }
  }

  @Test
  void concurrentFirstUseCreatesOneQueuePerJobType() throws Exception {
    List<JobQueue> created = java.util.Collections.synchronizedList(new ArrayList<>());
    try (QueueManager manager =
            new QueueManager(
                type -> {
                  JobQueue queue = new InMemoryJobQueue();
                  created.add(queue);
                  return queue;
                });
        var ignored = new ExecutorCloser(Executors.newFixedThreadPool(8))) {
      var executor = ignored.executor;
      List<java.util.concurrent.Callable<Void>> operations = new ArrayList<>();
      for (int index = 0; index < 50; index++) {
        int id = index;
        operations.add(
            () -> {
              manager.enqueue(job("job-" + id, "tenant-a", JobRecord.JobType.INDEX));
              return null;
            });
      }
      for (var result : executor.invokeAll(operations)) result.get();
      assertEquals(1, created.size());
      assertEquals(1, manager.queues().size());
    }
  }

  @Test
  void exposesLeaseOperationsWithoutLeakingTheUnderlyingQueue() {
    try (QueueManager manager = new QueueManager()) {
      manager.enqueue(job("job", "tenant", JobRecord.JobType.INDEX));
      Instant now = Instant.now();
      JobRecord claimed =
          manager
              .claim(JobRecord.JobType.INDEX, "worker", Duration.ofMinutes(1), now)
              .orElseThrow();

      assertEquals("job", claimed.id());
      assertTrue(manager.complete(JobRecord.JobType.INDEX, "job", "worker"));
    }
  }

  private static String claim(JobQueue queue, JobRecord.JobType type) {
    return queue
        .claim(type, "worker", Duration.ofMinutes(1), Instant.now())
        .orElseThrow()
        .id();
  }

  private static JobRecord job(String id, String tenant) {
    return job(id, tenant, JobRecord.JobType.INDEX);
  }

  private static JobRecord job(String id, String tenant, JobRecord.JobType type) {
    return JobRecord.pending(id, tenant, "artifact", type, Map.of());
  }

  private static final class ExecutorCloser implements AutoCloseable {
    private final java.util.concurrent.ExecutorService executor;

    private ExecutorCloser(java.util.concurrent.ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    public void close() {
      executor.shutdownNow();
    }
  }
}
