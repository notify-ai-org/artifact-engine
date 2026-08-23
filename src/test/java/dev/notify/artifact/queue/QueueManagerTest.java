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
  void createsAndRoutesToSeparateTenantQueues() {
    try (QueueManager manager = new QueueManager(tenant -> new InMemoryJobQueue())) {
      manager.enqueue(job("a-1", "tenant-a"));
      manager.enqueue(job("b-1", "tenant-b"));

      JobQueue tenantA = manager.queue("tenant-a").orElseThrow();
      JobQueue tenantB = manager.queue("tenant-b").orElseThrow();
      assertNotSame(tenantA, tenantB);
      assertEquals("a-1", claim(tenantA));
      assertEquals("b-1", claim(tenantB));
    }
  }

  @Test
  void addsAndRemovesQueuesWithoutAllowingSharingOrReplacement() {
    InMemoryJobQueue tenantA = new InMemoryJobQueue();
    InMemoryJobQueue tenantB = new InMemoryJobQueue();
    try (QueueManager manager = new QueueManager(tenant -> new InMemoryJobQueue())) {
      manager.addQueue("tenant-a", tenantA);
      manager.addQueue("tenant-b", tenantB);

      assertThrows(IllegalStateException.class, () -> manager.addQueue("tenant-a", tenantB));
      assertThrows(
          IllegalArgumentException.class, () -> manager.addQueue("tenant-c", tenantB));
      assertEquals(tenantA, manager.removeQueue("tenant-a").orElseThrow());
      assertTrue(manager.queue("tenant-a").isEmpty());
      assertTrue(manager.removeQueue("tenant-a").isEmpty());
    }
  }

  @Test
  void concurrentFirstUseCreatesOneQueuePerTenant() throws Exception {
    List<JobQueue> created = java.util.Collections.synchronizedList(new ArrayList<>());
    try (QueueManager manager =
            new QueueManager(
                tenant -> {
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
              manager.enqueue(job("job-" + id, "tenant-a"));
              return null;
            });
      }
      for (var result : executor.invokeAll(operations)) result.get();
      assertEquals(1, created.size());
      assertEquals(1, manager.queues().size());
    }
  }

  private static String claim(JobQueue queue) {
    return queue
        .claim(JobRecord.JobType.INDEX, "worker", Duration.ofMinutes(1), Instant.now())
        .orElseThrow()
        .id();
  }

  private static JobRecord job(String id, String tenant) {
    return JobRecord.pending(id, tenant, "artifact", JobRecord.JobType.INDEX, Map.of());
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
