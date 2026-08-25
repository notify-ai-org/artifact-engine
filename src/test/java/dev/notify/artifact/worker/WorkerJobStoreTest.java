package dev.notify.artifact.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.store.InMemoryJobStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WorkerJobStoreTest {
  @Test
  void workerPersistsClaimRunningAndCompletionFields() throws Exception {
    InMemoryJobStore jobs = new InMemoryJobStore();
    JobRecord pending =
        JobRecord.pending(
            "durable-worker-job", "tenant", "artifact", JobRecord.JobType.INDEX, Map.of());
    jobs.create(pending);

    try (QueueManager queues = new QueueManager();
        Worker worker =
            new Worker(
                "durable-worker",
                1,
                1,
                JobRecord.JobType.INDEX,
                queues,
                RetryPolicy.defaults(),
                Duration.ofSeconds(2),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                failure -> {
                  throw new AssertionError(failure.cause());
                },
                ignored -> () -> null,
                jobs)) {
      queues.enqueue(pending);
      worker.start();

      Instant deadline = Instant.now().plusSeconds(3);
      while (jobs.find(pending.id()).orElseThrow().status() != JobRecord.JobStatus.COMPLETED
          && Instant.now().isBefore(deadline)) {
        Thread.sleep(10);
      }

      JobRecord completed = jobs.find(pending.id()).orElseThrow();
      assertEquals(JobRecord.JobStatus.COMPLETED, completed.status());
      assertEquals(1, completed.attempts());
      assertNull(completed.leaseOwner());
      assertNull(completed.leaseExpiresAt());
    }
  }
}
