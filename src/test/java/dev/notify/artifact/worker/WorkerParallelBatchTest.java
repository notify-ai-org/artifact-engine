package dev.notify.artifact.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.job.Job;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class WorkerParallelBatchTest {
  @Test
  void runsEveryDrainedJobInParallel() throws Exception {
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    JobRecordExecutor jobs = ignored -> parallelJob(started, release);

    try (QueueManager queues = new QueueManager();
        Worker worker = worker(queues, jobs, 2, Duration.ofSeconds(2))) {
      queues.enqueue(job("first"));
      queues.enqueue(job("second"));
      worker.start();

      assertTrue(started.await(1, TimeUnit.SECONDS), "both jobs should start before either exits");
      release.countDown();
      awaitState(worker, "first", JobStateMachines.State.COMPLETED);
      awaitState(worker, "second", JobStateMachines.State.COMPLETED);
    }
  }

  @Test
  void coordinatorCancelsUnfinishedJobsAtLeaseDuration() throws Exception {
    CountDownLatch interrupted = new CountDownLatch(1);
    JobRecordExecutor jobs = ignored -> stuckJob(interrupted);

    try (QueueManager queues = new QueueManager();
        Worker worker = worker(queues, jobs, 1, Duration.ofMillis(200))) {
      queues.enqueue(job("stuck"));
      worker.start();

      assertTrue(interrupted.await(2, TimeUnit.SECONDS));
      awaitState(worker, "stuck", JobStateMachines.State.CANCELLED);
    }
  }

  private static Worker worker(
      QueueManager queues, JobRecordExecutor jobs, int batchSize, Duration leaseDuration) {
    return new Worker(
        "parallel-test",
        batchSize,
        batchSize,
        JobRecord.JobType.INDEX,
        queues,
        RetryPolicy.defaults(),
        leaseDuration,
        Duration.ofMillis(10),
        Duration.ofMillis(10),
        failure -> {},
        jobs);
  }

  private static Job<?> parallelJob(CountDownLatch started, CountDownLatch release) {
    return () -> {
      started.countDown();
      release.await();
      return null;
    };
  }

  private static Job<?> stuckJob(CountDownLatch interrupted) {
    return () -> {
      try {
        Thread.sleep(Duration.ofMinutes(1).toMillis());
      } catch (InterruptedException cancellation) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
        throw cancellation;
      }
      return null;
    };
  }

  private static JobRecord job(String id) {
    return JobRecord.pending(id, "tenant", "artifact", JobRecord.JobType.INDEX, Map.of());
  }

  private static void awaitState(
      Worker worker, String jobId, JobStateMachines.State expected) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (System.nanoTime() < deadline) {
      if (worker.states().get(jobId) == expected) {
        assertEquals(expected, worker.states().get(jobId));
        return;
      }
      Thread.sleep(10);
    }
    assertEquals(expected, worker.states().get(jobId));
  }
}
