package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.InMemoryJobQueue;
import dev.notify.artifact.queue.QueueManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class QueuingJobDispatcherTest {
  @Test
  void enqueuesDurableRecordWithoutExecutingProcessLocalJob() throws Exception {
    InMemoryJobQueue queue = new InMemoryJobQueue();
    AtomicBoolean executed = new AtomicBoolean();
    JobRecord record =
        JobRecord.pending(
            "operation-1", "tenant-a", "artifact-a", JobRecord.JobType.INDEX, Map.of());
    QueueableJob<String> job = queueable(record, "accepted", executed);

    try (QueueManager queueManager = new QueueManager(queue)) {
      QueuingJobDispatcher dispatcher = new QueuingJobDispatcher(queueManager);

      assertEquals("accepted", dispatcher.dispatch(job));
      assertFalse(executed.get(), "background workflow must not execute on the dispatch thread");
      assertEquals(
          "operation-1",
          queue
              .claim(
                  JobRecord.JobType.INDEX,
                  "worker-1",
                  Duration.ofMinutes(1),
                  Instant.now())
              .orElseThrow()
              .id());
    }
  }

  @Test
  void delegatesJobsThatCannotBeSerializedDurably() throws Exception {
    AtomicBoolean executed = new AtomicBoolean();
    Job<String> inlineJob =
        () -> {
          executed.set(true);
          return "complete";
        };

    try (QueueManager queueManager = new QueueManager(new InMemoryJobQueue())) {
      QueuingJobDispatcher dispatcher = new QueuingJobDispatcher(queueManager);

      assertEquals("complete", dispatcher.dispatch(inlineJob));
      assertTrue(executed.get());
    }
  }

  @Test
  void repeatedDispatchUsesTheStableOperationId() throws Exception {
    InMemoryJobQueue queue = new InMemoryJobQueue();
    JobRecord record =
        JobRecord.pending(
            "operation-3", "tenant-a", "artifact-a", JobRecord.JobType.STORE, Map.of());
    QueueableJob<String> job = queueable(record, "accepted", new AtomicBoolean());

    try (QueueManager queueManager = new QueueManager(queue)) {
      QueuingJobDispatcher dispatcher = new QueuingJobDispatcher(queueManager);

      dispatcher.dispatch(job);
      dispatcher.dispatch(job);

      assertTrue(
          queue
              .claim(
                  JobRecord.JobType.STORE,
                  "worker-1",
                  Duration.ofMinutes(1),
                  Instant.now())
              .isPresent());
      assertTrue(
          queue
              .claim(
                  JobRecord.JobType.STORE,
                  "worker-2",
                  Duration.ofMinutes(1),
                  Instant.now())
              .isEmpty());
    }
  }

  @Test
  void rejectsNonPendingRecordsBeforeTheyReachTheQueue() {
    Instant now = Instant.now();
    JobRecord claimed =
        new JobRecord(
            "operation-2",
            "tenant-a",
            "artifact-a",
            JobRecord.JobType.STORE,
            JobRecord.JobStatus.CLAIMED,
            1,
            now,
            "worker-1",
            now.plusSeconds(30),
            Map.of(),
            null,
            now,
            now);

    try (QueueManager queueManager = new QueueManager(new InMemoryJobQueue())) {
      QueuingJobDispatcher dispatcher = new QueuingJobDispatcher(queueManager);

      assertThrows(
          IllegalArgumentException.class,
          () -> dispatcher.dispatch(queueable(claimed, "accepted", new AtomicBoolean())));
    }
  }

  private static QueueableJob<String> queueable(
      JobRecord record, String acceptedResult, AtomicBoolean executed) {
    return new QueueableJob<>() {
      @Override
      public JobRecord queueRecord() {
        return record;
      }

      @Override
      public String queuedResult() {
        return acceptedResult;
      }

      @Override
      public String execute() {
        executed.set(true);
        return "executed";
      }
    };
  }
}
