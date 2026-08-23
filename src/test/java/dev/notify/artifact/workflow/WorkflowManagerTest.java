package dev.notify.artifact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.InMemoryJobQueue;
import dev.notify.artifact.queue.QueueManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WorkflowManagerTest {
  @Test
  void submitsStepsSequentiallyAndCompletesWorkflow() {
    InMemoryJobQueue queue = new InMemoryJobQueue();
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    try (QueueManager queues = new QueueManager(queue);
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.create("ingest", List.of(job("store", JobRecord.JobType.STORE),
          job("index", JobRecord.JobType.INDEX)));

      manager.runOnce();
      assertTrue(queue.claim(JobRecord.JobType.INDEX, "worker", Duration.ofMinutes(1), Instant.now()).isEmpty());
      assertEquals("store", queue.claim(JobRecord.JobType.STORE, "worker", Duration.ofMinutes(1), Instant.now()).orElseThrow().id());

      manager.onUpdate("store", JobUpdateListener.JobUpdate.COMPLETED, null);
      manager.runOnce();
      assertEquals("index", queue.claim(JobRecord.JobType.INDEX, "index-worker", Duration.ofMinutes(1), Instant.now()).orElseThrow().id());
      manager.onUpdate("index", JobUpdateListener.JobUpdate.COMPLETED, null);

      assertEquals(WorkflowStatus.COMPLETED, store.find(workflow.id()).orElseThrow().status());
    }
  }

  @Test
  void deadLetterCrashesWorkflowAndDoesNotSubmitSuccessor() {
    InMemoryJobQueue queue = new InMemoryJobQueue();
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    try (QueueManager queues = new QueueManager(queue);
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.create("ingest", List.of(job("store", JobRecord.JobType.STORE),
          job("index", JobRecord.JobType.INDEX)));
      manager.runOnce();
      manager.onUpdate("store", JobUpdateListener.JobUpdate.DEAD_LETTER, "upload failed");
      manager.runOnce();

      Workflow failed = store.find(workflow.id()).orElseThrow();
      assertEquals(WorkflowStatus.CRASHED, failed.status());
      assertEquals("upload failed", failed.failureMessage());
      assertTrue(queue.claim(JobRecord.JobType.INDEX, "worker", Duration.ofMinutes(1), Instant.now()).isEmpty());
    }
  }

  private static JobRecord job(String id, JobRecord.JobType type) {
    return JobRecord.pending(id, "tenant", "artifact", type, Map.of());
  }
}
