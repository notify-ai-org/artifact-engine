package dev.notify.artifact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.worker.JobStateMachines;
import dev.notify.artifact.worker.StateMachine;
import dev.notify.artifact.worker.Worker;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class WorkflowManagerTest {
  @Test
  void startImmediatelyDispatchesPendingSubmittedAndRunningSteps() {
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    int workflowCount = 501;
    try (QueueManager setupQueues = new QueueManager();
        WorkflowManager setup =
            new WorkflowManager(store, setupQueues, Duration.ofHours(1), failure -> {})) {
      IntStream.range(0, workflowCount).forEach(index -> {
        Workflow workflow = setup.create("workflow-" + index,
            List.of(job("first-" + index, JobRecord.JobType.STORE),
                job("second-" + index, JobRecord.JobType.STORE)));
        if (index % 3 != 0) setup.runOnce();
        if (index % 3 == 2) {
          setup.accept(stateChange("first-" + index, JobRecord.JobType.STORE,
              JobStateMachines.State.RUNNING, "job running"));
        }
      });
    }

    try (QueueManager recoveredQueues = new QueueManager();
        WorkflowManager recovered =
            new WorkflowManager(store, recoveredQueues, Duration.ofHours(1), failure -> {})) {
      recovered.start();

      for (int index = 0; index < workflowCount * 2; index++) {
        assertTrue(recoveredQueues.claim(JobRecord.JobType.STORE, "worker", Duration.ofMinutes(1),
            Instant.now()).isPresent());
      }
    }
  }

  @Test
  void submitsStepsSequentiallyAndCompletesWorkflow() {
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    try (QueueManager queues = new QueueManager();
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.create("ingest", List.of(job("store", JobRecord.JobType.STORE),
          job("index", JobRecord.JobType.INDEX)));

      manager.runOnce();
      assertTrue(queues.claim(JobRecord.JobType.INDEX, "worker", Duration.ofMinutes(1), Instant.now()).isEmpty());
      assertEquals("store", queues.claim(JobRecord.JobType.STORE, "worker", Duration.ofMinutes(1), Instant.now()).orElseThrow().id());

      manager.accept(stateChange("store", JobRecord.JobType.STORE, JobStateMachines.State.COMPLETED, "job completed"));
      manager.runOnce();
      assertEquals("index", queues.claim(JobRecord.JobType.INDEX, "index-worker", Duration.ofMinutes(1), Instant.now()).orElseThrow().id());
      manager.accept(stateChange("index", JobRecord.JobType.INDEX, JobStateMachines.State.COMPLETED, "job completed"));

      assertEquals(WorkflowStatus.COMPLETED, store.find(workflow.id()).orElseThrow().status());
    }
  }

  @Test
  void deadLetterCrashesWorkflowAndDoesNotSubmitSuccessor() {
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    try (QueueManager queues = new QueueManager();
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.create("ingest", List.of(job("store", JobRecord.JobType.STORE),
          job("index", JobRecord.JobType.INDEX)));
      manager.runOnce();
      manager.accept(stateChange("store", JobRecord.JobType.STORE, JobStateMachines.State.DEAD_LETTER, "upload failed"));
      manager.runOnce();

      Workflow failed = store.find(workflow.id()).orElseThrow();
      assertEquals(WorkflowStatus.CRASHED, failed.status());
      assertEquals("upload failed", failed.failureMessage());
      assertTrue(queues.claim(JobRecord.JobType.INDEX, "worker", Duration.ofMinutes(1), Instant.now()).isEmpty());
    }
  }

  private static JobRecord job(String id, JobRecord.JobType type) {
    return JobRecord.pending(id, "tenant", "artifact", type, Map.of());
  }

  private static Worker.StateChange stateChange(
      String jobId,
      JobRecord.JobType type,
      JobStateMachines.State state,
      String reason) {
    return new Worker.StateChange(
        "worker",
        jobId,
        type,
        new StateMachine.Transition<>(JobStateMachines.State.RUNNING, state, reason, Instant.now()));
  }
}
