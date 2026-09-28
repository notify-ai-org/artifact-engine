package dev.notify.artifact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.workflow.Workflow.WorkflowStatus;
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
  void startRecoversDispatchedStepsAndSubmitsOnlyReadyPendingSteps() {
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

      // Every workflow's first step is dispatched once (re-queued if it was already SUBMITTED or
      // RUNNING, submitted if still PENDING). Second steps wait for the first to complete.
      for (int index = 0; index < workflowCount; index++) {
        assertTrue(recoveredQueues.claim(JobRecord.JobType.STORE, "worker", Duration.ofMinutes(1),
            Instant.now()).orElseThrow().id().startsWith("first-"));
      }
      assertTrue(recoveredQueues.claim(JobRecord.JobType.STORE, "worker", Duration.ofMinutes(1),
          Instant.now()).isEmpty());
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

  @Test
  void submitsAWholeStageTogetherAndWaitsForAllOfItBeforeTheNext() {
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    try (QueueManager queues = new QueueManager();
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.createStaged("multipart", List.of(
          List.of(job("init", JobRecord.JobType.STORE_INIT)),
          List.of(job("part-1", JobRecord.JobType.STORE_PART),
              job("part-2", JobRecord.JobType.STORE_PART),
              job("part-3", JobRecord.JobType.STORE_PART)),
          List.of(job("complete", JobRecord.JobType.STORE_COMPLETE))), Map.of());

      manager.runOnce();
      assertEquals(List.of("init"), drain(queues, JobRecord.JobType.STORE_INIT));
      assertEquals(List.of(), drain(queues, JobRecord.JobType.STORE_PART));

      complete(manager, "init", JobRecord.JobType.STORE_INIT);
      manager.runOnce();
      assertEquals(List.of("part-1", "part-2", "part-3"), drain(queues, JobRecord.JobType.STORE_PART));

      complete(manager, "part-1", JobRecord.JobType.STORE_PART);
      complete(manager, "part-3", JobRecord.JobType.STORE_PART);
      manager.runOnce();
      assertEquals(List.of(), drain(queues, JobRecord.JobType.STORE_COMPLETE));

      complete(manager, "part-2", JobRecord.JobType.STORE_PART);
      manager.runOnce();
      assertEquals(List.of("complete"), drain(queues, JobRecord.JobType.STORE_COMPLETE));
      complete(manager, "complete", JobRecord.JobType.STORE_COMPLETE);

      Workflow done = store.find(workflow.id()).orElseThrow();
      assertEquals(WorkflowStatus.COMPLETED, done.status());
      assertEquals(List.of(0, 1, 1, 1, 2),
          done.workflowSteps().stream().map(WorkflowStep::stage).toList());
    }
  }

  @Test
  void aCrashedWorkflowStaysCrashedWhenParallelSiblingsFinishAndNotifiesOnce() {
    InMemoryWorkflowStore store = new InMemoryWorkflowStore();
    List<String> crashed = new java.util.ArrayList<>();
    try (QueueManager queues = new QueueManager();
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      manager.addCrashListener(workflow -> crashed.add(workflow.id()));
      Workflow workflow = manager.createStaged("multipart", List.of(
          List.of(job("part-1", JobRecord.JobType.STORE_PART),
              job("part-2", JobRecord.JobType.STORE_PART)),
          List.of(job("complete", JobRecord.JobType.STORE_COMPLETE))), Map.of());
      manager.runOnce();

      manager.accept(stateChange("part-1", JobRecord.JobType.STORE_PART,
          JobStateMachines.State.DEAD_LETTER, "part failed"));
      complete(manager, "part-2", JobRecord.JobType.STORE_PART);
      manager.accept(stateChange("part-1", JobRecord.JobType.STORE_PART,
          JobStateMachines.State.DEAD_LETTER, "part failed again"));
      manager.runOnce();

      Workflow failed = store.find(workflow.id()).orElseThrow();
      assertEquals(WorkflowStatus.CRASHED, failed.status());
      assertEquals(WorkflowStep.WorkflowStepStatus.COMPLETED, failed.workflowSteps().get(1).status());
      assertEquals(List.of(workflow.id()), crashed);
      assertEquals(List.of(), drain(queues, JobRecord.JobType.STORE_COMPLETE));
    }
  }

  @Test
  void aStepThatFinishesBeforeItIsMarkedSubmittedKeepsItsNewerState() {
    RacingStore store = new RacingStore(new InMemoryWorkflowStore());
    try (QueueManager queues = new QueueManager();
        WorkflowManager manager =
            new WorkflowManager(store, queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.create("ingest", List.of(job("store", JobRecord.JobType.STORE)));
      // A fast worker finishes after the enqueue but before the manager writes SUBMITTED.
      store.beforeNextUpdate = () -> complete(manager, "store", JobRecord.JobType.STORE);

      manager.runOnce();

      Workflow current = store.find(workflow.id()).orElseThrow();
      assertEquals(WorkflowStep.WorkflowStepStatus.COMPLETED, current.workflowSteps().get(0).status());
      assertEquals(WorkflowStatus.COMPLETED, current.status());
    }
  }

  /** Runs a hook just before the next update, to interleave a worker report deterministically. */
  private static final class RacingStore implements WorkflowStore {
    private final WorkflowStore delegate;
    private Runnable beforeNextUpdate;

    private RacingStore(WorkflowStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public Workflow update(String workflowId, java.util.function.UnaryOperator<Workflow> update) {
      Runnable hook = beforeNextUpdate;
      beforeNextUpdate = null;
      if (hook != null) hook.run();
      return delegate.update(workflowId, update);
    }

    @Override
    public Workflow create(Workflow workflow) {
      return delegate.create(workflow);
    }

    @Override
    public java.util.Optional<Workflow> find(String workflowId) {
      return delegate.find(workflowId);
    }

    @Override
    public java.util.Optional<Workflow> findByJobRecordId(String jobRecordId) {
      return delegate.findByJobRecordId(jobRecordId);
    }

    @Override
    public List<Workflow> recoverable() {
      return delegate.recoverable();
    }

    @Override
    public List<Workflow> incomplete(int limit) {
      return delegate.incomplete(limit);
    }

    @Override
    public List<Workflow> retryCandidates(Instant now, int limit) {
      return delegate.retryCandidates(now, limit);
    }
  }

  @Test
  void logsTheWorkflowLifecycleAsStructuredEvents() {
    java.util.logging.Logger julLogger =
        java.util.logging.Logger.getLogger(WorkflowManager.class.getName());
    List<String> lines = new java.util.concurrent.CopyOnWriteArrayList<>();
    java.util.logging.Handler capture = new java.util.logging.Handler() {
      @Override
      public void publish(java.util.logging.LogRecord record) {
        lines.add(record.getMessage());
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    julLogger.addHandler(capture);
    try (QueueManager queues = new QueueManager();
        WorkflowManager manager = new WorkflowManager(
            new InMemoryWorkflowStore(), queues, Duration.ofSeconds(1), failure -> {})) {
      Workflow workflow = manager.createStaged("multipart", List.of(
          List.of(job("part-1", JobRecord.JobType.STORE_PART),
              job("part-2", JobRecord.JobType.STORE_PART)),
          List.of(job("complete", JobRecord.JobType.STORE_COMPLETE))),
          Map.of("artifactId", "artifact-7"));
      manager.runOnce();
      complete(manager, "part-1", JobRecord.JobType.STORE_PART);
      manager.accept(stateChange("part-2", JobRecord.JobType.STORE_PART,
          JobStateMachines.State.DEAD_LETTER, "S3 unavailable"));

      assertTrue(lines.stream().anyMatch(line -> line.startsWith("event=workflow_created ")
          && line.contains("workflow=" + workflow.id())
          && line.contains("artifact=artifact-7")
          && line.contains("plan=STORE_PARTx2,STORE_COMPLETE")), lines::toString);
      assertTrue(lines.stream().anyMatch(line -> line.startsWith("event=stage_submitted ")
          && line.contains("stage=0") && line.contains("steps=2")), lines::toString);
      assertTrue(lines.stream().anyMatch(line -> line.startsWith("event=step_dead_lettered ")
          && line.contains("job=part-2") && line.contains("reason=\"S3 unavailable\"")),
          lines::toString);
      assertTrue(lines.stream().anyMatch(line -> line.startsWith("event=workflow_crashed ")
          && line.contains("completedSteps=1/3")), lines::toString);
    } finally {
      julLogger.removeHandler(capture);
    }
  }

  private static void complete(WorkflowManager manager, String jobId, JobRecord.JobType type) {
    manager.accept(stateChange(jobId, type, JobStateMachines.State.COMPLETED, "job completed"));
  }

  private static List<String> drain(QueueManager queues, JobRecord.JobType type) {
    List<String> ids = new java.util.ArrayList<>();
    for (var claimed = queues.claim(type, "worker", Duration.ofMinutes(1), Instant.now());
        claimed.isPresent();
        claimed = queues.claim(type, "worker", Duration.ofMinutes(1), Instant.now())) {
      ids.add(claimed.get().id());
    }
    java.util.Collections.sort(ids);
    return ids;
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
