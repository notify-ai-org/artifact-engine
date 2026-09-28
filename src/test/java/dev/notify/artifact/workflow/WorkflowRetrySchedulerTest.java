package dev.notify.artifact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.dlq.DeadLetter;
import dev.notify.artifact.dlq.DeadLetterHandler;
import dev.notify.artifact.dlq.InMemoryDeadLetterQueue;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.worker.JobStateMachines;
import dev.notify.artifact.worker.StateMachine;
import dev.notify.artifact.worker.Worker;
import dev.notify.artifact.workflow.Workflow.WorkflowStatus;
import dev.notify.artifact.workflow.WorkflowStep.WorkflowStepStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowRetrySchedulerTest {
  private static final Duration FIRST_DELAY = Duration.ofMinutes(1);

  private final MutableClock clock = new MutableClock(Instant.now());
  private final List<DeadLetter> handled = new ArrayList<>();
  private final List<Throwable> failures = new ArrayList<>();
  private InMemoryWorkflowStore store;
  private QueueManager queues;
  private WorkflowManager manager;
  private InMemoryDeadLetterQueue deadLetters;

  @BeforeEach
  void setUp() {
    store = new InMemoryWorkflowStore();
    queues = new QueueManager();
    manager = new WorkflowManager(store, queues, Duration.ofHours(1), failures::add);
    deadLetters = new InMemoryDeadLetterQueue();
  }

  @AfterEach
  void tearDown() {
    manager.close();
    queues.close();
  }

  @Test
  void resumesFromTheFailedStepAfterTheBackoffWithRetryPriority() {
    Workflow workflow = storeIndexRelease();
    runToCrash(workflow, "index");
    WorkflowRetryScheduler scheduler = scheduler(policy(3, 0));

    WorkflowRetryScheduler.Report planned = scheduler.runOnce();
    Workflow crashed = store.find(workflow.id()).orElseThrow();
    assertEquals(new WorkflowRetryScheduler.Report(1, 0, 0), planned);
    assertEquals(crashed.processEndAt().plus(FIRST_DELAY), crashed.nextRetryAt());

    clock.advance(FIRST_DELAY.plusSeconds(1));
    assertEquals(new WorkflowRetryScheduler.Report(0, 1, 0), scheduler.runOnce());

    Workflow resumed = store.find(workflow.id()).orElseThrow();
    assertEquals(WorkflowStatus.RUNNING, resumed.status());
    assertEquals(1, resumed.retryAttempts());
    assertNull(resumed.nextRetryAt());
    WorkflowStep storeStep = resumed.workflowSteps().get(0);
    WorkflowStep indexStep = resumed.workflowSteps().get(1);
    assertEquals(WorkflowStepStatus.COMPLETED, storeStep.status());
    assertEquals("store", storeStep.jobRecordId(), "completed steps are not re-run");
    assertEquals(WorkflowStepStatus.PENDING, indexStep.status());
    assertNotEquals("index", indexStep.jobRecordId(), "a retry runs under a fresh job id");
    assertEquals(JobRecord.Priority.RETRY, indexStep.jobRecord().priority());
    assertEquals(0, indexStep.jobRecord().attempts());

    manager.runOnce();
    JobRecord claimed = claim(JobRecord.JobType.INDEX).orElseThrow();
    assertEquals(indexStep.jobRecordId(), claimed.id());
    assertEquals(JobRecord.Priority.RETRY, claimed.priority());
  }

  @Test
  void backoffGrowsExponentiallyBetweenRetries() {
    Workflow workflow = storeIndexRelease();
    runToCrash(workflow, "index");
    WorkflowRetryScheduler scheduler = scheduler(policy(5, 0));
    scheduler.runOnce();
    clock.advance(FIRST_DELAY.plusSeconds(1));
    scheduler.runOnce();

    String retryJob = store.find(workflow.id()).orElseThrow().workflowSteps().get(1).jobRecordId();
    manager.runOnce();
    report(retryJob, JobRecord.JobType.INDEX, JobStateMachines.State.DEAD_LETTER);
    scheduler.runOnce();

    Workflow crashedAgain = store.find(workflow.id()).orElseThrow();
    assertEquals(
        FIRST_DELAY.multipliedBy(2),
        Duration.between(crashedAgain.processEndAt(), crashedAgain.nextRetryAt()));
  }

  @Test
  void jitterKeepsTheDelayWithinTheConfiguredBand() {
    for (int run = 0; run < 20; run++) {
      setUp();
      Workflow workflow = storeIndexRelease();
      runToCrash(workflow, "index");
      scheduler(policy(3, 0.5)).runOnce();

      Workflow crashed = store.find(workflow.id()).orElseThrow();
      Duration delay = Duration.between(crashed.processEndAt(), crashed.nextRetryAt());
      assertTrue(delay.compareTo(Duration.ofSeconds(30)) >= 0, delay::toString);
      assertTrue(delay.compareTo(Duration.ofSeconds(90)) <= 0, delay::toString);
      tearDown();
    }
    setUp();
  }

  @Test
  void deadLettersOnceRetriesAreExhaustedAndHandlesItExactlyOnce() {
    Workflow workflow = storeIndexRelease();
    runToCrash(workflow, "index");
    WorkflowRetryScheduler scheduler = scheduler(policy(2, 0));
    scheduler.runOnce();
    clock.advance(FIRST_DELAY.plusSeconds(1));
    scheduler.runOnce();
    String retryJob = store.find(workflow.id()).orElseThrow().workflowSteps().get(1).jobRecordId();
    manager.runOnce();
    report(retryJob, JobRecord.JobType.INDEX, JobStateMachines.State.DEAD_LETTER);

    assertEquals(new WorkflowRetryScheduler.Report(0, 0, 1), scheduler.runOnce());
    assertEquals(new WorkflowRetryScheduler.Report(0, 0, 0), scheduler.runOnce());

    assertEquals(WorkflowStatus.DEAD_LETTERED, store.find(workflow.id()).orElseThrow().status());
    DeadLetter letter = deadLetters.find(workflow.id()).orElseThrow();
    assertEquals(2, letter.attempts());
    assertEquals(List.of(JobRecord.JobType.INDEX),
        letter.failedSteps().stream().map(DeadLetter.FailedStep::jobType).toList());
    assertEquals(List.of(letter), handled);
  }

  @Test
  void retriesOnlyTheCrashedStepsOfAParallelStage() {
    Workflow workflow = manager.createStaged("multipart", List.of(
        List.of(job("part-1", JobRecord.JobType.STORE_PART),
            job("part-2", JobRecord.JobType.STORE_PART)),
        List.of(job("complete", JobRecord.JobType.STORE_COMPLETE))), Map.of());
    manager.runOnce();
    drain(JobRecord.JobType.STORE_PART);
    report("part-1", JobRecord.JobType.STORE_PART, JobStateMachines.State.COMPLETED);
    report("part-2", JobRecord.JobType.STORE_PART, JobStateMachines.State.DEAD_LETTER);
    WorkflowRetryScheduler scheduler = scheduler(policy(3, 0));
    scheduler.runOnce();
    clock.advance(FIRST_DELAY.plusSeconds(1));
    scheduler.runOnce();

    manager.runOnce();

    List<JobRecord> resubmitted = drain(JobRecord.JobType.STORE_PART);
    assertEquals(1, resubmitted.size());
    assertEquals(
        store.find(workflow.id()).orElseThrow().workflowSteps().get(1).jobRecordId(),
        resubmitted.get(0).id());
    assertTrue(claim(JobRecord.JobType.STORE_COMPLETE).isEmpty(), "the next stage still waits");
  }

  @Test
  void aSecondSchedulerWithAStaleViewDoesNotRetryTwice() {
    Workflow workflow = storeIndexRelease();
    runToCrash(workflow, "index");
    scheduler(policy(3, 0)).runOnce();
    clock.advance(FIRST_DELAY.plusSeconds(1));
    List<Workflow> staleView = store.retryCandidates(clock.instant(), 10);

    WorkflowRetryScheduler first = scheduler(policy(3, 0));
    WorkflowRetryScheduler second =
        new WorkflowRetryScheduler(new FixedCandidates(store, staleView), manager, policy(3, 0),
            TriggerSchedule.every(Duration.ofHours(1)), deadLetters, handled::add, clock, 10,
            failures::add);

    assertEquals(1, first.runOnce().retried());
    assertEquals(0, second.runOnce().retried());
    assertEquals(1, store.find(workflow.id()).orElseThrow().retryAttempts());
  }

  @Test
  void lateSiblingReportsDoNotReviveADeadLetteredWorkflow() {
    Workflow workflow = manager.createStaged("parallel", List.of(
        List.of(job("a", JobRecord.JobType.STORE_PART), job("b", JobRecord.JobType.STORE_PART))),
        Map.of());
    manager.runOnce();
    report("a", JobRecord.JobType.STORE_PART, JobStateMachines.State.DEAD_LETTER);
    scheduler(policy(1, 0)).runOnce();
    assertEquals(WorkflowStatus.DEAD_LETTERED, store.find(workflow.id()).orElseThrow().status());

    report("b", JobRecord.JobType.STORE_PART, JobStateMachines.State.COMPLETED);

    assertEquals(WorkflowStatus.DEAD_LETTERED, store.find(workflow.id()).orElseThrow().status());
  }

  @Test
  void aFailingHandlerIsReportedWithoutUndoingTheDeadLetter() {
    Workflow workflow = storeIndexRelease();
    runToCrash(workflow, "index");
    WorkflowRetryScheduler scheduler =
        new WorkflowRetryScheduler(store, manager, policy(1, 0),
            TriggerSchedule.every(Duration.ofHours(1)), deadLetters,
            letter -> { throw new IllegalStateException("alerting is down"); }, clock, 10,
            failures::add);

    assertEquals(1, scheduler.runOnce().deadLettered());

    assertEquals(WorkflowStatus.DEAD_LETTERED, store.find(workflow.id()).orElseThrow().status());
    assertEquals(1, deadLetters.size());
    assertEquals("alerting is down", failures.get(0).getMessage());
  }

  @Test
  void firesOnItsTriggerSchedule() throws Exception {
    CountDownLatch fired = new CountDownLatch(2);
    WorkflowStore counting = new FixedCandidates(store, List.of()) {
      @Override
      public List<Workflow> retryCandidates(Instant now, int limit) {
        fired.countDown();
        return List.of();
      }
    };
    try (WorkflowRetryScheduler scheduler =
        new WorkflowRetryScheduler(counting, manager, policy(3, 0),
            TriggerSchedule.every(Duration.ofMillis(20)), deadLetters, handled::add,
            Clock.systemUTC(), 10, failures::add)) {
      scheduler.start();
      assertTrue(fired.await(5, TimeUnit.SECONDS));
    }
  }

  // ---- helpers -------------------------------------------------------------------------------

  private Workflow storeIndexRelease() {
    return manager.create("ingest-store-index", List.of(
        job("store", JobRecord.JobType.STORE),
        job("index", JobRecord.JobType.INDEX),
        job("release", JobRecord.JobType.RELEASE_SPOOL)), Map.of());
  }

  /** Completes steps in order until {@code failingJob} dead-letters, claiming each like a worker. */
  private void runToCrash(Workflow workflow, String failingJob) {
    for (WorkflowStep step : workflow.workflowSteps()) {
      manager.runOnce();
      drain(step.jobRecord().type());
      boolean fails = step.jobRecordId().equals(failingJob);
      report(step.jobRecordId(), step.jobRecord().type(),
          fails ? JobStateMachines.State.DEAD_LETTER : JobStateMachines.State.COMPLETED);
      if (fails) break;
    }
    assertEquals(WorkflowStatus.CRASHED, store.find(workflow.id()).orElseThrow().status());
  }

  private WorkflowRetryScheduler scheduler(RetryPolicy policy) {
    return new WorkflowRetryScheduler(store, manager, policy,
        TriggerSchedule.every(Duration.ofHours(1)), deadLetters, recording(), clock, 10,
        failures::add);
  }

  private DeadLetterHandler recording() {
    return handled::add;
  }

  private static RetryPolicy policy(int maxAttempts, double jitter) {
    return new RetryPolicy(maxAttempts, FIRST_DELAY, Duration.ofHours(1), 2, jitter, t -> true);
  }

  private void report(String jobId, JobRecord.JobType type, JobStateMachines.State state) {
    manager.accept(new Worker.StateChange("worker", jobId, type,
        new StateMachine.Transition<>(JobStateMachines.State.RUNNING, state, state.name(),
            Instant.now())));
  }

  /** Claims at the scheduler's clock: retry records become ready at the time they were retried. */
  private Optional<JobRecord> claim(JobRecord.JobType type) {
    return queues.claim(type, "worker", Duration.ofMinutes(1), clock.instant().plusSeconds(1));
  }

  private List<JobRecord> drain(JobRecord.JobType type) {
    List<JobRecord> claimed = new ArrayList<>();
    for (var next = claim(type); next.isPresent(); next = claim(type)) claimed.add(next.get());
    return claimed;
  }

  private static JobRecord job(String id, JobRecord.JobType type) {
    return JobRecord.pending(id, "tenant", "artifact", type, Map.of());
  }

  private static final class MutableClock extends Clock {
    private Instant now;

    private MutableClock(Instant start) {
      this.now = start;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public java.time.ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }
  }

  /** Serves a fixed candidate list, as a scheduler instance that read before another acted. */
  private static class FixedCandidates implements WorkflowStore {
    private final WorkflowStore delegate;
    private final List<Workflow> candidates;

    FixedCandidates(WorkflowStore delegate, List<Workflow> candidates) {
      this.delegate = delegate;
      this.candidates = candidates;
    }

    @Override
    public List<Workflow> retryCandidates(Instant now, int limit) {
      return candidates;
    }

    @Override
    public Workflow create(Workflow workflow) {
      return delegate.create(workflow);
    }

    @Override
    public Optional<Workflow> find(String workflowId) {
      return delegate.find(workflowId);
    }

    @Override
    public Optional<Workflow> findByJobRecordId(String jobRecordId) {
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
    public Workflow update(String workflowId, UnaryOperator<Workflow> update) {
      return delegate.update(workflowId, update);
    }
  }
}
