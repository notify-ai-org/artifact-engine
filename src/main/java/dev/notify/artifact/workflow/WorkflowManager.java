package dev.notify.artifact.workflow;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.util.StructuredLog;
import dev.notify.artifact.worker.JobStateMachines;
import dev.notify.artifact.worker.Worker;
import dev.notify.artifact.worker.WorkerManager;
import dev.notify.artifact.workflow.Workflow.WorkflowStatus;
import dev.notify.artifact.workflow.WorkflowStep.WorkflowStepStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.function.Consumer;

/**
 * Persists staged workflows, submits ready steps, and resumes incomplete workflows.
 *
 * <p>Steps are grouped into stages. Every step of a stage is submitted together, so they run in
 * parallel; the next stage starts once all of them have completed. A workflow crashes when any
 * step dead-letters, and stays crashed even if parallel siblings complete afterwards.
 */
public final class WorkflowManager implements AutoCloseable, Consumer<Worker.StateChange> {
  private static final StructuredLog LOG = StructuredLog.of(WorkflowManager.class);
  private static final int RECOVERY_BATCH = 500;
  private final WorkflowStore store;
  private final QueueManager queues;
  private final Duration pollInterval;
  private final Consumer<Throwable> failureHandler;
  private final WorkerManager workerManager;
  private final AtomicBoolean started = new AtomicBoolean();
  private final ScheduledExecutorService scheduler;
  private final List<Consumer<Workflow>> crashListeners = new CopyOnWriteArrayList<>();

  public WorkflowManager(
      WorkflowStore store,
      QueueManager queues,
      Duration pollInterval,
      Consumer<Throwable> failureHandler) {
    this(store, queues, pollInterval, failureHandler, null);
  }

  public WorkflowManager(
      WorkflowStore store,
      QueueManager queues,
      Duration pollInterval,
      Consumer<Throwable> failureHandler,
      WorkerManager workerManager) {
    this.store = Objects.requireNonNull(store, "store");
    this.queues = Objects.requireNonNull(queues, "queues");
    this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.workerManager = workerManager;
    if (pollInterval.isZero() || pollInterval.isNegative())
      throw new IllegalArgumentException("pollInterval must be positive");
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-workflow-manager");
              thread.setDaemon(true);
              return thread;
            });
    if (workerManager != null) {
      workerManager.addStateChangeListener(this);
    }
  }

  public Workflow create(String name, List<JobRecord> jobs) {
    return create(name, jobs, Map.of());
  }

  /** Creates a linear workflow: each job runs after the previous one completes. */
  public Workflow create(String name, List<JobRecord> jobs, Map<String, String> attributes) {
    return createStaged(
        name, List.copyOf(jobs).stream().map(List::of).toList(), attributes);
  }

  /**
   * Creates a staged workflow. Jobs within a stage run in parallel; each stage starts once every
   * job of the previous stage has completed.
   */
  public Workflow createStaged(
      String name, List<List<JobRecord>> stages, Map<String, String> attributes) {
    return createPlan(
        name,
        List.copyOf(stages).stream()
            .map(stage -> stage.stream().map(PlannedStep::independent).toList())
            .toList(),
        attributes);
  }

  /**
   * A job and the jobs of the same stage it waits for.
   *
   * @param dependsOnJobIds ids of jobs listed earlier in the same stage
   */
  public record PlannedStep(JobRecord job, List<String> dependsOnJobIds) {
    public PlannedStep {
      Objects.requireNonNull(job, "job");
      dependsOnJobIds = dependsOnJobIds == null ? List.of() : List.copyOf(dependsOnJobIds);
    }

    public static PlannedStep independent(JobRecord job) {
      return new PlannedStep(job, List.of());
    }

    public static PlannedStep after(JobRecord job, JobRecord... dependencies) {
      return new PlannedStep(
          job, java.util.Arrays.stream(dependencies).map(JobRecord::id).toList());
    }
  }

  /**
   * Creates a workflow whose stages run in order, with dependencies between steps inside a stage.
   * A dependency must name a job listed earlier in the same stage, which also rules out cycles.
   */
  public Workflow createPlan(
      String name, List<List<PlannedStep>> stages, Map<String, String> attributes) {
    if (name == null || name.isBlank()) throw new IllegalArgumentException("name is required");
    List<List<PlannedStep>> planned = List.copyOf(stages).stream().map(List::copyOf).toList();
    if (planned.isEmpty() || planned.stream().anyMatch(List::isEmpty)) {
      throw new IllegalArgumentException("workflow stages must be non-empty");
    }
    List<List<JobRecord>> plan =
        planned.stream().map(stage -> stage.stream().map(PlannedStep::job).toList()).toList();
    String workflowId = UUID.randomUUID().toString();
    Instant now = Instant.now();
    int total = plan.stream().mapToInt(List::size).sum();
    List<String> ids = new ArrayList<>(total);
    for (int index = 0; index < total; index++) ids.add(UUID.randomUUID().toString());
    List<WorkflowStep> steps = new ArrayList<>(total);
    int sequence = 0;
    for (int stage = 0; stage < planned.size(); stage++) {
      Map<String, String> stepIdsByJobId = new java.util.HashMap<>();
      for (PlannedStep plannedStep : planned.get(stage)) {
        JobRecord job = plannedStep.job();
        List<String> dependsOn = new ArrayList<>();
        for (String dependency : plannedStep.dependsOnJobIds()) {
          String stepId = stepIdsByJobId.get(dependency);
          if (stepId == null) {
            throw new IllegalArgumentException(
                "Job " + job.id() + " depends on " + dependency
                    + ", which is not listed earlier in stage " + stage);
          }
          dependsOn.add(stepId);
        }
        steps.add(
            new WorkflowStep(
                ids.get(sequence), workflowId, now, now, job.id(), job, WorkflowStepStatus.PENDING,
                null, null, sequence == 0 ? null : ids.get(sequence - 1),
                sequence + 1 == total ? null : ids.get(sequence + 1), sequence, Map.of(), null,
                stage, dependsOn));
        stepIdsByJobId.put(job.id(), ids.get(sequence));
        sequence++;
      }
    }
    Workflow workflow =
        new Workflow(
            workflowId, name, now, now, WorkflowStatus.PENDING, null, null, steps,
            attributes, null);
    Workflow persisted = store.create(workflow);
    LOG.info("workflow_created", "workflow", workflowId, "name", name,
        "artifact", attributes.get("artifactId"), "stages", plan.size(), "steps", total,
        "plan", describe(plan));
    wakeUp();
    return persisted;
  }

  /**
   * Stage shapes such as {@code STORE_INIT,STORE_PARTx80,STORE_COMPLETE,INDEX}; a stage mixing
   * job types lists each, as in {@code STORE_INIT+BUFFER_CHUNKx80+STORE_CHUNKx80}.
   */
  private static String describe(List<List<JobRecord>> plan) {
    return plan.stream().map(WorkflowManager::describeJobs).collect(Collectors.joining(","));
  }

  private static String describeJobs(List<JobRecord> jobs) {
    Map<JobRecord.JobType, Long> counts = jobs.stream().collect(
        Collectors.groupingBy(JobRecord::type, java.util.LinkedHashMap::new, Collectors.counting()));
    return counts.entrySet().stream()
        .map(entry -> entry.getValue() == 1
            ? entry.getKey().name()
            : entry.getKey().name() + "x" + entry.getValue())
        .collect(Collectors.joining("+"));
  }

  /** Called once for each workflow that transitions to {@link WorkflowStatus#CRASHED}. */
  public void addCrashListener(Consumer<Workflow> listener) {
    crashListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  public void start() {
    if (!started.compareAndSet(false, true)) return;
    LOG.info("workflow_manager_started", "pollInterval", pollInterval,
        "crashListeners", crashListeners.size());
    safeRecover();
    scheduler.scheduleWithFixedDelay(
        this::safeDispatch, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void runOnce() {
    for (Workflow workflow : store.incomplete(RECOVERY_BATCH)) submitReadySteps(workflow);
  }

  @Override
  public void accept(Worker.StateChange stateChange) {
    Objects.requireNonNull(stateChange, "stateChange");
    String jobRecordId = stateChange.jobId();
    JobStateMachines.State state = stateChange.transition().to();
    String failureMessage =
        state == JobStateMachines.State.DEAD_LETTER
                || state == JobStateMachines.State.RETRY_PENDING
            ? stateChange.transition().reason()
            : null;
    var owner = store.findByJobRecordId(jobRecordId);
    if (owner.isEmpty()) {
      LOG.debug("step_state_unmatched", "job", jobRecordId, "type", stateChange.jobType(),
          "state", state);
    }
    owner.ifPresent(
        workflow -> {
          AtomicReference<WorkflowStatus> before = new AtomicReference<>();
          Workflow updated =
              store.update(
                  workflow.id(),
                  current -> {
                    before.set(current.status());
                    return apply(current, jobRecordId, state, failureMessage);
                  });
          logTransition(updated, before.get(), stateChange, failureMessage);
          if (updated.status() == WorkflowStatus.CRASHED
              && before.get() != WorkflowStatus.CRASHED) {
            notifyCrashed(updated);
          }
        });
    wakeUp();
  }

  private static void logTransition(
      Workflow workflow,
      WorkflowStatus before,
      Worker.StateChange change,
      String failureMessage) {
    WorkflowStep step = workflow.workflowSteps().stream()
        .filter(candidate -> candidate.jobRecordId().equals(change.jobId()))
        .findFirst()
        .orElse(null);
    JobStateMachines.State state = change.transition().to();
    if (state == JobStateMachines.State.DEAD_LETTER) {
      LOG.warn("step_dead_lettered", "workflow", workflow.id(), "job", change.jobId(),
          "type", change.jobType(), "stage", step == null ? null : step.stage(),
          "worker", change.workerId(), "reason", failureMessage);
    } else if (state == JobStateMachines.State.RETRY_PENDING) {
      LOG.info("step_retry_scheduled", "workflow", workflow.id(), "job", change.jobId(),
          "type", change.jobType(), "worker", change.workerId(), "reason", failureMessage);
    } else if (LOG.debugEnabled()) {
      LOG.debug("step_state", "workflow", workflow.id(), "job", change.jobId(),
          "type", change.jobType(), "state", state, "step",
          step == null ? null : step.status(), "worker", change.workerId(),
          "completedSteps", completedSteps(workflow) + "/" + workflow.workflowSteps().size());
    }
    if (workflow.status() == before) return;
    Duration elapsed = workflow.processStartAt() == null || workflow.processEndAt() == null
        ? null
        : Duration.between(workflow.processStartAt(), workflow.processEndAt());
    if (workflow.status() == WorkflowStatus.COMPLETED) {
      LOG.info("workflow_completed", "workflow", workflow.id(), "name", workflow.name(),
          "artifact", workflow.attributes().get("artifactId"),
          "steps", workflow.workflowSteps().size(), "retries", workflow.retryAttempts(),
          "duration", elapsed);
    } else if (workflow.status() == WorkflowStatus.CRASHED) {
      LOG.warn("workflow_crashed", "workflow", workflow.id(), "name", workflow.name(),
          "artifact", workflow.attributes().get("artifactId"),
          "failedJob", change.jobId(), "type", change.jobType(),
          "completedSteps", completedSteps(workflow) + "/" + workflow.workflowSteps().size(),
          "retries", workflow.retryAttempts(), "duration", elapsed,
          "reason", workflow.failureMessage());
    }
  }

  private static long completedSteps(Workflow workflow) {
    return workflow.workflowSteps().stream()
        .filter(step -> step.status() == WorkflowStepStatus.COMPLETED)
        .count();
  }

  private void submitReadySteps(Workflow workflow) {
    // Computed once per pass: a chunked ingest puts thousands of steps in one stage.
    Set<String> completed = new java.util.HashSet<>();
    int firstIncompleteStage = Integer.MAX_VALUE;
    for (WorkflowStep step : workflow.workflowSteps()) {
      if (step.status() == WorkflowStepStatus.COMPLETED) {
        completed.add(step.id());
      } else {
        firstIncompleteStage = Math.min(firstIncompleteStage, step.stage());
      }
    }
    int openStage = firstIncompleteStage;
    List<WorkflowStep> ready = workflow.workflowSteps().stream()
        .filter(step -> step.status() == WorkflowStepStatus.PENDING)
        .filter(step -> step.stage() <= openStage)
        .filter(step -> completed.containsAll(step.dependsOn()))
        .toList();
    if (ready.isEmpty()) return;
    // Enqueue first: a crash before the status write is repaired by recovery re-queuing
    // PENDING steps, and every job tolerates at-least-once delivery.
    ready.forEach(step -> queues.enqueue(step.jobRecord()));
    Set<String> readyIds = ready.stream().map(WorkflowStep::id).collect(Collectors.toSet());
    Instant now = Instant.now();
    store.update(workflow.id(), current -> markSubmitted(current, readyIds, now));
    if (LOG.debugEnabled()) {
      for (WorkflowStep step : ready) {
        LOG.debug("step_submitted", "workflow", workflow.id(), "step", step.id(),
            "job", step.jobRecordId(), "type", step.jobRecord().type(), "stage", step.stage(),
            "priority", step.jobRecord().priority());
      }
    }
    LOG.info("stage_submitted", "workflow", workflow.id(), "name", workflow.name(),
        "stage", ready.get(0).stage(),
        "type", describeJobs(ready.stream().map(WorkflowStep::jobRecord).toList()),
        "steps", ready.size(), "priority", ready.get(0).jobRecord().priority(),
        "retry", workflow.retryAttempts());
  }

  /**
   * Only PENDING steps become SUBMITTED. A fast job may already have reported RUNNING or COMPLETED
   * between the enqueue and this write, and that newer state must not be overwritten.
   */
  private static Workflow markSubmitted(Workflow workflow, Set<String> stepIds, Instant now) {
    List<WorkflowStep> steps = workflow.workflowSteps().stream()
        .map(step -> stepIds.contains(step.id()) && step.status() == WorkflowStepStatus.PENDING
            ? copyStep(step, WorkflowStepStatus.SUBMITTED, now, null, null) : step).toList();
    WorkflowStatus status =
        workflow.status() == WorkflowStatus.PENDING ? WorkflowStatus.RUNNING : workflow.status();
    return new Workflow(workflow.id(), workflow.name(), workflow.createdAt(), Instant.now(), status,
        workflow.processStartAt() == null ? now : workflow.processStartAt(),
        workflow.processEndAt(), steps, workflow.attributes(), workflow.failureMessage(),
        workflow.retryAttempts(), workflow.nextRetryAt());
  }

  private static Workflow apply(
      Workflow workflow,
      String jobId,
      JobStateMachines.State state,
      String failureMessage) {
    WorkflowStep target = workflow.workflowSteps().stream()
        .filter(step -> step.jobRecordId().equals(jobId)).findFirst().orElse(null);
    if (target == null) return workflow;
    Instant now = Instant.now();
    WorkflowStepStatus stepStatus = switch (state) {
      case PENDING -> WorkflowStepStatus.PENDING;
      case VALIDATING, BUFFERED, RUNNING, RETRY_PENDING, CANCELLED ->
          WorkflowStepStatus.RUNNING;
      case COMPLETED -> WorkflowStepStatus.COMPLETED;
      case DEAD_LETTER -> WorkflowStepStatus.CRASHED;
    };
    boolean stepCrashed = state == JobStateMachines.State.DEAD_LETTER;
    // Parallel siblings keep reporting after a crash; their step rows update, but a crashed or
    // dead-lettered workflow keeps its status until the retry scheduler acts on it.
    boolean alreadyFailed = workflow.status() == WorkflowStatus.CRASHED
        || workflow.status() == WorkflowStatus.DEAD_LETTERED;
    boolean crashed = alreadyFailed || stepCrashed;
    boolean completed = !crashed
        && state == JobStateMachines.State.COMPLETED
        && workflow.workflowSteps().stream()
            .allMatch(step -> step.id().equals(target.id()) || step.status() == WorkflowStepStatus.COMPLETED);
    WorkflowStatus status = alreadyFailed
        ? workflow.status()
        : crashed ? WorkflowStatus.CRASHED : completed ? WorkflowStatus.COMPLETED : WorkflowStatus.RUNNING;
    return replaceStep(
        workflow,
        target.id(),
        step -> copyStep(step, stepStatus,
            step.processStartAt() == null ? now : step.processStartAt(),
            state == JobStateMachines.State.COMPLETED || stepCrashed ? now : null, failureMessage),
        status,
        workflow.processStartAt() == null ? now : workflow.processStartAt(),
        alreadyFailed ? workflow.processEndAt() : crashed || completed ? now : null);
  }

  private static Workflow replaceStep(
      Workflow workflow,
      String stepId,
      java.util.function.UnaryOperator<WorkflowStep> update,
      WorkflowStatus status,
      Instant startedAt,
      Instant endedAt) {
    List<WorkflowStep> steps = workflow.workflowSteps().stream()
        .map(step -> step.id().equals(stepId) ? update.apply(step) : step).toList();
    String failure = steps.stream().filter(step -> step.status() == WorkflowStepStatus.CRASHED)
        .map(WorkflowStep::failureMessage).filter(Objects::nonNull).findFirst().orElse(null);
    return new Workflow(workflow.id(), workflow.name(), workflow.createdAt(), Instant.now(), status,
        startedAt, endedAt, steps, workflow.attributes(), failure, workflow.retryAttempts(),
        workflow.nextRetryAt());
  }

  private static WorkflowStep copyStep(
      WorkflowStep step, WorkflowStepStatus status, Instant startedAt, Instant endedAt, String failure) {
    return new WorkflowStep(step.id(), step.workflowId(), step.createdAt(), Instant.now(),
        step.jobRecordId(), step.jobRecord(), status, startedAt, endedAt, step.prevStepId(),
        step.nextStepId(), step.sequence(), step.attributes(), failure, step.stage(),
        step.dependsOn());
  }

  private void notifyCrashed(Workflow workflow) {
    for (Consumer<Workflow> listener : crashListeners) {
      try {
        listener.accept(workflow);
      } catch (Throwable failure) {
        LOG.error("crash_listener_failed", failure, "workflow", workflow.id(),
            "listener", listener.getClass().getName());
        failureHandler.accept(failure);
      }
    }
  }

  private void safeDispatch() {
    try {
      runOnce();
    } catch (Throwable failure) {
      LOG.error("workflow_dispatch_failed", failure);
      failureHandler.accept(failure);
    }
  }

  private void safeRecover() {
    try {
      List<Workflow> recoverable = store.recoverable();
      int requeued = 0;
      for (Workflow workflow : recoverable) requeued += dispatchPendingSteps(workflow);
      LOG.info("workflow_recovery_completed", "workflows", recoverable.size(),
          "requeuedSteps", requeued);
    } catch (Throwable failure) {
      LOG.error("workflow_recovery_failed", failure);
      failureHandler.accept(failure);
    }
  }

  /**
   * Re-queues steps that were already dispatched before a restart, then submits PENDING steps
   * through the normal stage-aware path so a later stage never starts before an earlier one ends.
   *
   * @return how many dispatched steps were re-queued
   */
  private int dispatchPendingSteps(Workflow workflow) {
    Instant now = Instant.now();
    int requeued = 0;
    for (WorkflowStep step : workflow.workflowSteps()) {
      if (step.status() == WorkflowStepStatus.SUBMITTED
          || step.status() == WorkflowStepStatus.RUNNING) {
        queues.requeue(step.jobRecord(), now);
        requeued++;
        LOG.debug("step_requeued", "workflow", workflow.id(), "job", step.jobRecordId(),
            "type", step.jobRecord().type(), "status", step.status());
      }
    }
    submitReadySteps(workflow);
    return requeued;
  }

  /** Submits ready steps now instead of at the next poll, e.g. after a workflow was resumed. */
  public void dispatchNow() {
    wakeUp();
  }

  private void wakeUp() {
    if (started.get()) scheduler.execute(this::safeDispatch);
  }

  @Override
  public void close() {
    if (started.get()) LOG.info("workflow_manager_stopped");
    started.set(false);
    if (workerManager != null) {
      workerManager.removeStateChangeListener(this);
    }
    scheduler.shutdownNow();
  }
}
