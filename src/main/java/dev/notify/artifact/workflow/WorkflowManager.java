package dev.notify.artifact.workflow;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
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
    if (name == null || name.isBlank()) throw new IllegalArgumentException("name is required");
    List<List<JobRecord>> plan = List.copyOf(stages).stream().map(List::copyOf).toList();
    if (plan.isEmpty() || plan.stream().anyMatch(List::isEmpty)) {
      throw new IllegalArgumentException("workflow stages must be non-empty");
    }
    String workflowId = UUID.randomUUID().toString();
    Instant now = Instant.now();
    int total = plan.stream().mapToInt(List::size).sum();
    List<String> ids = new ArrayList<>(total);
    for (int index = 0; index < total; index++) ids.add(UUID.randomUUID().toString());
    List<WorkflowStep> steps = new ArrayList<>(total);
    int sequence = 0;
    for (int stage = 0; stage < plan.size(); stage++) {
      for (JobRecord job : plan.get(stage)) {
        steps.add(
            new WorkflowStep(
                ids.get(sequence), workflowId, now, now, job.id(), job, WorkflowStepStatus.PENDING,
                null, null, sequence == 0 ? null : ids.get(sequence - 1),
                sequence + 1 == total ? null : ids.get(sequence + 1), sequence, Map.of(), null,
                stage));
        sequence++;
      }
    }
    Workflow workflow =
        new Workflow(
            workflowId, name, now, now, WorkflowStatus.PENDING, null, null, steps,
            attributes, null);
    Workflow persisted = store.create(workflow);
    wakeUp();
    return persisted;
  }

  /** Called once for each workflow that transitions to {@link WorkflowStatus#CRASHED}. */
  public void addCrashListener(Consumer<Workflow> listener) {
    crashListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  public void start() {
    if (!started.compareAndSet(false, true)) return;
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
    store
        .findByJobRecordId(jobRecordId)
        .ifPresent(
            workflow -> {
              AtomicReference<WorkflowStatus> before = new AtomicReference<>();
              Workflow updated =
                  store.update(
                      workflow.id(),
                      current -> {
                        before.set(current.status());
                        return apply(current, jobRecordId, state, failureMessage);
                      });
              if (updated.status() == WorkflowStatus.CRASHED
                  && before.get() != WorkflowStatus.CRASHED) {
                notifyCrashed(updated);
              }
            });
    wakeUp();
  }

  private void submitReadySteps(Workflow workflow) {
    List<WorkflowStep> ready = workflow.workflowSteps().stream()
        .filter(step -> step.status() == WorkflowStepStatus.PENDING)
        .filter(step -> earlierStagesCompleted(workflow, step.stage()))
        .toList();
    if (ready.isEmpty()) return;
    // Enqueue first: a crash before the status write is repaired by recovery re-queuing
    // PENDING steps, and every job tolerates at-least-once delivery.
    ready.forEach(step -> queues.enqueue(step.jobRecord()));
    Set<String> readyIds = ready.stream().map(WorkflowStep::id).collect(Collectors.toSet());
    Instant now = Instant.now();
    store.update(workflow.id(), current -> markSubmitted(current, readyIds, now));
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

  private static boolean earlierStagesCompleted(Workflow workflow, int stage) {
    return workflow.workflowSteps().stream()
        .filter(step -> step.stage() < stage)
        .allMatch(step -> step.status() == WorkflowStepStatus.COMPLETED);
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
        step.nextStepId(), step.sequence(), step.attributes(), failure, step.stage());
  }

  private void notifyCrashed(Workflow workflow) {
    for (Consumer<Workflow> listener : crashListeners) {
      try {
        listener.accept(workflow);
      } catch (Throwable failure) {
        failureHandler.accept(failure);
      }
    }
  }

  private void safeDispatch() {
    try {
      runOnce();
    } catch (Throwable failure) {
      failureHandler.accept(failure);
    }
  }

  private void safeRecover() {
    try {
      for (Workflow workflow : store.recoverable()) dispatchPendingSteps(workflow);
    } catch (Throwable failure) {
      failureHandler.accept(failure);
    }
  }

  /**
   * Re-queues steps that were already dispatched before a restart, then submits PENDING steps
   * through the normal stage-aware path so a later stage never starts before an earlier one ends.
   */
  private void dispatchPendingSteps(Workflow workflow) {
    Instant now = Instant.now();
    for (WorkflowStep step : workflow.workflowSteps()) {
      if (step.status() == WorkflowStepStatus.SUBMITTED
          || step.status() == WorkflowStepStatus.RUNNING) {
        queues.requeue(step.jobRecord(), now);
      }
    }
    submitReadySteps(workflow);
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
    started.set(false);
    if (workerManager != null) {
      workerManager.removeStateChangeListener(this);
    }
    scheduler.shutdownNow();
  }
}
