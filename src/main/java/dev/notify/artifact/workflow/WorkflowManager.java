package dev.notify.artifact.workflow;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Persists sequential workflows, submits ready steps, and resumes incomplete workflows. */
public final class WorkflowManager implements AutoCloseable, JobUpdateListener {
  private static final int RECOVERY_BATCH = 500;
  private final WorkflowStore store;
  private final QueueManager queues;
  private final Duration pollInterval;
  private final Consumer<Throwable> failureHandler;
  private final AtomicBoolean started = new AtomicBoolean();
  private final ScheduledExecutorService scheduler;

  public WorkflowManager(
      WorkflowStore store,
      QueueManager queues,
      Duration pollInterval,
      Consumer<Throwable> failureHandler) {
    this.store = Objects.requireNonNull(store, "store");
    this.queues = Objects.requireNonNull(queues, "queues");
    this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    if (pollInterval.isZero() || pollInterval.isNegative())
      throw new IllegalArgumentException("pollInterval must be positive");
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-workflow-manager");
              thread.setDaemon(true);
              return thread;
            });
  }

  public Workflow create(String name, List<JobRecord> jobs) {
    return create(name, jobs, Map.of());
  }

  public Workflow create(String name, List<JobRecord> jobs, Map<String, String> attributes) {
    if (name == null || name.isBlank()) throw new IllegalArgumentException("name is required");
    List<JobRecord> records = List.copyOf(jobs);
    if (records.isEmpty()) throw new IllegalArgumentException("workflow requires at least one job");
    String workflowId = UUID.randomUUID().toString();
    Instant now = Instant.now();
    List<String> ids = records.stream().map(ignored -> UUID.randomUUID().toString()).toList();
    List<WorkflowStep> steps = new ArrayList<>();
    for (int index = 0; index < records.size(); index++) {
      JobRecord job = records.get(index);
      steps.add(
          new WorkflowStep(
              ids.get(index), workflowId, now, now, job.id(), job, WorkflowStepStatus.PENDING,
              null, null, index == 0 ? null : ids.get(index - 1),
              index + 1 == ids.size() ? null : ids.get(index + 1), index, Map.of(), null));
    }
    Workflow workflow =
        new Workflow(
            workflowId, name, now, now, WorkflowStatus.PENDING, null, null, steps,
            attributes, null);
    Workflow persisted = store.create(workflow);
    wakeUp();
    return persisted;
  }

  public void start() {
    if (!started.compareAndSet(false, true)) return;
    scheduler.scheduleWithFixedDelay(
        this::safeDispatch, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void runOnce() {
    for (Workflow workflow : store.incomplete(RECOVERY_BATCH)) submitReadyStep(workflow);
  }

  @Override
  public void onUpdate(String jobRecordId, JobUpdate status, String failureMessage) {
    store.findByJobRecordId(jobRecordId)
        .ifPresent(workflow -> store.update(workflow.id(), current -> apply(current, jobRecordId, status, failureMessage)));
    wakeUp();
  }

  private void submitReadyStep(Workflow workflow) {
    WorkflowStep ready = workflow.workflowSteps().stream()
        .filter(step -> step.status() == WorkflowStepStatus.PENDING)
        .filter(step -> step.prevStepId() == null || completed(workflow, step.prevStepId()))
        .findFirst()
        .orElse(null);
    if (ready == null) return;
    queues.enqueue(ready.jobRecord());
    Instant now = Instant.now();
    store.update(workflow.id(), current -> replaceStep(current, ready.id(),
        step -> copyStep(step, WorkflowStepStatus.SUBMITTED, now, null, null),
        WorkflowStatus.RUNNING, current.processStartAt() == null ? now : current.processStartAt(), null));
  }

  private static boolean completed(Workflow workflow, String stepId) {
    return workflow.workflowSteps().stream()
        .anyMatch(step -> step.id().equals(stepId) && step.status() == WorkflowStepStatus.COMPLETED);
  }

  private static Workflow apply(
      Workflow workflow, String jobId, JobUpdate update, String failureMessage) {
    WorkflowStep target = workflow.workflowSteps().stream()
        .filter(step -> step.jobRecordId().equals(jobId)).findFirst().orElse(null);
    if (target == null) return workflow;
    Instant now = Instant.now();
    WorkflowStepStatus stepStatus = switch (update) {
      case RUNNING, RETRY_PENDING -> WorkflowStepStatus.RUNNING;
      case COMPLETED -> WorkflowStepStatus.COMPLETED;
      case DEAD_LETTER -> WorkflowStepStatus.CRASHED;
    };
    boolean crashed = update == JobUpdate.DEAD_LETTER;
    boolean completed = update == JobUpdate.COMPLETED
        && workflow.workflowSteps().stream()
            .allMatch(step -> step.id().equals(target.id()) || step.status() == WorkflowStepStatus.COMPLETED);
    return replaceStep(
        workflow,
        target.id(),
        step -> copyStep(step, stepStatus,
            step.processStartAt() == null ? now : step.processStartAt(),
            update == JobUpdate.COMPLETED || crashed ? now : null, failureMessage),
        crashed ? WorkflowStatus.CRASHED : completed ? WorkflowStatus.COMPLETED : WorkflowStatus.RUNNING,
        workflow.processStartAt() == null ? now : workflow.processStartAt(),
        crashed || completed ? now : null);
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
        startedAt, endedAt, steps, workflow.attributes(), failure);
  }

  private static WorkflowStep copyStep(
      WorkflowStep step, WorkflowStepStatus status, Instant startedAt, Instant endedAt, String failure) {
    return new WorkflowStep(step.id(), step.workflowId(), step.createdAt(), Instant.now(),
        step.jobRecordId(), step.jobRecord(), status, startedAt, endedAt, step.prevStepId(),
        step.nextStepId(), step.sequence(), step.attributes(), failure);
  }

  private void safeDispatch() {
    try {
      runOnce();
    } catch (Throwable failure) {
      failureHandler.accept(failure);
    }
  }

  private void wakeUp() {
    if (started.get()) scheduler.execute(this::safeDispatch);
  }

  @Override
  public void close() {
    started.set(false);
    scheduler.shutdownNow();
  }
}
