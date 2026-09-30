package dev.notify.artifact.workflow;

import dev.notify.artifact.dlq.DeadLetter;
import dev.notify.artifact.dlq.DeadLetterHandler;
import dev.notify.artifact.dlq.DeadLetterQueue;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.workflow.Workflow.WorkflowStatus;
import dev.notify.artifact.workflow.WorkflowStep.WorkflowStepStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Periodically resumes CRASHED workflows from their failed steps, and dead-letters them once the
 * retry policy is exhausted.
 *
 * <p>Each run handles every CRASHED workflow that is due:
 *
 * <ol>
 *   <li><b>Plan.</b> A newly crashed workflow gets {@code nextRetryAt = failedAt +
 *       policy.delay(failedAttempts)} &mdash; the policy's exponential backoff with jitter.
 *   <li><b>Retry.</b> Once due, every CRASHED step gets a fresh {@link
 *       dev.notify.artifact.model.JobRecord.Priority#RETRY RETRY}-priority job record (new id, full
 *       attempt budget) and goes back to PENDING; completed steps are kept. The workflow manager
 *       then resubmits it through the normal stage-aware path, and queues serve it only after fresh
 *       work of the same type.
 *   <li><b>Dead-letter.</b> When {@code retryAttempts + 1 >= policy.maxAttempts()}, the workflow is
 *       pushed to the {@link DeadLetterQueue}, marked DEAD_LETTERED, and handed to the {@link
 *       DeadLetterHandler}.
 * </ol>
 *
 * <p>Every transition is a conditional update of the workflow, so several scheduler instances can
 * run against one store: exactly one wins each transition and the others skip it.
 */
public final class WorkflowRetryScheduler implements AutoCloseable {
  private static final System.Logger LOGGER = System.getLogger(WorkflowRetryScheduler.class.getName());

  private final WorkflowStore store;
  private final WorkflowManager manager;
  private final RetryPolicy policy;
  private final TriggerSchedule schedule;
  private final DeadLetterQueue deadLetters;
  private final DeadLetterHandler deadLetterHandler;
  private final Clock clock;
  private final int batchSize;
  private final Consumer<Throwable> failureHandler;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();

  public WorkflowRetryScheduler(
      WorkflowStore store,
      WorkflowManager manager,
      RetryPolicy policy,
      TriggerSchedule schedule,
      DeadLetterQueue deadLetters,
      DeadLetterHandler deadLetterHandler,
      Clock clock,
      int batchSize,
      Consumer<Throwable> failureHandler) {
    this.store = Objects.requireNonNull(store, "store");
    this.manager = manager;
    this.policy = Objects.requireNonNull(policy, "policy");
    this.schedule = Objects.requireNonNull(schedule, "schedule");
    this.deadLetters = Objects.requireNonNull(deadLetters, "deadLetters");
    this.deadLetterHandler = Objects.requireNonNull(deadLetterHandler, "deadLetterHandler");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    if (batchSize < 1) throw new IllegalArgumentException("batchSize must be positive");
    this.batchSize = batchSize;
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-workflow-retry");
              thread.setDaemon(true);
              return thread;
            });
  }

  public void start() {
    if (started.compareAndSet(false, true)) scheduleNext();
  }

  /** Handles every due CRASHED workflow once. Safe to call concurrently with other instances. */
  public Report runOnce() {
    Instant now = clock.instant();
    int planned = 0;
    int retried = 0;
    int deadLettered = 0;
    List<Workflow> candidates = store.retryCandidates(now, batchSize);
    for (Workflow workflow : candidates) {
      try {
        int failedAttempts = workflow.retryAttempts() + 1;
        if (failedAttempts >= policy.maxAttempts()) {
          if (deadLetter(workflow, now)) deadLettered++;
          continue;
        }
        Workflow current = workflow;
        if (current.nextRetryAt() == null) {
          current = plan(workflow, failedAttempts, now);
          if (current == null) continue;
          planned++;
        }
        if (!current.nextRetryAt().isAfter(now) && retry(current, now)) retried++;
      } catch (RuntimeException failure) {
        failureHandler.accept(failure);
      }
    }
    if (retried > 0 && manager != null) manager.dispatchNow();
    return new Report(planned, retried, deadLettered);
  }

  /** Sets when a newly crashed workflow is retried. Returns null if another instance got there. */
  private Workflow plan(Workflow workflow, int failedAttempts, Instant now) {
    Instant failedAt = workflow.processEndAt() == null ? now : workflow.processEndAt();
    Instant retryAt = failedAt.plus(policy.delay(failedAttempts));
    AtomicBoolean won = new AtomicBoolean();
    Workflow updated =
        store.update(
            workflow.id(),
            current -> {
              if (current.status() != WorkflowStatus.CRASHED || current.nextRetryAt() != null) {
                return current;
              }
              won.set(true);
              return copy(current, current.status(), current.workflowSteps(),
                  current.processEndAt(), current.failureMessage(), current.retryAttempts(),
                  retryAt);
            });
    return won.get() ? updated : null;
  }

  /** Resets the failed steps to PENDING under fresh retry-priority job records. */
  private boolean retry(Workflow workflow, Instant now) {
    AtomicBoolean won = new AtomicBoolean();
    store.update(
        workflow.id(),
        current -> {
          if (current.status() != WorkflowStatus.CRASHED
              || current.nextRetryAt() == null
              || current.nextRetryAt().isAfter(now)) {
            return current;
          }
          won.set(true);
          int attempt = current.retryAttempts() + 1;
          List<WorkflowStep> steps =
              current.workflowSteps().stream()
                  .map(step -> step.status() == WorkflowStepStatus.CRASHED
                      ? resetForRetry(step, attempt, now)
                      : step)
                  .toList();
          return copy(current, WorkflowStatus.RUNNING, steps, null, null, attempt, null);
        });
    if (won.get()) {
      LOGGER.log(System.Logger.Level.INFO, "workflow={0} event=retried attempt={1}",
          workflow.id(), workflow.retryAttempts() + 1);
    }
    return won.get();
  }

  /**
   * Pushes to the DLQ first (idempotent), then marks the workflow. If the process dies in between,
   * the next run pushes again and completes the transition; the handler runs once, for the winner.
   */
  private boolean deadLetter(Workflow workflow, Instant now) {
    DeadLetter letter = deadLetters.push(DeadLetter.of(workflow, now));
    AtomicBoolean won = new AtomicBoolean();
    store.update(
        workflow.id(),
        current -> {
          if (current.status() != WorkflowStatus.CRASHED) return current;
          won.set(true);
          return copy(current, WorkflowStatus.DEAD_LETTERED, current.workflowSteps(),
              current.processEndAt(), current.failureMessage(), current.retryAttempts(), null);
        });
    if (!won.get()) return false;
    LOGGER.log(System.Logger.Level.WARNING, "workflow={0} event=dead_lettered attempts={1}",
        workflow.id(), letter.attempts());
    try {
      deadLetterHandler.handle(letter);
    } catch (Exception failure) {
      failureHandler.accept(failure);
    }
    return true;
  }

  private static WorkflowStep resetForRetry(WorkflowStep step, int attempt, Instant now) {
    String retryJobId = Checksum.sha256(step.id() + ":retry:" + attempt);
    return new WorkflowStep(step.id(), step.workflowId(), step.createdAt(), now, retryJobId,
        step.jobRecord().retryAs(retryJobId, now), WorkflowStepStatus.PENDING, null, null,
        step.prevStepId(), step.nextStepId(), step.sequence(), step.attributes(), null,
        step.stage(), step.dependsOn());
  }

  private static Workflow copy(
      Workflow workflow,
      WorkflowStatus status,
      List<WorkflowStep> steps,
      Instant processEndAt,
      String failureMessage,
      int retryAttempts,
      Instant nextRetryAt) {
    return new Workflow(workflow.id(), workflow.name(), workflow.createdAt(), Instant.now(), status,
        workflow.processStartAt(), processEndAt, steps, workflow.attributes(), failureMessage,
        retryAttempts, nextRetryAt);
  }

  private void scheduleNext() {
    if (!started.get()) return;
    Instant now = clock.instant();
    Instant next = schedule.next(now);
    if (next == null) return;
    long delay = Math.max(0, Duration.between(now, next).toMillis());
    executor.schedule(
        () -> {
          try {
            runOnce();
          } catch (Throwable failure) {
            failureHandler.accept(failure);
          } finally {
            scheduleNext();
          }
        },
        delay,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    started.set(false);
    executor.shutdownNow();
  }

  /** What one run did. */
  public record Report(int planned, int retried, int deadLettered) {}
}
