package dev.notify.artifact.worker;

import dev.notify.artifact.job.Job;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.store.JobStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Single-threaded bounded worker. Backpressure is explicit: submit returns false when full. */
public final class Worker implements AutoCloseable {
  private static final System.Logger LOGGER = System.getLogger(Worker.class.getName());

  private final String id;
  private final Buffer<BufferedJob> buffer;

  private Thread workerThread;

  private final String owner = UUID.randomUUID().toString();
  private final JobRecord.JobType type;
  private final QueueManager queueManager;
  private final RetryPolicy retryPolicy;
  private final Duration leaseDuration;
  private final Duration idlePollInterval;
  private final Consumer<JobFailure> failureHandler;
  private final JobRecordExecutor jobExecutor;
  private final JobStore jobStore;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicBoolean batchRunning = new AtomicBoolean();
  private final ExecutorService jobExecutorService;
  private final ExecutorService batchWaiter;
  private volatile List<SubmittedJob> activeBatch = List.of();
  private final java.util.Map<String, StateMachine<JobStateMachines.State>> stateMachines =
      new ConcurrentHashMap<>();
  private volatile Consumer<StateChange> stateChangeListener = ignored -> {};

  public Worker(
      String id,
      int batchSize,
      long batchBytes,
      JobRecord.JobType type,
      QueueManager queueManager,
      RetryPolicy retryPolicy,
      Duration leaseDuration,
      Duration idlePollInterval,
      Duration flushInterval,
      Consumer<JobFailure> failureHandler) {
    this(
        id, batchSize, batchBytes, type, queueManager, retryPolicy, leaseDuration,
        idlePollInterval, flushInterval, failureHandler,
        ignored -> () -> null, null);
  }

  public Worker(
      String id,
      int batchSize,
      long batchBytes,
      JobRecord.JobType type,
      QueueManager queueManager,
      RetryPolicy retryPolicy,
      Duration leaseDuration,
      Duration idlePollInterval,
      Duration flushInterval,
      Consumer<JobFailure> failureHandler,
      JobRecordExecutor jobExecutor) {
    this(
        id, batchSize, batchBytes, type, queueManager, retryPolicy, leaseDuration,
        idlePollInterval, flushInterval, failureHandler, jobExecutor, null);
  }

  public Worker(
      String id,
      int batchSize,
      long batchBytes,
      JobRecord.JobType type,
      QueueManager queueManager,
      RetryPolicy retryPolicy,
      Duration leaseDuration,
      Duration idlePollInterval,
      Duration flushInterval,
      Consumer<JobFailure> failureHandler,
      JobRecordExecutor jobExecutor,
      JobStore jobStore) {
    this.id = id;
    this.buffer = new Buffer<>(batchSize, batchBytes, flushInterval);
    this.type = type;
    this.queueManager = Objects.requireNonNull(queueManager, "queueManager");
    this.retryPolicy = retryPolicy;
    this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
    this.idlePollInterval = Objects.requireNonNull(idlePollInterval, "idlePollInterval");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.jobExecutor = Objects.requireNonNull(jobExecutor, "jobExecutor");
    this.jobStore = jobStore;
    if (leaseDuration.isZero() || leaseDuration.isNegative()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    this.jobExecutorService =
        Executors.newCachedThreadPool(
            runnable -> daemonThread(runnable, "artifact-job-execution-" + id));
    this.batchWaiter =
        Executors.newSingleThreadExecutor(
            runnable -> daemonThread(runnable, "artifact-batch-waiter-" + id));
  }

  public synchronized void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    workerThread = new Thread(this::run, "artifact-durable-worker-" + type + "-" + owner);
    workerThread.setDaemon(true);
    workerThread.start();
    LOGGER.log(
        System.Logger.Level.INFO,
        "worker={0} event=started type={1} leaseMillis={2}",
        id,
        type,
        leaseDuration.toMillis());
  }

  private void run() {
    while (running.get()) {
      try {
        if (batchRunning.get()) {
          Thread.sleep(idlePollInterval.toMillis());
          continue;
        }
        Optional<JobRecord> claimed =
            queueManager.claim(type, owner, leaseDuration, Instant.now());
        if (claimed.isEmpty()) {
          if (buffer.shouldFlush()) {
            execute(buffer.drain());
            continue;
          }
          Thread.sleep(idlePollInterval.toMillis());
          continue;
        }
        saveClaimed(claimed.get());
        LOGGER.log(
            System.Logger.Level.DEBUG,
            "worker={0} event=claimed job={1} type={2} attempt={3}",
            id,
            claimed.get().id(),
            claimed.get().type(),
            claimed.get().attempts());
        StateMachine<JobStateMachines.State> stateMachine = stateMachine(claimed.get().type());
        stateMachine.onTransition(
            transition ->
                stateChangeListener.accept(
                    new StateChange(id, claimed.get().id(), claimed.get().type(), transition)));
        stateMachines.put(claimed.get().id(), stateMachine);
        addToBuffer(claimed.get(), stateMachine);
        if (buffer.shouldFlush() && !batchRunning.get()) {
          execute(buffer.drain());
        }
      } catch (InterruptedException interrupted) {
        if (running.get()) {
          Thread.currentThread().interrupt();
          break;
        }
      } catch (RuntimeException infrastructureFailure) {
        failureHandler.accept(new JobFailure(null, infrastructureFailure));
      }
    }
  }

  private void validate(JobRecord record) {
    if (record.status() != JobRecord.JobStatus.CLAIMED) {
      throw new IllegalStateException("Job is not claimed: " + record.id());
    }
    if (!owner.equals(record.leaseOwner())) {
      throw new IllegalStateException("Job lease is not owned by this worker: " + record.id());
    }
    if (record.leaseExpiresAt() == null) {
      throw new IllegalStateException("Job lease expiry is required: " + record.id());
    }
    if (record.type() != type) {
      throw new IllegalStateException("Job type does not match this worker: " + record.id());
    }
    if (record.id() == null || record.id().isBlank()) {
      throw new IllegalStateException("Job id is required");
    }
    if (record.tenantId() == null || record.tenantId().isBlank()) {
      throw new IllegalStateException("Job tenant is required: " + record.id());
    }
  }


  private void addToBuffer(JobRecord record, StateMachine<JobStateMachines.State> stateMachine) {
    BufferedJob bufferedJob =
        new BufferedJob(record, jobExecutor.toJob(record), stateMachine, new AtomicBoolean());
    if (!buffer.add(bufferedJob, 1)) {
      execute(buffer.drain());
    }
  }

  private void execute(List<BufferedJob> jobs) {
    if (jobs.isEmpty()) {
      return;
    }
    if (!batchRunning.compareAndSet(false, true)) {
      throw new IllegalStateException("Worker already has a batch in progress");
    }
    List<SubmittedJob> submitted =
        jobs.stream()
            .map(job -> new SubmittedJob(job, jobExecutorService.submit(() -> execute(job))))
            .toList();
    activeBatch = submitted;
    batchWaiter.execute(() -> awaitBatch(submitted));
  }

  private void execute(BufferedJob bufferedJob) {
    if (bufferedJob.timedOut().get()) {
      return;
    }
    JobRecord record = bufferedJob.record();
    StateMachine<JobStateMachines.State> stateMachine = bufferedJob.stateMachine();
    try {
      stateMachine.transition(JobStateMachines.State.VALIDATING, "job claimed");
      if (leaseExpired(record, Instant.now())) {
        updateJob(record, JobRecord.JobStatus.DEAD_LETTER, record.nextAttemptAt(), "Lease expired", false);
        stateMachine.transition(JobStateMachines.State.CANCELLED, "job lease expired");
        LOGGER.log(
            System.Logger.Level.WARNING,
            "worker={0} event=discarded job={1} reason=lease_expired",
            id,
            record.id());
        return;
      }
      validate(record);
      stateMachine.transition(JobStateMachines.State.BUFFERED, "job validated");
      updateJob(record, JobRecord.JobStatus.RUNNING, record.nextAttemptAt(), null, true);
      stateMachine.transition(JobStateMachines.State.RUNNING, "job execution started");
      LOGGER.log(
          System.Logger.Level.INFO,
          "worker={0} event=execution_started job={1} type={2}",
          id,
          record.id(),
          record.type());
      bufferedJob.job().execute();
      if (bufferedJob.timedOut().get()) {
        return;
      }
      if (!queueManager.complete(type, record.id(), owner)) {
        failureHandler.accept(
            new JobFailure(
                bufferedJob.job(),
                new IllegalStateException("Completion rejected for job " + record.id())));
      } else {
        updateJob(record, JobRecord.JobStatus.COMPLETED, null, null, false);
        stateMachine.transition(JobStateMachines.State.COMPLETED, "job completed");
        LOGGER.log(
            System.Logger.Level.INFO,
            "worker={0} event=completed job={1}",
            id,
            record.id());
      }
    } catch (Exception failure) {
      if (!bufferedJob.timedOut().get()) {
        handleFailure(bufferedJob, stateMachine, failure);
      }
    }
  }

  private void awaitBatch(List<SubmittedJob> submitted) {
    long deadline = System.nanoTime() + leaseDuration.toNanos();
    try {
      for (SubmittedJob job : submitted) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        try {
          job.future().get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException | CancellationException ignored) {
          // Per-job execution records its own failure outcome.
        } catch (TimeoutException timeout) {
          break;
        }
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } finally {
      for (SubmittedJob job : submitted) {
        if (!job.future().isDone()) {
          cancelTimedOut(job);
        }
      }
      activeBatch = List.of();
      batchRunning.set(false);
    }
  }

  private void cancelTimedOut(SubmittedJob submitted) {
    BufferedJob job = submitted.job();
    if (job.timedOut().compareAndSet(false, true)) {
      JobStateMachines.State state = job.stateMachine().state();
      if (state == JobStateMachines.State.PENDING
          || state == JobStateMachines.State.VALIDATING
          || state == JobStateMachines.State.BUFFERED
          || state == JobStateMachines.State.RUNNING) {
        job.stateMachine().transition(JobStateMachines.State.CANCELLED, "batch timed out");
      }
      LOGGER.log(
          System.Logger.Level.WARNING,
          "worker={0} event=execution_cancelled job={1} reason=batch_timeout",
          id,
          job.record().id());
    }
    submitted.future().cancel(true);
  }

  private void handleFailure(
      BufferedJob bufferedJob,
      StateMachine<JobStateMachines.State> stateMachine,
      Exception failure) {
    JobRecord record = bufferedJob.record();
    failureHandler.accept(new JobFailure(bufferedJob.job(), failure));
    if (record.attempts() >= retryPolicy.maxAttempts()) {
      String error = safeMessage(failure);
      if (queueManager.deadLetter(type, record.id(), owner, error)) {
        updateJob(record, JobRecord.JobStatus.DEAD_LETTER, null, error, false);
      }
      stateMachine.transition(JobStateMachines.State.DEAD_LETTER, error);
      LOGGER.log(
          System.Logger.Level.ERROR,
          "worker={0} event=dead_lettered job={1} error={2}",
          id,
          record.id(),
          safeMessage(failure));
    } else {
      Instant retryAt = Instant.now().plus(retryPolicy.delay(record.attempts()));
      String error = safeMessage(failure);
      if (queueManager.retry(type, record.id(), owner, retryAt, error)) {
        updateJob(record, JobRecord.JobStatus.RETRY_PENDING, retryAt, error, false);
      }
      stateMachine.transition(JobStateMachines.State.RETRY_PENDING, error);
      LOGGER.log(
          System.Logger.Level.WARNING,
          "worker={0} event=retry_scheduled job={1} retryAt={2} error={3}",
          id,
          record.id(),
          retryAt,
          safeMessage(failure));
    }
  }

  private void saveClaimed(JobRecord claimed) {
    if (jobStore != null) {
      jobStore.save(claimed);
    }
  }

  private void updateJob(
      JobRecord record,
      JobRecord.JobStatus status,
      Instant nextAttemptAt,
      String lastError,
      boolean retainLease) {
    if (jobStore == null) return;
    jobStore.update(
        record.id(),
        current ->
            new JobRecord(
                current.id(), current.tenantId(), current.artifactId(), current.type(), status,
                record.attempts(), nextAttemptAt,
                retainLease ? record.leaseOwner() : null,
                retainLease ? record.leaseExpiresAt() : null,
                current.attributes(), lastError, current.createdAt(), Instant.now()));
  }

  private static boolean leaseExpired(JobRecord record, Instant now) {
    return record.leaseExpiresAt() != null && !record.leaseExpiresAt().isAfter(now);
  }

  @Override
  public synchronized void close() {
    running.set(false);
    for (SubmittedJob job : activeBatch) {
      job.job().timedOut().set(true);
      job.future().cancel(true);
    }
    batchWaiter.shutdownNow();
    jobExecutorService.shutdownNow();
    if (workerThread != null) {
      workerThread.interrupt();
    }
    LOGGER.log(System.Logger.Level.INFO, "worker={0} event=stopped", id);
  }

  private static String safeMessage(Exception failure) {
    String message = failure.getMessage();
    return message == null
        ? failure.getClass().getSimpleName()
        : message.substring(0, Math.min(500, message.length()));
  }

  private record BufferedJob(
      JobRecord record,
      Job<?> job,
      StateMachine<JobStateMachines.State> stateMachine,
      AtomicBoolean timedOut) {}

  private record SubmittedJob(BufferedJob job, Future<?> future) {}

  private static Thread daemonThread(Runnable runnable, String name) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(true);
    return thread;
  }

  public String id() {
    return id;
  }


  public void onStateChange(Consumer<StateChange> listener) {
    stateChangeListener = Objects.requireNonNull(listener, "listener");
  }

  public java.util.Map<String, JobStateMachines.State> states() {
    java.util.Map<String, JobStateMachines.State> states = new java.util.HashMap<>();
    stateMachines.forEach((jobId, stateMachine) -> states.put(jobId, stateMachine.state()));
    return java.util.Map.copyOf(states);
  }

  private static StateMachine<JobStateMachines.State> stateMachine(JobRecord.JobType type) {
    return switch (type) {
      case INGEST, STORE -> new JobStateMachines.Ingest();
      case FETCH -> new JobStateMachines.Fetch();
      case INDEX -> new JobStateMachines.Index();
      case RETRIEVAL -> new JobStateMachines.Retrieval();
    };
  }

  public record JobFailure(Job<?> job, Exception cause) {}

  public record StateChange(
      String workerId,
      String jobId,
      JobRecord.JobType jobType,
      StateMachine.Transition<JobStateMachines.State> transition) {}
}
