package dev.notify.artifact.worker;

import dev.notify.artifact.job.Job;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.JobQueue;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.workflow.JobUpdateListener;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Single-threaded bounded worker. Backpressure is explicit: submit returns false when full. */
public final class Worker implements AutoCloseable {
  private final String id;
  private final int queueCapacity;
  private final Buffer<Job<?>> buffer;
  private volatile JobRecord activeJob;
  private Thread workerThread;

  private final String owner = UUID.randomUUID().toString();
  private final JobRecord.JobType type;
  private final JobQueue queue;
  private final RetryPolicy retryPolicy;
  private final Duration leaseDuration;
  private final Duration idlePollInterval;
  private final Consumer<JobFailure> failureHandler;
  private final JobRecordExecutor jobExecutor;
  private final JobUpdateListener jobUpdateListener;
  private final AtomicBoolean running = new AtomicBoolean();
  private final ScheduledExecutorService heartbeat;
  private final java.util.Map<String, StateMachine<JobStateMachines.State>> stateMachines =
      new ConcurrentHashMap<>();
  private volatile Consumer<StateChange> stateChangeListener = ignored -> {};

  public Worker(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      JobRecord.JobType type,
      JobQueue queue,
      RetryPolicy retryPolicy,
      Duration leaseDuration,
      Duration idlePollInterval,
      Duration flushInterval,
      Consumer<JobFailure> failureHandler) {
    this(
        id, queueCapacity, batchSize, batchBytes, type, queue, retryPolicy, leaseDuration,
        idlePollInterval, flushInterval, failureHandler,
        ignored -> {}, (jobId, status, failure) -> {});
  }

  public Worker(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      JobRecord.JobType type,
      JobQueue queue,
      RetryPolicy retryPolicy,
      Duration leaseDuration,
      Duration idlePollInterval,
      Duration flushInterval,
      Consumer<JobFailure> failureHandler,
      JobRecordExecutor jobExecutor,
      JobUpdateListener jobUpdateListener) {
    this.id = id;
    this.queueCapacity = queueCapacity;
    this.buffer = new Buffer<>(batchSize, batchBytes, flushInterval);
    this.type = type;
    this.queue = queue;
    this.retryPolicy = retryPolicy;
    this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
    this.idlePollInterval = Objects.requireNonNull(idlePollInterval, "idlePollInterval");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.jobExecutor = Objects.requireNonNull(jobExecutor, "jobExecutor");
    this.jobUpdateListener = Objects.requireNonNull(jobUpdateListener, "jobUpdateListener");
    if (leaseDuration.isZero() || leaseDuration.isNegative()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    this.heartbeat =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-lease-heartbeat-" + type);
              thread.setDaemon(true);
              return thread;
            });
  }

  public synchronized void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    long heartbeatMillis = Math.max(100, leaseDuration.toMillis() / 3);
    heartbeat.scheduleWithFixedDelay(
        this::renewActiveLease, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
    workerThread = new Thread(this::run, "artifact-durable-worker-" + type + "-" + owner);
    workerThread.start();
  }

  private void run() {
    if (queue == null) {
      runProcessLocalWorker();
      return;
    }
    while (running.get()) {
      try {
        var claimed = queue.claim(type, owner, leaseDuration, Instant.now());
        if (claimed.isEmpty()) {
          Thread.sleep(idlePollInterval.toMillis());
          continue;
        }
        execute(claimed.get());
        if (buffer.shouldFlush()) {
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
    execute(buffer.drain());
  }

  private void runProcessLocalWorker() {
    while (running.get()) {
      try {
        Thread.sleep(idlePollInterval.toMillis());
      } catch (InterruptedException interrupted) {
        if (running.get()) Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void addToBuffer(Job<?> job) {
    if (!buffer.add(job, 1)) {
      execute(buffer.drain());
      if (!buffer.add(job, 1)) {
        failureHandler.accept(
            new JobFailure(job, new IllegalStateException("Job exceeds worker buffer capacity")));
      }
    }
  }

  private void execute(List<Job<?>> jobs) {
    for (Job<?> job : jobs) {
      try {
        job.execute();
      } catch (Exception failure) {
        failureHandler.accept(new JobFailure(job, failure));
      }
    }
  }

  private void execute(JobRecord job) {
    StateMachine<JobStateMachines.State> stateMachine = stateMachine(job.type());
    stateMachine.onTransition(
        transition ->
            stateChangeListener.accept(new StateChange(id, job.id(), job.type(), transition)));
    stateMachines.put(job.id(), stateMachine);
    activeJob = job;
    try {
      stateMachine.transition(JobStateMachines.State.VALIDATING, "job claimed");
      stateMachine.transition(JobStateMachines.State.BUFFERED, "job validated");
      stateMachine.transition(JobStateMachines.State.RUNNING, "job execution started");
      jobUpdateListener.onUpdate(job.id(), JobUpdateListener.JobUpdate.RUNNING, null);
      jobExecutor.execute(job);
      if (!queue.complete(job.id(), owner)) {
        failureHandler.accept(
            new JobFailure(
                null, new IllegalStateException("Completion rejected for job " + job.id())));
      } else {
        stateMachine.transition(JobStateMachines.State.COMPLETED, "job completed");
        jobUpdateListener.onUpdate(job.id(), JobUpdateListener.JobUpdate.COMPLETED, null);
      }
    } catch (Exception failure) {
      failureHandler.accept(new JobFailure(null, failure));
      if (job.attempts() >= retryPolicy.maxAttempts()) {
        queue.deadLetter(job.id(), owner, safeMessage(failure));
        stateMachine.transition(JobStateMachines.State.DEAD_LETTER, safeMessage(failure));
        jobUpdateListener.onUpdate(
            job.id(), JobUpdateListener.JobUpdate.DEAD_LETTER, safeMessage(failure));
      } else {
        Instant retryAt = Instant.now().plus(retryPolicy.delay(job.attempts()));
        queue.retry(job.id(), owner, retryAt, safeMessage(failure));
        stateMachine.transition(JobStateMachines.State.RETRY_PENDING, safeMessage(failure));
        jobUpdateListener.onUpdate(
            job.id(), JobUpdateListener.JobUpdate.RETRY_PENDING, safeMessage(failure));
      }
    } finally {
      activeJob = null;
    }
  }

  private void renewActiveLease() {
    JobRecord job = activeJob;
    if (job == null) {
      return;
    }
    try {
      if (!queue.renew(job.id(), owner, leaseDuration, Instant.now())) {
        failureHandler.accept(
            new JobFailure(
                null, new IllegalStateException("Lease renewal rejected for job " + job.id())));
      }
    } catch (RuntimeException failure) {
      failureHandler.accept(new JobFailure(null, failure));
    }
  }

  @Override
  public synchronized void close() {
    running.set(false);
    heartbeat.shutdownNow();
    if (workerThread != null) {
      workerThread.interrupt();
    }
  }

  private static String safeMessage(Exception failure) {
    String message = failure.getMessage();
    return message == null
        ? failure.getClass().getSimpleName()
        : message.substring(0, Math.min(500, message.length()));
  }

  public String id() {
    return id;
  }

  public int queued() {
    return 0;
  }

  public int capacity() {
    return queueCapacity;
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
