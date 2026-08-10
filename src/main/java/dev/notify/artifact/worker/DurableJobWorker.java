package dev.notify.artifact.worker;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.JobQueue;
import dev.notify.artifact.retry.RetryPolicy;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Claims leased durable operations, renews active leases, and schedules bounded retries. */
public final class DurableJobWorker implements AutoCloseable {
  private final String owner = UUID.randomUUID().toString();
  private final JobRecord.JobType type;
  private final JobQueue queue;
  private final Handler handler;
  private final RetryPolicy retryPolicy;
  private final Duration leaseDuration;
  private final Duration idlePollInterval;
  private final Consumer<Throwable> failureHandler;
  private final AtomicBoolean running = new AtomicBoolean();
  private final ScheduledExecutorService heartbeat;

  private volatile JobRecord activeJob;
  private Thread workerThread;

  public DurableJobWorker(
      JobRecord.JobType type,
      JobQueue queue,
      Handler handler,
      RetryPolicy retryPolicy,
      Duration leaseDuration,
      Duration idlePollInterval,
      Consumer<Throwable> failureHandler) {
    this.type = Objects.requireNonNull(type, "type");
    this.queue = Objects.requireNonNull(queue, "queue");
    this.handler = Objects.requireNonNull(handler, "handler");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
    this.idlePollInterval = Objects.requireNonNull(idlePollInterval, "idlePollInterval");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
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
    while (running.get()) {
      try {
        var claimed = queue.claim(type, owner, leaseDuration, Instant.now());
        if (claimed.isEmpty()) {
          Thread.sleep(idlePollInterval.toMillis());
          continue;
        }
        execute(claimed.get());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        break;
      } catch (RuntimeException infrastructureFailure) {
        failureHandler.accept(infrastructureFailure);
      }
    }
  }

  private void execute(JobRecord job) {
    activeJob = job;
    try {
      handler.execute(job);
      if (!queue.complete(job.id(), owner)) {
        failureHandler.accept(new IllegalStateException("Completion rejected for job " + job.id()));
      }
    } catch (Exception failure) {
      failureHandler.accept(failure);
      if (job.attempts() >= retryPolicy.maxAttempts()) {
        queue.deadLetter(job.id(), owner, safeMessage(failure));
      } else {
        Instant retryAt = Instant.now().plus(retryPolicy.delay(job.attempts()));
        queue.retry(job.id(), owner, retryAt, safeMessage(failure));
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
            new IllegalStateException("Lease renewal rejected for job " + job.id()));
      }
    } catch (RuntimeException failure) {
      failureHandler.accept(failure);
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

  @FunctionalInterface
  public interface Handler {
    void execute(JobRecord job) throws Exception;
  }
}
