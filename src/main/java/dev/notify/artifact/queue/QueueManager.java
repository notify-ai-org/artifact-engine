package dev.notify.artifact.queue;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Owns queue access, dispatches the transactional outbox, and recovers expired worker leases. */
public final class QueueManager implements AutoCloseable {
  private static final int DEFAULT_OUTBOX_BATCH = 100;

  private final JobQueue queue;
  private final MetadataStore metadataStore;
  private final Duration interval;
  private final Consumer<Throwable> failureHandler;
  private final ScheduledExecutorService maintenance;

  public QueueManager(JobQueue queue) {
    this(queue, null, Duration.ofSeconds(30), failure -> {});
  }

  public QueueManager(
      JobQueue queue,
      MetadataStore metadataStore,
      Duration interval,
      Consumer<Throwable> failureHandler) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.metadataStore = metadataStore;
    this.interval = Objects.requireNonNull(interval, "interval");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.maintenance =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-queue-maintenance");
              thread.setDaemon(true);
              return thread;
            });
  }

  public void start() {
    maintenance.scheduleWithFixedDelay(
        this::runMaintenanceSafely, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Visible for deterministic startup and tests; dispatch is safe to repeat by operation id. */
  public void runMaintenance() {
    queue.recoverExpired(Instant.now());
    if (metadataStore == null) {
      return;
    }
    List<JobRecord> pending = metadataStore.outboxBatch(DEFAULT_OUTBOX_BATCH);
    for (JobRecord operation : pending) {
      enqueue(operation);
      metadataStore.markOutboxDispatched(operation.id());
    }
  }

  /**
   * Enqueues a restart-safe operation in the queue initialized by this manager.
   *
   * <p>Queue implementations must make this idempotent by {@link JobRecord#id() operation id}.
   */
  public void enqueue(JobRecord operation) {
    queue.enqueue(Objects.requireNonNull(operation, "operation"));
  }

  public JobQueue queue() {
    return queue;
  }

  @Override
  public void close() {
    maintenance.shutdownNow();
  }

  private void runMaintenanceSafely() {
    try {
      runMaintenance();
    } catch (RuntimeException failure) {
      failureHandler.accept(failure);
    }
  }
}
