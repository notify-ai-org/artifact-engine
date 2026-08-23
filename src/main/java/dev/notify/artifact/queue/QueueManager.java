package dev.notify.artifact.queue;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/** Owns tenant-isolated queues, dispatches the outbox, and recovers expired worker leases. */
public final class QueueManager implements AutoCloseable {
  private static final int DEFAULT_OUTBOX_BATCH = 100;

  private final Map<String, JobQueue> queues = new java.util.LinkedHashMap<>();
  private final ReadWriteLock registryLock = new ReentrantReadWriteLock();
  private final JobQueue initialQueue;
  private final Function<String, JobQueue> queueFactory;
  private final MetadataStore metadataStore;
  private final Duration interval;
  private final Consumer<Throwable> failureHandler;
  private final ScheduledExecutorService maintenance;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Creates a single-tenant manager. The supplied queue is atomically bound to the first tenant
   * enqueued or explicitly registered.
   */
  public QueueManager(JobQueue queue) {
    this(queue, null, null, Duration.ofSeconds(30), failure -> {});
  }

  public QueueManager(
      JobQueue queue,
      MetadataStore metadataStore,
      Duration interval,
      Consumer<Throwable> failureHandler) {
    this(queue, null, metadataStore, interval, failureHandler);
  }

  /** Creates a manager that can lazily create a distinct queue for every tenant. */
  public QueueManager(Function<String, JobQueue> queueFactory) {
    this(null, queueFactory, null, Duration.ofSeconds(30), failure -> {});
  }

  public QueueManager(
      Function<String, JobQueue> queueFactory,
      MetadataStore metadataStore,
      Duration interval,
      Consumer<Throwable> failureHandler) {
    this(null, queueFactory, metadataStore, interval, failureHandler);
  }

  private QueueManager(
      JobQueue initialQueue,
      Function<String, JobQueue> queueFactory,
      MetadataStore metadataStore,
      Duration interval,
      Consumer<Throwable> failureHandler) {
    if (initialQueue == null && queueFactory == null) {
      throw new IllegalArgumentException("queue or queueFactory is required");
    }
    this.initialQueue = initialQueue;
    this.queueFactory = queueFactory;
    this.metadataStore = metadataStore;
    this.interval = Objects.requireNonNull(interval, "interval");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException("interval must be positive");
    }
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.maintenance =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-queue-maintenance");
              thread.setDaemon(true);
              return thread;
            });
  }

  /** Starts one maintenance task. Repeated concurrent calls are harmless. */
  public synchronized void start() {
    ensureOpen();
    if (started.compareAndSet(false, true)) {
      maintenance.scheduleWithFixedDelay(
          this::runMaintenanceSafely, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  /** Adds a queue for exactly one tenant. Existing registrations are never overwritten. */
  public void addQueue(String tenantId, JobQueue queue) {
    String tenant = requireTenant(tenantId);
    Objects.requireNonNull(queue, "queue");
    registryLock.writeLock().lock();
    try {
      ensureOpen();
      if (queues.containsKey(tenant)) {
        throw new IllegalStateException("Queue already registered for tenant: " + tenant);
      }
      if (queues.containsValue(queue)) {
        throw new IllegalArgumentException("A queue instance cannot be shared by tenants");
      }
      if (initialQueue != null && queue != initialQueue && queues.isEmpty()) {
        throw new IllegalArgumentException("The single-tenant manager must use its supplied queue");
      }
      queues.put(tenant, queue);
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  /** Removes and returns a tenant queue after all in-flight manager operations have completed. */
  public Optional<JobQueue> removeQueue(String tenantId) {
    String tenant = requireTenant(tenantId);
    registryLock.writeLock().lock();
    try {
      ensureOpen();
      return Optional.ofNullable(queues.remove(tenant));
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  public Optional<JobQueue> queue(String tenantId) {
    String tenant = requireTenant(tenantId);
    registryLock.readLock().lock();
    try {
      return Optional.ofNullable(queues.get(tenant));
    } finally {
      registryLock.readLock().unlock();
    }
  }

  public Map<String, JobQueue> queues() {
    registryLock.readLock().lock();
    try {
      return Map.copyOf(queues);
    } finally {
      registryLock.readLock().unlock();
    }
  }

  /** Visible for deterministic startup and tests; dispatch is safe to repeat by operation id. */
  public void runMaintenance() {
    ensureOpen();
    recoverAllQueues();
    if (metadataStore == null) return;
    List<JobRecord> pending = metadataStore.outboxBatch(DEFAULT_OUTBOX_BATCH);
    for (JobRecord operation : pending) {
      enqueue(operation);
      metadataStore.markOutboxDispatched(operation.id());
    }
  }

  /** Routes an operation only to the queue registered for its tenant. */
  public void enqueue(JobRecord operation) {
    JobRecord job = Objects.requireNonNull(operation, "operation");
    String tenant = requireTenant(job.tenantId());
    ensureQueue(tenant);
    registryLock.readLock().lock();
    try {
      ensureOpen();
      JobQueue queue = queues.get(tenant);
      if (queue == null) throw missingQueue(tenant);
      queue.enqueue(job);
    } finally {
      registryLock.readLock().unlock();
    }
  }

  /** Legacy single-tenant accessor. */
  public JobQueue queue() {
    registryLock.readLock().lock();
    try {
      if (queues.size() == 1) return queues.values().iterator().next();
      if (queues.isEmpty() && initialQueue != null) return initialQueue;
      throw new IllegalStateException("Specify a tenant when multiple queues are configured");
    } finally {
      registryLock.readLock().unlock();
    }
  }

  @Override
  public synchronized void close() {
    registryLock.writeLock().lock();
    try {
      if (closed.compareAndSet(false, true)) maintenance.shutdownNow();
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  private void recoverAllQueues() {
    Instant now = Instant.now();
    registryLock.readLock().lock();
    try {
      ensureOpen();
      queues.values().forEach(queue -> queue.recoverExpired(now));
    } finally {
      registryLock.readLock().unlock();
    }
  }

  private void ensureQueue(String tenant) {
    registryLock.readLock().lock();
    try {
      if (queues.containsKey(tenant)) return;
    } finally {
      registryLock.readLock().unlock();
    }
    registryLock.writeLock().lock();
    try {
      ensureOpen();
      if (queues.containsKey(tenant)) return;
      JobQueue created;
      if (queueFactory != null) {
        created = Objects.requireNonNull(queueFactory.apply(tenant), "queueFactory result");
        if (queues.containsValue(created)) {
          throw new IllegalStateException("Queue factory returned a shared queue instance");
        }
      } else if (queues.isEmpty()) {
        created = initialQueue;
      } else {
        throw missingQueue(tenant);
      }
      queues.put(tenant, created);
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  private void runMaintenanceSafely() {
    try {
      runMaintenance();
    } catch (RuntimeException failure) {
      failureHandler.accept(failure);
    }
  }

  private void ensureOpen() {
    if (closed.get()) throw new IllegalStateException("Queue manager is closed");
  }

  private static String requireTenant(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId is required");
    }
    return tenantId;
  }

  private static IllegalStateException missingQueue(String tenant) {
    return new IllegalStateException("No queue registered for tenant: " + tenant);
  }
}
