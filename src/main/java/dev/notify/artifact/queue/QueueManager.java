package dev.notify.artifact.queue;

import dev.notify.artifact.model.JobRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
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

/** Owns one durable queue per job type and provides all queue lease operations to workers. */
public final class QueueManager implements AutoCloseable {
  private final Map<JobRecord.JobType, JobQueue> queues = new LinkedHashMap<>();
  private final ReadWriteLock registryLock = new ReentrantReadWriteLock();
  private final Function<JobRecord.JobType, JobQueue> queueFactory;
  private final Duration interval;
  private final Consumer<Throwable> failureHandler;
  private final ScheduledExecutorService maintenance;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();

  public QueueManager() {
    this(ignored -> new InMemoryJobQueue(), Duration.ofSeconds(30), failure -> {});
  }

  public QueueManager(Duration interval, Consumer<Throwable> failureHandler) {
    this(ignored -> new InMemoryJobQueue(), interval, failureHandler);
  }

  public QueueManager(Function<JobRecord.JobType, JobQueue> queueFactory) {
    this(queueFactory, Duration.ofSeconds(30), failure -> {});
  }

  public QueueManager(
      Function<JobRecord.JobType, JobQueue> queueFactory,
      Duration interval,
      Consumer<Throwable> failureHandler) {
    this.queueFactory = Objects.requireNonNull(queueFactory, "queueFactory");
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

  public synchronized void start() {
    ensureOpen();
    if (started.compareAndSet(false, true)) {
      maintenance.scheduleWithFixedDelay(
          this::runMaintenanceSafely, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  public void addQueue(JobRecord.JobType type, JobQueue queue) {
    JobRecord.JobType jobType = requireType(type);
    Objects.requireNonNull(queue, "queue");
    registryLock.writeLock().lock();
    try {
      ensureOpen();
      if (queues.containsKey(jobType)) {
        throw new IllegalStateException("Queue already registered for job type: " + jobType);
      }
      if (queues.containsValue(queue)) {
        throw new IllegalArgumentException("A queue instance cannot be shared by job types");
      }
      queues.put(jobType, queue);
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  public Optional<JobQueue> removeQueue(JobRecord.JobType type) {
    JobRecord.JobType jobType = requireType(type);
    registryLock.writeLock().lock();
    try {
      ensureOpen();
      return Optional.ofNullable(queues.remove(jobType));
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  public Optional<JobQueue> queue(JobRecord.JobType type) {
    JobRecord.JobType jobType = requireType(type);
    registryLock.readLock().lock();
    try {
      return Optional.ofNullable(queues.get(jobType));
    } finally {
      registryLock.readLock().unlock();
    }
  }

  public Map<JobRecord.JobType, JobQueue> queues() {
    registryLock.readLock().lock();
    try {
      return Map.copyOf(queues);
    } finally {
      registryLock.readLock().unlock();
    }
  }

  public void enqueue(JobRecord operation) {
    JobRecord job = Objects.requireNonNull(operation, "operation");
    queueFor(job.type()).enqueue(job);
  }

  public void requeue(JobRecord operation, Instant now) {
    JobRecord job = Objects.requireNonNull(operation, "operation");
    Instant recoveredAt = Objects.requireNonNull(now, "now");
    JobRecord recovered =
        new JobRecord(
            job.id(),
            job.tenantId(),
            job.artifactId(),
            job.type(),
            JobRecord.JobStatus.RETRY_PENDING,
            job.attempts(),
            recoveredAt,
            null,
            null,
            job.attributes(),
            job.lastError(),
            job.createdAt(),
            recoveredAt);
    queueFor(job.type()).requeue(recovered);
  }

  public Optional<JobRecord> claim(
      JobRecord.JobType type, String owner, Duration lease, Instant now) {
    return queueFor(type).claim(type, owner, lease, now);
  }

  public boolean complete(JobRecord.JobType type, String jobId, String owner) {
    return queueFor(type).complete(jobId, owner);
  }

  public boolean retry(
      JobRecord.JobType type, String jobId, String owner, Instant nextAttempt, String error) {
    return queueFor(type).retry(jobId, owner, nextAttempt, error);
  }

  public boolean deadLetter(
      JobRecord.JobType type, String jobId, String owner, String error) {
    return queueFor(type).deadLetter(jobId, owner, error);
  }

  public void runMaintenance() {
    ensureOpen();
    Instant now = Instant.now();
    registryLock.readLock().lock();
    try {
      queues.values().forEach(queue -> queue.recoverExpired(now));
    } finally {
      registryLock.readLock().unlock();
    }
  }

  @Override
  public synchronized void close() {
    registryLock.writeLock().lock();
    try {
      if (closed.compareAndSet(false, true)) {
        maintenance.shutdownNow();
      }
    } finally {
      registryLock.writeLock().unlock();
    }
  }

  private JobQueue queueFor(JobRecord.JobType type) {
    JobRecord.JobType jobType = requireType(type);
    registryLock.readLock().lock();
    try {
      ensureOpen();
      JobQueue existing = queues.get(jobType);
      if (existing != null) {
        return existing;
      }
    } finally {
      registryLock.readLock().unlock();
    }

    registryLock.writeLock().lock();
    try {
      ensureOpen();
      JobQueue existing = queues.get(jobType);
      if (existing != null) {
        return existing;
      }
      JobQueue created =
          Objects.requireNonNull(queueFactory.apply(jobType), "queueFactory result");
      if (queues.containsValue(created)) {
        throw new IllegalStateException("Queue factory returned a shared queue instance");
      }
      queues.put(jobType, created);
      return created;
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
    if (closed.get()) {
      throw new IllegalStateException("Queue manager is closed");
    }
  }

  private static JobRecord.JobType requireType(JobRecord.JobType type) {
    return Objects.requireNonNull(type, "job type");
  }
}
