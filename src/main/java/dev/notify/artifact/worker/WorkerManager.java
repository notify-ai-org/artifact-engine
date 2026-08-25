package dev.notify.artifact.worker;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.store.JobStore;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Owns worker lifecycle and exposes snapshots that a durable implementation can persist/restore.
 */
public final class WorkerManager implements AutoCloseable {
  public static final int DEFAULT_BATCH_SIZE = 16;
  public static final long DEFAULT_BATCH_BYTES = 16;
  public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMillis(250);
  public static final Duration DEFAULT_LEASE_DURATION = Duration.ofMinutes(15);
  public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(250);
  public static final int DEFAULT_MAX_WORKERS = 64;

  private final Map<String, Managed> workers = new ConcurrentHashMap<>();
  private final java.util.function.Consumer<Worker.JobFailure> failureHandler;
  private final WorkerSnapshotStore snapshotStore;
  private final int maxWorkers;
  private final QueueManager queueManager;
  private final JobRecordExecutor jobExecutor;
  private final JobStore jobStore;
  private final Duration leaseDuration;
  private final Duration pollInterval;
  private final List<Consumer<Worker.StateChange>> stateChangeListeners =
      new CopyOnWriteArrayList<>();
  private final Map<String, Worker.StateChange> latestStateChanges = new ConcurrentHashMap<>();

  public WorkerManager(java.util.function.Consumer<Worker.JobFailure> failureHandler) {
    this(failureHandler, null, List.of(), DEFAULT_MAX_WORKERS);
  }

  public WorkerManager(
      java.util.function.Consumer<Worker.JobFailure> failureHandler,
      WorkerSnapshotStore snapshotStore) {
    this(failureHandler, snapshotStore, List.of(), DEFAULT_MAX_WORKERS);
  }

  public WorkerManager(List<WorkerConfiguration> initialWorkers, int maxWorkers) {
    this(failure -> {}, null, initialWorkers, maxWorkers);
  }

  public WorkerManager(
      java.util.function.Consumer<Worker.JobFailure> failureHandler,
      WorkerSnapshotStore snapshotStore,
      List<WorkerConfiguration> initialWorkers,
      int maxWorkers) {
    this(
        failureHandler,
        snapshotStore,
        initialWorkers,
        maxWorkers,
        new QueueManager(),
        ignored -> () -> null);
  }

  public WorkerManager(
      java.util.function.Consumer<Worker.JobFailure> failureHandler,
      WorkerSnapshotStore snapshotStore,
      List<WorkerConfiguration> initialWorkers,
      int maxWorkers,
      QueueManager queueManager,
      JobRecordExecutor jobExecutor) {
    this(
        failureHandler, snapshotStore, initialWorkers, maxWorkers, queueManager, jobExecutor, null);
  }

  public WorkerManager(
      java.util.function.Consumer<Worker.JobFailure> failureHandler,
      WorkerSnapshotStore snapshotStore,
      List<WorkerConfiguration> initialWorkers,
      int maxWorkers,
      QueueManager queueManager,
      JobRecordExecutor jobExecutor,
      JobStore jobStore) {
    this(
        failureHandler,
        snapshotStore,
        initialWorkers,
        maxWorkers,
        queueManager,
        jobExecutor,
        jobStore,
        DEFAULT_LEASE_DURATION,
        DEFAULT_POLL_INTERVAL);
  }

  public WorkerManager(
      java.util.function.Consumer<Worker.JobFailure> failureHandler,
      WorkerSnapshotStore snapshotStore,
      List<WorkerConfiguration> initialWorkers,
      int maxWorkers,
      QueueManager queueManager,
      JobRecordExecutor jobExecutor,
      JobStore jobStore,
      Duration leaseDuration,
      Duration pollInterval) {
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.snapshotStore = snapshotStore;
    this.queueManager = Objects.requireNonNull(queueManager, "queueManager");
    this.jobExecutor = Objects.requireNonNull(jobExecutor, "jobExecutor");
    this.jobStore = jobStore;
    this.leaseDuration = requirePositive(leaseDuration, "leaseDuration");
    this.pollInterval = requirePositive(pollInterval, "pollInterval");
    if (maxWorkers < 1) {
      throw new IllegalArgumentException("maxWorkers must be positive");
    }
    List<WorkerConfiguration> configuredWorkers =
        List.copyOf(Objects.requireNonNull(initialWorkers, "initialWorkers"));
    if (configuredWorkers.size() > maxWorkers) {
      throw new IllegalArgumentException("Initial worker count exceeds maxWorkers");
    }
    this.maxWorkers = maxWorkers;
    try {
      for (WorkerConfiguration worker : configuredWorkers) {
        add(
            worker.id(),
            worker.queueCapacity(),
            worker.batchSize(),
            worker.batchBytes(),
            worker.flushInterval(),
            worker.type());
      }
    } catch (RuntimeException failure) {
      close();
      throw failure;
    }
  }

  public WorkerManager() {
    this(failure -> {}, null, List.of(), DEFAULT_MAX_WORKERS);
  }

  public int restore() throws IOException {
    if (snapshotStore == null) {
      return 0;
    }
    int restored = 0;
    for (WorkerSnapshot snapshot : snapshotStore.load()) {
      if (!workers.containsKey(snapshot.id())) {
        add(
            snapshot.id(),
            snapshot.capacity(),
            snapshot.batchSize(),
            snapshot.batchBytes(),
            snapshot.flushInterval(),
            snapshot.type(),
            snapshot.lastUsed());
        restored++;
      }
    }
    return restored;
  }

  public void flushSnapshots() throws IOException {
    if (snapshotStore != null) {
      snapshotStore.save(List.copyOf(snapshots().values()));
    }
  }

  /** Adds a worker using the manager's backwards-compatible batching defaults. */
  public Worker add(String id, int queueCapacity) {
    return add(
        id,
        queueCapacity,
        DEFAULT_BATCH_SIZE,
        DEFAULT_BATCH_BYTES,
        DEFAULT_FLUSH_INTERVAL);
  }

  /**
   * Adds and starts a worker with explicit queue and batch limits.
   *
   * @param id unique worker identifier
   * @param queueCapacity maximum number of jobs waiting in the worker's inbound queue
   * @param batchSize maximum number of jobs collected before a batch is flushed
   * @param batchBytes maximum accumulated batch weight before a batch is flushed
   * @param flushInterval maximum time a non-empty batch may wait before execution
   */
  public Worker add(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval) {
    return add(
        id,
        queueCapacity,
        batchSize,
        batchBytes,
        flushInterval,
        JobRecord.JobType.INDEX);
  }

  public Worker add(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      JobRecord.JobType type) {
    return add(id, queueCapacity, batchSize, batchBytes, flushInterval, type, Instant.now());
  }

  private synchronized Worker add(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      JobRecord.JobType type,
      Instant lastUsed) {
    WorkerSettings settings =
        new WorkerSettings(queueCapacity, batchSize, batchBytes, flushInterval, type);
    if (workers.containsKey(id)) {
      throw new IllegalArgumentException("Worker exists: " + id);
    }
    if (workers.size() >= maxWorkers) {
      throw new IllegalStateException("Worker pool limit reached: " + maxWorkers);
    }
    Worker worker =
        new Worker(
            id,
            settings.batchSize(),
            settings.batchBytes(),
            settings.type(),
            queueManager,
            RetryPolicy.defaults(),
            leaseDuration,
            pollInterval,
            flushInterval,
            failureHandler,
            jobExecutor,
            jobStore);
    Managed managed = new Managed(worker, settings, Objects.requireNonNull(lastUsed, "lastUsed"));
    workers.put(id, managed);
    worker.onStateChange(this::handleStateChange);
    worker.start();
    return worker;
  }

  private static Duration requirePositive(Duration duration, String name) {
    Objects.requireNonNull(duration, name);
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return duration;
  }

  public synchronized void remove(String id) {
    Managed managed = workers.remove(id);
    if (managed != null) managed.worker.close();
  }

  public int removeIdle(Duration idle) {
    int before = workers.size();
    Instant cutoff = Instant.now().minus(idle);
    workers
        .entrySet()
        .removeIf(
            e -> {
              if (e.getValue().lastUsed.isBefore(cutoff)) {
                e.getValue().worker.close();
                return true;
              }
              return false;
            });
    return before - workers.size();
  }

  public Map<String, WorkerSnapshot> snapshots() {
    Map<String, WorkerSnapshot> result = new HashMap<>();
    workers.forEach(
        (id, m) ->
            result.put(
                id,
                new WorkerSnapshot(
                    id,
                    m.settings.queueCapacity(),
                    0,
                    m.lastUsed,
                    m.settings.batchSize(),
                    m.settings.batchBytes(),
                    m.settings.flushInterval(),
                    m.settings.type())));
    return Map.copyOf(result);
  }

  public int size() {
    return workers.size();
  }

  public int maxWorkers() {
    return maxWorkers;
  }

  public void addStateChangeListener(Consumer<Worker.StateChange> listener) {
    stateChangeListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  public void removeStateChangeListener(Consumer<Worker.StateChange> listener) {
    stateChangeListeners.remove(Objects.requireNonNull(listener, "listener"));
  }

  public Map<String, Worker.StateChange> stateChanges() {
    return Map.copyOf(latestStateChanges);
  }

  private void handleStateChange(Worker.StateChange stateChange) {
    latestStateChanges.put(stateChange.jobId(), stateChange);
    for (Consumer<Worker.StateChange> listener : stateChangeListeners) {
      try {
        listener.accept(stateChange);
      } catch (RuntimeException failure) {
        failureHandler.accept(new Worker.JobFailure(null, failure));
      }
    }
  }

  public synchronized void close() {
    workers.values().forEach(m -> m.worker.close());
    workers.clear();
  }

  private static final class Managed {
    private final Worker worker;
    private final WorkerSettings settings;
    private volatile Instant lastUsed;

    private Managed(Worker worker, WorkerSettings settings, Instant lastUsed) {
      this.worker = worker;
      this.settings = settings;
      this.lastUsed = lastUsed;
    }
  }

  private record WorkerSettings(
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      JobRecord.JobType type) {
    private WorkerSettings {
      if (queueCapacity < 1) {
        throw new IllegalArgumentException("queueCapacity must be positive");
      }
      if (batchSize < 1) {
        throw new IllegalArgumentException("batchSize must be positive");
      }
      if (batchBytes < 1) {
        throw new IllegalArgumentException("batchBytes must be positive");
      }
      if (flushInterval == null || flushInterval.isZero() || flushInterval.isNegative()) {
        throw new IllegalArgumentException("flushInterval must be positive");
      }
      Objects.requireNonNull(type, "type");
    }
  }

  /** Configuration for a worker created as part of the manager's initial pool. */
  public record WorkerConfiguration(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      JobRecord.JobType type) {
    public WorkerConfiguration(String id, int queueCapacity) {
      this(
          id,
          queueCapacity,
          DEFAULT_BATCH_SIZE,
          DEFAULT_BATCH_BYTES,
          DEFAULT_FLUSH_INTERVAL,
          JobRecord.JobType.INDEX);
    }

    public WorkerConfiguration(
        String id,
        int queueCapacity,
        int batchSize,
        long batchBytes,
        Duration flushInterval) {
      this(id, queueCapacity, batchSize, batchBytes, flushInterval, JobRecord.JobType.INDEX);
    }

    public WorkerConfiguration {
      if (id == null || id.isBlank()) {
        throw new IllegalArgumentException("Worker id is required");
      }
      new WorkerSettings(queueCapacity, batchSize, batchBytes, flushInterval, type);
    }
  }

  /** Durable worker topology and batching configuration. */
  public record WorkerSnapshot(
      String id,
      int capacity,
      int queuedJobs,
      Instant lastUsed,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      JobRecord.JobType type) {

    /** Source-compatible constructor for snapshot stores written before batch settings existed. */
    public WorkerSnapshot(String id, int capacity, int queuedJobs, Instant lastUsed) {
      this(
          id,
          capacity,
          queuedJobs,
          lastUsed,
          DEFAULT_BATCH_SIZE,
          DEFAULT_BATCH_BYTES,
          DEFAULT_FLUSH_INTERVAL,
          JobRecord.JobType.INDEX);
    }

    public WorkerSnapshot(
        String id,
        int capacity,
        int queuedJobs,
        Instant lastUsed,
        int batchSize,
        long batchBytes,
        Duration flushInterval) {
      this(
          id,
          capacity,
          queuedJobs,
          lastUsed,
          batchSize,
          batchBytes,
          flushInterval,
          JobRecord.JobType.INDEX);
    }

    public WorkerSnapshot {
      // Jackson supplies zero/null for fields missing from legacy JSON snapshots.
      batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
      batchBytes = batchBytes > 0 ? batchBytes : DEFAULT_BATCH_BYTES;
      flushInterval =
          flushInterval == null || flushInterval.isZero() || flushInterval.isNegative()
              ? DEFAULT_FLUSH_INTERVAL
              : flushInterval;
      type = type == null ? JobRecord.JobType.INDEX : type;
    }
  }
}
