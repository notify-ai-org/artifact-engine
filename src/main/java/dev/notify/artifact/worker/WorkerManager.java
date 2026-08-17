package dev.notify.artifact.worker;

import dev.notify.artifact.job.Job;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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

  private final Map<String, Managed> workers = new ConcurrentHashMap<>();
  private final java.util.function.Consumer<Worker.JobFailure> failureHandler;
  private final WorkerSnapshotStore snapshotStore;
  private final List<Consumer<Worker.StateChange>> stateChangeListeners =
      new CopyOnWriteArrayList<>();
  private final Map<String, Worker.StateChange> latestStateChanges = new ConcurrentHashMap<>();

  public WorkerManager(java.util.function.Consumer<Worker.JobFailure> failureHandler) {
    this(failureHandler, null);
  }

  public WorkerManager(
      java.util.function.Consumer<Worker.JobFailure> failureHandler,
      WorkerSnapshotStore snapshotStore) {
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    this.snapshotStore = snapshotStore;
  }

  public WorkerManager() {
    this(failure -> {});
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
    return add(id, queueCapacity, batchSize, batchBytes, flushInterval, Instant.now());
  }

  private Worker add(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      Instant lastUsed) {
    WorkerSettings settings =
        new WorkerSettings(queueCapacity, batchSize, batchBytes, flushInterval);
    Worker worker =
        new Worker(
            id,
            settings.queueCapacity(),
            settings.batchSize(),
            settings.batchBytes(),
            null, null, null, settings.flushInterval(),
            flushInterval, flushInterval, failureHandler);
    Managed managed = new Managed(worker, settings, Objects.requireNonNull(lastUsed, "lastUsed"));
    if (workers.putIfAbsent(id, managed) != null)
      throw new IllegalArgumentException("Worker exists: " + id);
    worker.onStateChange(this::handleStateChange);
    worker.start();
    return worker;
  }

  public void remove(String id) {
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
              if (e.getValue().lastUsed.isBefore(cutoff) && e.getValue().worker.queued() == 0) {
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
                    m.worker.queued(),
                    m.lastUsed,
                    m.settings.batchSize(),
                    m.settings.batchBytes(),
                    m.settings.flushInterval())));
    return Map.copyOf(result);
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

  public void close() {
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
      int queueCapacity, int batchSize, long batchBytes, Duration flushInterval) {
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
      Duration flushInterval) {

    /** Source-compatible constructor for snapshot stores written before batch settings existed. */
    public WorkerSnapshot(String id, int capacity, int queuedJobs, Instant lastUsed) {
      this(
          id,
          capacity,
          queuedJobs,
          lastUsed,
          DEFAULT_BATCH_SIZE,
          DEFAULT_BATCH_BYTES,
          DEFAULT_FLUSH_INTERVAL);
    }

    public WorkerSnapshot {
      // Jackson supplies zero/null for fields missing from legacy JSON snapshots.
      batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
      batchBytes = batchBytes > 0 ? batchBytes : DEFAULT_BATCH_BYTES;
      flushInterval =
          flushInterval == null || flushInterval.isZero() || flushInterval.isNegative()
              ? DEFAULT_FLUSH_INTERVAL
              : flushInterval;
    }
  }
}
