package dev.notify.artifact.worker;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.workflow.JobUpdateListener;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Maintains one durable worker per tenant and supported job type. */
public final class DurableWorkerPool implements AutoCloseable {
  private final QueueManager queues;
  private final JobRecordExecutor executor;
  private final JobUpdateListener listener;
  private final RetryPolicy retryPolicy;
  private final Duration lease;
  private final Duration poll;
  private final Consumer<Worker.JobFailure> failures;
  private final Map<String, Worker> workers = new ConcurrentHashMap<>();
  private final ScheduledExecutorService manager;

  public DurableWorkerPool(
      QueueManager queues,
      JobRecordExecutor executor,
      JobUpdateListener listener,
      RetryPolicy retryPolicy,
      Duration lease,
      Duration poll,
      Consumer<Worker.JobFailure> failures) {
    this.queues = Objects.requireNonNull(queues);
    this.executor = Objects.requireNonNull(executor);
    this.listener = Objects.requireNonNull(listener);
    this.retryPolicy = Objects.requireNonNull(retryPolicy);
    this.lease = Objects.requireNonNull(lease);
    this.poll = Objects.requireNonNull(poll);
    this.failures = Objects.requireNonNull(failures);
    manager = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(r, "artifact-durable-worker-manager");
      thread.setDaemon(true);
      return thread;
    });
  }

  public void start() {
    manager.scheduleWithFixedDelay(this::reconcile, 0, poll.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void reconcile() {
    queues.queues().forEach((tenant, queue) -> {
      for (JobRecord.JobType type : new JobRecord.JobType[] {
          JobRecord.JobType.STORE, JobRecord.JobType.INDEX}) {
        String key = tenant + ":" + type;
        workers.computeIfAbsent(key, ignored -> {
          Worker worker = new Worker(
              key, 1, 1, 1, type, queue, retryPolicy, lease, poll, poll, failures,
              executor, listener);
          worker.start();
          return worker;
        });
      }
    });
  }

  @Override
  public void close() {
    manager.shutdownNow();
    workers.values().forEach(Worker::close);
    workers.clear();
  }
}
