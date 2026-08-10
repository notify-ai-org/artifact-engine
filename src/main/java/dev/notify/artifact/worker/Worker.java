package dev.notify.artifact.worker;

import dev.notify.artifact.filter.FilterChain;
import dev.notify.artifact.job.Job;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Single-threaded bounded worker. Backpressure is explicit: submit returns false when full. */
public final class Worker implements AutoCloseable {
  private final String id;
  private final BlockingQueue<Job<?>> incoming;
  private final FilterChain<Job<?>> filters;
  private final Buffer<Job<?>> buffer;
  private final Consumer<JobFailure> failureHandler;
  private final AtomicBoolean running = new AtomicBoolean();
  private Thread thread;

  public Worker(
      String id,
      int queueCapacity,
      int batchSize,
      long batchBytes,
      Duration flushInterval,
      FilterChain<Job<?>> filters,
      Consumer<JobFailure> failureHandler) {
    this.id = id;
    this.incoming = new ArrayBlockingQueue<>(queueCapacity);
    this.filters = filters;
    this.buffer = new Buffer<>(batchSize, batchBytes, flushInterval);
    this.failureHandler = failureHandler;
  }

  public synchronized void start() {
    if (running.compareAndSet(false, true)) {
      thread = new Thread(this::run, "artifact-worker-" + id);
      thread.start();
    }
  }

  public boolean submit(Job<?> job, Duration timeout) throws InterruptedException {
    return incoming.offer(job, timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void run() {
    while (running.get() || !incoming.isEmpty()) {
      try {
        Job<?> job = incoming.poll(100, TimeUnit.MILLISECONDS);
        if (job != null) {
          addToBuffer(filters.apply(job));
        }
        if (buffer.shouldFlush()) {
          execute(buffer.drain());
        }
      } catch (InterruptedException interrupted) {
        if (running.get()) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    execute(buffer.drain());
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

  public String id() {
    return id;
  }

  public int queued() {
    return incoming.size();
  }

  public int capacity() {
    return incoming.size() + incoming.remainingCapacity();
  }

  public void close() {
    running.set(false);
    if (thread != null) thread.interrupt();
  }

  public record JobFailure(Job<?> job, Exception cause) {}
}
