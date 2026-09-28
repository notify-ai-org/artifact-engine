package dev.notify.artifact.worker;

import dev.notify.artifact.job.DirectJob;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Bounded worker dedicated to process-local request/response jobs. */
public final class DirectJobWorker implements AutoCloseable {
  private final ThreadPoolExecutor executor;

  public DirectJobWorker(int threads, int capacity) {
    if (threads < 1 || capacity < 1) throw new IllegalArgumentException("threads and capacity must be positive");
    executor =
        new ThreadPoolExecutor(
            threads,
            threads,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(capacity),
            runnable -> {
              Thread thread = new Thread(runnable, "artifact-direct-worker");
              thread.setDaemon(true);
              return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());
  }

  public <R> R execute(DirectJob<R> job) throws Exception {
    Objects.requireNonNull(job, "job");
    job.prepare();
    FutureTask<R> task = new FutureTask<>(job::execute);
    try {
      executor.execute(task);
    } catch (RejectedExecutionException rejected) {
      job.abandon();
      throw rejected;
    }
    try {
      return task.get();
    } catch (InterruptedException interrupted) {
      task.cancel(true);
      job.abandon();
      Thread.currentThread().interrupt();
      throw interrupted;
    } catch (ExecutionException failed) {
      Throwable cause = failed.getCause();
      if (cause instanceof Exception exception) throw exception;
      if (cause instanceof Error error) throw error;
      throw new IllegalStateException(cause);
    }
  }

  public void close(Duration timeout) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) executor.shutdownNow();
    } catch (InterruptedException interrupted) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    close(Duration.ofSeconds(10));
  }
}
