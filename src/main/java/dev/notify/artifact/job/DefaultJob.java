package dev.notify.artifact.job;

import java.util.Objects;
import java.util.concurrent.Callable;

/** Reusable {@link Job} implementation backed by a checked Java {@link Callable}. */
public final class DefaultJob<R> implements Job<R> {
  private final Callable<? extends R> operation;

  public DefaultJob(Callable<? extends R> operation) {
    this.operation = Objects.requireNonNull(operation, "operation");
  }

  @Override
  public R execute() throws Exception {
    return operation.call();
  }
}
