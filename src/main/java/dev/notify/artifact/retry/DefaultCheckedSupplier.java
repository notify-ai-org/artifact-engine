package dev.notify.artifact.retry;

import java.util.Objects;
import java.util.concurrent.Callable;

/** Default checked retry action backed by a Java {@link Callable}. */
public final class DefaultCheckedSupplier<T> implements RetryExecutor.CheckedSupplier<T> {
  private final Callable<? extends T> operation;

  public DefaultCheckedSupplier(Callable<? extends T> operation) {
    this.operation = Objects.requireNonNull(operation, "operation");
  }

  @Override
  public T get() throws Exception {
    return operation.call();
  }
}
