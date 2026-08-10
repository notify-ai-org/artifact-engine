package dev.notify.artifact.retry;

import java.util.function.Consumer;

public final class RetryExecutor {
  public <T> T execute(RetryPolicy policy, CheckedSupplier<T> action, Consumer<Attempt> listener)
      throws Exception {
    for (int attempt = 1; ; attempt++) {
      try {
        T result = action.get();
        listener.accept(new Attempt(attempt, null, true));
        return result;
      } catch (Exception failure) {
        listener.accept(new Attempt(attempt, failure, false));
        if (attempt >= policy.maxAttempts() || !policy.retryOn().test(failure)) throw failure;
        Thread.sleep(policy.delay(attempt).toMillis());
      }
    }
  }

  @FunctionalInterface
  public interface CheckedSupplier<T> {
    T get() throws Exception;
  }

  public record Attempt(int number, Throwable failure, boolean successful) {}
}
