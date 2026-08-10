package dev.notify.artifact.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/** Immutable retry4j-style policy with exponential backoff, jitter, and exception filtering. */
public record RetryPolicy(
    int maxAttempts,
    Duration initialDelay,
    Duration maxDelay,
    double multiplier,
    double jitter,
    Predicate<Throwable> retryOn) {
  public RetryPolicy {
    if (maxAttempts < 1 || multiplier < 1 || jitter < 0 || jitter > 1)
      throw new IllegalArgumentException("Invalid retry policy");
    Objects.requireNonNull(initialDelay);
    Objects.requireNonNull(maxDelay);
    Objects.requireNonNull(retryOn);
  }

  public Duration delay(int completedAttempts) {
    double base =
        initialDelay.toMillis() * Math.pow(multiplier, Math.max(0, completedAttempts - 1));
    double randomized = base * (1 + ThreadLocalRandom.current().nextDouble(-jitter, jitter));
    return Duration.ofMillis(Math.min(maxDelay.toMillis(), Math.max(0, (long) randomized)));
  }

  public static RetryPolicy defaults() {
    return new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofMinutes(2), 2, .2, t -> true);
  }
}
