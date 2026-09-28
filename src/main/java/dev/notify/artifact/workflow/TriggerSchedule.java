package dev.notify.artifact.workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * When a periodic task fires. The engine stays free of scheduling frameworks; a cron expression
 * is adapted by the host application (for example with Spring's {@code CronExpression}).
 */
@FunctionalInterface
public interface TriggerSchedule {
  /** The next fire time strictly after {@code after}, or null when the schedule has ended. */
  Instant next(Instant after);

  static TriggerSchedule every(Duration interval) {
    Objects.requireNonNull(interval, "interval");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException("interval must be positive");
    }
    return after -> after.plus(interval);
  }
}
