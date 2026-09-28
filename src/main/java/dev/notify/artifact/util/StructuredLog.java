package dev.notify.artifact.util;

import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Objects;

/**
 * {@code event=<name> key=value ...} lines on {@link System.Logger}, so every component logs in
 * one greppable, machine-parsable shape.
 *
 * <p>Values are rendered without locale formatting (no digit grouping), durations as
 * milliseconds ({@code 1532ms}), nulls as {@code -}, and text containing spaces or quotes is
 * quoted and cut at {@value #MAX_VALUE_CHARACTERS} characters. Nothing is formatted when the level
 * is disabled. Callers must not pass tenant ids, file names, or content: identify work by artifact,
 * workflow, and job ids.
 */
public final class StructuredLog {
  private static final int MAX_VALUE_CHARACTERS = 300;

  private final System.Logger logger;

  private StructuredLog(System.Logger logger) {
    this.logger = Objects.requireNonNull(logger, "logger");
  }

  public static StructuredLog of(Class<?> type) {
    return new StructuredLog(System.getLogger(type.getName()));
  }

  public boolean debugEnabled() {
    return logger.isLoggable(Level.DEBUG);
  }

  public void debug(String event, Object... fields) {
    log(Level.DEBUG, null, event, fields);
  }

  public void info(String event, Object... fields) {
    log(Level.INFO, null, event, fields);
  }

  public void warn(String event, Object... fields) {
    log(Level.WARNING, null, event, fields);
  }

  public void warn(String event, Throwable failure, Object... fields) {
    log(Level.WARNING, failure, event, fields);
  }

  public void error(String event, Throwable failure, Object... fields) {
    log(Level.ERROR, failure, event, fields);
  }

  /** Renders a line without logging it; exposed for tests. */
  static String format(String event, Object... fields) {
    StringBuilder line = new StringBuilder("event=").append(event);
    for (int index = 0; index + 1 < fields.length; index += 2) {
      line.append(' ').append(fields[index]).append('=').append(value(fields[index + 1]));
    }
    if (fields.length % 2 != 0) {
      line.append(' ').append(fields[fields.length - 1]).append("=?");
    }
    return line.toString();
  }

  /** Bytes per second as MiB/s with one decimal, for throughput fields. */
  public static String mibPerSecond(long bytes, Duration elapsed) {
    long millis = Math.max(1, elapsed.toMillis());
    return String.format(java.util.Locale.ROOT, "%.1f", bytes / 1048576.0 / (millis / 1000.0));
  }

  private void log(Level level, Throwable failure, String event, Object[] fields) {
    if (!logger.isLoggable(level)) return;
    String line = format(event, fields);
    if (failure == null) {
      logger.log(level, line);
    } else {
      logger.log(level, line, failure);
    }
  }

  private static String value(Object value) {
    if (value == null) return "-";
    if (value instanceof Duration duration) return duration.toMillis() + "ms";
    String text = value.toString();
    if (text.length() > MAX_VALUE_CHARACTERS) {
      text = text.substring(0, MAX_VALUE_CHARACTERS) + "...";
    }
    if (text.isEmpty() || text.chars().anyMatch(c -> Character.isWhitespace(c) || c == '"')) {
      return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"").replaceAll("\\s+", " ") + '"';
    }
    return text;
  }
}
