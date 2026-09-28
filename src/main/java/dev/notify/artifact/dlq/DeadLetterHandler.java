package dev.notify.artifact.dlq;

import java.util.List;
import java.util.Objects;

/**
 * Reacts to a workflow being dead-lettered: alerting, releasing resources the workflow held (such as
 * an unfinished multipart upload), or recording it elsewhere.
 *
 * <p>Called once per dead-lettered workflow, after the letter is in the queue. A handler failure is
 * reported but never removes the letter or reverses the dead-lettering.
 */
@FunctionalInterface
public interface DeadLetterHandler {
  void handle(DeadLetter letter) throws Exception;

  /** Runs every handler even if an earlier one fails, then rethrows the first failure. */
  static DeadLetterHandler composite(List<DeadLetterHandler> handlers) {
    List<DeadLetterHandler> all = List.copyOf(handlers);
    all.forEach(handler -> Objects.requireNonNull(handler, "handler"));
    return letter -> {
      Exception first = null;
      for (DeadLetterHandler handler : all) {
        try {
          handler.handle(letter);
        } catch (Exception failure) {
          if (first == null) first = failure;
          else first.addSuppressed(failure);
        }
      }
      if (first != null) throw first;
    };
  }
}
