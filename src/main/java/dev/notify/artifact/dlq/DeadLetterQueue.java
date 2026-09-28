package dev.notify.artifact.dlq;

import java.util.List;
import java.util.Optional;

/**
 * Parking lot for workflows whose retries are exhausted. Letters stay until an operator (or tool)
 * removes them, for example to replay or discard them.
 */
public interface DeadLetterQueue {
  /**
   * Adds a letter unless one with the same id is already queued. Pushing the same workflow twice
   * (a retried dead-letter pass, or two scheduler instances) keeps the first letter.
   *
   * @return the letter now stored under that id
   */
  DeadLetter push(DeadLetter letter);

  Optional<DeadLetter> find(String id);

  /** Letters in the order they were dead-lettered, oldest first. */
  List<DeadLetter> list(int limit);

  /** Removes a letter so it can be replayed or discarded. */
  Optional<DeadLetter> remove(String id);

  int size();
}
