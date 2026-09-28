package dev.notify.artifact.dlq;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Process-local dead-letter queue for tests and single-node development. Letters are lost on
 * restart and are not shared between instances; use a durable implementation in production.
 */
public final class InMemoryDeadLetterQueue implements DeadLetterQueue {
  private final Map<String, DeadLetter> letters = new LinkedHashMap<>();

  @Override
  public synchronized DeadLetter push(DeadLetter letter) {
    Objects.requireNonNull(letter, "letter");
    DeadLetter existing = letters.putIfAbsent(letter.id(), letter);
    return existing == null ? letter : existing;
  }

  @Override
  public synchronized Optional<DeadLetter> find(String id) {
    return Optional.ofNullable(letters.get(id));
  }

  @Override
  public synchronized List<DeadLetter> list(int limit) {
    return letters.values().stream().limit(Math.max(0, limit)).toList();
  }

  @Override
  public synchronized Optional<DeadLetter> remove(String id) {
    return Optional.ofNullable(letters.remove(id));
  }

  @Override
  public synchronized int size() {
    return letters.size();
  }
}
