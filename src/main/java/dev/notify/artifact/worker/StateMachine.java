package dev.notify.artifact.worker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Synchronized state machine retaining transition history and callbacks for snapshotting/audit. */
public class StateMachine<S extends Enum<S>> {
  private S current;
  private final Map<S, Set<S>> transitions;
  private final List<Transition<S>> history = new ArrayList<>();
  private final List<Consumer<Transition<S>>> callbacks = new CopyOnWriteArrayList<>();

  public StateMachine(S initial, Map<S, Set<S>> transitions) {
    this.current = initial;
    this.transitions = Map.copyOf(transitions);
  }

  public synchronized S state() {
    return current;
  }

  public synchronized List<Transition<S>> history() {
    return List.copyOf(history);
  }

  public synchronized void transition(S next, String reason) {
    if (!transitions.getOrDefault(current, Set.of()).contains(next))
      throw new IllegalStateException("Invalid transition " + current + " -> " + next);
    var event = new Transition<>(current, next, reason, Instant.now());
    current = next;
    history.add(event);
    callbacks.forEach(c -> c.accept(event));
  }

  public void onTransition(Consumer<Transition<S>> callback) {
    callbacks.add(callback);
  }

  public record Transition<S>(S from, S to, String reason, Instant at) {}
}
