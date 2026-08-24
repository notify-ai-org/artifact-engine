package dev.notify.artifact.worker;

import java.util.Map;
import java.util.Set;

public final class JobStateMachines {
  private JobStateMachines() {}

  public enum State {
    PENDING,
    VALIDATING,
    BUFFERED,
    RUNNING,
    RETRY_PENDING,
    COMPLETED,
    DEAD_LETTER,
    CANCELLED
  }

  private static Map<State, Set<State>> graph() {
    return Map.of(
        State.PENDING, Set.of(State.VALIDATING, State.CANCELLED),
        State.VALIDATING, Set.of(State.BUFFERED, State.DEAD_LETTER, State.CANCELLED),
        State.BUFFERED, Set.of(State.RUNNING, State.CANCELLED),
        State.RUNNING,
            Set.of(State.COMPLETED, State.RETRY_PENDING, State.DEAD_LETTER, State.CANCELLED),
        State.RETRY_PENDING, Set.of(State.BUFFERED, State.DEAD_LETTER));
  }

  public static class Ingest extends StateMachine<State> {
    public Ingest() {
      super(State.PENDING, graph());
    }
  }

  public static class Fetch extends StateMachine<State> {
    public Fetch() {
      super(State.PENDING, graph());
    }
  }

  public static class Index extends StateMachine<State> {
    public Index() {
      super(State.PENDING, graph());
    }
  }

  public static class Retrieval extends StateMachine<State> {
    public Retrieval() {
      super(State.PENDING, graph());
    }
  }
}
