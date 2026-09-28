package dev.notify.artifact.dlq;

/** Leaves dead letters in the queue without further action. */
public enum NoOpDeadLetterHandler implements DeadLetterHandler {
  INSTANCE;

  @Override
  public void handle(DeadLetter letter) {}
}
