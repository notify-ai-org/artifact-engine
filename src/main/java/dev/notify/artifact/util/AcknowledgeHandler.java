package dev.notify.artifact.util;

@FunctionalInterface
public interface AcknowledgeHandler {
  void acknowledge(String operationId, Outcome outcome, String message);

  enum Outcome {
    ACCEPTED,
    COMPLETED,
    RETRYING,
    REJECTED
  }
}
