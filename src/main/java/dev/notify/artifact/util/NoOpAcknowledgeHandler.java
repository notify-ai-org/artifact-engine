package dev.notify.artifact.util;

/** Default acknowledgement callback for callers that do not need delivery notifications. */
public final class NoOpAcknowledgeHandler implements AcknowledgeHandler {
  public static final NoOpAcknowledgeHandler INSTANCE = new NoOpAcknowledgeHandler();

  private NoOpAcknowledgeHandler() {}

  @Override
  public void acknowledge(String operationId, Outcome outcome, String message) {
    // Acknowledgements are intentionally optional; do not log potentially sensitive messages.
  }
}
