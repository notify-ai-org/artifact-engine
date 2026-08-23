package dev.notify.artifact.workflow;

/** Receives durable worker outcomes; retryable failures are not terminal. */
@FunctionalInterface
public interface JobUpdateListener {
  void onUpdate(String jobRecordId, JobUpdate status, String failureMessage);

  enum JobUpdate {
    RUNNING,
    RETRY_PENDING,
    COMPLETED,
    DEAD_LETTER
  }
}
