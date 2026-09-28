package dev.notify.artifact.workflow;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * @param retryAttempts how many times the workflow retry scheduler has resumed this workflow
 * @param nextRetryAt when a {@link WorkflowStatus#CRASHED} workflow becomes due for its next retry;
 *     null until the scheduler has planned it
 */
public record Workflow(
    String id,
    String name,
    Instant createdAt,
    Instant updatedAt,
    WorkflowStatus status,
    Instant processStartAt,
    Instant processEndAt,
    List<WorkflowStep> workflowSteps,
    Map<String, String> attributes,
    String failureMessage,
    int retryAttempts,
    Instant nextRetryAt) {
  public Workflow {
    workflowSteps = List.copyOf(workflowSteps);
    attributes = Map.copyOf(attributes);
    if (retryAttempts < 0) throw new IllegalArgumentException("retryAttempts cannot be negative");
  }

  /** A workflow that has never been retried. */
  public Workflow(
      String id,
      String name,
      Instant createdAt,
      Instant updatedAt,
      WorkflowStatus status,
      Instant processStartAt,
      Instant processEndAt,
      List<WorkflowStep> workflowSteps,
      Map<String, String> attributes,
      String failureMessage) {
    this(id, name, createdAt, updatedAt, status, processStartAt, processEndAt, workflowSteps,
        attributes, failureMessage, 0, null);
  }

  public enum WorkflowStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    /** A step dead-lettered. The retry scheduler either resumes it or moves it to the DLQ. */
    CRASHED,
    /** Retries are exhausted and the workflow was pushed to the dead-letter queue. Terminal. */
    DEAD_LETTERED
  }
}
