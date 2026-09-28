package dev.notify.artifact.workflow;

import dev.notify.artifact.model.JobRecord;
import java.time.Instant;
import java.util.Map;

/**
 * One job within a workflow.
 *
 * @param sequence unique position of the step within its workflow
 * @param stage execution stage: steps sharing a stage run in parallel, and a step becomes ready
 *     once every step in an earlier stage has completed. A linear workflow uses one step per stage
 *     ({@code stage == sequence}).
 */
public record WorkflowStep(
    String id,
    String workflowId,
    Instant createdAt,
    Instant updatedAt,
    String jobRecordId,
    JobRecord jobRecord,
    WorkflowStepStatus status,
    Instant processStartAt,
    Instant processEndAt,
    String prevStepId,
    String nextStepId,
    int sequence,
    Map<String, String> attributes,
    String failureMessage,
    int stage) {

  public WorkflowStep {
    attributes = Map.copyOf(attributes);
    if (stage < 0) throw new IllegalArgumentException("stage cannot be negative");
  }

  /** A step in a linear workflow, where each step is its own stage. */
  public WorkflowStep(
      String id,
      String workflowId,
      Instant createdAt,
      Instant updatedAt,
      String jobRecordId,
      JobRecord jobRecord,
      WorkflowStepStatus status,
      Instant processStartAt,
      Instant processEndAt,
      String prevStepId,
      String nextStepId,
      int sequence,
      Map<String, String> attributes,
      String failureMessage) {
    this(id, workflowId, createdAt, updatedAt, jobRecordId, jobRecord, status, processStartAt,
        processEndAt, prevStepId, nextStepId, sequence, attributes, failureMessage, sequence);
  }

  public enum WorkflowStepStatus {
    PENDING,
    SUBMITTED,
    RUNNING,
    COMPLETED,
    CRASHED
  }
}
