package dev.notify.artifact.workflow;

import dev.notify.artifact.model.JobRecord;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * One job within a workflow.
 *
 * @param sequence unique position of the step within its workflow
 * @param stage execution stage: a step becomes ready once every step in an earlier stage has
 *     completed. A linear workflow uses one step per stage ({@code stage == sequence}).
 * @param dependsOn ids of steps in the same stage that must complete before this one starts; steps
 *     of a stage without dependencies between them run in parallel
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
    int stage,
    List<String> dependsOn) {

  public WorkflowStep {
    attributes = Map.copyOf(attributes);
    dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    if (stage < 0) throw new IllegalArgumentException("stage cannot be negative");
  }

  /** A step with no dependencies inside its stage. */
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
      String failureMessage,
      int stage) {
    this(id, workflowId, createdAt, updatedAt, jobRecordId, jobRecord, status, processStartAt,
        processEndAt, prevStepId, nextStepId, sequence, attributes, failureMessage, stage,
        List.of());
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
