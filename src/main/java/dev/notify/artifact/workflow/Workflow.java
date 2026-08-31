package dev.notify.artifact.workflow;

import java.time.Instant;
import java.util.List;
import java.util.Map;

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
    String failureMessage) {
  public Workflow {
    workflowSteps = List.copyOf(workflowSteps);
    attributes = Map.copyOf(attributes);
  }

  public enum WorkflowStatus {
  PENDING,
  RUNNING,
  COMPLETED,
  CRASHED
}

}
