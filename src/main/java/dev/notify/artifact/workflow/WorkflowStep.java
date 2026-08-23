package dev.notify.artifact.workflow;

import dev.notify.artifact.model.JobRecord;
import java.time.Instant;
import java.util.Map;

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
    String failureMessage) {
  public WorkflowStep {
    attributes = Map.copyOf(attributes);
  }
}
