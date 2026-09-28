package dev.notify.artifact.dlq;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.workflow.Workflow;
import dev.notify.artifact.workflow.WorkflowStep;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A workflow whose retries are exhausted, as it stood when it was dead-lettered.
 *
 * @param id the workflow id; a workflow is dead-lettered at most once
 * @param attempts total executions, the original run plus every retry
 * @param failedSteps the steps that dead-lettered on the final attempt
 */
public record DeadLetter(
    String id,
    String workflowName,
    Map<String, String> attributes,
    List<FailedStep> failedSteps,
    String failureMessage,
    int attempts,
    Instant firstStartedAt,
    Instant deadLetteredAt) {
  public DeadLetter {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(deadLetteredAt, "deadLetteredAt");
    attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    failedSteps = failedSteps == null ? List.of() : List.copyOf(failedSteps);
  }

  public static DeadLetter of(Workflow workflow, Instant deadLetteredAt) {
    List<FailedStep> failed =
        workflow.workflowSteps().stream()
            .filter(step -> step.status() == WorkflowStep.WorkflowStepStatus.CRASHED)
            .map(
                step ->
                    new FailedStep(
                        step.id(),
                        step.jobRecordId(),
                        step.jobRecord().type(),
                        step.stage(),
                        step.failureMessage()))
            .toList();
    return new DeadLetter(
        workflow.id(),
        workflow.name(),
        workflow.attributes(),
        failed,
        workflow.failureMessage(),
        workflow.retryAttempts() + 1,
        workflow.processStartAt(),
        deadLetteredAt);
  }

  public record FailedStep(
      String stepId, String jobRecordId, JobRecord.JobType jobType, int stage, String failure) {}
}
