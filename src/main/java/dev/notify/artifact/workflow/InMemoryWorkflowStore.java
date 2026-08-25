package dev.notify.artifact.workflow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.UnaryOperator;

public final class InMemoryWorkflowStore implements WorkflowStore {
  private final java.util.Map<String, Workflow> workflows = new LinkedHashMap<>();

  @Override
  public synchronized Workflow create(Workflow workflow) {
    if (workflows.putIfAbsent(workflow.id(), workflow) != null)
      throw new IllegalArgumentException("Workflow exists: " + workflow.id());
    return workflow;
  }

  @Override
  public synchronized Optional<Workflow> find(String workflowId) {
    return Optional.ofNullable(workflows.get(workflowId));
  }

  @Override
  public synchronized Optional<Workflow> findByJobRecordId(String jobRecordId) {
    return workflows.values().stream()
        .filter(w -> w.workflowSteps().stream().anyMatch(s -> s.jobRecordId().equals(jobRecordId)))
        .findFirst();
  }

  @Override
  public synchronized List<Workflow> recoverable() {
    return workflows.values().stream()
        .filter(workflow -> workflow.status() == WorkflowStatus.PENDING
            || workflow.status() == WorkflowStatus.RUNNING)
        .toList();
  }

  @Override
  public synchronized List<Workflow> incomplete(int limit) {
    List<Workflow> result = new ArrayList<>();
    for (Workflow workflow : workflows.values()) {
      if (workflow.status() != WorkflowStatus.COMPLETED
          && workflow.status() != WorkflowStatus.CRASHED) result.add(workflow);
      if (result.size() == limit) break;
    }
    return List.copyOf(result);
  }

  @Override
  public synchronized Workflow update(String workflowId, UnaryOperator<Workflow> update) {
    Workflow current = Optional.ofNullable(workflows.get(workflowId))
        .orElseThrow(() -> new NoSuchElementException("Workflow not found: " + workflowId));
    Workflow changed = java.util.Objects.requireNonNull(update.apply(current));
    workflows.put(workflowId, changed);
    return changed;
  }
}
