package dev.notify.artifact.workflow;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

/** Durable workflow persistence boundary. Implementations must update a workflow atomically. */
public interface WorkflowStore {
  Workflow create(Workflow workflow);

  Optional<Workflow> find(String workflowId);

  Optional<Workflow> findByJobRecordId(String jobRecordId);

  /** Returns all non-terminal workflows for startup recovery. */
  List<Workflow> recoverable();

  List<Workflow> incomplete(int limit);

  Workflow update(String workflowId, UnaryOperator<Workflow> update);
}
