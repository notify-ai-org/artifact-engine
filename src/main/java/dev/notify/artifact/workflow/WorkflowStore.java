package dev.notify.artifact.workflow;

import java.time.Instant;
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

  /**
   * CRASHED workflows the retry scheduler must act on: those not yet planned ({@code nextRetryAt}
   * is null) and those whose retry is due at {@code now}, unplanned first, then oldest due first.
   */
  List<Workflow> retryCandidates(Instant now, int limit);

  Workflow update(String workflowId, UnaryOperator<Workflow> update);
}
