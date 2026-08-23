package dev.notify.artifact.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.workflow.Workflow;
import dev.notify.artifact.workflow.WorkflowStatus;
import dev.notify.artifact.workflow.WorkflowStore;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.jdbi.v3.core.Jdbi;

/** PostgreSQL workflow store using row locks for atomic state transitions. */
public final class JdbiWorkflowStore implements WorkflowStore {
  private final Jdbi jdbi;
  private final ObjectMapper json;

  public JdbiWorkflowStore(Jdbi jdbi, ObjectMapper objectMapper) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper").copy().findAndRegisterModules();
  }

  @Override
  public Workflow create(Workflow workflow) {
    Objects.requireNonNull(workflow, "workflow");
    jdbi.useTransaction(
        handle -> {
          handle.createUpdate(
                  """
                  INSERT INTO artifact_workflow
                    (id, name, status, created_at, updated_at, process_start_at,
                     process_end_at, workflow_json)
                  VALUES (:id, :name, :status, :createdAt, :updatedAt, :startAt, :endAt, :json)
                  """)
              .bind("id", workflow.id()).bind("name", workflow.name())
              .bind("status", workflow.status().name()).bind("createdAt", workflow.createdAt())
              .bind("updatedAt", workflow.updatedAt()).bind("startAt", workflow.processStartAt())
              .bind("endAt", workflow.processEndAt()).bind("json", serialize(workflow)).execute();
          workflow.workflowSteps().forEach(
              step -> handle.createUpdate(
                      "INSERT INTO artifact_workflow_step (id, workflow_id, job_record_id, step_order) "
                          + "VALUES (:id, :workflowId, :jobId, :stepOrder)")
                  .bind("id", step.id()).bind("workflowId", workflow.id())
                  .bind("jobId", step.jobRecordId()).bind("stepOrder", step.sequence()).execute());
        });
    return workflow;
  }

  @Override
  public Optional<Workflow> find(String workflowId) {
    return jdbi.withHandle(handle -> handle.createQuery(
            "SELECT workflow_json FROM artifact_workflow WHERE id = :id")
        .bind("id", workflowId).mapTo(String.class).findOne().map(this::deserialize));
  }

  @Override
  public Optional<Workflow> findByJobRecordId(String jobRecordId) {
    return jdbi.withHandle(handle -> handle.createQuery(
            """
            SELECT w.workflow_json FROM artifact_workflow w
            JOIN artifact_workflow_step s ON s.workflow_id = w.id
            WHERE s.job_record_id = :jobId
            """)
        .bind("jobId", jobRecordId).mapTo(String.class).findOne().map(this::deserialize));
  }

  @Override
  public List<Workflow> incomplete(int limit) {
    return jdbi.withHandle(handle -> handle.createQuery(
            """
            SELECT workflow_json FROM artifact_workflow
            WHERE status IN ('PENDING', 'RUNNING') ORDER BY created_at LIMIT :limit
            """)
        .bind("limit", Math.max(1, Math.min(limit, 1000)))
        .mapTo(String.class).map(this::deserialize).list());
  }

  @Override
  public Workflow update(String workflowId, UnaryOperator<Workflow> update) {
    return jdbi.inTransaction(handle -> {
      Workflow current = handle.createQuery(
              "SELECT workflow_json FROM artifact_workflow WHERE id = :id FOR UPDATE")
          .bind("id", workflowId).mapTo(String.class).findOne().map(this::deserialize)
          .orElseThrow(() -> new NoSuchElementException("Workflow not found: " + workflowId));
      Workflow changed = Objects.requireNonNull(update.apply(current));
      handle.createUpdate(
              """
              UPDATE artifact_workflow SET status = :status, updated_at = :updatedAt,
                process_start_at = :startAt, process_end_at = :endAt, workflow_json = :json
              WHERE id = :id
              """)
          .bind("id", changed.id()).bind("status", changed.status().name())
          .bind("updatedAt", changed.updatedAt()).bind("startAt", changed.processStartAt())
          .bind("endAt", changed.processEndAt()).bind("json", serialize(changed)).execute();
      return changed;
    });
  }

  private String serialize(Workflow workflow) {
    try {
      return json.writeValueAsString(workflow);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Workflow cannot be serialized", failure);
    }
  }

  private Workflow deserialize(String value) {
    try {
      return json.readValue(value, Workflow.class);
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Stored workflow is invalid", failure);
    }
  }
}
