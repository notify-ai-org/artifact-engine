package dev.notify.artifact.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.workflow.Workflow;
import dev.notify.artifact.workflow.WorkflowStep;
import dev.notify.artifact.workflow.WorkflowStepStatus;
import dev.notify.artifact.workflow.WorkflowStatus;
import dev.notify.artifact.workflow.WorkflowStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

/** PostgreSQL workflow store using row locks for atomic state transitions. */
public final class JdbiWorkflowStore implements WorkflowStore {
  private final Jdbi jdbi;
  private final ObjectMapper json;
  private final JdbiJobStore jobs;

  public JdbiWorkflowStore(Jdbi jdbi, ObjectMapper objectMapper) {
    this(jdbi, objectMapper, new JdbiJobStore(jdbi, objectMapper));
  }

  public JdbiWorkflowStore(Jdbi jdbi, ObjectMapper objectMapper, JdbiJobStore jobStore) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper").copy().findAndRegisterModules();
    this.jobs = Objects.requireNonNull(jobStore, "jobStore");
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
                     process_end_at, attributes_json, failure_message)
                  VALUES (:id, :name, :status, :createdAt, :updatedAt, :startAt, :endAt,
                          :attributesJson, :failureMessage)
                  """)
              .bind("id", workflow.id()).bind("name", workflow.name())
              .bind("status", workflow.status().name()).bind("createdAt", workflow.createdAt())
              .bind("updatedAt", workflow.updatedAt()).bind("startAt", workflow.processStartAt())
              .bind("endAt", workflow.processEndAt())
              .bind("attributesJson", serializeValue(workflow.attributes(), "Workflow attributes"))
              .bind("failureMessage", workflow.failureMessage())
              .execute();
          replaceSteps(handle, workflow);
        });
    return workflow;
  }

  @Override
  public Optional<Workflow> find(String workflowId) {
    return jdbi.withHandle(handle -> find(handle, workflowId, false));
  }

  @Override
  public Optional<Workflow> findByJobRecordId(String jobRecordId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT w.* FROM artifact_workflow w
                    JOIN artifact_workflow_step s ON s.workflow_id = w.id
                    WHERE s.job_record_id = :jobId
                    """)
                .bind("jobId", jobRecordId)
                .map((resultSet, context) -> mapWorkflow(resultSet))
                .findOne()
                .map(workflow -> withStoredSteps(handle, workflow)));
  }

  @Override
  public List<Workflow> recoverable() {
    return jdbi.withHandle(
        handle -> {
          List<Workflow> workflows =
              handle
                  .createQuery(
                      """
                      SELECT * FROM artifact_workflow
                      WHERE status IN ('PENDING', 'RUNNING') ORDER BY created_at
                      """)
                  .map((resultSet, context) -> mapWorkflow(resultSet))
                  .list();
          return workflows.stream().map(workflow -> withStoredSteps(handle, workflow)).toList();
        });
  }

  @Override
  public List<Workflow> incomplete(int limit) {
    return jdbi.withHandle(
        handle -> {
          List<Workflow> workflows =
              handle
                  .createQuery(
                      """
                      SELECT * FROM artifact_workflow
                      WHERE status IN ('PENDING', 'RUNNING') ORDER BY created_at LIMIT :limit
                      """)
                  .bind("limit", Math.max(1, Math.min(limit, 1000)))
                  .map((resultSet, context) -> mapWorkflow(resultSet))
                  .list();
          return workflows.stream().map(workflow -> withStoredSteps(handle, workflow)).toList();
        });
  }

  @Override
  public Workflow update(String workflowId, UnaryOperator<Workflow> update) {
    return jdbi.inTransaction(handle -> {
      Workflow current = find(handle, workflowId, true)
          .orElseThrow(() -> new NoSuchElementException("Workflow not found: " + workflowId));
      Workflow changed = Objects.requireNonNull(update.apply(current));
      if (!workflowId.equals(changed.id())) {
        throw new IllegalArgumentException("Workflow update cannot change its id");
      }
      handle.createUpdate(
              """
              UPDATE artifact_workflow SET name = :name, status = :status, updated_at = :updatedAt,
                process_start_at = :startAt, process_end_at = :endAt,
                attributes_json = :attributesJson, failure_message = :failureMessage
              WHERE id = :id
              """)
          .bind("id", changed.id()).bind("name", changed.name())
          .bind("status", changed.status().name())
          .bind("updatedAt", changed.updatedAt()).bind("startAt", changed.processStartAt())
          .bind("endAt", changed.processEndAt())
          .bind("attributesJson", serializeValue(changed.attributes(), "Workflow attributes"))
          .bind("failureMessage", changed.failureMessage())
          .execute();
      replaceSteps(handle, changed);
      return changed;
    });
  }

  private Optional<Workflow> find(Handle handle, String workflowId, boolean lock) {
    String sql = "SELECT * FROM artifact_workflow WHERE id = :id" + (lock ? " FOR UPDATE" : "");
    return handle.createQuery(sql)
        .bind("id", workflowId)
        .map((resultSet, context) -> mapWorkflow(resultSet))
        .findOne()
        .map(workflow -> withStoredSteps(handle, workflow));
  }

  /** Uses normalized columns for mutable state instead of returning a stale JSON snapshot. */
  private Workflow mapWorkflow(ResultSet resultSet) throws SQLException {
    return new Workflow(
        resultSet.getString("id"),
        resultSet.getString("name"),
        instant(resultSet, "created_at"),
        instant(resultSet, "updated_at"),
        WorkflowStatus.valueOf(resultSet.getString("status")),
        instant(resultSet, "process_start_at"),
        instant(resultSet, "process_end_at"),
        List.of(),
        deserializeValue(
            resultSet.getString("attributes_json"), new TypeReference<Map<String, String>>() {},
            "workflow attributes"),
        resultSet.getString("failure_message"));
  }

  private Workflow withStoredSteps(Handle handle, Workflow workflow) {
    List<WorkflowStep> steps =
        handle
            .createQuery(
                """
                SELECT s.*,
                       j.id AS job_id, j.tenant_id AS job_tenant_id,
                       j.artifact_id AS job_artifact_id, j.job_type AS job_type,
                       j.status AS job_status, j.attempts AS job_attempts,
                       j.next_attempt_at AS job_next_attempt_at,
                       j.lease_owner AS job_lease_owner,
                       j.lease_expires_at AS job_lease_expires_at,
                       j.attributes_json AS job_attributes_json,
                       j.last_error AS job_last_error,
                       j.created_at AS job_created_at, j.updated_at AS job_updated_at
                FROM artifact_workflow_step s
                JOIN artifact_job j ON j.id = s.job_record_id
                WHERE s.workflow_id = :workflowId ORDER BY s.step_order
                """)
            .bind("workflowId", workflow.id())
            .map((resultSet, context) -> mapStep(resultSet))
            .list();
    return new Workflow(
        workflow.id(), workflow.name(), workflow.createdAt(), workflow.updatedAt(), workflow.status(),
        workflow.processStartAt(), workflow.processEndAt(), steps, workflow.attributes(),
        workflow.failureMessage());
  }

  private WorkflowStep mapStep(ResultSet resultSet) throws SQLException {
    return new WorkflowStep(
        resultSet.getString("id"),
        resultSet.getString("workflow_id"),
        instant(resultSet, "created_at"),
        instant(resultSet, "updated_at"),
        resultSet.getString("job_record_id"),
        jobs.map(resultSet, "job_"),
        WorkflowStepStatus.valueOf(resultSet.getString("status")),
        instant(resultSet, "process_start_at"),
        instant(resultSet, "process_end_at"),
        resultSet.getString("prev_step_id"),
        resultSet.getString("next_step_id"),
        resultSet.getInt("step_order"),
        deserializeValue(
            resultSet.getString("attributes_json"), new TypeReference<Map<String, String>>() {},
            "step attributes"),
        resultSet.getString("failure_message"));
  }

  private static Instant instant(ResultSet resultSet, String column) throws SQLException {
    java.sql.Timestamp timestamp = resultSet.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
  }

  /** Keeps job records and workflow-step references consistent in one transaction. */
  private void replaceSteps(Handle handle, Workflow workflow) {
    handle.createUpdate("DELETE FROM artifact_workflow_step WHERE workflow_id = :workflowId")
        .bind("workflowId", workflow.id())
        .execute();

    workflow.workflowSteps().forEach(step -> jobs.create(handle, step.jobRecord()));

    var batch = handle.prepareBatch(
        """
        INSERT INTO artifact_workflow_step
          (id, workflow_id, created_at, updated_at, job_record_id,
           status, process_start_at, process_end_at, prev_step_id, next_step_id,
           step_order, attributes_json, failure_message)
        VALUES
          (:id, :workflowId, :createdAt, :updatedAt, :jobId,
           :status, :startAt, :endAt, :prevStepId, :nextStepId,
           :stepOrder, :attributesJson, :failureMessage)
        """);
    workflow.workflowSteps().forEach(
        step -> {
          if (!workflow.id().equals(step.workflowId())) {
            throw new IllegalArgumentException(
                "Workflow step " + step.id() + " belongs to a different workflow");
          }
          batch.bind("id", step.id())
              .bind("workflowId", workflow.id())
              .bind("createdAt", step.createdAt())
              .bind("updatedAt", step.updatedAt())
              .bind("jobId", step.jobRecordId())
              .bind("status", step.status().name())
              .bind("startAt", step.processStartAt())
              .bind("endAt", step.processEndAt())
              .bind("prevStepId", step.prevStepId())
              .bind("nextStepId", step.nextStepId())
              .bind("stepOrder", step.sequence())
              .bind("attributesJson", serializeValue(step.attributes(), "Workflow step attributes"))
              .bind("failureMessage", step.failureMessage())
              .add();
        });
    batch.execute();
  }

  private String serializeValue(Object value, String description) {
    try {
      return json.writeValueAsString(value);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException(description + " cannot be serialized", failure);
    }
  }

  private <T> T deserializeValue(String value, TypeReference<T> type, String description) {
    try {
      return json.readValue(value, type);
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Stored " + description + " is invalid", failure);
    }
  }
}
