package dev.notify.artifact.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.JobStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

/** PostgreSQL-backed normalized job repository. */
public final class JdbiJobStore implements JobStore {
  private final Jdbi jdbi;
  private final ObjectMapper json;

  public JdbiJobStore(Jdbi jdbi, ObjectMapper objectMapper) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper").copy().findAndRegisterModules();
  }

  @Override
  public JobRecord create(JobRecord job) {
    Objects.requireNonNull(job, "job");
    return jdbi.inTransaction(
        handle -> {
          create(handle, job);
          return find(handle, job.id()).orElseThrow();
        });
  }

  @Override
  public JobRecord save(JobRecord job) {
    Objects.requireNonNull(job, "job");
    jdbi.useHandle(handle -> save(handle, job));
    return job;
  }

  void create(Handle handle, JobRecord job) {
    handle.createUpdate(
            """
            INSERT INTO artifact_job
              (id, tenant_id, artifact_id, job_type, status, attempts, next_attempt_at,
               lease_owner, lease_expires_at, attributes_json, last_error, created_at, updated_at)
            VALUES
              (:id, :tenantId, :artifactId, :jobType, :status, :attempts, :nextAttemptAt,
               :leaseOwner, :leaseExpiresAt, :attributesJson, :lastError, :createdAt, :updatedAt)
            ON CONFLICT (id) DO NOTHING
            """)
        .bind("id", job.id()).bind("tenantId", job.tenantId())
        .bind("artifactId", job.artifactId()).bind("jobType", job.type().name())
        .bind("status", job.status().name()).bind("attempts", job.attempts())
        .bind("nextAttemptAt", job.nextAttemptAt()).bind("leaseOwner", job.leaseOwner())
        .bind("leaseExpiresAt", job.leaseExpiresAt())
        .bind("attributesJson", serialize(job.attributes())).bind("lastError", job.lastError())
        .bind("createdAt", job.createdAt()).bind("updatedAt", job.updatedAt()).execute();
  }

  @Override
  public Optional<JobRecord> find(String jobId) {
    return jdbi.withHandle(handle -> find(handle, jobId));
  }

  @Override
  public JobRecord update(String jobId, UnaryOperator<JobRecord> update) {
    Objects.requireNonNull(jobId, "jobId");
    Objects.requireNonNull(update, "update");
    return jdbi.inTransaction(
        handle -> {
          JobRecord current =
              find(handle, jobId, true)
                  .orElseThrow(() -> new NoSuchElementException("Job not found: " + jobId));
          JobRecord changed = Objects.requireNonNull(update.apply(current), "updated job");
          if (!jobId.equals(changed.id())) {
            throw new IllegalArgumentException("Job update cannot change its id");
          }
          save(handle, changed);
          return changed;
        });
  }

  void save(Handle handle, JobRecord job) {
    handle.createUpdate(
            """
            INSERT INTO artifact_job
              (id, tenant_id, artifact_id, job_type, status, attempts, next_attempt_at,
               lease_owner, lease_expires_at, attributes_json, last_error, created_at, updated_at)
            VALUES
              (:id, :tenantId, :artifactId, :jobType, :status, :attempts, :nextAttemptAt,
               :leaseOwner, :leaseExpiresAt, :attributesJson, :lastError, :createdAt, :updatedAt)
            ON CONFLICT (id) DO UPDATE SET
              tenant_id = EXCLUDED.tenant_id, artifact_id = EXCLUDED.artifact_id,
              job_type = EXCLUDED.job_type, status = EXCLUDED.status,
              attempts = EXCLUDED.attempts, next_attempt_at = EXCLUDED.next_attempt_at,
              lease_owner = EXCLUDED.lease_owner, lease_expires_at = EXCLUDED.lease_expires_at,
              attributes_json = EXCLUDED.attributes_json, last_error = EXCLUDED.last_error,
              updated_at = EXCLUDED.updated_at
            """)
        .bind("id", job.id()).bind("tenantId", job.tenantId())
        .bind("artifactId", job.artifactId()).bind("jobType", job.type().name())
        .bind("status", job.status().name()).bind("attempts", job.attempts())
        .bind("nextAttemptAt", job.nextAttemptAt()).bind("leaseOwner", job.leaseOwner())
        .bind("leaseExpiresAt", job.leaseExpiresAt())
        .bind("attributesJson", serialize(job.attributes())).bind("lastError", job.lastError())
        .bind("createdAt", job.createdAt()).bind("updatedAt", job.updatedAt()).execute();
  }

  Optional<JobRecord> find(Handle handle, String jobId) {
    return find(handle, jobId, false);
  }

  private Optional<JobRecord> find(Handle handle, String jobId, boolean lock) {
    return handle.createQuery(
            """
            SELECT id AS job_id, tenant_id AS job_tenant_id, artifact_id AS job_artifact_id,
                   job_type AS job_type, status AS job_status, attempts AS job_attempts,
                   next_attempt_at AS job_next_attempt_at, lease_owner AS job_lease_owner,
                   lease_expires_at AS job_lease_expires_at,
                   attributes_json AS job_attributes_json, last_error AS job_last_error,
                   created_at AS job_created_at, updated_at AS job_updated_at
            FROM artifact_job WHERE id = :id
            """ + (lock ? " FOR UPDATE" : ""))
        .bind("id", jobId).map((resultSet, context) -> map(resultSet, "job_")).findOne();
  }

  JobRecord map(ResultSet resultSet, String prefix) throws SQLException {
    return new JobRecord(
        resultSet.getString(prefix + "id"), resultSet.getString(prefix + "tenant_id"),
        resultSet.getString(prefix + "artifact_id"),
        JobRecord.JobType.valueOf(resultSet.getString(prefix + "type")),
        JobRecord.JobStatus.valueOf(resultSet.getString(prefix + "status")),
        resultSet.getInt(prefix + "attempts"), instant(resultSet, prefix + "next_attempt_at"),
        resultSet.getString(prefix + "lease_owner"),
        instant(resultSet, prefix + "lease_expires_at"),
        attributes(resultSet.getString(prefix + "attributes_json")),
        resultSet.getString(prefix + "last_error"), instant(resultSet, prefix + "created_at"),
        instant(resultSet, prefix + "updated_at"));
  }

  private String serialize(Map<String, String> attributes) {
    try {
      return json.writeValueAsString(attributes);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Job attributes cannot be serialized", failure);
    }
  }

  private Map<String, String> attributes(String value) {
    try {
      return json.readValue(value, new TypeReference<>() {});
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Stored job attributes are invalid", failure);
    }
  }

  private static Instant instant(ResultSet resultSet, String column) throws SQLException {
    java.sql.Timestamp timestamp = resultSet.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
  }
}
