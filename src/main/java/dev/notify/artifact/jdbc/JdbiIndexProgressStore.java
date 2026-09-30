package dev.notify.artifact.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.store.IndexProgressStore;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.jdbi.v3.core.Jdbi;

/** PostgreSQL index progress; the carried state is stored as one JSON document per version. */
public final class JdbiIndexProgressStore implements IndexProgressStore {
  private final Jdbi jdbi;
  private final ObjectMapper json;

  public JdbiIndexProgressStore(Jdbi jdbi, ObjectMapper objectMapper) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper").copy().findAndRegisterModules();
  }

  @Override
  public Optional<IndexProgress> find(String tenantId, String artifactId, long version) {
    return jdbi.withHandle(handle -> handle
        .createQuery(
            """
            SELECT state_json FROM artifact_index_progress
            WHERE tenant_id = :tenantId AND artifact_id = :artifactId AND version = :version
            """)
        .bind("tenantId", tenantId)
        .bind("artifactId", artifactId)
        .bind("version", version)
        .mapTo(String.class)
        .findOne()
        .map(this::deserialize));
  }

  @Override
  public void save(IndexProgress progress) {
    jdbi.useHandle(handle -> handle
        .createUpdate(
            """
            INSERT INTO artifact_index_progress
              (tenant_id, artifact_id, version, last_chunk, state_json, updated_at)
            VALUES (:tenantId, :artifactId, :version, :lastChunk, :state, :updatedAt)
            ON CONFLICT (tenant_id, artifact_id, version) DO UPDATE SET
              last_chunk = EXCLUDED.last_chunk, state_json = EXCLUDED.state_json,
              updated_at = EXCLUDED.updated_at
            """)
        .bind("tenantId", progress.tenantId())
        .bind("artifactId", progress.artifactId())
        .bind("version", progress.version())
        .bind("lastChunk", progress.lastChunk())
        .bind("state", serialize(progress))
        .bind("updatedAt", Instant.now())
        .execute());
  }

  @Override
  public void delete(String tenantId, String artifactId, long version) {
    jdbi.useHandle(handle -> handle
        .createUpdate(
            """
            DELETE FROM artifact_index_progress
            WHERE tenant_id = :tenantId AND artifact_id = :artifactId AND version = :version
            """)
        .bind("tenantId", tenantId)
        .bind("artifactId", artifactId)
        .bind("version", version)
        .execute());
  }

  private String serialize(IndexProgress progress) {
    try {
      return json.writeValueAsString(progress);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Index progress cannot be serialized", failure);
    }
  }

  private IndexProgress deserialize(String value) {
    try {
      return json.readValue(value, IndexProgress.class);
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Stored index progress is invalid", failure);
    }
  }
}
