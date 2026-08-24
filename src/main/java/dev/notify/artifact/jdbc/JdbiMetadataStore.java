package dev.notify.artifact.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.exception.IdempotencyConflictException;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.store.MetadataStore;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

/** PostgreSQL metadata and transactional-outbox store implemented without Spring JDBC or JPA. */
public final class JdbiMetadataStore implements MetadataStore {
  private static final int MAX_LIMIT = 1_000;

  private final Jdbi jdbi;
  private final ObjectMapper json;

  public JdbiMetadataStore(Jdbi jdbi, ObjectMapper objectMapper) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper").copy().findAndRegisterModules();
  }

  @Override
  public Artifact save(Artifact artifact) {
    Objects.requireNonNull(artifact, "artifact");
    int changed =
        jdbi.withHandle(
            handle ->
                bindArtifact(
                        handle.createUpdate(
                            """
                            INSERT INTO artifact (
                                id, tenant_id, idempotency_key, idempotency_fingerprint, sha256,
                                spool_path, created_at, updated_at, original_name, media_type,
                                size_bytes, storage_key, storage_status, index_status,
                                artifact_version, tags_csv, payload_json
                            ) VALUES (
                                :id, :tenantId, :idempotencyKey, :idempotencyFingerprint, :sha256,
                                :spoolPath, :createdAt, :updatedAt, :originalName, :mediaType,
                                :sizeBytes, :storageKey, :storageStatus, :indexStatus,
                                :artifactVersion, :tagsCsv, :payloadJson
                            )
                            ON CONFLICT (id) DO UPDATE SET
                                spool_path = EXCLUDED.spool_path,
                                updated_at = EXCLUDED.updated_at,
                                original_name = EXCLUDED.original_name,
                                media_type = EXCLUDED.media_type,
                                size_bytes = EXCLUDED.size_bytes,
                                storage_key = EXCLUDED.storage_key,
                                storage_status = EXCLUDED.storage_status,
                                index_status = EXCLUDED.index_status,
                                artifact_version = EXCLUDED.artifact_version,
                                tags_csv = EXCLUDED.tags_csv,
                                payload_json = EXCLUDED.payload_json,
                                lock_version = artifact.lock_version + 1
                            WHERE artifact.tenant_id = EXCLUDED.tenant_id
                            """),
                        artifact)
                    .execute());
    if (changed != 1) {
      throw new IllegalStateException("Artifact id belongs to another tenant: " + artifact.id());
    }
    return artifact;
  }

  @Override
  public Artifact update(
      String tenantId, String artifactId, UnaryOperator<Artifact> updateFunction) {
    requireIdentity(tenantId, artifactId);
    Objects.requireNonNull(updateFunction, "updateFunction");
    return jdbi.inTransaction(
        handle -> {
          Artifact current =
              find(handle, tenantId, artifactId, true)
                  .orElseThrow(
                      () -> new NoSuchElementException("Artifact not found: " + artifactId));
          Artifact updated = Objects.requireNonNull(updateFunction.apply(current), "updated artifact");
          if (!tenantId.equals(updated.tenantId()) || !artifactId.equals(updated.id())) {
            throw new IllegalArgumentException("Artifact identity cannot be changed by an update");
          }
          int changed =
              bindArtifact(
                      handle.createUpdate(
                          """
                          UPDATE artifact SET
                              spool_path = :spoolPath,
                              updated_at = :updatedAt,
                              original_name = :originalName,
                              media_type = :mediaType,
                              size_bytes = :sizeBytes,
                              storage_key = :storageKey,
                              storage_status = :storageStatus,
                              index_status = :indexStatus,
                              artifact_version = :artifactVersion,
                              tags_csv = :tagsCsv,
                              payload_json = :payloadJson,
                              lock_version = lock_version + 1
                          WHERE tenant_id = :tenantId AND id = :id
                          """),
                      updated)
                  .execute();
          if (changed != 1) {
            throw new IllegalStateException("Concurrent artifact update failed: " + artifactId);
          }
          return updated;
        });
  }

  @Override
  public Registration register(
      Artifact candidate, boolean deduplicateByChecksum) {
    Objects.requireNonNull(candidate, "candidate");
    return jdbi.inTransaction(
        handle -> {
          Optional<Artifact> idempotent =
              findByIdempotencyKey(handle, candidate.tenantId(), candidate.idempotencyKey());
          if (idempotent.isPresent()) {
            return replay(candidate, idempotent.get());
          }
          if (deduplicateByChecksum) {
            Optional<Artifact> duplicate =
                findByChecksum(handle, candidate.tenantId(), candidate.sha256());
            if (duplicate.isPresent()) {
              return new Registration(Registration.Outcome.CONTENT_DUPLICATE, duplicate.get());
            }
          }

          int inserted = insert(handle, candidate);
          if (inserted == 0) {
            Artifact raced =
                findByIdempotencyKey(handle, candidate.tenantId(), candidate.idempotencyKey())
                    .orElseThrow(
                        () -> new IllegalStateException("Artifact registration conflict"));
            return replay(candidate, raced);
          }
          return new Registration(Registration.Outcome.CREATED, candidate);
        });
  }

  @Override
  public Optional<Artifact> find(String tenantId, String artifactId) {
    requireIdentity(tenantId, artifactId);
    return jdbi.withHandle(handle -> find(handle, tenantId, artifactId, false));
  }

  @Override
  public Optional<Artifact> findByIdempotencyKey(String tenantId, String key) {
    requireTenant(tenantId);
    if (key == null) return Optional.empty();
    return jdbi.withHandle(handle -> findByIdempotencyKey(handle, tenantId, key));
  }

  @Override
  public Optional<Artifact> findByChecksum(String tenantId, String sha256) {
    requireTenant(tenantId);
    if (sha256 == null) return Optional.empty();
    return jdbi.withHandle(handle -> findByChecksum(handle, tenantId, sha256));
  }

  @Override
  public Optional<Artifact> findBySpoolPath(Path spoolPath) {
    if (spoolPath == null) return Optional.empty();
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT payload_json FROM artifact WHERE spool_path = :spoolPath")
                .bind("spoolPath", path(spoolPath))
                .mapTo(String.class)
                .findOne()
                .map(this::deserializeArtifact));
  }

  @Override
  public List<Artifact> list(String tenantId, int limit) {
    requireTenant(tenantId);
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT payload_json FROM artifact
                    WHERE tenant_id = :tenantId
                    ORDER BY created_at DESC LIMIT :limit
                    """)
                .bind("tenantId", tenantId)
                .bind("limit", boundedLimit(limit))
                .mapTo(String.class)
                .map(this::deserializeArtifact)
                .list());
  }

  private int insert(Handle handle, Artifact artifact) {
    return bindArtifact(
            handle.createUpdate(
                """
                INSERT INTO artifact (
                    id, tenant_id, idempotency_key, idempotency_fingerprint, sha256,
                    spool_path, created_at, updated_at, original_name, media_type,
                    size_bytes, storage_key, storage_status, index_status,
                    artifact_version, tags_csv, payload_json
                ) VALUES (
                    :id, :tenantId, :idempotencyKey, :idempotencyFingerprint, :sha256,
                    :spoolPath, :createdAt, :updatedAt, :originalName, :mediaType,
                    :sizeBytes, :storageKey, :storageStatus, :indexStatus,
                    :artifactVersion, :tagsCsv, :payloadJson
                )
                ON CONFLICT DO NOTHING
                """),
            artifact)
        .execute();
  }

  private org.jdbi.v3.core.statement.Update bindArtifact(
      org.jdbi.v3.core.statement.Update statement, Artifact artifact) {
    return statement
        .bind("id", artifact.id())
        .bind("tenantId", artifact.tenantId())
        .bind("idempotencyKey", artifact.idempotencyKey())
        .bind("idempotencyFingerprint", artifact.idempotencyFingerprint())
        .bind("sha256", artifact.sha256())
        .bindByType("spoolPath", path(artifact.spoolPath()), String.class)
        .bind("createdAt", artifact.createdAt())
        .bind("updatedAt", artifact.updatedAt())
        .bind("originalName", artifact.originalName())
        .bind("mediaType", artifact.mediaType())
        .bind("sizeBytes", artifact.sizeBytes())
        .bindByType("storageKey", artifact.storageKey(), String.class)
        .bind("storageStatus", artifact.storageStatus().name())
        .bind("indexStatus", artifact.indexStatus().name())
        .bind("artifactVersion", artifact.version())
        .bindByType("tagsCsv", normalizeTags(artifact.metadata().get("tags")), String.class)
        .bind("payloadJson", serialize(artifact));
  }

  private Optional<Artifact> find(
      Handle handle, String tenantId, String artifactId, boolean forUpdate) {
    String suffix = forUpdate ? " FOR UPDATE" : "";
    return handle
        .createQuery(
            "SELECT payload_json FROM artifact WHERE tenant_id = :tenantId AND id = :id" + suffix)
        .bind("tenantId", tenantId)
        .bind("id", artifactId)
        .mapTo(String.class)
        .findOne()
        .map(this::deserializeArtifact);
  }

  private Optional<Artifact> findByIdempotencyKey(Handle handle, String tenantId, String key) {
    return handle
        .createQuery(
            """
            SELECT payload_json FROM artifact
            WHERE tenant_id = :tenantId AND idempotency_key = :key
            """)
        .bind("tenantId", tenantId)
        .bind("key", key)
        .mapTo(String.class)
        .findOne()
        .map(this::deserializeArtifact);
  }

  private Optional<Artifact> findByChecksum(Handle handle, String tenantId, String sha256) {
    return handle
        .createQuery(
            """
            SELECT payload_json FROM artifact
            WHERE tenant_id = :tenantId AND sha256 = :sha256
            ORDER BY created_at LIMIT 1
            """)
        .bind("tenantId", tenantId)
        .bind("sha256", sha256)
        .mapTo(String.class)
        .findOne()
        .map(this::deserializeArtifact);
  }

  private Registration replay(Artifact candidate, Artifact existing) {
    if (!Objects.equals(existing.idempotencyFingerprint(), candidate.idempotencyFingerprint())) {
      throw new IdempotencyConflictException(candidate.idempotencyKey());
    }
    return new Registration(Registration.Outcome.IDEMPOTENT_REPLAY, existing);
  }

  private String serialize(Object value) {
    try {
      return json.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Persistence payload cannot be serialized", exception);
    }
  }

  private Artifact deserializeArtifact(String value) {
    return deserialize(value, Artifact.class);
  }

  private <T> T deserialize(String value, Class<T> type) {
    try {
      return json.readValue(value, type);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Persistence payload is invalid", exception);
    }
  }

  private static String normalizeTags(String tags) {
    if (tags == null || tags.isBlank()) return null;
    return Arrays.stream(tags.split(","))
        .map(String::trim)
        .filter(tag -> !tag.isBlank())
        .distinct()
        .sorted()
        .collect(java.util.stream.Collectors.joining(","));
  }

  private static String path(Path value) {
    return value == null ? null : value.toAbsolutePath().normalize().toString();
  }

  private static int boundedLimit(int limit) {
    return Math.max(1, Math.min(limit, MAX_LIMIT));
  }

  private static void requireIdentity(String tenantId, String artifactId) {
    requireTenant(tenantId);
    if (artifactId == null || artifactId.isBlank()) {
      throw new IllegalArgumentException("artifactId is required");
    }
  }

  private static void requireTenant(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId is required");
    }
  }
}
