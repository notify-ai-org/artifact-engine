package dev.notify.artifact.spring.jpa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.exception.IdempotencyConflictException;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** JPA metadata and transactional-outbox adapter. */
@Repository
@Primary
public class JpaMetadataStore implements MetadataStore {
  private final ArtifactJpaRepository artifacts;
  private final ArtifactOutboxJpaRepository outbox;
  private final ObjectMapper json;

  public JpaMetadataStore(
      ArtifactJpaRepository artifacts,
      ArtifactOutboxJpaRepository outbox,
      ObjectMapper objectMapper) {
    this.artifacts = artifacts;
    this.outbox = outbox;
    this.json = objectMapper.copy().findAndRegisterModules();
  }

  @Override
  @Transactional
  public Artifact save(Artifact artifact) {
    ArtifactEntity entity =
        artifacts
            .findByTenantIdAndId(artifact.tenantId(), artifact.id())
            .orElseGet(() -> toEntity(artifact));
    entity.replace(artifact, serialize(artifact));
    artifacts.save(entity);
    return artifact;
  }

  @Override
  @Transactional
  public Artifact update(
      String tenantId, String artifactId, UnaryOperator<Artifact> updateFunction) {
    ArtifactEntity entity =
        artifacts
            .findLockedByTenantIdAndId(tenantId, artifactId)
            .orElseThrow(() -> new NoSuchElementException("Artifact not found: " + artifactId));
    Artifact updated = updateFunction.apply(deserializeArtifact(entity.getPayloadJson()));
    if (!tenantId.equals(updated.tenantId()) || !artifactId.equals(updated.id())) {
      throw new IllegalArgumentException("Artifact identity cannot be changed by an update");
    }
    entity.replace(updated, serialize(updated));
    artifacts.save(entity);
    return updated;
  }

  @Override
  @Transactional
  public synchronized Registration register(
      Artifact candidate, List<JobRecord> initialOperations, boolean deduplicateByChecksum) {
    Optional<Artifact> idempotent =
        findByIdempotencyKey(candidate.tenantId(), candidate.idempotencyKey());
    if (idempotent.isPresent()) {
      Artifact existing = idempotent.get();
      if (!Objects.equals(existing.idempotencyFingerprint(), candidate.idempotencyFingerprint())) {
        throw new IdempotencyConflictException(candidate.idempotencyKey());
      }
      return new Registration(Registration.Outcome.IDEMPOTENT_REPLAY, existing);
    }

    if (deduplicateByChecksum) {
      Optional<Artifact> duplicate = findByChecksum(candidate.tenantId(), candidate.sha256());
      if (duplicate.isPresent()) {
        return new Registration(Registration.Outcome.CONTENT_DUPLICATE, duplicate.get());
      }
    }

    artifacts.save(toEntity(candidate));
    outbox.saveAll(
        initialOperations.stream()
            .map(
                operation ->
                    new ArtifactOutboxEntity(
                        operation.id(), serialize(operation), operation.createdAt()))
            .toList());
    return new Registration(Registration.Outcome.CREATED, candidate);
  }

  @Override
  @Transactional(readOnly = true)
  public Optional<Artifact> find(String tenantId, String artifactId) {
    return artifacts
        .findByTenantIdAndId(tenantId, artifactId)
        .map(entity -> deserializeArtifact(entity.getPayloadJson()));
  }

  @Override
  @Transactional(readOnly = true)
  public Optional<Artifact> findByIdempotencyKey(String tenantId, String key) {
    if (key == null) {
      return Optional.empty();
    }
    return artifacts
        .findByTenantIdAndIdempotencyKey(tenantId, key)
        .map(entity -> deserializeArtifact(entity.getPayloadJson()));
  }

  @Override
  @Transactional(readOnly = true)
  public Optional<Artifact> findByChecksum(String tenantId, String sha256) {
    return artifacts
        .findFirstByTenantIdAndSha256(tenantId, sha256)
        .map(entity -> deserializeArtifact(entity.getPayloadJson()));
  }

  @Override
  @Transactional(readOnly = true)
  public Optional<Artifact> findBySpoolPath(Path spoolPath) {
    return artifacts
        .findBySpoolPath(path(spoolPath))
        .map(entity -> deserializeArtifact(entity.getPayloadJson()));
  }

  @Override
  @Transactional(readOnly = true)
  public List<Artifact> list(String tenantId, int limit) {
    return artifacts
        .findByTenantIdOrderByCreatedAtDesc(tenantId, PageRequest.of(0, Math.max(1, limit)))
        .stream()
        .map(entity -> deserializeArtifact(entity.getPayloadJson()))
        .toList();
  }

  @Override
  @Transactional
  public List<JobRecord> outboxBatch(int limit) {
    return outbox.findAllByOrderByCreatedAtAsc(PageRequest.of(0, Math.max(1, limit))).stream()
        .map(entity -> deserializeJob(entity.getJobJson()))
        .toList();
  }

  @Override
  @Transactional
  public void markOutboxDispatched(String operationId) {
    outbox.deleteById(operationId);
  }

  private ArtifactEntity toEntity(Artifact artifact) {
    return new ArtifactEntity(artifact, serialize(artifact));
  }

  private Artifact deserializeArtifact(String value) {
    return deserialize(value, Artifact.class);
  }

  private JobRecord deserializeJob(String value) {
    return deserialize(value, JobRecord.class);
  }

  private String serialize(Object value) {
    try {
      return json.writeValueAsString(value);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Persistence payload cannot be serialized", failure);
    }
  }

  private <T> T deserialize(String value, Class<T> type) {
    try {
      return json.readValue(value, type);
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Persistence payload is not valid", failure);
    }
  }

  private static String path(Path value) {
    return value == null ? null : value.toAbsolutePath().normalize().toString();
  }
}
