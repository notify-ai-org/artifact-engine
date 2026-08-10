package dev.notify.artifact.store;

import dev.notify.artifact.exception.IdempotencyConflictException;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.util.VectorUtils;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/** Thread-safe development stores; production deployments should use durable adapters. */
public final class InMemoryStores {
  private static final String KEY_SEPARATOR = "\u0000";

  private InMemoryStores() {}

  private static String key(String tenantId, String id) {
    return tenantId + KEY_SEPARATOR + id;
  }

  public static final class Metadata implements MetadataStore {
    private final Map<String, Artifact> artifacts = new ConcurrentHashMap<>();
    private final Map<String, JobRecord> outbox = new LinkedHashMap<>();

    @Override
    public Artifact save(Artifact artifact) {
      artifacts.put(key(artifact.tenantId(), artifact.id()), artifact);
      return artifact;
    }

    @Override
    public Artifact update(
        String tenantId, String artifactId, UnaryOperator<Artifact> updateFunction) {
      return artifacts.compute(
          key(tenantId, artifactId),
          (ignored, current) -> {
            if (current == null) {
              throw new NoSuchElementException("Artifact not found: " + artifactId);
            }
            Artifact updated = updateFunction.apply(current);
            if (!tenantId.equals(updated.tenantId()) || !artifactId.equals(updated.id())) {
              throw new IllegalArgumentException(
                  "Artifact identity cannot be changed by an update");
            }
            return updated;
          });
    }

    @Override
    public synchronized Registration register(
        Artifact candidate, List<JobRecord> initialOperations, boolean deduplicateByChecksum) {
      Optional<Artifact> idempotent =
          findByIdempotencyKey(candidate.tenantId(), candidate.idempotencyKey());
      if (idempotent.isPresent()) {
        Artifact existing = idempotent.get();
        if (!Objects.equals(
            existing.idempotencyFingerprint(), candidate.idempotencyFingerprint())) {
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
      artifacts.put(key(candidate.tenantId(), candidate.id()), candidate);
      initialOperations.forEach(operation -> outbox.putIfAbsent(operation.id(), operation));
      return new Registration(Registration.Outcome.CREATED, candidate);
    }

    @Override
    public Optional<Artifact> find(String tenantId, String artifactId) {
      return Optional.ofNullable(artifacts.get(key(tenantId, artifactId)));
    }

    @Override
    public Optional<Artifact> findByIdempotencyKey(String tenantId, String idempotencyKey) {
      return artifacts.values().stream()
          .filter(
              artifact ->
                  tenantId.equals(artifact.tenantId())
                      && Objects.equals(idempotencyKey, artifact.idempotencyKey()))
          .findFirst();
    }

    @Override
    public Optional<Artifact> findByChecksum(String tenantId, String sha256) {
      return artifacts.values().stream()
          .filter(
              artifact -> tenantId.equals(artifact.tenantId()) && sha256.equals(artifact.sha256()))
          .findFirst();
    }

    @Override
    public Optional<Artifact> findBySpoolPath(Path spoolPath) {
      Path normalized = spoolPath.toAbsolutePath().normalize();
      return artifacts.values().stream()
          .filter(artifact -> artifact.spoolPath() != null)
          .filter(artifact -> artifact.spoolPath().toAbsolutePath().normalize().equals(normalized))
          .findFirst();
    }

    @Override
    public List<Artifact> list(String tenantId, int limit) {
      return artifacts.values().stream()
          .filter(artifact -> tenantId.equals(artifact.tenantId()))
          .sorted(Comparator.comparing(Artifact::createdAt).reversed())
          .limit(Math.max(0, limit))
          .toList();
    }

    @Override
    public synchronized List<JobRecord> outboxBatch(int limit) {
      return outbox.values().stream().limit(Math.max(0, limit)).toList();
    }

    @Override
    public synchronized void markOutboxDispatched(String operationId) {
      outbox.remove(operationId);
    }
  }

  public static final class Vectors implements VectorStore {
    private final Map<String, ArtifactChunk> chunks = new ConcurrentHashMap<>();

    @Override
    public void upsert(ArtifactChunk chunk) {
      chunks.put(key(chunk.tenantId(), chunk.id()), chunk);
    }

    @Override
    public List<ScoredChunk> search(
        String tenantId, float[] query, int limit, SearchFilter filter) {
      return chunks.values().stream()
          .filter(chunk -> tenantId.equals(chunk.tenantId()) && chunk.embedding() != null)
          .map(chunk -> new ScoredChunk(chunk, VectorUtils.cosine(query, chunk.embedding())))
          .sorted(Comparator.comparingDouble(ScoredChunk::score).reversed())
          .limit(Math.max(0, limit))
          .toList();
    }

    @Override
    public List<ScoredChunk> keywordSearch(
        String tenantId, String query, int limit, SearchFilter filter) {
      List<String> terms =
          Arrays.stream(query.toLowerCase(Locale.ROOT).split("\\s+"))
              .filter(term -> !term.isBlank())
              .distinct()
              .toList();
      return chunks.values().stream()
          .filter(chunk -> tenantId.equals(chunk.tenantId()))
          .map(chunk -> new ScoredChunk(chunk, keywordScore(chunk.text(), terms)))
          .filter(scored -> scored.score() > 0)
          .sorted(Comparator.comparingDouble(ScoredChunk::score).reversed())
          .limit(Math.max(0, limit))
          .toList();
    }

    @Override
    public List<ArtifactChunk> chunks(String tenantId, String artifactId) {
      return chunks.values().stream()
          .filter(
              chunk -> tenantId.equals(chunk.tenantId()) && artifactId.equals(chunk.artifactId()))
          .sorted(Comparator.comparingInt(ArtifactChunk::index))
          .toList();
    }

    @Override
    public void deleteArtifact(String tenantId, String artifactId) {
      chunks
          .entrySet()
          .removeIf(
              entry ->
                  tenantId.equals(entry.getValue().tenantId())
                      && artifactId.equals(entry.getValue().artifactId()));
    }
  }

  private static double keywordScore(String text, List<String> terms) {
    if (terms.isEmpty()) {
      return 0;
    }
    String normalized = text.toLowerCase(Locale.ROOT);
    long matches = terms.stream().filter(normalized::contains).count();
    return (double) matches / terms.size();
  }
}
