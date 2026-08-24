package dev.notify.artifact.store;

import dev.notify.artifact.model.Artifact;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

public interface MetadataStore extends Store<Artifact> {
  Artifact save(Artifact artifact);

  /** Atomically applies a lifecycle update to the latest persisted version. */
  Artifact update(String tenantId, String artifactId, UnaryOperator<Artifact> update);

  /** Atomically stores a new artifact */
  Registration register(
      Artifact candidate, boolean deduplicateByChecksum);

  Optional<Artifact> find(String tenantId, String artifactId);

  Optional<Artifact> findByIdempotencyKey(String tenantId, String key);

  Optional<Artifact> findByChecksum(String tenantId, String sha256);

  Optional<Artifact> findBySpoolPath(Path spoolPath);

  List<Artifact> list(String tenantId, int limit);

  record Registration(Outcome outcome, Artifact artifact) {
    public enum Outcome {
      CREATED,
      IDEMPOTENT_REPLAY,
      CONTENT_DUPLICATE
    }
  }
}
