package dev.notify.artifact.store;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Metadata store decorator that reports when an artifact's search visibility may have changed:
 * its index status moved (becoming READY, or leaving READY for a re-index or failure) or it was
 * deleted. Every job's metadata update passes through here, so no job has to remember to
 * invalidate search caches.
 */
public final class InvalidatingMetadataStore implements MetadataStore {
  private final MetadataStore delegate;
  private final Consumer<String> onVisibilityChange;

  /** @param onVisibilityChange receives the tenant id after the change is committed */
  public InvalidatingMetadataStore(MetadataStore delegate, Consumer<String> onVisibilityChange) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.onVisibilityChange = Objects.requireNonNull(onVisibilityChange, "onVisibilityChange");
  }

  @Override
  public Artifact update(String tenantId, String artifactId, UnaryOperator<Artifact> update) {
    AtomicBoolean changed = new AtomicBoolean();
    Artifact updated = delegate.update(tenantId, artifactId, current -> {
      Artifact next = update.apply(current);
      if (current.indexStatus() != next.indexStatus()
          || (next.storageStatus() == ArtifactStatus.Storage.DELETED
              && current.storageStatus() != ArtifactStatus.Storage.DELETED)) {
        changed.set(true);
      }
      return next;
    });
    if (changed.get()) onVisibilityChange.accept(tenantId);
    return updated;
  }

  @Override
  public Artifact save(Artifact artifact) {
    Artifact saved = delegate.save(artifact);
    onVisibilityChange.accept(artifact.tenantId());
    return saved;
  }

  @Override
  public Registration register(Artifact candidate, boolean deduplicateByChecksum) {
    return delegate.register(candidate, deduplicateByChecksum);
  }

  @Override
  public Optional<Artifact> find(String tenantId, String artifactId) {
    return delegate.find(tenantId, artifactId);
  }

  @Override
  public Optional<Artifact> findByIdempotencyKey(String tenantId, String key) {
    return delegate.findByIdempotencyKey(tenantId, key);
  }

  @Override
  public Optional<Artifact> findByChecksum(String tenantId, String sha256) {
    return delegate.findByChecksum(tenantId, sha256);
  }

  @Override
  public Optional<Artifact> findBySpoolPath(Path spoolPath) {
    return delegate.findBySpoolPath(spoolPath);
  }

  @Override
  public List<Artifact> list(String tenantId, int limit) {
    return delegate.list(tenantId, limit);
  }

  @Override
  public List<Artifact> awaitingStorage(int limit) {
    return delegate.awaitingStorage(limit);
  }
}
