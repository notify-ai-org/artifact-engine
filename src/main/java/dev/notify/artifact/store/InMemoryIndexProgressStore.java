package dev.notify.artifact.store;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Process-local index progress for tests and single-node deployments. */
public final class InMemoryIndexProgressStore implements IndexProgressStore {
  private final Map<String, IndexProgress> progress = new ConcurrentHashMap<>();

  @Override
  public Optional<IndexProgress> find(String tenantId, String artifactId, long version) {
    return Optional.ofNullable(progress.get(key(tenantId, artifactId, version)));
  }

  @Override
  public void save(IndexProgress value) {
    progress.put(key(value.tenantId(), value.artifactId(), value.version()), value);
  }

  @Override
  public void delete(String tenantId, String artifactId, long version) {
    progress.remove(key(tenantId, artifactId, version));
  }

  private static String key(String tenantId, String artifactId, long version) {
    return tenantId + "\u0000" + artifactId + "\u0000" + version;
  }
}
