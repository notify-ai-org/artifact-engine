package dev.notify.artifact.embed;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Thread-safe, bounded LRU cache for local development and single-node deployments. */
public final class InMemoryEmbeddingCache implements EmbeddingCache {
  private final Map<Key, float[]> entries;

  public InMemoryEmbeddingCache(int maximumEntries) {
    if (maximumEntries < 1) {
      throw new IllegalArgumentException("maximumEntries must be positive");
    }
    this.entries =
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<Key, float[]> eldest) {
            return size() > maximumEntries;
          }
        };
  }

  @Override
  public synchronized Optional<float[]> get(String model, String version, String contentSha256) {
    float[] value = entries.get(new Key(model, version, contentSha256));
    return value == null ? Optional.empty() : Optional.of(value.clone());
  }

  @Override
  public synchronized void put(String model, String version, String contentSha256, float[] vector) {
    entries.put(new Key(model, version, contentSha256), vector.clone());
  }

  private record Key(String model, String version, String contentSha256) {}
}
