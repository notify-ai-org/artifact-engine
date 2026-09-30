package dev.notify.artifact.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.notify.artifact.util.Checksum;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * Query vectors keyed by model, model version, and the normalized query text.
 *
 * <p>Bounded by bytes, not entries, so the budget holds whatever the model's dimension. Entries
 * expire a configurable time after their last read. Eviction is Caffeine's W-TinyLFU: close to
 * LRU, but a burst of one-off queries cannot flush the popular ones. Concurrent requests for the
 * same missing key share one embedding call. Document chunks never pass through here; they are
 * persisted in the vector store and would only evict queries.
 */
public final class QueryEmbeddingCache {
  /** Key, map node, and float[] header overhead per entry, in bytes (approximate). */
  private static final int ENTRY_OVERHEAD_BYTES = 200;

  private final Cache<Key, float[]> vectors;

  public QueryEmbeddingCache(long maxBytes, Duration expireAfterAccess) {
    if (maxBytes < 1) throw new IllegalArgumentException("maxBytes must be positive");
    this.vectors = Caffeine.newBuilder()
        .maximumWeight(maxBytes)
        .weigher((Key key, float[] vector) -> ENTRY_OVERHEAD_BYTES + vector.length * Float.BYTES)
        .expireAfterAccess(Objects.requireNonNull(expireAfterAccess, "expireAfterAccess"))
        .recordStats()
        .build();
  }

  /**
   * Returns the cached vector for the query, or embeds the normalized query with {@code embedder}
   * and caches the result. The returned array is a copy the caller may modify.
   */
  public float[] get(
      String model, String version, String query, Function<String, float[]> embedder) {
    String normalized = QueryText.normalize(query);
    float[] vector = vectors.get(
        new Key(model, version, Checksum.sha256(normalized)),
        key -> embedder.apply(normalized).clone());
    return vector.clone();
  }

  public Stats stats() {
    var stats = vectors.stats();
    return new Stats(stats.hitCount(), stats.missCount(), stats.evictionCount(),
        vectors.policy().eviction().map(e -> e.weightedSize().orElse(0)).orElse(0L),
        vectors.estimatedSize());
  }

  public void invalidateAll() {
    vectors.invalidateAll();
  }

  /** Runs pending evictions now; Caffeine otherwise amortizes them over later operations. */
  public void cleanUp() {
    vectors.cleanUp();
  }

  /** @param weightedBytes approximate bytes currently held */
  public record Stats(long hits, long misses, long evictions, long weightedBytes, long entries) {}

  private record Key(String model, String version, String querySha256) {}
}
