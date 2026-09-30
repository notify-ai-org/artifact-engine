package dev.notify.artifact.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.util.StructuredLog;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Search results keyed by tenant, principal, query, and filters.
 *
 * <p><b>Consistency.</b> Every tenant has a generation number that {@link #invalidateTenant}
 * advances whenever an artifact's search visibility changes (see {@code
 * InvalidatingMetadataStore}). The generation is part of the key and is read before searching,
 * so a search that raced an index change is stored under the old generation and never served.
 * Invalidation is local to this process: other instances converge within the TTL.
 *
 * <p><b>Memory.</b> Bounded by an estimate of the bytes held; hits are cached without their
 * embedding vectors, which callers never need. Entries expire a configurable time after their
 * last read. The principal is part of the key so a future per-principal access rule cannot leak
 * cached hits across principals.
 */
public final class RetrievalResultCache {
  private static final StructuredLog LOG = StructuredLog.of(RetrievalResultCache.class);
  private static final int HIT_OVERHEAD_BYTES = 1024;

  private final Cache<Key, List<Requests.SearchHit>> results;
  private final Map<String, AtomicLong> generations = new ConcurrentHashMap<>();

  public RetrievalResultCache(long maxBytes, Duration expireAfterAccess) {
    if (maxBytes < 1) throw new IllegalArgumentException("maxBytes must be positive");
    this.results = Caffeine.newBuilder()
        .maximumWeight(maxBytes)
        .weigher((Key key, List<Requests.SearchHit> hits) -> weigh(key, hits))
        .expireAfterAccess(Objects.requireNonNull(expireAfterAccess, "expireAfterAccess"))
        .recordStats()
        .build();
  }

  /** Returns cached hits for the request, or runs {@code search} and caches its result. */
  public List<Requests.SearchHit> get(
      Requests.Search request, Supplier<List<Requests.SearchHit>> search) {
    Key key = new Key(
        request.tenantId(),
        request.principalId(),
        generation(request.tenantId()).get(),
        QueryText.normalize(request.query()),
        request.limit(),
        request.mediaTypes().stream().sorted().toList(),
        request.tags().stream().sorted().toList(),
        request.createdAfter());
    return results.get(key, ignored -> withoutVectors(search.get()));
  }

  /** Drops every cached result for the tenant; searches started earlier cannot repopulate it. */
  public void invalidateTenant(String tenantId) {
    generation(tenantId).incrementAndGet();
    results.asMap().keySet().removeIf(key -> key.tenantId().equals(tenantId));
    LOG.debug("retrieval_cache_invalidated", "tenantGeneration", generation(tenantId).get());
  }

  public QueryEmbeddingCache.Stats stats() {
    var stats = results.stats();
    return new QueryEmbeddingCache.Stats(stats.hitCount(), stats.missCount(),
        stats.evictionCount(),
        results.policy().eviction().map(e -> e.weightedSize().orElse(0)).orElse(0L),
        results.estimatedSize());
  }

  /** Runs pending evictions now; Caffeine otherwise amortizes them over later operations. */
  public void cleanUp() {
    results.cleanUp();
  }

  private AtomicLong generation(String tenantId) {
    return generations.computeIfAbsent(tenantId, ignored -> new AtomicLong());
  }

  private static List<Requests.SearchHit> withoutVectors(List<Requests.SearchHit> hits) {
    return hits.stream()
        .map(hit -> {
          ArtifactChunk chunk = hit.chunk();
          ArtifactChunk slim = new ArtifactChunk(chunk.id(), chunk.artifactId(), chunk.tenantId(),
              chunk.index(), chunk.text(), chunk.tokenCount(), chunk.pageNumber(),
              chunk.section(), chunk.coordinates(), chunk.contentSha256(),
              chunk.embeddingModel(), chunk.embeddingVersion(), null);
          return new Requests.SearchHit(hit.artifact(), slim, hit.score(), hit.resourceUri());
        })
        .toList();
  }

  private static int weigh(Key key, List<Requests.SearchHit> hits) {
    long bytes = 256L + key.query().length() * 2L;
    for (Requests.SearchHit hit : hits) {
      bytes += HIT_OVERHEAD_BYTES + hit.chunk().text().length() * 2L;
    }
    return (int) Math.min(Integer.MAX_VALUE, bytes);
  }

  private record Key(
      String tenantId,
      String principalId,
      long generation,
      String query,
      int limit,
      List<String> mediaTypes,
      List<String> tags,
      Instant createdAfter) {}
}
