package dev.notify.artifact.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.embed.EmbeddingCache;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.InvalidatingMetadataStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SearchCachesTest {
  private static final Duration TTL = Duration.ofMinutes(10);

  @Test
  void normalizedQueriesShareOneEmbeddingAndDocumentsNeverEnterTheCache() {
    List<String> embedded = new CopyOnWriteArrayList<>();
    try (EmbeddingService service = service(embedded, new QueryEmbeddingCache(1 << 20, TTL))) {
      service.embedQuery("refund   policy ");
      service.embedQuery("refund policy");
      service.embedQuery("ｒｅｆｕｎｄ policy");
      assertEquals(List.of("refund policy"), embedded, "whitespace and NFKC variants hit");

      service.embedDocuments(List.of("chunk text"));
      service.embedDocuments(List.of("chunk text"));
      service.embedQuery("chunk text");
      assertEquals(4, embedded.size(), "document vectors are neither read nor written by caches");
      assertEquals(2, service.queryCache().stats().entries());
    }
  }

  @Test
  void concurrentIdenticalQueriesMakeOneProviderCall() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    QueryEmbeddingCache cache = new QueryEmbeddingCache(1 << 20, TTL);
    ExecutorService callers = Executors.newFixedThreadPool(8);
    try {
      List<Future<float[]>> results = new ArrayList<>();
      for (int caller = 0; caller < 8; caller++) {
        results.add(callers.submit(() -> cache.get("m", "1", "same query", query -> {
          calls.incrementAndGet();
          try {
            release.await();
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
          }
          return new float[] {1, 2, 3};
        })));
      }
      Thread.sleep(100);
      release.countDown();
      for (Future<float[]> result : results) assertEquals(3, result.get().length);
    } finally {
      callers.shutdownNow();
    }
    assertEquals(1, calls.get());
  }

  @Test
  void theQueryCacheStaysWithinItsByteBudget() {
    QueryEmbeddingCache cache = new QueryEmbeddingCache(64 * 1024, TTL);
    for (int query = 0; query < 200; query++) {
      cache.get("m", "1", "query " + query, ignored -> new float[1536]);
    }
    cache.cleanUp();

    assertTrue(cache.stats().weightedBytes() <= 64 * 1024, cache.stats()::toString);
    assertTrue(cache.stats().evictions() > 0);
  }

  @Test
  void servesRepeatedSearchesAndInvalidatesPerTenant() {
    RetrievalResultCache cache = new RetrievalResultCache(1 << 20, TTL);
    AtomicInteger searches = new AtomicInteger();

    cache.get(search("t1", "p", "refund policy"), () -> hits(searches));
    cache.get(search("t1", "p", " refund  policy"), () -> hits(searches));
    assertEquals(1, searches.get());

    cache.get(search("t2", "p", "refund policy"), () -> hits(searches));
    cache.get(search("t1", "other-principal", "refund policy"), () -> hits(searches));
    assertEquals(3, searches.get(), "tenants and principals never share results");

    cache.invalidateTenant("t1");
    cache.get(search("t1", "p", "refund policy"), () -> hits(searches));
    cache.get(search("t2", "p", "refund policy"), () -> hits(searches));
    assertEquals(4, searches.get(), "only the invalidated tenant searches again");
  }

  @Test
  void aSearchThatRacedAnInvalidationIsNeverServed() {
    RetrievalResultCache cache = new RetrievalResultCache(1 << 20, TTL);
    AtomicInteger searches = new AtomicInteger();

    cache.get(search("t1", "p", "q"), () -> {
      cache.invalidateTenant("t1"); // an index finishes while this search runs
      return hits(searches);
    });
    cache.get(search("t1", "p", "q"), () -> hits(searches));

    assertEquals(2, searches.get());
  }

  @Test
  void cachedHitsCarryNoEmbeddingVectors() {
    RetrievalResultCache cache = new RetrievalResultCache(1 << 20, TTL);
    List<Requests.SearchHit> cached =
        cache.get(search("t1", "p", "q"), () -> hits(new AtomicInteger()));

    assertNull(cached.get(0).chunk().embedding());
    assertEquals("chunk text", cached.get(0).chunk().text());
  }

  @Test
  void metadataUpdatesInvalidateOnlyWhenSearchVisibilityChanges() {
    List<String> invalidated = new ArrayList<>();
    InMemoryStores.Metadata raw = new InMemoryStores.Metadata();
    InvalidatingMetadataStore store = new InvalidatingMetadataStore(raw, invalidated::add);
    raw.save(artifact());

    store.update("t1", "a1", current -> current.withStorage(ArtifactStatus.Storage.STORED, "k"));
    assertEquals(List.of(), invalidated);

    store.update("t1", "a1", current -> current.withIndex(ArtifactStatus.Index.READY));
    store.update("t1", "a1", current -> current.withIndex(ArtifactStatus.Index.READY));
    assertEquals(List.of("t1"), invalidated, "only real transitions count");

    store.update("t1", "a1", current -> current.withStorage(ArtifactStatus.Storage.DELETED, "k"));
    assertEquals(List.of("t1", "t1"), invalidated);
  }

  private static Requests.Search search(String tenant, String principal, String query) {
    return new Requests.Search(tenant, principal, query, 5, List.of(), List.of(), null);
  }

  private static List<Requests.SearchHit> hits(AtomicInteger searches) {
    searches.incrementAndGet();
    ArtifactChunk chunk = new ArtifactChunk("c1", "a1", "t1", 0, "chunk text", 3, null, null,
        Map.of(), "sha", "m", "1", new float[] {1, 2, 3});
    return List.of(new Requests.SearchHit(artifact(), chunk, 0.9, "artifact://a1/chunks/c1"));
  }

  private static Artifact artifact() {
    Instant now = Instant.now();
    return new Artifact("a1", "t1", "k", "f", "UPLOAD", null, "doc.txt", "text/plain", 10, "sha",
        null, null, ArtifactStatus.Storage.SPOOLED, ArtifactStatus.Index.PENDING, 1, Map.of(),
        null, null, now, now);
  }

  private static EmbeddingService service(List<String> embedded, QueryEmbeddingCache queryCache) {
    EmbeddingProvider provider = new EmbeddingProvider() {
      public String model() {
        return "m";
      }

      public String version() {
        return "1";
      }

      public List<float[]> embed(List<String> texts) {
        embedded.addAll(texts);
        return texts.stream().map(text -> new float[] {text.length()}).toList();
      }
    };
    EmbeddingCache none = new EmbeddingCache() {
      public Optional<float[]> get(String model, String version, String hash) {
        return Optional.empty();
      }

      public void put(String model, String version, String hash, float[] vector) {}
    };
    return new EmbeddingService(List.of(provider), none, 8, Duration.ZERO, TTL,
        new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1, 0, failure -> false), queryCache);
  }
}
