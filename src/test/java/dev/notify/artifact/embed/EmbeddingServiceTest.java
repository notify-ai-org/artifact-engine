package dev.notify.artifact.embed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.retry.RetryPolicy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class EmbeddingServiceTest {
  private static final RetryPolicy NO_DELAY_RETRY =
      new RetryPolicy(3, Duration.ZERO, Duration.ZERO, 1, 0, failure -> true);

  @Test
  void coalescesConcurrentRequestsUpToMaximumBatchSize() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    EmbeddingProvider provider = provider("model-a", calls, false);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try (EmbeddingService service =
        new EmbeddingService(
            List.of(provider),
            new InMemoryEmbeddingCache(100),
            2,
            Duration.ofSeconds(1),
            Duration.ofMinutes(1),
            NO_DELAY_RETRY)) {
      List<Callable<List<float[]>>> requests =
          List.of(() -> service.embed(List.of("one")), () -> service.embed(List.of("two")));
      var results = callers.invokeAll(requests);
      assertEquals(1, calls.get());
      assertEquals(3, results.get(0).get().get(0)[0]);
      assertEquals(3, results.get(1).get().get(0)[0]);
    } finally {
      callers.shutdownNow();
    }
  }

  @Test
  void selectsModelsRetriesAndCachesResults() {
    AtomicInteger primaryCalls = new AtomicInteger();
    AtomicInteger secondaryCalls = new AtomicInteger();
    try (EmbeddingService service =
        new EmbeddingService(
            List.of(
                provider("primary", primaryCalls, false),
                provider("secondary", secondaryCalls, true)),
            new InMemoryEmbeddingCache(100),
            8,
            Duration.ZERO,
            Duration.ofMinutes(1),
            NO_DELAY_RETRY)) {
      assertEquals(List.of("primary", "secondary"), service.models());
      service.embed("secondary", List.of("value"));
      service.embed("secondary", List.of("value"));
      assertEquals(2, secondaryCalls.get());
      assertEquals(0, primaryCalls.get());
    }
  }

  private static EmbeddingProvider provider(
      String model, AtomicInteger calls, boolean failFirstCall) {
    return new EmbeddingProvider() {
      @Override
      public String model() {
        return model;
      }

      @Override
      public String version() {
        return "v1";
      }

      @Override
      public List<float[]> embed(List<String> texts) {
        int call = calls.incrementAndGet();
        if (failFirstCall && call == 1) throw new IllegalStateException("temporary");
        assertTrue(texts.size() <= 8);
        return texts.stream().map(text -> new float[] {text.length()}).toList();
      }
    };
  }
}
