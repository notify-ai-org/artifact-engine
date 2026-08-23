package dev.notify.artifact.embed;

import dev.notify.artifact.retry.RetryExecutor;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.util.Checksum;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Coalesces embedding requests by model, retries provider calls, and caches successful vectors. */
public final class EmbeddingService implements AutoCloseable {
  private final Map<String, ModelBatcher> batchers;
  private final String defaultModel;
  private final EmbeddingCache cache;
  private final Duration cacheTtl;
  private final ScheduledExecutorService executor;

  public EmbeddingService(EmbeddingProvider provider, EmbeddingCache cache, int maxBatchSize) {
    this(
        List.of(provider),
        cache,
        maxBatchSize,
        Duration.ZERO,
        Duration.ofHours(1),
        RetryPolicy.defaults());
  }

  public EmbeddingService(
      List<EmbeddingProvider> providers,
      EmbeddingCache cache,
      int maxBatchSize,
      Duration maxWait,
      Duration cacheTtl,
      RetryPolicy retryPolicy) {
    if (providers == null || providers.isEmpty()) {
      throw new IllegalArgumentException("at least one embedding model is required");
    }
    if (maxBatchSize < 1) throw new IllegalArgumentException("maxBatchSize must be positive");
    if (maxWait == null || maxWait.isNegative()) {
      throw new IllegalArgumentException("maxWait must not be negative");
    }
    if (cacheTtl == null || cacheTtl.isZero() || cacheTtl.isNegative()) {
      throw new IllegalArgumentException("cacheTtl must be positive");
    }
    this.cache = Objects.requireNonNull(cache, "cache");
    this.cacheTtl = cacheTtl;
    RetryPolicy policy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    ThreadFactory threads =
        task -> {
          Thread thread = new Thread(task, "artifact-embedding-batcher");
          thread.setDaemon(true);
          return thread;
        };
    this.executor = Executors.newScheduledThreadPool(providers.size(), threads);
    Map<String, ModelBatcher> configured = new LinkedHashMap<>();
    for (EmbeddingProvider provider : providers) {
      Objects.requireNonNull(provider, "embedding provider");
      if (provider.model() == null || provider.model().isBlank()) {
        throw new IllegalArgumentException("embedding model must not be blank");
      }
      if (configured.putIfAbsent(
              provider.model(), new ModelBatcher(provider, maxBatchSize, maxWait, policy))
          != null) {
        throw new IllegalArgumentException("duplicate embedding model: " + provider.model());
      }
    }
    this.batchers = Collections.unmodifiableMap(new LinkedHashMap<>(configured));
    this.defaultModel = providers.get(0).model();
  }

  public List<float[]> embed(List<String> texts) {
    return embed(defaultModel, texts);
  }

  public List<float[]> embed(String model, List<String> texts) {
    ModelBatcher batcher = batchers.get(model);
    if (batcher == null) throw new IllegalArgumentException("Unknown embedding model: " + model);
    Objects.requireNonNull(texts, "texts");
    List<float[]> output = new ArrayList<>(Collections.nCopies(texts.size(), null));
    List<Pending> misses = new ArrayList<>();
    for (int index = 0; index < texts.size(); index++) {
      String text = Objects.requireNonNull(texts.get(index), "embedding text");
      String hash = Checksum.sha256(text);
      var hit = cache.get(batcher.provider.model(), batcher.provider.version(), hash);
      if (hit.isPresent()) output.set(index, hit.get());
      else misses.add(new Pending(index, text, hash, new CompletableFuture<>()));
    }
    batcher.submit(misses);
    for (Pending pending : misses) {
      try {
        output.set(pending.index, pending.result.join());
      } catch (CompletionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException runtime) throw runtime;
        throw new IllegalStateException("Embedding provider failed", cause);
      }
    }
    return output;
  }

  public List<String> models() {
    return List.copyOf(batchers.keySet());
  }

  public String model() {
    return defaultModel;
  }

  public String version() {
    return batchers.get(defaultModel).provider.version();
  }

  public String version(String model) {
    ModelBatcher batcher = batchers.get(model);
    if (batcher == null) throw new IllegalArgumentException("Unknown embedding model: " + model);
    return batcher.provider.version();
  }

  @Override
  public void close() {
    executor.shutdown();
  }

  private final class ModelBatcher {
    private final EmbeddingProvider provider;
    private final int maxBatchSize;
    private final Duration maxWait;
    private final RetryPolicy retryPolicy;
    private final List<Pending> pending = new ArrayList<>();
    private ScheduledFuture<?> scheduledFlush;

    private ModelBatcher(
        EmbeddingProvider provider, int maxBatchSize, Duration maxWait, RetryPolicy retryPolicy) {
      this.provider = provider;
      this.maxBatchSize = maxBatchSize;
      this.maxWait = maxWait;
      this.retryPolicy = retryPolicy;
    }

    private synchronized void submit(List<Pending> additions) {
      if (additions.isEmpty()) return;
      pending.addAll(additions);
      if (pending.size() >= maxBatchSize) {
        if (scheduledFlush != null) scheduledFlush.cancel(false);
        schedule(Duration.ZERO);
      } else if (scheduledFlush == null) schedule(maxWait);
    }

    private void schedule(Duration delay) {
      scheduledFlush = executor.schedule(this::flush, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void flush() {
      List<Pending> batch;
      synchronized (this) {
        int count = Math.min(maxBatchSize, pending.size());
        batch = new ArrayList<>(pending.subList(0, count));
        pending.subList(0, count).clear();
        scheduledFlush = null;
        if (!pending.isEmpty()) schedule(pending.size() >= maxBatchSize ? Duration.ZERO : maxWait);
      }
      if (batch.isEmpty()) return;
      try {
        List<String> input = batch.stream().map(Pending::text).toList();
        List<float[]> vectors =
            new RetryExecutor().execute(retryPolicy, () -> provider.embed(input), attempt -> {});
        if (vectors.size() != batch.size()) {
          throw new IllegalStateException("Embedding provider returned wrong batch size");
        }
        for (int index = 0; index < batch.size(); index++) {
          Pending item = batch.get(index);
          float[] vector = Objects.requireNonNull(vectors.get(index), "embedding vector");
          cache.put(provider.model(), provider.version(), item.hash, vector, cacheTtl);
          item.result.complete(vector.clone());
        }
      } catch (Throwable failure) {
        batch.forEach(item -> item.result.completeExceptionally(failure));
      }
    }
  }

  private record Pending(
      int index, String text, String hash, CompletableFuture<float[]> result) {}
}
