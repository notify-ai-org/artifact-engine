package dev.notify.artifact.embed;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Thread-safe, bounded LRU cache for local development and single-node deployments. */
public final class InMemoryEmbeddingCache implements EmbeddingCache {
  private final Map<Key, Entry> entries;
  private final Duration defaultTtl;
  private final Clock clock;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  public InMemoryEmbeddingCache(int maximumEntries) {
    this(maximumEntries, Duration.ofHours(1));
  }

  public InMemoryEmbeddingCache(int maximumEntries, Duration defaultTtl) {
    this(maximumEntries, defaultTtl, Clock.systemUTC());
  }

  InMemoryEmbeddingCache(int maximumEntries, Duration defaultTtl, Clock clock) {
    if (maximumEntries < 1) {
      throw new IllegalArgumentException("maximumEntries must be positive");
    }
    if (defaultTtl == null || defaultTtl.isZero() || defaultTtl.isNegative()) {
      throw new IllegalArgumentException("defaultTtl must be positive");
    }
    this.defaultTtl = defaultTtl;
    this.clock = java.util.Objects.requireNonNull(clock, "clock");
    this.entries =
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<Key, Entry> eldest) {
            return size() > maximumEntries;
          }
        };
  }

  @Override
  public Optional<float[]> get(String model, String version, String contentSha256) {
    lock.writeLock().lock();
    try {
      Key key = new Key(model, version, contentSha256);
      Entry entry = entries.get(key);
      if (entry == null) return Optional.empty();
      if (!entry.expiresAt().isAfter(clock.instant())) {
        entries.remove(key);
        return Optional.empty();
      }
      return Optional.of(entry.vector().clone());
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void put(String model, String version, String contentSha256, float[] vector) {
    put(model, version, contentSha256, vector, defaultTtl);
  }

  @Override
  public void put(
      String model, String version, String contentSha256, float[] vector, Duration ttl) {
    if (ttl == null || ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
    lock.writeLock().lock();
    try {
      entries.put(
          new Key(model, version, contentSha256), new Entry(vector.clone(), clock.instant().plus(ttl)));
    } finally {
      lock.writeLock().unlock();
    }
  }

  private record Key(String model, String version, String contentSha256) {}

  private record Entry(float[] vector, Instant expiresAt) {}
}
