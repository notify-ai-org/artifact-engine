package dev.notify.artifact.embed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class InMemoryEmbeddingCacheTest {
  @Test
  void expiresEntriesAtTheirTtl() {
    MutableClock clock = new MutableClock();
    InMemoryEmbeddingCache cache =
        new InMemoryEmbeddingCache(10, Duration.ofMinutes(1), clock);
    cache.put("model", "v1", "hash", new float[] {1}, Duration.ofSeconds(5));
    assertTrue(cache.get("model", "v1", "hash").isPresent());
    clock.advance(Duration.ofSeconds(5));
    assertTrue(cache.get("model", "v1", "hash").isEmpty());
  }

  private static final class MutableClock extends Clock {
    private Instant now = Instant.parse("2026-01-01T00:00:00Z");

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
