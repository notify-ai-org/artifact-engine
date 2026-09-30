package dev.notify.artifact.chunk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ChunkBufferPoolTest {
  private static final Duration NO_WAIT = Duration.ZERO;

  @Test
  void loadsAChunkOnceAndSharesItWithEveryReader() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch loading = new CountDownLatch(1);
    try (ChunkBufferPool pool = new ChunkBufferPool(16, 2, Duration.ofMinutes(1))) {
      ExecutorService readers = Executors.newFixedThreadPool(4);
      try {
        List<Future<String>> results = new ArrayList<>();
        for (int reader = 0; reader < 4; reader++) {
          results.add(readers.submit(() -> {
            try (ChunkBufferPool.Lease lease = pool.acquire("a:1", 5, target -> {
              loads.incrementAndGet();
              awaitQuietly(loading);
              target.put("hello".getBytes());
            }, Duration.ofSeconds(5))) {
              return text(lease.buffer());
            }
          }));
        }
        Thread.sleep(100);
        loading.countDown();
        for (Future<String> result : results) assertEquals("hello", result.get());
      } finally {
        readers.shutdownNow();
      }
      assertEquals(1, loads.get());
    }
  }

  @Test
  void neverExceedsItsSlabsAndRecyclesReleasedChunks() throws Exception {
    try (ChunkBufferPool pool = new ChunkBufferPool(8, 2, Duration.ofMinutes(1))) {
      ChunkBufferPool.Lease first = pool.acquire("a:1", 8, fill('1'), NO_WAIT);
      ChunkBufferPool.Lease second = pool.acquire("a:2", 8, fill('2'), NO_WAIT);

      assertThrows(ChunkBufferPool.PoolExhaustedException.class,
          () -> pool.acquire("a:3", 8, fill('3'), NO_WAIT));

      first.close();
      pool.release("a:1");
      try (ChunkBufferPool.Lease third = pool.acquire("a:3", 8, fill('3'), NO_WAIT)) {
        assertEquals("33333333", text(third.buffer()));
      }
      second.close();
      assertEquals(2, pool.stats().allocatedSlabs());
      assertEquals(16, pool.stats().maxBytes());
    }
  }

  @Test
  void aFullPoolWaitsForALeaseToBeReturned() throws Exception {
    try (ChunkBufferPool pool = new ChunkBufferPool(4, 1, Duration.ofMinutes(1))) {
      ChunkBufferPool.Lease held = pool.acquire("a:1", 4, fill('1'), NO_WAIT);
      pool.release("a:1");
      Thread returner = new Thread(() -> {
        sleepQuietly(150);
        held.close();
      });
      returner.start();

      try (ChunkBufferPool.Lease next = pool.acquire("a:2", 4, fill('2'), Duration.ofSeconds(5))) {
        assertEquals("2222", text(next.buffer()));
      }
      returner.join();
    }
  }

  @Test
  void idleChunksAreEvictedAndReloadedOnDemand() throws Exception {
    MutableClock clock = new MutableClock();
    AtomicInteger loads = new AtomicInteger();
    try (ChunkBufferPool pool = new ChunkBufferPool(4, 1, Duration.ofMinutes(5), clock)) {
      ChunkBufferPool.ChunkLoader loader = target -> {
        loads.incrementAndGet();
        target.put("data".getBytes());
      };
      pool.acquire("a:1", 4, loader, NO_WAIT).close();
      clock.advance(Duration.ofMinutes(6));

      assertEquals(1, pool.evictIdle());
      try (ChunkBufferPool.Lease again = pool.acquire("a:1", 4, loader, NO_WAIT)) {
        assertEquals("data", text(again.buffer()));
      }
      assertEquals(2, loads.get());
    }
  }

  @Test
  void aFailedLoadFreesItsSlabAndSurfacesTheError() throws Exception {
    try (ChunkBufferPool pool = new ChunkBufferPool(4, 1, Duration.ofMinutes(1))) {
      assertThrows(IOException.class, () -> pool.acquire("a:1", 4, target -> {
        throw new IOException("source down");
      }, NO_WAIT));
      assertThrows(IOException.class, () -> pool.acquire("a:2", 4, target -> target.put((byte) 1),
          NO_WAIT), "a short read is an error");

      try (ChunkBufferPool.Lease lease = pool.acquire("a:3", 4, fill('x'), NO_WAIT)) {
        assertEquals("xxxx", text(lease.buffer()));
      }
      assertEquals(1, pool.stats().allocatedSlabs());
    }
  }

  @Test
  void rejectsChunksLargerThanASlab() {
    try (ChunkBufferPool pool = new ChunkBufferPool(4, 1, Duration.ofMinutes(1))) {
      assertThrows(IllegalArgumentException.class, () -> pool.acquire("a:1", 5, fill('x'), NO_WAIT));
    }
  }

  @Test
  void leasesAreReadOnlyViews() throws Exception {
    try (ChunkBufferPool pool = new ChunkBufferPool(4, 1, Duration.ofMinutes(1));
        ChunkBufferPool.Lease lease = pool.acquire("a:1", 4, fill('x'), NO_WAIT)) {
      assertTrue(lease.buffer().isReadOnly());
    }
  }

  private static ChunkBufferPool.ChunkLoader fill(char value) {
    return target -> {
      while (target.hasRemaining()) target.put((byte) value);
    };
  }

  private static String text(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class MutableClock extends Clock {
    private Instant now = Instant.parse("2026-01-01T00:00:00Z");

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public java.time.ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }
  }
}
