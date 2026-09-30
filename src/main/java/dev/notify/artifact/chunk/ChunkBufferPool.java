package dev.notify.artifact.chunk;

import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Memory-bounded store of chunk bytes shared by the jobs that process one chunk (spool, store,
 * index), so the source is read once per chunk.
 *
 * <p><b>Memory.</b> Chunks live in fixed-size direct slabs, allocated lazily up to {@code
 * slabCount} and then recycled, never freed and reallocated. Resident native memory is therefore
 * at most {@code slabCount x slabBytes} regardless of GC timing. The JVM's
 * {@code -XX:MaxDirectMemorySize} must allow that much.
 *
 * <p><b>Lifecycle.</b> {@link #acquire} returns a {@link Lease}; a slab is reused only when no
 * lease holds it and either its chunk was {@link #release released} or it has been idle and its
 * space is needed. An evicted or never-loaded chunk is simply loaded again from its source, so the
 * pool is safe across restarts and across workers on different instances; it only saves reads.
 *
 * <p><b>Back-pressure.</b> When every slab is leased, {@code acquire} waits up to the given time
 * and then throws {@link PoolExhaustedException}, which a worker treats as a retryable failure.
 */
public final class ChunkBufferPool implements AutoCloseable {
  private static final StructuredLog LOG = StructuredLog.of(ChunkBufferPool.class);

  private final int slabBytes;
  private final int slabCount;
  private final Duration idleTtl;
  private final Clock clock;
  private final Deque<ByteBuffer> freeSlabs = new ArrayDeque<>();
  private final Map<String, Entry> entries = new LinkedHashMap<>();
  private int allocatedSlabs;
  private boolean closed;
  private java.util.concurrent.ScheduledExecutorService sweeper;

  public ChunkBufferPool(int slabBytes, int slabCount, Duration idleTtl) {
    this(slabBytes, slabCount, idleTtl, Clock.systemUTC());
  }

  ChunkBufferPool(int slabBytes, int slabCount, Duration idleTtl, Clock clock) {
    if (slabBytes < 1 || slabCount < 1) {
      throw new IllegalArgumentException("slabBytes and slabCount must be positive");
    }
    this.slabBytes = slabBytes;
    this.slabCount = slabCount;
    this.idleTtl = Objects.requireNonNull(idleTtl, "idleTtl");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** Loads exactly {@code target.remaining()} bytes of a chunk into {@code target}. */
  @FunctionalInterface
  public interface ChunkLoader {
    void load(ByteBuffer target) throws IOException;
  }

  /**
   * Returns the chunk's bytes, loading them with {@code loader} unless another caller already did.
   *
   * @param length the chunk length; at most the slab size
   * @param maxWait how long to wait for a free slab when the pool is full
   */
  public Lease acquire(String key, int length, ChunkLoader loader, Duration maxWait)
      throws IOException, InterruptedException {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    if (length < 0 || length > slabBytes) {
      throw new IllegalArgumentException(
          "Chunk length " + length + " exceeds the buffer slab size " + slabBytes);
    }
    Entry entry;
    boolean mustLoad = false;
    synchronized (this) {
      ensureOpen();
      entry = entries.get(key);
      if (entry == null) {
        ByteBuffer slab = takeSlab(maxWait);
        entry = new Entry(key, slab, length);
        entries.put(key, entry);
        mustLoad = true;
      } else if (entry.length != length) {
        throw new IllegalStateException("Chunk " + key + " was loaded with a different length");
      }
      entry.references++;
      entry.lastUsed = clock.instant();
    }

    if (mustLoad) {
      Instant started = clock.instant();
      try {
        ByteBuffer target = entry.slab.duplicate().clear().limit(length);
        loader.load(target);
        if (target.hasRemaining()) {
          throw new IOException("Chunk source returned " + target.position() + " of " + length
              + " bytes for " + key);
        }
        entry.loading.complete(null);
        LOG.debug("chunk_buffered", "chunk", key, "bytes", length,
            "duration", Duration.between(started, clock.instant()));
      } catch (IOException | RuntimeException failure) {
        entry.loading.completeExceptionally(failure);
        synchronized (this) {
          entry.references--;
          discard(entry);
        }
        throw failure;
      }
    } else {
      try {
        entry.loading.join();
      } catch (CompletionException failed) {
        synchronized (this) {
          entry.references--;
        }
        Throwable cause = failed.getCause();
        if (cause instanceof IOException io) throw io;
        if (cause instanceof RuntimeException runtime) throw runtime;
        throw new IOException(cause);
      }
    }
    return new Lease(entry);
  }

  /** Marks a chunk as no longer needed; its slab is recycled once no lease holds it. */
  public synchronized void release(String key) {
    Entry entry = entries.get(key);
    if (entry == null) return;
    entry.released = true;
    if (entry.references == 0) discard(entry);
  }

  /** Recycles chunks that have been unused for longer than the idle TTL. */
  public synchronized int evictIdle() {
    Instant cutoff = clock.instant().minus(idleTtl);
    int evicted = 0;
    for (Entry entry : entries.values().toArray(Entry[]::new)) {
      if (entry.references == 0 && entry.lastUsed.isBefore(cutoff)) {
        discard(entry);
        evicted++;
      }
    }
    if (evicted > 0) LOG.info("chunk_buffers_evicted", "evicted", evicted, "reason", "idle");
    return evicted;
  }

  public synchronized Stats stats() {
    int leased = (int) entries.values().stream().filter(entry -> entry.references > 0).count();
    return new Stats(slabBytes, slabCount, allocatedSlabs, freeSlabs.size(), entries.size(),
        leased);
  }

  /** @param cachedChunks chunks resident in slabs; @param leasedChunks those currently in use */
  public record Stats(
      int slabBytes, int slabCount, int allocatedSlabs, int freeSlabs, int cachedChunks,
      int leasedChunks) {
    public long maxBytes() {
      return (long) slabBytes * slabCount;
    }
  }

  /** Periodically recycles idle chunks, so buffers of abandoned workflows cannot pin memory. */
  public synchronized void startIdleSweeper(Duration interval) {
    if (sweeper != null) return;
    sweeper = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "chunk-buffer-sweeper");
      thread.setDaemon(true);
      return thread;
    });
    sweeper.scheduleWithFixedDelay(() -> {
      try {
        evictIdle();
      } catch (RuntimeException failure) {
        LOG.warn("chunk_buffer_sweep_failed", failure);
      }
    }, interval.toMillis(), interval.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void close() {
    if (sweeper != null) sweeper.shutdownNow();
    closed = true;
    entries.clear();
    freeSlabs.clear();
    notifyAll();
  }

  /** Called with the monitor held. Waits for a free slab, evicting idle chunks if needed. */
  private ByteBuffer takeSlab(Duration maxWait) throws InterruptedException, PoolExhaustedException {
    long deadline = System.nanoTime() + maxWait.toNanos();
    while (true) {
      ensureOpen();
      if (!freeSlabs.isEmpty()) return freeSlabs.pop();
      if (allocatedSlabs < slabCount) {
        allocatedSlabs++;
        return ByteBuffer.allocateDirect(slabBytes);
      }
      Entry idle = entries.values().stream()
          .filter(entry -> entry.references == 0 && entry.loading.isDone())
          .min(Comparator.comparing((Entry entry) -> !entry.released)
              .thenComparing(entry -> entry.lastUsed))
          .orElse(null);
      if (idle != null) {
        discard(idle);
        continue;
      }
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        LOG.warn("chunk_buffer_pool_exhausted", "slabs", slabCount, "slabBytes", slabBytes,
            "waited", maxWait);
        throw new PoolExhaustedException(
            "All " + slabCount + " chunk buffers are in use; retry later");
      }
      wait(Math.max(1, remaining / 1_000_000));
    }
  }

  /** Called with the monitor held. */
  private void discard(Entry entry) {
    if (entries.remove(entry.key, entry)) {
      freeSlabs.push(entry.slab);
      notifyAll();
    }
  }

  private void ensureOpen() {
    if (closed) throw new IllegalStateException("Chunk buffer pool is closed");
  }

  /** Read access to a buffered chunk; close it when done. */
  public final class Lease implements AutoCloseable {
    private final Entry entry;
    private boolean closed;

    private Lease(Entry entry) {
      this.entry = entry;
    }

    /** A read-only view of the chunk, positioned at its first byte. Each call returns a new view. */
    public ByteBuffer buffer() {
      if (closed) throw new IllegalStateException("Lease is closed");
      return entry.slab.asReadOnlyBuffer().clear().limit(entry.length);
    }

    public int length() {
      return entry.length;
    }

    @Override
    public void close() {
      synchronized (ChunkBufferPool.this) {
        if (closed) return;
        closed = true;
        entry.references--;
        entry.lastUsed = clock.instant();
        if (entry.references == 0 && entry.released) discard(entry);
        ChunkBufferPool.this.notifyAll();
      }
    }
  }

  private static final class Entry {
    private final String key;
    private final ByteBuffer slab;
    private final int length;
    private final CompletableFuture<Void> loading = new CompletableFuture<>();
    private int references;
    private boolean released;
    private Instant lastUsed = Instant.EPOCH;

    private Entry(String key, ByteBuffer slab, int length) {
      this.key = key;
      this.slab = slab;
      this.length = length;
    }
  }

  /** Every buffer is leased; retryable. */
  public static final class PoolExhaustedException extends IOException {
    public PoolExhaustedException(String message) {
      super(message);
    }
  }
}
