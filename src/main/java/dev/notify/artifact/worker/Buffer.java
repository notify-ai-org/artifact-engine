package dev.notify.artifact.worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Flushes when either item/byte bounds or maximum residence time is reached. */
public final class Buffer<T> {
  private final int maxItems;
  private final long maxBytes;
  private final Duration maxAge;
  private final List<T> items = new ArrayList<>();
  private long bytes;
  private long openedNanos;

  public Buffer(int maxItems, long maxBytes, Duration maxAge) {
    if (maxItems < 1 || maxBytes < 1 || maxAge == null || maxAge.isNegative() || maxAge.isZero()) {
      throw new IllegalArgumentException("Buffer limits must be positive");
    }
    this.maxItems = maxItems;
    this.maxBytes = maxBytes;
    this.maxAge = maxAge;
  }

  public synchronized boolean add(T item, long size) {
    if (item == null || size < 0) {
      throw new IllegalArgumentException("Buffer item is required and size cannot be negative");
    }
    if (size > maxBytes || bytes + size > maxBytes || items.size() >= maxItems) {
      return false;
    }
    if (items.isEmpty()) {
      openedNanos = System.nanoTime();
    }
    items.add(item);
    bytes += size;
    return true;
  }

  public synchronized boolean shouldFlush() {
    return items.size() >= maxItems
        || bytes >= maxBytes
        || (!items.isEmpty() && System.nanoTime() - openedNanos >= maxAge.toNanos());
  }

  public synchronized List<T> drain() {
    var copy = List.copyOf(items);
    items.clear();
    bytes = 0;
    openedNanos = 0;
    return copy;
  }
}
