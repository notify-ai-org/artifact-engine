package dev.notify.artifact.connector;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class ConnectorPool implements AutoCloseable {
  private final Map<String, BlockingQueue<Connector<?, ?>>> pools = new ConcurrentHashMap<>();

  public void register(String kind, Connector<?, ?> connector) {
    pools.computeIfAbsent(kind, ignored -> new LinkedBlockingQueue<>()).add(connector);
  }

  public Lease borrow(String kind, Duration timeout) throws InterruptedException {
    var connector =
        pools
            .computeIfAbsent(kind, ignored -> new LinkedBlockingQueue<>())
            .poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    if (connector == null) throw new IllegalStateException("No connector available for " + kind);
    return new Lease(kind, connector);
  }

  public final class Lease implements AutoCloseable {
    private final String kind;
    private Connector<?, ?> connector;

    private Lease(String kind, Connector<?, ?> connector) {
      this.kind = kind;
      this.connector = connector;
    }

    public Connector<?, ?> connector() {
      return connector;
    }

    public void close() {
      if (connector != null) {
        pools.get(kind).offer(connector);
        connector = null;
      }
    }
  }

  public void close() throws Exception {
    for (var queue : pools.values()) for (var connector : queue) connector.close();
    pools.clear();
  }
}
