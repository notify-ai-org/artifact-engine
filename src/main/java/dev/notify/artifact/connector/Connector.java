package dev.notify.artifact.connector;

import java.util.concurrent.atomic.AtomicInteger;

public interface Connector<I, O> extends AutoCloseable {
  String id();

  Status status();

  int useCount();

  void init() throws Exception;

  void bind(ConnectorContext context) throws Exception;

  O process(I input) throws Exception;

  @Override
  void close() throws Exception;

  enum Status {
    NEW,
    READY,
    BOUND,
    BUSY,
    FAILED,
    CLOSED
  }

  record ConnectorContext(String tenantId, String principalId) {}

  abstract class Base<I, O> implements Connector<I, O> {
    private final String id;
    protected volatile Status status = Status.NEW;
    protected final AtomicInteger uses = new AtomicInteger();

    protected Base(String id) {
      this.id = id;
    }

    public String id() {
      return id;
    }

    public Status status() {
      return status;
    }

    public int useCount() {
      return uses.get();
    }
  }
}
