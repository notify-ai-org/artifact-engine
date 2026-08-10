package dev.notify.artifact.connector;

import dev.notify.artifact.store.ObjectStore;
import java.io.InputStream;

/** SDK-neutral outbound S3 connector; provide an ObjectStore backed by the chosen S3 SDK. */
public final class S3Connector extends Connector.Base<S3Connector.Upload, Void> {
  private final ObjectStore store;
  private ConnectorContext context;

  public S3Connector(String id, ObjectStore store) {
    super(id);
    this.store = store;
  }

  public void init() {
    status = Status.READY;
  }

  public void bind(ConnectorContext context) {
    this.context = context;
    status = Status.BOUND;
  }

  public Void process(Upload upload) throws Exception {
    status = Status.BUSY;
    uses.incrementAndGet();
    try {
      store.put(
          context.tenantId(), upload.key(), upload.content(), upload.length(), upload.sha256());
      return null;
    } finally {
      status = Status.BOUND;
    }
  }

  public void close() {
    status = Status.CLOSED;
  }

  public record Upload(String key, InputStream content, long length, String sha256) {}
}
