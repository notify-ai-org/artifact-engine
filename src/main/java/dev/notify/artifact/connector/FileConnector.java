package dev.notify.artifact.connector;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public final class FileConnector extends Connector.Base<Path, InputStream> {
  private final Path allowedRoot;

  public FileConnector(String id, Path allowedRoot) {
    super(id);
    this.allowedRoot = allowedRoot.toAbsolutePath().normalize();
  }

  public void init() {
    status = Status.READY;
  }

  public void bind(ConnectorContext context) {
    status = Status.BOUND;
  }

  public InputStream process(Path path) throws Exception {
    Path safe = path.toAbsolutePath().normalize();
    if (!safe.startsWith(allowedRoot))
      throw new SecurityException("Path is outside connector root");
    uses.incrementAndGet();
    return Files.newInputStream(safe);
  }

  public void close() {
    status = Status.CLOSED;
  }
}
