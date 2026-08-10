package dev.notify.artifact.util;

import dev.notify.artifact.model.Artifact;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Builds stable, non-user-controlled object keys for at-least-once uploads. */
public final class StorageKeyFactory {
  private static final DateTimeFormatter YEAR =
      DateTimeFormatter.ofPattern("uuuu").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter MONTH =
      DateTimeFormatter.ofPattern("MM").withZone(ZoneOffset.UTC);

  private final String environment;
  private final Clock clock;

  public StorageKeyFactory(String environment, Clock clock) {
    if (environment == null || !environment.matches("[a-zA-Z0-9_-]+")) {
      throw new IllegalArgumentException("Environment must be path-safe");
    }
    this.environment = environment;
    this.clock = clock;
  }

  public String key(Artifact artifact) {
    String tenantHash = Checksum.sha256(artifact.tenantId());
    String type = artifact.mediaType().startsWith("image/") ? "image" : "document";
    return String.join(
        "/",
        environment,
        tenantHash,
        type,
        YEAR.format(artifact.createdAt()),
        MONTH.format(artifact.createdAt()),
        artifact.id(),
        Long.toString(artifact.version()),
        "content");
  }
}
