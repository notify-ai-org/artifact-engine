package dev.notify.artifact.util;

import java.util.Map;

public final class Idempotency {
  private Idempotency() {}

  public static String fingerprint(
      String tenant, String operation, String checksum, Map<String, String> metadata) {
    var sorted = new java.util.TreeMap<>(metadata == null ? Map.of() : metadata);
    return Checksum.sha256(tenant + "\n" + operation + "\n" + checksum + "\n" + sorted);
  }
}
