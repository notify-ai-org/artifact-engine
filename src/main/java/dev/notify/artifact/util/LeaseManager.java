package dev.notify.artifact.util;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class LeaseManager {
  private final Map<String, Lease> leases = new ConcurrentHashMap<>();

  public Optional<String> acquire(String resource, Duration ttl) {
    if (resource == null || resource.isBlank() || ttl == null || ttl.isNegative() || ttl.isZero()) {
      throw new IllegalArgumentException("Resource and positive lease duration are required");
    }
    String owner = UUID.randomUUID().toString();
    Instant now = Instant.now();
    Lease updated =
        leases.compute(
            resource,
            (key, old) ->
                old == null || old.expiresAt.isBefore(now) ? new Lease(owner, now.plus(ttl)) : old);
    return updated.owner.equals(owner) ? Optional.of(owner) : Optional.empty();
  }

  public boolean release(String resource, String owner) {
    Lease current = leases.get(resource);
    return current != null && current.owner().equals(owner) && leases.remove(resource, current);
  }

  private record Lease(String owner, Instant expiresAt) {}
}
