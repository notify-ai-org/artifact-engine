package dev.notify.artifact.security;

import java.time.Instant;
import java.util.Set;

/** Identity returned by the deployment's OAuth/session authenticator. */
public record SecurityIdentity(
    String principalId, String tenantId, Set<String> permissions, Instant expiresAt) {
  public SecurityIdentity {
    if (principalId == null || principalId.isBlank() || tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("Authenticated identity is incomplete");
    }
    permissions = permissions == null ? Set.of() : Set.copyOf(permissions);
  }

  public boolean expired(Instant now) {
    return expiresAt != null && !expiresAt.isAfter(now);
  }
}
