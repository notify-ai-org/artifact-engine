package dev.notify.artifact.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable, fail-closed authorization service for applications with a fixed permission map.
 *
 * <p>The no-argument configuration grants no permissions. Production applications can either
 * provide trusted grants at construction time or replace this adapter with their policy engine.
 */
public final class DefaultAuthorizationService implements AuthorizationService {
  private final Map<Subject, Set<Permission>> grants;

  public DefaultAuthorizationService() {
    this(Map.of());
  }

  public DefaultAuthorizationService(Map<Subject, ? extends Set<Permission>> grants) {
    Objects.requireNonNull(grants, "grants");
    Map<Subject, Set<Permission>> immutableGrants = new HashMap<>();
    grants.forEach(
        (subject, permissions) ->
            immutableGrants.put(
                Objects.requireNonNull(subject, "grant subject"),
                Set.copyOf(Objects.requireNonNull(permissions, "grant permissions"))));
    this.grants = Map.copyOf(immutableGrants);
  }

  @Override
  public void require(String principalId, String tenantId, Permission permission) {
    Permission requiredPermission = Objects.requireNonNull(permission, "permission");
    Subject subject = new Subject(principalId, tenantId);
    if (!grants.getOrDefault(subject, Set.of()).contains(requiredPermission)) {
      throw new SecurityException("Permission denied");
    }
  }

  /** Exact principal and tenant pair used as the authorization lookup key. */
  public record Subject(String principalId, String tenantId) {
    public Subject {
      if (principalId == null || principalId.isBlank()) {
        throw new IllegalArgumentException("principalId must not be blank");
      }
      if (tenantId == null || tenantId.isBlank()) {
        throw new IllegalArgumentException("tenantId must not be blank");
      }
    }
  }
}
