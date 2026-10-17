package dev.notify.artifact.auth;

import java.util.Objects;
import java.util.Set;

/**
 * Fail-closed authorization for a process bound to one tenant that serves many principals.
 *
 * <p>The tenant and the permission ceiling are fixed when the process starts. The principal comes
 * with each request from the trusted host; any non-blank principal of the bound tenant receives
 * the ceiling, which the MCP gateway narrows further to the scopes of that request.
 */
public final class TenantAuthorizationService implements AuthorizationService {
  private final String tenantId;
  private final Set<Permission> permissions;

  public TenantAuthorizationService(String tenantId, Set<Permission> permissions) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId must not be blank");
    }
    this.tenantId = tenantId;
    this.permissions = Set.copyOf(Objects.requireNonNull(permissions, "permissions"));
  }

  @Override
  public void require(String principalId, String tenantId, Permission permission) {
    Permission requiredPermission = Objects.requireNonNull(permission, "permission");
    if (principalId == null || principalId.isBlank()
        || !this.tenantId.equals(tenantId)
        || !permissions.contains(requiredPermission)) {
      throw new SecurityException("Permission denied");
    }
  }
}
