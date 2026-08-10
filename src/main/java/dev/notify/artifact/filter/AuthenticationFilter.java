package dev.notify.artifact.security;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/** Authenticates an opaque connection/session handle and enforces the operation permission. */
public final class AuthenticationFilter<C extends AuthenticatedOperationContext>
    implements SecurityFilter<C> {
  private final AuthenticationService authenticationService;
  private final Clock clock;

  public AuthenticationFilter(AuthenticationService authenticationService, Clock clock) {
    this.authenticationService =
        Objects.requireNonNull(authenticationService, "authenticationService");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public String name() {
    return "authentication";
  }

  @Override
  public void verify(C context) {
    if (context.authenticationHandle() == null || context.authenticationHandle().isBlank()) {
      reject("AUTHENTICATION_REQUIRED", "Authentication is required");
    }
    SecurityIdentity identity = authenticationService.authenticate(context.authenticationHandle());
    if (identity == null || identity.expired(Instant.now(clock))) {
      reject("AUTHENTICATION_EXPIRED", "Authentication is invalid or expired");
    }
    if (!identity.principalId().equals(context.claimedPrincipalId())) {
      reject("PRINCIPAL_MISMATCH", "Authenticated principal does not match the operation");
    }
    if (!identity.permissions().contains(context.requiredPermission())) {
      reject("PERMISSION_DENIED", "Operation permission is required");
    }
    context.identity(identity);
  }

  private void reject(String code, String message) {
    throw new SecurityFilterException(code, name(), message);
  }
}
