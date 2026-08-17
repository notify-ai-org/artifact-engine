package dev.notify.artifact.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import dev.notify.artifact.security.SecurityIdentity;

/**
 * Immutable authentication-handle registry suitable for local deployments and tests.
 *
 * <p>The empty default rejects every handle. Handles are treated as opaque secrets and are never
 * included in exception messages.
 */
public final class DefaultAuthenticationService implements AuthenticationService {
  private final Map<String, SecurityIdentity> identities;

  public DefaultAuthenticationService() {
    this(Map.of());
  }

  public DefaultAuthenticationService(Map<String, SecurityIdentity> identities) {
    Objects.requireNonNull(identities, "identities");
    Map<String, SecurityIdentity> validated = new HashMap<>();
    identities.forEach(
        (handle, identity) -> {
          if (handle == null || handle.isBlank()) {
            throw new IllegalArgumentException("Authentication handles must not be blank");
          }
          validated.put(handle, Objects.requireNonNull(identity, "identity"));
        });
    this.identities = Map.copyOf(validated);
  }

  @Override
  public SecurityIdentity authenticate(String authenticationHandle) {
    if (authenticationHandle == null || authenticationHandle.isBlank()) {
      throw new SecurityException("Authentication failed");
    }
    SecurityIdentity identity = identities.get(authenticationHandle);
    if (identity == null) {
      throw new SecurityException("Authentication failed");
    }
    return identity;
  }
}
