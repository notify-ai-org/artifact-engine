package dev.notify.artifact.auth;

import dev.notify.artifact.security.SecurityIdentity;

/** Validates the connection-bound authentication handle without exposing credentials to filters. */
@FunctionalInterface
public interface AuthenticationService {
  SecurityIdentity authenticate(String authenticationHandle);
}
