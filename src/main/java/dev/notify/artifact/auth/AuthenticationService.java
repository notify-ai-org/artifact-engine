package dev.notify.artifact.security;

/** Validates the connection-bound authentication handle without exposing credentials to filters. */
@FunctionalInterface
public interface AuthenticationService {
  SecurityIdentity authenticate(String authenticationHandle);
}
