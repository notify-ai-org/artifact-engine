package dev.notify.artifact.security;

public interface AuthenticatedOperationContext {
  String claimedPrincipalId();

  String requestedTenantId();

  String authenticationHandle();

  String requiredPermission();

  SecurityIdentity identity();

  void identity(SecurityIdentity identity);
}
