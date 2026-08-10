package dev.notify.artifact.security;

import dev.notify.artifact.model.Artifact;
import java.util.Objects;

public final class RetrievalSecurityContext
    implements ProtectedOperationContext, ExtractedContentContext {
  private final String principalId;
  private final String tenantId;
  private final String authenticationHandle;
  private final String requiredPermission;
  private final Artifact artifact;
  private final TransportFacts transport;

  private SecurityIdentity identity;
  private String extractedContent;

  public RetrievalSecurityContext(
      String principalId,
      String tenantId,
      String authenticationHandle,
      String requiredPermission,
      Artifact artifact,
      TransportFacts transport,
      String extractedContent) {
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.authenticationHandle = authenticationHandle;
    this.requiredPermission = requiredPermission;
    this.artifact = artifact;
    this.transport = Objects.requireNonNull(transport, "transport");
    this.extractedContent = extractedContent;
  }

  public String principalId() {
    return principalId;
  }

  public String tenantId() {
    return tenantId;
  }

  @Override
  public String claimedPrincipalId() {
    return principalId;
  }

  @Override
  public String requestedTenantId() {
    return tenantId;
  }

  public String authenticationHandle() {
    return authenticationHandle;
  }

  public String requiredPermission() {
    return requiredPermission;
  }

  public Artifact artifact() {
    return artifact;
  }

  @Override
  public Artifact scopedArtifact() {
    return artifact;
  }

  public TransportFacts transport() {
    return transport;
  }

  public SecurityIdentity identity() {
    return identity;
  }

  public void identity(SecurityIdentity identity) {
    this.identity = identity;
  }

  public String extractedContent() {
    return extractedContent;
  }

  public void extractedContent(String extractedContent) {
    this.extractedContent = extractedContent;
  }

  @Override
  public String mediaType() {
    return artifact == null ? null : artifact.mediaType();
  }
}
