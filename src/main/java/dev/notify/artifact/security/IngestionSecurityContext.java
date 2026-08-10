package dev.notify.artifact.security;

import dev.notify.artifact.model.Requests;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** Mutable only for derived, trusted filter outputs; request inputs remain immutable. */
public final class IngestionSecurityContext implements ProtectedOperationContext {
  private final Requests.Ingest request;
  private final Path contentPath;
  private final String authenticationHandle;
  private final TransportFacts transport;
  private final StorageEncryption storageEncryption;
  private final URI sourceUri;

  private SecurityIdentity identity;
  private String detectedMediaType;
  private String sanitizedFilename;
  private MalwareScanner.Verdict scanVerdict;
  private List<InetAddress> approvedSourceAddresses = List.of();

  public IngestionSecurityContext(
      Requests.Ingest request,
      Path contentPath,
      String authenticationHandle,
      TransportFacts transport,
      StorageEncryption storageEncryption,
      URI sourceUri) {
    this.request = Objects.requireNonNull(request, "request");
    this.contentPath = Objects.requireNonNull(contentPath, "contentPath");
    this.authenticationHandle = authenticationHandle;
    this.transport = Objects.requireNonNull(transport, "transport");
    this.storageEncryption = Objects.requireNonNull(storageEncryption, "storageEncryption");
    this.sourceUri = sourceUri;
  }

  public Requests.Ingest request() {
    return request;
  }

  public Path contentPath() {
    return contentPath;
  }

  public String authenticationHandle() {
    return authenticationHandle;
  }

  public TransportFacts transport() {
    return transport;
  }

  public StorageEncryption storageEncryption() {
    return storageEncryption;
  }

  public URI sourceUri() {
    return sourceUri;
  }

  public SecurityIdentity identity() {
    return identity;
  }

  public void identity(SecurityIdentity identity) {
    this.identity = identity;
  }

  public String detectedMediaType() {
    return detectedMediaType;
  }

  public void detectedMediaType(String detectedMediaType) {
    this.detectedMediaType = detectedMediaType;
  }

  public String sanitizedFilename() {
    return sanitizedFilename;
  }

  public void sanitizedFilename(String sanitizedFilename) {
    this.sanitizedFilename = sanitizedFilename;
  }

  public MalwareScanner.Verdict scanVerdict() {
    return scanVerdict;
  }

  public void scanVerdict(MalwareScanner.Verdict scanVerdict) {
    this.scanVerdict = scanVerdict;
  }

  public List<InetAddress> approvedSourceAddresses() {
    return approvedSourceAddresses;
  }

  public void approvedSourceAddresses(List<InetAddress> addresses) {
    this.approvedSourceAddresses = List.copyOf(addresses);
  }

  @Override
  public String claimedPrincipalId() {
    return request.principalId();
  }

  @Override
  public String requestedTenantId() {
    return request.tenantId();
  }

  @Override
  public String requiredPermission() {
    return "INGEST";
  }
}
