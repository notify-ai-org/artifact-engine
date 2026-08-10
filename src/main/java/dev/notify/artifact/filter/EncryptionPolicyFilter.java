package dev.notify.artifact.security;

import java.util.Set;

/** Requires modern TLS in transit and configured encryption/KMS for stored originals. */
public final class EncryptionPolicyFilter<C extends ProtectedOperationContext>
    implements SecurityFilter<C> {
  private final Set<String> allowedTlsProtocols;
  private final boolean requireKms;

  public EncryptionPolicyFilter(Set<String> allowedTlsProtocols, boolean requireKms) {
    this.allowedTlsProtocols = Set.copyOf(allowedTlsProtocols);
    this.requireKms = requireKms;
  }

  @Override
  public String name() {
    return "encryption-policy";
  }

  @Override
  public void verify(C context) {
    TransportFacts transport = context.transport();
    if (!transport.tls()) {
      reject("TLS_REQUIRED", "Encrypted transport is required");
    }
    if (!"INTERNAL".equals(transport.tlsProtocol())
        && !allowedTlsProtocols.contains(transport.tlsProtocol())) {
      reject("TLS_PROTOCOL_REJECTED", "TLS protocol is not allowed");
    }

    StorageEncryption storage = context.storageEncryption();
    if (storage == null) {
      return;
    }
    if (!storage.encryptedAtRest()) {
      reject("AT_REST_ENCRYPTION_REQUIRED", "Object encryption at rest is required");
    }
    if (requireKms
        && (!storage.kmsBacked() || storage.keyAlias() == null || storage.keyAlias().isBlank())) {
      reject("KMS_ENCRYPTION_REQUIRED", "KMS-backed object encryption is required");
    }
  }

  private void reject(String code, String message) {
    throw new SecurityFilterException(code, name(), message);
  }
}
