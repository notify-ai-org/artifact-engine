package dev.notify.artifact.security;

import dev.notify.artifact.model.Artifact;

public interface ProtectedOperationContext extends AuthenticatedOperationContext {
  TransportFacts transport();

  default StorageEncryption storageEncryption() {
    return null;
  }

  default Artifact scopedArtifact() {
    return null;
  }
}
