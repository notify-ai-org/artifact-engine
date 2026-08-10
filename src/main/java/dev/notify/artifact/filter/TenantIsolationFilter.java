package dev.notify.artifact.security;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.util.Checksum;

/** Enforces tenant identity at request, artifact, and generated storage-key boundaries. */
public final class TenantIsolationFilter<C extends ProtectedOperationContext>
    implements SecurityFilter<C> {
  @Override
  public String name() {
    return "tenant-isolation";
  }

  @Override
  public void verify(C context) {
    SecurityIdentity identity = context.identity();
    if (identity == null) {
      reject("FILTER_ORDER_INVALID", "Authentication must run before tenant isolation");
    }
    if (!identity.tenantId().equals(context.requestedTenantId())) {
      reject("TENANT_MISMATCH", "Requested tenant does not match the authenticated tenant");
    }

    Artifact artifact = context.scopedArtifact();
    if (artifact == null) {
      return;
    }
    if (!artifact.tenantId().equals(identity.tenantId())) {
      reject("ARTIFACT_TENANT_MISMATCH", "Artifact does not belong to the authenticated tenant");
    }
    if (artifact.storageKey() != null
        && !storageKeyMatches(artifact.storageKey(), identity.tenantId())) {
      reject("STORAGE_KEY_TENANT_MISMATCH", "Artifact storage key is outside the tenant boundary");
    }
  }

  private static boolean storageKeyMatches(String storageKey, String tenantId) {
    String[] segments = storageKey.split("/", -1);
    return segments.length >= 3 && segments[1].equals(Checksum.sha256(tenantId));
  }

  private void reject(String code, String message) {
    throw new SecurityFilterException(code, name(), message);
  }
}
