package dev.notify.artifact.security;

import dev.notify.artifact.model.Artifact;

/** Prevents retrieval of artifacts without an explicit clean malware-scan status. */
public final class RetrievalScanGateFilter implements SecurityFilter<RetrievalSecurityContext> {
  public static final String SCAN_STATUS_METADATA = "artifact.scan.status";

  @Override
  public String name() {
    return "retrieval-scan-gate";
  }

  @Override
  public void verify(RetrievalSecurityContext context) {
    Artifact artifact = context.scopedArtifact();
    if (artifact == null) {
      return;
    }
    String status = artifact.metadata().get(SCAN_STATUS_METADATA);
    if (!MalwareScanner.Verdict.CLEAN.name().equals(status)) {
      throw new SecurityFilterException(
          "ARTIFACT_NOT_SCAN_CLEAN",
          name(),
          "Artifact is unavailable until malware scanning succeeds");
    }
  }
}
