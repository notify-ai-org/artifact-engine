package dev.notify.artifact.security;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import java.nio.file.Path;

/** Bridges trusted edge/session facts into core security filters. */
public interface SecurityContextFactory {
  IngestionSecurityContext ingestion(Requests.Ingest request, Path durableContentPath);

  RetrievalSecurityContext retrieval(
      String principalId,
      String tenantId,
      String permission,
      Artifact artifact,
      String extractedContent);
}
