package dev.notify.artifact;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/** Tenant-bound public facade. Implementations authorize every call before touching a store. */
public interface ArtifactEngine {
  Artifact ingest(Requests.Ingest request) throws IOException;

  Artifact metadata(String principalId, String tenantId, String artifactId);

  default List<Artifact> listMetadata(String principalId, String tenantId, int limit) {
    throw new UnsupportedOperationException("Artifact metadata listing is not supported");
  }

  InputStream content(String principalId, String tenantId, String artifactId) throws IOException;

  String extractedText(String principalId, String tenantId, String artifactId, int maxCharacters);

  List<Requests.SearchHit> search(Requests.Search request);

  void delete(String principalId, String tenantId, String artifactId) throws IOException;
}
