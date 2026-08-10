package dev.notify.artifact.job;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import java.io.InputStream;
import java.util.List;

/** Creates a typed job for every operation exposed by the artifact facade. */
public interface ArtifactJobFactory {
  Job<Artifact> createIngest(Requests.Ingest request);

  Job<Artifact> createMetadata(String principalId, String tenantId, String artifactId);

  Job<List<Artifact>> createListMetadata(String principalId, String tenantId, int limit);

  Job<InputStream> createFetch(String principalId, String tenantId, String artifactId);

  Job<String> createExtractedText(
      String principalId, String tenantId, String artifactId, int maxCharacters);

  Job<List<Requests.SearchHit>> createRetrieval(Requests.Search request);

  Job<Void> createDelete(String principalId, String tenantId, String artifactId);
}
