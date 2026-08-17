package dev.notify.artifact.store;

import dev.notify.artifact.model.ArtifactChunk;
import java.util.List;

public interface VectorStore extends Store<ArtifactChunk> {
  void upsert(ArtifactChunk chunk);

  List<ScoredChunk> search(String tenantId, float[] query, int limit, SearchFilter filter);

  List<ScoredChunk> keywordSearch(String tenantId, String query, int limit, SearchFilter filter);

  List<ArtifactChunk> chunks(String tenantId, String artifactId);

  void deleteArtifact(String tenantId, String artifactId);

  record ScoredChunk(ArtifactChunk chunk, double score) {}

  record SearchFilter(List<String> mediaTypes, List<String> tags) {
    public static SearchFilter none() {
      return new SearchFilter(List.of(), List.of());
    }
  }
}
