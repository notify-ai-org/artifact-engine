package dev.notify.artifact.store;

import dev.notify.artifact.model.ArtifactChunk;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface VectorStore extends Store<ArtifactChunk> {
  void upsert(ArtifactChunk chunk);

  /** Upserts a batch of chunks. Implementations should override with a single round trip. */
  default void upsertAll(List<ArtifactChunk> chunks) {
    chunks.forEach(this::upsert);
  }

  List<ScoredChunk> search(String tenantId, float[] query, int limit, SearchFilter filter);

  List<ScoredChunk> keywordSearch(String tenantId, String query, int limit, SearchFilter filter);

  List<ArtifactChunk> chunks(String tenantId, String artifactId);

  /**
   * Returns stored chunk ids keyed by chunk index for {@code [fromIndex, toIndex)} of one embedding
   * model/version. Indexing uses this to skip chunks already committed by an earlier attempt.
   */
  default Map<Integer, String> chunkIds(
      String tenantId,
      String artifactId,
      String embeddingModel,
      String embeddingVersion,
      int fromIndex,
      int toIndex) {
    return chunks(tenantId, artifactId).stream()
        .filter(
            chunk ->
                chunk.index() >= fromIndex
                    && chunk.index() < toIndex
                    && embeddingModel.equals(chunk.embeddingModel())
                    && embeddingVersion.equals(chunk.embeddingVersion()))
        .collect(Collectors.toMap(ArtifactChunk::index, ArtifactChunk::id, (a, b) -> b));
  }

  /** Removes chunks at or beyond {@code fromIndex} left over from a longer earlier indexing. */
  void deleteChunksFrom(
      String tenantId,
      String artifactId,
      String embeddingModel,
      String embeddingVersion,
      int fromIndex);

  void deleteArtifact(String tenantId, String artifactId);

  record ScoredChunk(ArtifactChunk chunk, double score) {}

  record SearchFilter(List<String> mediaTypes, List<String> tags) {
    public static SearchFilter none() {
      return new SearchFilter(List.of(), List.of());
    }
  }
}
