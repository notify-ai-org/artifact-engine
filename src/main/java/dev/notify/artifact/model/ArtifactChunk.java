package dev.notify.artifact.model;

import java.util.Map;

public record ArtifactChunk(
    String id,
    String artifactId,
    String tenantId,
    int index,
    String text,
    int tokenCount,
    Integer pageNumber,
    String section,
    Map<String, Object> coordinates,
    String contentSha256,
    String embeddingModel,
    String embeddingVersion,
    float[] embedding) {
  public ArtifactChunk {
    coordinates = coordinates == null ? Map.of() : Map.copyOf(coordinates);
    embedding = embedding == null ? null : embedding.clone();
  }

  @Override
  public float[] embedding() {
    return embedding == null ? null : embedding.clone();
  }
}
