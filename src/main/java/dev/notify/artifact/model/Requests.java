package dev.notify.artifact.model;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Requests {
  private Requests() {}

  public record Ingest(
      String tenantId,
      String principalId,
      String idempotencyKey,
      String originalName,
      String declaredMediaType,
      InputStream content,
      long contentLength,
      Map<String, String> metadata) {
    public Ingest {
      requireText(tenantId, "tenantId");
      requireText(principalId, "principalId");
      Objects.requireNonNull(content, "content");
      if (contentLength < -1) {
        throw new IllegalArgumentException("contentLength must be -1 when unknown or non-negative");
      }
      metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
  }

  public record Search(
      String tenantId,
      String principalId,
      String query,
      int limit,
      List<String> mediaTypes,
      List<String> tags,
      Instant createdAfter) {
    public Search {
      requireText(tenantId, "tenantId");
      requireText(principalId, "principalId");
      requireText(query, "query");
      limit = Math.max(1, Math.min(limit, 100));
      mediaTypes = mediaTypes == null ? List.of() : List.copyOf(mediaTypes);
      tags = tags == null ? List.of() : List.copyOf(tags);
    }
  }

  public record SearchHit(
      Artifact artifact, ArtifactChunk chunk, double score, String resourceUri) {}

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " is required");
    }
  }
}
