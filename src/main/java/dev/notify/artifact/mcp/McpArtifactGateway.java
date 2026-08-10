package dev.notify.artifact.mcp;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Transport-neutral MCP tool/resource adapter.
 *
 * <p>Tool inputs deliberately contain no tenant field. Tenant and principal are derived from the
 * authenticated MCP session, and every operation delegates to the authorized native facade.
 */
public final class McpArtifactGateway {
  public static final int MAX_TEXT_CHARACTER_LIMIT = 1_000_000;
  public static final int MAX_INLINE_CONTENT_BYTE_LIMIT = 4 * 1_024 * 1_024;

  private final ArtifactEngine engine;
  private final int maxTextCharacters;
  private final int maxInlineContentBytes;

  public McpArtifactGateway(
      ArtifactEngine engine, int maxTextCharacters, int maxInlineContentBytes) {
    this.engine = Objects.requireNonNull(engine, "engine");
    if (maxTextCharacters < 1 || maxTextCharacters > MAX_TEXT_CHARACTER_LIMIT) {
      throw new IllegalArgumentException(
          "MCP text limit must be between 1 and " + MAX_TEXT_CHARACTER_LIMIT);
    }
    if (maxInlineContentBytes < 1
        || maxInlineContentBytes > MAX_INLINE_CONTENT_BYTE_LIMIT) {
      throw new IllegalArgumentException(
          "MCP content limit must be between 1 and " + MAX_INLINE_CONTENT_BYTE_LIMIT);
    }
    this.maxTextCharacters = maxTextCharacters;
    this.maxInlineContentBytes = maxInlineContentBytes;
  }

  public List<SearchResult> searchArtifacts(Session session, SearchInput input) {
    requireScope(session, "artifact.search");
    Requests.Search request =
        new Requests.Search(
            session.tenantId(),
            session.principalId(),
            input.query(),
            input.limit(),
            input.mediaTypes(),
            input.tags(),
            input.createdAfter());
    return engine.search(request).stream()
        .map(
            hit ->
                new SearchResult(
                    hit.artifact().id(),
                    hit.artifact().originalName(),
                    hit.artifact().mediaType(),
                    hit.score(),
                    cap(hit.chunk().text()),
                    hit.chunk().pageNumber(),
                    hit.chunk().section(),
                    hit.resourceUri(),
                    true))
        .toList();
  }

  public MetadataResult getArtifactMetadata(Session session, String artifactId) {
    requireScope(session, "artifact.metadata");
    Artifact artifact = engine.metadata(session.principalId(), session.tenantId(), artifactId);
    return new MetadataResult(
        artifact.id(),
        artifact.originalName(),
        artifact.mediaType(),
        artifact.sizeBytes(),
        artifact.sha256(),
        artifact.storageStatus().name(),
        artifact.indexStatus().name(),
        artifact.createdAt(),
        "artifact://" + artifact.id() + "/metadata");
  }

  public TextResult getArtifactText(Session session, String artifactId, Integer requestedLimit) {
    requireScope(session, "artifact.text");
    int limit =
        requestedLimit == null
            ? maxTextCharacters
            : Math.max(1, Math.min(requestedLimit, maxTextCharacters));
    String text =
        engine.extractedText(session.principalId(), session.tenantId(), artifactId, limit);
    return new TextResult(text, "artifact://" + artifactId + "/text", true);
  }

  public ContentResult getArtifactContent(Session session, String artifactId) throws IOException {
    requireScope(session, "artifact.content");
    Artifact artifact = engine.metadata(session.principalId(), session.tenantId(), artifactId);
    try (InputStream content =
        engine.content(session.principalId(), session.tenantId(), artifactId)) {
      if (artifact.sizeBytes() > maxInlineContentBytes) {
        return new ResourceContent(
            "artifact://" + artifactId + "/content", artifact.mediaType(), artifact.sizeBytes());
      }
      byte[] bytes = content.readNBytes(maxInlineContentBytes + 1);
      if (bytes.length > maxInlineContentBytes) {
        return new ResourceContent(
            "artifact://" + artifactId + "/content", artifact.mediaType(), artifact.sizeBytes());
      }
      return new InlineContent(
          Base64.getEncoder().encodeToString(bytes), artifact.mediaType(), bytes.length);
    }
  }

  /**
   * Reads one bounded segment of an original artifact.
   *
   * <p>The response limit is enforced by this library rather than trusted to the MCP caller. This
   * keeps large originals available through stdio without ever materializing the full object in
   * application memory.
   */
  public ContentChunkResult getArtifactContentChunk(
      Session session, String artifactId, long offset, Integer requestedBytes) throws IOException {
    requireScope(session, "artifact.content");
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be non-negative");
    }
    int limit =
        requestedBytes == null
            ? maxInlineContentBytes
            : Math.max(1, Math.min(requestedBytes, maxInlineContentBytes));
    Artifact artifact = engine.metadata(session.principalId(), session.tenantId(), artifactId);

    try (InputStream content =
        engine.content(session.principalId(), session.tenantId(), artifactId)) {
      long skipped = skipAtMost(content, offset);
      if (skipped < offset) {
        return new ContentChunkResult(
            "", artifact.mediaType(), artifact.sizeBytes(), offset, offset, true);
      }

      byte[] bytes = content.readNBytes(limit + 1);
      int payloadLength = Math.min(bytes.length, limit);
      byte[] payload =
          payloadLength == bytes.length ? bytes : java.util.Arrays.copyOf(bytes, payloadLength);
      long nextOffset = offset + payloadLength;
      boolean endOfFile = bytes.length <= limit;
      return new ContentChunkResult(
          Base64.getEncoder().encodeToString(payload),
          artifact.mediaType(),
          artifact.sizeBytes(),
          offset,
          nextOffset,
          endOfFile);
    }
  }

  private static long skipAtMost(InputStream input, long requested) throws IOException {
    long skipped = 0;
    while (skipped < requested) {
      long count = input.skip(requested - skipped);
      if (count > 0) {
        skipped += count;
      } else if (input.read() == -1) {
        break;
      } else {
        skipped++;
      }
    }
    return skipped;
  }

  private static void requireScope(Session session, String requiredScope) {
    Objects.requireNonNull(session, "session");
    if (!session.scopes().contains(requiredScope) && !session.scopes().contains("artifact.*")) {
      throw new SecurityException("MCP session is not authorized for " + requiredScope);
    }
  }

  private String cap(String value) {
    int excerptLimit = Math.min(maxTextCharacters, 8_192);
    return value.substring(0, Math.min(value.length(), excerptLimit));
  }

  public record Session(String principalId, String tenantId, Set<String> scopes) {
    public Session {
      if (principalId == null || principalId.isBlank() || tenantId == null || tenantId.isBlank()) {
        throw new IllegalArgumentException("MCP session must be bound to a principal and tenant");
      }
      scopes = scopes == null ? Set.of() : Set.copyOf(scopes);
    }
  }

  public record SearchInput(
      String query, int limit, List<String> mediaTypes, List<String> tags, Instant createdAfter) {
    public SearchInput {
      mediaTypes = mediaTypes == null ? List.of() : List.copyOf(mediaTypes);
      tags = tags == null ? List.of() : List.copyOf(tags);
    }
  }

  public record SearchResult(
      String artifactId,
      String name,
      String mediaType,
      double score,
      String excerpt,
      Integer pageNumber,
      String section,
      String resourceUri,
      boolean untrustedContent) {}

  public record MetadataResult(
      String id,
      String name,
      String mediaType,
      long sizeBytes,
      String sha256,
      String storageStatus,
      String indexStatus,
      Instant createdAt,
      String resourceUri) {}

  public record TextResult(String text, String resourceUri, boolean untrustedContent) {}

  public sealed interface ContentResult permits InlineContent, ResourceContent {}

  public record InlineContent(String base64, String mediaType, long sizeBytes)
      implements ContentResult {}

  public record ResourceContent(String resourceUri, String mediaType, long sizeBytes)
      implements ContentResult {}

  public record ContentChunkResult(
      String base64,
      String mediaType,
      long totalSizeBytes,
      long offset,
      long nextOffset,
      boolean endOfFile) {}
}
