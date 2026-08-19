package dev.notify.artifact.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.store.VectorStore;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultBearing;
import org.jdbi.v3.core.statement.Query;

/** PostgreSQL/pgvector adapter using Jdbi and tenant-first SQL. */
public final class JdbiVectorStore implements VectorStore {
  private static final int MAX_LIMIT = 1_000;

  private final Jdbi jdbi;
  private final ObjectMapper json;
  private final int dimensions;

  public JdbiVectorStore(Jdbi jdbi, ObjectMapper objectMapper, int dimensions) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper");
    if (dimensions < 1 || dimensions > 2_000) {
      throw new IllegalArgumentException("pgvector dimensions must be between 1 and 2000");
    }
    this.dimensions = dimensions;
  }

  @Override
  public void upsert(ArtifactChunk chunk) {
    validateChunk(chunk);
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    INSERT INTO artifact_chunk (
                        id, artifact_id, tenant_id, chunk_index, text, token_count, page_number,
                        section, coordinates_json, content_sha256, embedding_model,
                        embedding_version, embedding
                    ) VALUES (
                        :id, :artifactId, :tenantId, :chunkIndex, :text, :tokenCount, :pageNumber,
                        :section, CAST(:coordinates AS jsonb), :contentSha256, :embeddingModel,
                        :embeddingVersion, CAST(:embedding AS vector)
                    )
                    ON CONFLICT (
                        tenant_id, artifact_id, chunk_index, embedding_model, embedding_version
                    ) DO UPDATE SET
                        id = EXCLUDED.id,
                        text = EXCLUDED.text,
                        token_count = EXCLUDED.token_count,
                        page_number = EXCLUDED.page_number,
                        section = EXCLUDED.section,
                        coordinates_json = EXCLUDED.coordinates_json,
                        content_sha256 = EXCLUDED.content_sha256,
                        embedding = EXCLUDED.embedding
                    """)
                .bind("id", chunk.id())
                .bind("artifactId", chunk.artifactId())
                .bind("tenantId", chunk.tenantId())
                .bind("chunkIndex", chunk.index())
                .bind("text", chunk.text())
                .bind("tokenCount", chunk.tokenCount())
                .bindByType("pageNumber", chunk.pageNumber(), Integer.class)
                .bindByType("section", chunk.section(), String.class)
                .bind("coordinates", serialize(chunk.coordinates()))
                .bind("contentSha256", chunk.contentSha256())
                .bind("embeddingModel", chunk.embeddingModel())
                .bind("embeddingVersion", chunk.embeddingVersion())
                .bind("embedding", vectorLiteral(chunk.embedding()))
                .execute());
  }

  @Override
  public List<ScoredChunk> search(
      String tenantId, float[] queryVector, int limit, SearchFilter filter) {
    requireTenant(tenantId);
    validateVector(queryVector);
    FilterParts filters = filters(filter);
    String sql =
        """
        SELECT c.*, c.embedding::text AS embedding_text,
               1 - (c.embedding <=> CAST(:embedding AS vector)) AS score
        FROM artifact_chunk c
        JOIN artifact a ON a.id = c.artifact_id AND a.tenant_id = c.tenant_id
        WHERE c.tenant_id = :tenantId AND a.index_status = 'READY'
        """
            + filters.sql()
            + " ORDER BY c.embedding <=> CAST(:embedding AS vector) LIMIT :limit";
    return jdbi.withHandle(
        handle -> {
          Query query =
              handle
                  .createQuery(sql)
                  .bind("embedding", vectorLiteral(queryVector))
                  .bind("tenantId", tenantId)
                  .bind("limit", boundedLimit(limit));
          filters.bind(query);
          return mapScored(query).list();
        });
  }

  @Override
  public List<ScoredChunk> keywordSearch(
      String tenantId, String searchText, int limit, SearchFilter filter) {
    requireTenant(tenantId);
    if (searchText == null || searchText.isBlank()) return List.of();
    FilterParts filters = filters(filter);
    String sql =
        """
        SELECT c.*, c.embedding::text AS embedding_text,
               ts_rank_cd(c.text_search, websearch_to_tsquery('simple', :searchText)) AS score
        FROM artifact_chunk c
        JOIN artifact a ON a.id = c.artifact_id AND a.tenant_id = c.tenant_id
        WHERE c.tenant_id = :tenantId AND a.index_status = 'READY'
          AND c.text_search @@ websearch_to_tsquery('simple', :searchText)
        """
            + filters.sql()
            + " ORDER BY score DESC LIMIT :limit";
    return jdbi.withHandle(
        handle -> {
          Query query =
              handle
                  .createQuery(sql)
                  .bind("searchText", searchText)
                  .bind("tenantId", tenantId)
                  .bind("limit", boundedLimit(limit));
          filters.bind(query);
          return mapScored(query).list();
        });
  }

  @Override
  public List<ArtifactChunk> chunks(String tenantId, String artifactId) {
    requireIdentity(tenantId, artifactId);
    return jdbi.withHandle(
        handle ->
            mapChunks(
                    handle
                        .createQuery(
                            """
                            SELECT c.*, c.embedding::text AS embedding_text
                            FROM artifact_chunk c
                            WHERE c.tenant_id = :tenantId AND c.artifact_id = :artifactId
                            ORDER BY c.chunk_index
                            """)
                        .bind("tenantId", tenantId)
                        .bind("artifactId", artifactId))
                .list());
  }

  @Override
  public void deleteArtifact(String tenantId, String artifactId) {
    requireIdentity(tenantId, artifactId);
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    DELETE FROM artifact_chunk
                    WHERE tenant_id = :tenantId AND artifact_id = :artifactId
                    """)
                .bind("tenantId", tenantId)
                .bind("artifactId", artifactId)
                .execute());
  }

  private org.jdbi.v3.core.result.ResultIterable<ArtifactChunk> mapChunks(ResultBearing rows) {
    return rows.map(
        (result, context) ->
            new ArtifactChunk(
                result.getString("id"),
                result.getString("artifact_id"),
                result.getString("tenant_id"),
                result.getInt("chunk_index"),
                result.getString("text"),
                result.getInt("token_count"),
                nullableInteger(result, "page_number"),
                result.getString("section"),
                deserializeCoordinates(result.getString("coordinates_json")),
                result.getString("content_sha256"),
                result.getString("embedding_model"),
                result.getString("embedding_version"),
                parseVector(result.getString("embedding_text"))));
  }

  private org.jdbi.v3.core.result.ResultIterable<ScoredChunk> mapScored(ResultBearing rows) {
    return rows.map(
        (result, context) -> {
          ArtifactChunk chunk =
              new ArtifactChunk(
                  result.getString("id"),
                  result.getString("artifact_id"),
                  result.getString("tenant_id"),
                  result.getInt("chunk_index"),
                  result.getString("text"),
                  result.getInt("token_count"),
                  nullableInteger(result, "page_number"),
                  result.getString("section"),
                  deserializeCoordinates(result.getString("coordinates_json")),
                  result.getString("content_sha256"),
                  result.getString("embedding_model"),
                  result.getString("embedding_version"),
                  parseVector(result.getString("embedding_text")));
          return new ScoredChunk(chunk, result.getDouble("score"));
        });
  }

  private FilterParts filters(SearchFilter filter) {
    List<String> mediaTypes =
        filter == null || filter.mediaTypes() == null ? List.of() : filter.mediaTypes();
    List<String> tags = filter == null || filter.tags() == null ? List.of() : filter.tags();
    StringBuilder sql = new StringBuilder();
    if (!mediaTypes.isEmpty()) sql.append(" AND a.media_type IN (<mediaTypes>)");
    for (int index = 0; index < tags.size(); index++) {
      String tag = tags.get(index);
      if (tag == null || tag.isBlank() || tag.contains(",")) {
        throw new IllegalArgumentException("Search tags must be non-blank single values");
      }
      sql.append(
          " AND position(',' || :tag"
              + index
              + " || ',' in ',' || COALESCE(a.tags_csv, '') || ',') > 0");
    }
    return new FilterParts(sql.toString(), List.copyOf(mediaTypes), List.copyOf(tags));
  }

  private void validateChunk(ArtifactChunk chunk) {
    Objects.requireNonNull(chunk, "chunk");
    requireIdentity(chunk.tenantId(), chunk.artifactId());
    if (blank(chunk.id())
        || blank(chunk.text())
        || blank(chunk.contentSha256())
        || blank(chunk.embeddingModel())
        || blank(chunk.embeddingVersion())) {
      throw new IllegalArgumentException("Chunk identity, text, hashes, and model are required");
    }
    if (chunk.index() < 0 || chunk.tokenCount() < 0) {
      throw new IllegalArgumentException("Chunk index and token count cannot be negative");
    }
    validateVector(chunk.embedding());
  }

  private void validateVector(float[] vector) {
    if (vector == null || vector.length != dimensions) {
      throw new IllegalArgumentException("Embedding dimensions do not match the configured store");
    }
    for (float value : vector) {
      if (!Float.isFinite(value)) {
        throw new IllegalArgumentException("Embedding values must be finite");
      }
    }
  }

  private String serialize(Map<String, Object> coordinates) {
    try {
      return json.writeValueAsString(coordinates == null ? Map.of() : coordinates);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Chunk coordinates cannot be serialized", exception);
    }
  }

  private Map<String, Object> deserializeCoordinates(String value) {
    if (value == null || value.isBlank()) return Map.of();
    try {
      return json.readValue(value, new TypeReference<Map<String, Object>>() {});
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Stored chunk coordinates are invalid", exception);
    }
  }

  private static String vectorLiteral(float[] vector) {
    StringBuilder value = new StringBuilder(vector.length * 12).append('[');
    for (int index = 0; index < vector.length; index++) {
      if (index > 0) value.append(',');
      value.append(Float.toString(vector[index]));
    }
    return value.append(']').toString();
  }

  private static float[] parseVector(String value) {
    if (value == null || value.length() < 2) return null;
    String body = value.substring(1, value.length() - 1);
    if (body.isBlank()) return new float[0];
    String[] elements = body.split(",");
    float[] vector = new float[elements.length];
    for (int index = 0; index < elements.length; index++) {
      vector[index] = Float.parseFloat(elements[index]);
    }
    return vector;
  }

  private static Integer nullableInteger(java.sql.ResultSet result, String column)
      throws java.sql.SQLException {
    int value = result.getInt(column);
    return result.wasNull() ? null : value;
  }

  private static int boundedLimit(int limit) {
    return Math.max(1, Math.min(limit, MAX_LIMIT));
  }

  private static void requireIdentity(String tenantId, String artifactId) {
    requireTenant(tenantId);
    if (artifactId == null || artifactId.isBlank()) {
      throw new IllegalArgumentException("artifactId is required");
    }
  }

  private static void requireTenant(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId is required");
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private record FilterParts(String sql, List<String> mediaTypes, List<String> tags) {
    void bind(Query query) {
      if (!mediaTypes.isEmpty()) query.bindList("mediaTypes", mediaTypes);
      for (int index = 0; index < tags.size(); index++) {
        query.bind("tag" + index, tags.get(index).trim());
      }
    }
  }
}
