package dev.notify.artifact.store.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.store.VectorStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

/** PostgreSQL/pgvector store with tenant-first SQL, deterministic upserts, and full-text search. */
public class PostgresVectorStore implements VectorStore {
  private static final int MAX_LIMIT = 1_000;

  private final JdbcTemplate jdbc;
  private final ObjectMapper json;
  private final int dimensions;
  private final RowMapper<ArtifactChunk> chunkMapper = this::mapChunk;

  public PostgresVectorStore(JdbcTemplate jdbc, ObjectMapper objectMapper, int dimensions) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
    this.json = objectMapper;
    if (dimensions < 1 || dimensions > 2_000) {
      throw new IllegalArgumentException("pgvector dimensions must be between 1 and 2000");
    }
    this.dimensions = dimensions;
  }

  @Override
  @Transactional
  public void upsert(ArtifactChunk chunk) {
    validateChunk(chunk);
    String sql =
        """
        INSERT INTO artifact_chunk (
            id, artifact_id, tenant_id, chunk_index, text, token_count, page_number, section,
            coordinates_json, content_sha256, embedding_model, embedding_version, embedding
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?, CAST(? AS vector))
        ON CONFLICT (tenant_id, artifact_id, chunk_index, embedding_model, embedding_version)
        DO UPDATE SET
            id = EXCLUDED.id,
            text = EXCLUDED.text,
            token_count = EXCLUDED.token_count,
            page_number = EXCLUDED.page_number,
            section = EXCLUDED.section,
            coordinates_json = EXCLUDED.coordinates_json,
            content_sha256 = EXCLUDED.content_sha256,
            embedding = EXCLUDED.embedding
        """;
    jdbc.update(
        sql,
        chunk.id(),
        chunk.artifactId(),
        chunk.tenantId(),
        chunk.index(),
        chunk.text(),
        chunk.tokenCount(),
        chunk.pageNumber(),
        chunk.section(),
        serialize(chunk.coordinates()),
        chunk.contentSha256(),
        chunk.embeddingModel(),
        chunk.embeddingVersion(),
        vectorLiteral(chunk.embedding()));
  }

  @Override
  @Transactional(readOnly = true)
  public List<ScoredChunk> search(String tenantId, float[] query, int limit, SearchFilter filter) {
    requireTenant(tenantId);
    validateVector(query);
    QueryParts parts = filters(filter);
    String vector = vectorLiteral(query);
    String sql =
        """
        SELECT c.*, c.embedding::text AS embedding_text,
               1 - (c.embedding <=> CAST(? AS vector)) AS score
        FROM artifact_chunk c
        JOIN artifact a ON a.id = c.artifact_id AND a.tenant_id = c.tenant_id
        WHERE c.tenant_id = ? AND a.index_status = 'READY'
        """
            + parts.sql()
            + " ORDER BY c.embedding <=> CAST(? AS vector) LIMIT ?";
    List<Object> arguments = new ArrayList<>();
    arguments.add(vector);
    arguments.add(tenantId);
    arguments.addAll(parts.arguments());
    arguments.add(vector);
    arguments.add(boundedLimit(limit));
    return jdbc.query(sql, this::mapScoredChunk, arguments.toArray());
  }

  @Override
  @Transactional(readOnly = true)
  public List<ScoredChunk> keywordSearch(
      String tenantId, String query, int limit, SearchFilter filter) {
    requireTenant(tenantId);
    if (query == null || query.isBlank()) {
      return List.of();
    }
    QueryParts parts = filters(filter);
    String sql =
        """
        SELECT c.*, c.embedding::text AS embedding_text,
               ts_rank_cd(c.text_search, websearch_to_tsquery('simple', ?)) AS score
        FROM artifact_chunk c
        JOIN artifact a ON a.id = c.artifact_id AND a.tenant_id = c.tenant_id
        WHERE c.tenant_id = ? AND a.index_status = 'READY'
          AND c.text_search @@ websearch_to_tsquery('simple', ?)
        """
            + parts.sql()
            + " ORDER BY score DESC LIMIT ?";
    List<Object> arguments = new ArrayList<>();
    arguments.add(query);
    arguments.add(tenantId);
    arguments.add(query);
    arguments.addAll(parts.arguments());
    arguments.add(boundedLimit(limit));
    return jdbc.query(sql, this::mapScoredChunk, arguments.toArray());
  }

  @Override
  @Transactional(readOnly = true)
  public List<ArtifactChunk> chunks(String tenantId, String artifactId) {
    requireTenant(tenantId);
    if (artifactId == null || artifactId.isBlank()) {
      throw new IllegalArgumentException("artifactId is required");
    }
    return jdbc.query(
        """
        SELECT c.*, c.embedding::text AS embedding_text
        FROM artifact_chunk c
        WHERE c.tenant_id = ? AND c.artifact_id = ?
        ORDER BY c.chunk_index
        """,
        chunkMapper,
        tenantId,
        artifactId);
  }

  @Override
  @Transactional
  public void deleteArtifact(String tenantId, String artifactId) {
    requireTenant(tenantId);
    jdbc.update(
        "DELETE FROM artifact_chunk WHERE tenant_id = ? AND artifact_id = ?", tenantId, artifactId);
  }

  private QueryParts filters(SearchFilter filter) {
    List<String> mediaTypes =
        filter == null || filter.mediaTypes() == null ? List.of() : filter.mediaTypes();
    List<String> tags = filter == null || filter.tags() == null ? List.of() : filter.tags();
    StringBuilder sql = new StringBuilder();
    List<Object> arguments = new ArrayList<>();

    if (!mediaTypes.isEmpty()) {
      sql.append(" AND a.media_type IN (");
      appendPlaceholders(sql, mediaTypes.size());
      sql.append(')');
      arguments.addAll(mediaTypes);
    }
    for (String tag : tags) {
      if (tag == null || tag.isBlank() || tag.contains(",")) {
        throw new IllegalArgumentException("Search tags must be non-blank single values");
      }
      sql.append(" AND position(',' || ? || ',' in ',' || COALESCE(a.tags_csv, '') || ',') > 0");
      arguments.add(tag.trim());
    }
    return new QueryParts(sql.toString(), List.copyOf(arguments));
  }

  private ArtifactChunk mapChunk(ResultSet result, int rowNumber) throws SQLException {
    return new ArtifactChunk(
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
  }

  private ScoredChunk mapScoredChunk(ResultSet result, int rowNumber) throws SQLException {
    return new ScoredChunk(mapChunk(result, rowNumber), result.getDouble("score"));
  }

  private void validateChunk(ArtifactChunk chunk) {
    Objects.requireNonNull(chunk, "chunk");
    requireTenant(chunk.tenantId());
    if (blank(chunk.id())
        || blank(chunk.artifactId())
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
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Chunk coordinates cannot be serialized", failure);
    }
  }

  private Map<String, Object> deserializeCoordinates(String value) {
    if (value == null || value.isBlank()) {
      return Map.of();
    }
    try {
      return json.readValue(value, new TypeReference<Map<String, Object>>() {});
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Stored chunk coordinates are invalid", failure);
    }
  }

  private static String vectorLiteral(float[] vector) {
    StringBuilder value = new StringBuilder(vector.length * 12).append('[');
    for (int index = 0; index < vector.length; index++) {
      if (index > 0) {
        value.append(',');
      }
      value.append(Float.toString(vector[index]));
    }
    return value.append(']').toString();
  }

  private static float[] parseVector(String value) {
    if (value == null || value.length() < 2) {
      return null;
    }
    String body = value.substring(1, value.length() - 1);
    if (body.isBlank()) {
      return new float[0];
    }
    String[] elements = body.split(",");
    float[] vector = new float[elements.length];
    for (int index = 0; index < elements.length; index++) {
      vector[index] = Float.parseFloat(elements[index]);
    }
    return vector;
  }

  private static Integer nullableInteger(ResultSet result, String column) throws SQLException {
    int value = result.getInt(column);
    return result.wasNull() ? null : value;
  }

  private static int boundedLimit(int limit) {
    return Math.max(1, Math.min(limit, MAX_LIMIT));
  }

  private static void appendPlaceholders(StringBuilder sql, int count) {
    for (int index = 0; index < count; index++) {
      if (index > 0) {
        sql.append(',');
      }
      sql.append('?');
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

  private record QueryParts(String sql, List<Object> arguments) {}
}
