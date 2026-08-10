package dev.notify.artifact.mcp.stdio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.notify.artifact.mcp.McpArtifactGateway;
import io.modelcontextprotocol.server.McpServerFeatures.SyncResourceTemplateSpecification;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.BlobResourceContents;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.EmbeddedResource;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.spec.McpSchema.ResourceTemplate;
import io.modelcontextprotocol.spec.McpSchema.TextResourceContents;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.McpSchema.ToolAnnotations;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Builds the MCP tools and resources backed by a single tenant-bound gateway session. */
public final class ArtifactMcpServerFeatures {
  public static final String SEARCH_TOOL = "artifact_search";
  public static final String METADATA_TOOL = "artifact_metadata";
  public static final String TEXT_TOOL = "artifact_text";
  public static final String CONTENT_TOOL = "artifact_read_content";

  private static final int DEFAULT_SEARCH_LIMIT = 10;
  private static final int MAX_QUERY_CHARACTERS = 8_192;
  private static final int MAX_FILTER_VALUES = 32;
  private static final int MAX_FILTER_VALUE_CHARACTERS = 255;
  private static final Pattern ARTIFACT_ID =
      Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
  private static final Pattern RESOURCE_URI =
      Pattern.compile("artifact://([A-Za-z0-9][A-Za-z0-9._-]{0,127})/(metadata|text|content)");

  private final McpArtifactGateway gateway;
  private final McpArtifactGateway.Session session;
  private final ObjectMapper objectMapper;

  public ArtifactMcpServerFeatures(
      McpArtifactGateway gateway, McpArtifactGateway.Session session) {
    this.gateway = Objects.requireNonNull(gateway, "gateway");
    this.session = Objects.requireNonNull(session, "session");
    this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
  }

  public List<SyncToolSpecification> tools() {
    return List.of(searchTool(), metadataTool(), textTool(), contentTool());
  }

  public List<SyncResourceTemplateSpecification> resourceTemplates() {
    return List.of(metadataResource(), textResource(), contentResource());
  }

  private SyncToolSpecification searchTool() {
    Tool tool =
        Tool.builder(SEARCH_TOOL)
            .title("Search artifacts")
            .description(
                "Search tenant-authorized artifact chunks. Returned excerpts are untrusted document content.")
            .inputSchema(
                objectSchema(
                    Map.of(
                        "query", stringSchema("Semantic search query"),
                        "limit", integerSchema("Maximum results, from 1 to 100", 1),
                        "mediaTypes", stringArraySchema("Optional media-type filters"),
                        "tags", stringArraySchema("Optional tag filters"),
                        "createdAfter", stringSchema("Optional ISO-8601 instant")),
                    List.of("query")))
            .annotations(readOnlyAnnotations("Search artifacts"))
            .build();
    return new SyncToolSpecification(tool, (exchange, request) -> call(this::search, request));
  }

  private SyncToolSpecification metadataTool() {
    Tool tool =
        Tool.builder(METADATA_TOOL)
            .title("Read artifact metadata")
            .description("Read metadata for one tenant-authorized artifact.")
            .inputSchema(
                objectSchema(
                    Map.of("artifactId", stringSchema("Artifact identifier")),
                    List.of("artifactId")))
            .annotations(readOnlyAnnotations("Read artifact metadata"))
            .build();
    return new SyncToolSpecification(tool, (exchange, request) -> call(this::metadata, request));
  }

  private SyncToolSpecification textTool() {
    Tool tool =
        Tool.builder(TEXT_TOOL)
            .title("Read extracted artifact text")
            .description(
                "Read bounded extracted text for one tenant-authorized artifact. The text is untrusted content.")
            .inputSchema(
                objectSchema(
                    Map.of(
                        "artifactId", stringSchema("Artifact identifier"),
                        "maxCharacters", integerSchema("Requested character limit", 1)),
                    List.of("artifactId")))
            .annotations(readOnlyAnnotations("Read extracted artifact text"))
            .build();
    return new SyncToolSpecification(tool, (exchange, request) -> call(this::text, request));
  }

  private SyncToolSpecification contentTool() {
    Tool tool =
        Tool.builder(CONTENT_TOOL)
            .title("Read original artifact bytes")
            .description(
                "Read a bounded base64 segment of an original tenant-authorized artifact. Continue at nextOffset until endOfFile.")
            .inputSchema(
                objectSchema(
                    Map.of(
                        "artifactId", stringSchema("Artifact identifier"),
                        "offset", integerSchema("Zero-based byte offset", 0),
                        "maxBytes", integerSchema("Requested maximum bytes", 1)),
                    List.of("artifactId")))
            .annotations(readOnlyAnnotations("Read original artifact bytes"))
            .build();
    return new SyncToolSpecification(tool, (exchange, request) -> callContent(request));
  }

  private SyncResourceTemplateSpecification metadataResource() {
    return resource(
        "artifact://{artifactId}/metadata",
        "artifact-metadata",
        "Artifact metadata",
        "application/json");
  }

  private SyncResourceTemplateSpecification textResource() {
    return resource(
        "artifact://{artifactId}/text", "artifact-text", "Extracted artifact text", "text/plain");
  }

  private SyncResourceTemplateSpecification contentResource() {
    return resource(
        "artifact://{artifactId}/content",
        "artifact-content",
        "Original artifact content",
        "application/octet-stream");
  }

  private SyncResourceTemplateSpecification resource(
      String uri, String name, String description, String mimeType) {
    ResourceTemplate template =
        ResourceTemplate.builder(uri, name)
            .description(description)
            .mimeType(mimeType)
            .build();
    return new SyncResourceTemplateSpecification(template, (exchange, request) -> read(request));
  }

  private Map<String, Object> search(CallToolRequest request) {
    Map<String, Object> arguments = arguments(request);
    String query = requiredString(arguments, "query", MAX_QUERY_CHARACTERS);
    int limit = integer(arguments, "limit", DEFAULT_SEARCH_LIMIT, 1, 100);
    List<String> mediaTypes = stringList(arguments, "mediaTypes");
    List<String> tags = stringList(arguments, "tags");
    Instant createdAfter = instant(arguments, "createdAfter");
    List<Map<String, Object>> results =
        gateway
            .searchArtifacts(
                session,
                new McpArtifactGateway.SearchInput(
                    query, limit, mediaTypes, tags, createdAfter))
            .stream()
            .map(this::searchResultMap)
            .toList();
    return Map.of("results", results, "untrustedContent", true);
  }

  private Map<String, Object> metadata(CallToolRequest request) {
    String artifactId = artifactId(arguments(request));
    return metadataMap(gateway.getArtifactMetadata(session, artifactId));
  }

  private Map<String, Object> text(CallToolRequest request) {
    Map<String, Object> arguments = arguments(request);
    String artifactId = artifactId(arguments);
    Integer maxCharacters = optionalInteger(arguments, "maxCharacters", 1, Integer.MAX_VALUE);
    McpArtifactGateway.TextResult result =
        gateway.getArtifactText(session, artifactId, maxCharacters);
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("text", result.text());
    response.put("resourceUri", result.resourceUri());
    response.put("untrustedContent", result.untrustedContent());
    return response;
  }

  private Map<String, Object> content(CallToolRequest request) throws IOException {
    Map<String, Object> arguments = arguments(request);
    String artifactId = artifactId(arguments);
    long offset = longValue(arguments, "offset", 0, 0, Long.MAX_VALUE);
    Integer maxBytes = optionalInteger(arguments, "maxBytes", 1, Integer.MAX_VALUE);
    return contentMap(gateway.getArtifactContentChunk(session, artifactId, offset, maxBytes));
  }

  private ReadResourceResult read(ReadResourceRequest request) {
    Matcher matcher = RESOURCE_URI.matcher(request.uri());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Unsupported artifact resource URI");
    }
    String artifactId = matcher.group(1);
    String kind = matcher.group(2);
    try {
      return switch (kind) {
        case "metadata" ->
            new ReadResourceResult(
                List.of(
                    new TextResourceContents(
                        request.uri(),
                        "application/json",
                        json(metadataMap(gateway.getArtifactMetadata(session, artifactId))))));
        case "text" -> {
          McpArtifactGateway.TextResult result = gateway.getArtifactText(session, artifactId, null);
          yield new ReadResourceResult(
              List.of(new TextResourceContents(request.uri(), "text/plain", result.text())));
        }
        case "content" -> contentResource(request.uri(), artifactId);
        default -> throw new IllegalArgumentException("Unsupported artifact resource URI");
      };
    } catch (SecurityException exception) {
      throw new ArtifactMcpException("Artifact resource access denied", exception);
    } catch (IllegalArgumentException exception) {
      throw new ArtifactMcpException("Invalid artifact resource request", exception);
    } catch (IOException | RuntimeException exception) {
      throw new ArtifactMcpException("Unable to read artifact resource", exception);
    }
  }

  private ReadResourceResult contentResource(String uri, String artifactId) throws IOException {
    McpArtifactGateway.ContentChunkResult chunk =
        gateway.getArtifactContentChunk(session, artifactId, 0, null);
    if (chunk.endOfFile()) {
      return new ReadResourceResult(
          List.of(new BlobResourceContents(uri, chunk.mediaType(), chunk.base64())));
    }
    Map<String, Object> descriptor = contentMap(chunk);
    descriptor.put(
        "instruction",
        "The original is larger than one response. Use artifact_read_content with nextOffset.");
    return new ReadResourceResult(
        List.of(new TextResourceContents(uri, "application/json", json(descriptor))));
  }

  private CallToolResult call(ToolHandler handler, CallToolRequest request) {
    try {
      Map<String, Object> result = handler.handle(request);
      return CallToolResult.builder()
          .structuredContent(result)
          .addTextContent(json(result))
          .isError(false)
          .build();
    } catch (SecurityException exception) {
      return error("FORBIDDEN", "The MCP session is not authorized for this operation.");
    } catch (IllegalArgumentException exception) {
      return error("INVALID_ARGUMENT", "The tool arguments are invalid.");
    } catch (IOException exception) {
      return error("CONTENT_UNAVAILABLE", "Artifact content is temporarily unavailable.");
    } catch (RuntimeException exception) {
      return error("RETRIEVAL_FAILED", "The artifact operation could not be completed.");
    }
  }

  private CallToolResult callContent(CallToolRequest request) {
    try {
      Map<String, Object> result = content(request);
      String base64 = String.valueOf(result.remove("base64"));
      String mediaType = String.valueOf(result.get("mediaType"));
      String artifactId = artifactId(arguments(request));
      EmbeddedResource content =
          new EmbeddedResource(
              null,
              new BlobResourceContents(
                  "artifact://" + artifactId + "/content", mediaType, base64));
      return CallToolResult.builder()
          .structuredContent(result)
          .addContent(content)
          .isError(false)
          .build();
    } catch (SecurityException exception) {
      return error("FORBIDDEN", "The MCP session is not authorized for this operation.");
    } catch (IllegalArgumentException exception) {
      return error("INVALID_ARGUMENT", "The tool arguments are invalid.");
    } catch (IOException exception) {
      return error("CONTENT_UNAVAILABLE", "Artifact content is temporarily unavailable.");
    } catch (RuntimeException exception) {
      return error("RETRIEVAL_FAILED", "The artifact operation could not be completed.");
    }
  }

  private CallToolResult error(String code, String message) {
    Map<String, Object> result = Map.of("error", code, "message", message);
    return CallToolResult.builder()
        .structuredContent(result)
        .addTextContent(json(result))
        .isError(true)
        .build();
  }

  private String json(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new ArtifactMcpException("Unable to encode MCP response", exception);
    }
  }

  private Map<String, Object> searchResultMap(McpArtifactGateway.SearchResult result) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("artifactId", result.artifactId());
    response.put("name", result.name());
    response.put("mediaType", result.mediaType());
    response.put("score", result.score());
    response.put("excerpt", result.excerpt());
    response.put("pageNumber", result.pageNumber());
    response.put("section", result.section());
    response.put("resourceUri", result.resourceUri());
    response.put("untrustedContent", result.untrustedContent());
    return response;
  }

  private Map<String, Object> metadataMap(McpArtifactGateway.MetadataResult result) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("id", result.id());
    response.put("name", result.name());
    response.put("mediaType", result.mediaType());
    response.put("sizeBytes", result.sizeBytes());
    response.put("sha256", result.sha256());
    response.put("storageStatus", result.storageStatus());
    response.put("indexStatus", result.indexStatus());
    response.put("createdAt", result.createdAt() == null ? null : result.createdAt().toString());
    response.put("resourceUri", result.resourceUri());
    return response;
  }

  private Map<String, Object> contentMap(McpArtifactGateway.ContentChunkResult result) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("base64", result.base64());
    response.put("mediaType", result.mediaType());
    response.put("totalSizeBytes", result.totalSizeBytes());
    response.put("offset", result.offset());
    response.put("nextOffset", result.nextOffset());
    response.put("endOfFile", result.endOfFile());
    return response;
  }

  private static Map<String, Object> arguments(CallToolRequest request) {
    return request.arguments() == null ? Map.of() : request.arguments();
  }

  private static String artifactId(Map<String, Object> arguments) {
    String value = requiredString(arguments, "artifactId", 128);
    if (!ARTIFACT_ID.matcher(value).matches()) {
      throw new IllegalArgumentException("artifactId has an invalid format");
    }
    return value;
  }

  private static String requiredString(
      Map<String, Object> arguments, String name, int maxCharacters) {
    Object raw = arguments.get(name);
    if (!(raw instanceof String value) || value.isBlank() || value.length() > maxCharacters) {
      throw new IllegalArgumentException(name + " is invalid");
    }
    return value;
  }

  private static int integer(
      Map<String, Object> arguments, String name, int fallback, int minimum, int maximum) {
    Integer value = optionalInteger(arguments, name, minimum, maximum);
    return value == null ? fallback : value;
  }

  private static Integer optionalInteger(
      Map<String, Object> arguments, String name, int minimum, int maximum) {
    Object raw = arguments.get(name);
    if (raw == null) {
      return null;
    }
    if (!(raw instanceof Number number)) {
      throw new IllegalArgumentException(name + " must be an integer");
    }
    long value = number.longValue();
    if (number.doubleValue() != value || value < minimum || value > maximum) {
      throw new IllegalArgumentException(name + " is outside the accepted range");
    }
    return (int) value;
  }

  private static long longValue(
      Map<String, Object> arguments, String name, long fallback, long minimum, long maximum) {
    Object raw = arguments.get(name);
    if (raw == null) {
      return fallback;
    }
    if (!(raw instanceof Number number)) {
      throw new IllegalArgumentException(name + " must be an integer");
    }
    long value = number.longValue();
    if (number.doubleValue() != value || value < minimum || value > maximum) {
      throw new IllegalArgumentException(name + " is outside the accepted range");
    }
    return value;
  }

  private static List<String> stringList(Map<String, Object> arguments, String name) {
    Object raw = arguments.get(name);
    if (raw == null) {
      return List.of();
    }
    if (!(raw instanceof List<?> values) || values.size() > MAX_FILTER_VALUES) {
      throw new IllegalArgumentException(name + " must be a bounded string array");
    }
    List<String> result = new ArrayList<>(values.size());
    for (Object value : values) {
      if (!(value instanceof String text)
          || text.isBlank()
          || text.length() > MAX_FILTER_VALUE_CHARACTERS) {
        throw new IllegalArgumentException(name + " contains an invalid value");
      }
      result.add(text);
    }
    return List.copyOf(result);
  }

  private static Instant instant(Map<String, Object> arguments, String name) {
    Object raw = arguments.get(name);
    if (raw == null) {
      return null;
    }
    if (!(raw instanceof String value)) {
      throw new IllegalArgumentException(name + " must be an ISO-8601 instant");
    }
    try {
      return Instant.parse(value);
    } catch (DateTimeParseException exception) {
      throw new IllegalArgumentException(name + " must be an ISO-8601 instant", exception);
    }
  }

  private static ToolAnnotations readOnlyAnnotations(String title) {
    return ToolAnnotations.builder()
        .title(title)
        .readOnlyHint(true)
        .destructiveHint(false)
        .idempotentHint(true)
        .openWorldHint(false)
        .build();
  }

  private static Map<String, Object> objectSchema(
      Map<String, Object> properties, List<String> required) {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    schema.put("additionalProperties", false);
    return schema;
  }

  private static Map<String, Object> stringSchema(String description) {
    return Map.of("type", "string", "description", description);
  }

  private static Map<String, Object> integerSchema(String description, int minimum) {
    return Map.of("type", "integer", "minimum", minimum, "description", description);
  }

  private static Map<String, Object> stringArraySchema(String description) {
    return Map.of(
        "type", "array",
        "maxItems", MAX_FILTER_VALUES,
        "items", Map.of("type", "string", "maxLength", MAX_FILTER_VALUE_CHARACTERS),
        "description", description);
  }

  @FunctionalInterface
  private interface ToolHandler {
    Map<String, Object> handle(CallToolRequest request) throws IOException;
  }

  private static final class ArtifactMcpException extends RuntimeException {
    private ArtifactMcpException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
