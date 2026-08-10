package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import dev.notify.artifact.mcp.McpArtifactGateway;
import dev.notify.artifact.mcp.McpArtifactGatewayTest.RecordingEngine;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.BlobResourceContents;
import io.modelcontextprotocol.spec.McpSchema.EmbeddedResource;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ArtifactMcpServerFeaturesTest {

  @Test
  @SuppressWarnings("unchecked")
  void toolSchemasCannotAcceptCallerSuppliedTenantIdentity() {
    RecordingEngine engine = new RecordingEngine("content");
    ArtifactMcpServerFeatures features = features(engine);

    for (SyncToolSpecification specification : features.tools()) {
      Map<String, Object> properties =
          (Map<String, Object>) specification.tool().inputSchema().get("properties");
      assertFalse(properties.containsKey("tenantId"), specification.tool().name());
      assertFalse(properties.containsKey("principalId"), specification.tool().name());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void metadataToolUsesTheProcessBoundSession() {
    RecordingEngine engine = new RecordingEngine("content");
    ArtifactMcpServerFeatures features = features(engine);
    SyncToolSpecification specification = features.tools().stream()
        .filter(candidate -> ArtifactMcpServerFeatures.METADATA_TOOL.equals(candidate.tool().name()))
        .findFirst()
        .orElseThrow();

    CallToolResult result = specification.callHandler().apply(
        null,
        new CallToolRequest(
            ArtifactMcpServerFeatures.METADATA_TOOL,
            Map.of("artifactId", "artifact-1", "tenantId", "attacker")));

    Map<String, Object> content = (Map<String, Object>) result.structuredContent();
    assertEquals("artifact-1", content.get("id"));
    assertEquals("tenant-a", engine.lastTenant);
    assertEquals("principal-a", engine.lastPrincipal);
  }

  @Test
  @SuppressWarnings("unchecked")
  void contentToolReturnsOneBoundedEmbeddedBlobWithoutDuplicatingBase64InStructuredData() {
    RecordingEngine engine = new RecordingEngine("abcdefghij");
    ArtifactMcpServerFeatures features = features(engine);
    SyncToolSpecification specification =
        features.tools().stream()
            .filter(
                candidate ->
                    ArtifactMcpServerFeatures.CONTENT_TOOL.equals(candidate.tool().name()))
            .findFirst()
            .orElseThrow();

    CallToolResult result =
        specification
            .callHandler()
            .apply(
                null,
                new CallToolRequest(
                    ArtifactMcpServerFeatures.CONTENT_TOOL,
                    Map.of("artifactId", "artifact-1", "offset", 0, "maxBytes", 4)));

    Map<String, Object> structured = (Map<String, Object>) result.structuredContent();
    assertFalse(structured.containsKey("base64"));
    EmbeddedResource embedded = (EmbeddedResource) result.content().get(0);
    BlobResourceContents blob = (BlobResourceContents) embedded.resource();
    assertEquals("YWJjZA==", blob.blob());
    assertEquals(4L, structured.get("nextOffset"));
  }

  private static ArtifactMcpServerFeatures features(RecordingEngine engine) {
    McpArtifactGateway gateway = new McpArtifactGateway(engine, 100, 4);
    McpArtifactGateway.Session session = new McpArtifactGateway.Session(
        "principal-a",
        "tenant-a",
        Set.of("artifact.search", "artifact.metadata", "artifact.text", "artifact.content"));
    return new ArtifactMcpServerFeatures(gateway, session);
  }
}
