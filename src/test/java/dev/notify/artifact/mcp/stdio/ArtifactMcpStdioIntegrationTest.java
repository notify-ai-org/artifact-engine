package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import dev.notify.artifact.mcp.McpArtifactGateway;
import dev.notify.artifact.mcp.McpArtifactGatewayTest.RecordingEngine;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.spec.McpSchema.BlobResourceContents;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.EmbeddedResource;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class ArtifactMcpStdioIntegrationTest {

  @Test
  @SuppressWarnings("unchecked")
  void javaSdkClientCallsTheServerOverStdio() {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    String classpath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    ServerParameters parameters =
        ServerParameters.builder(java)
            .args("-cp", classpath, TestServerMain.class.getName())
            .build();
    StdioClientTransport transport =
        new StdioClientTransport(parameters, McpJsonDefaults.getMapper());
    transport.setStdErrorHandler(ignored -> {});

    try (McpSyncClient client =
        McpClient.sync(transport)
            .requestTimeout(Duration.ofSeconds(10))
            .initializationTimeout(Duration.ofSeconds(10))
            .build()) {
      client.initialize();
      assertEquals(4, client.listTools().tools().size());

      CallToolResult result =
          client.callTool(
              CallToolRequest.builder(ArtifactMcpServerFeatures.METADATA_TOOL)
                  .arguments(Map.of("artifactId", "artifact-1"))
                  .build());

      assertFalse(Boolean.TRUE.equals(result.isError()));
      Map<String, Object> structured = (Map<String, Object>) result.structuredContent();
      assertEquals("artifact-1", structured.get("id"));

      CallToolResult content =
          client.callTool(
              CallToolRequest.builder(ArtifactMcpServerFeatures.CONTENT_TOOL)
                  .arguments(
                      Map.of("artifactId", "artifact-1", "offset", 0, "maxBytes", 4))
                  .build());
      EmbeddedResource embedded = (EmbeddedResource) content.content().get(0);
      BlobResourceContents blob = (BlobResourceContents) embedded.resource();
      assertEquals("Y29udA==", blob.blob());
      Map<String, Object> contentMetadata = (Map<String, Object>) content.structuredContent();
      assertFalse(contentMetadata.containsKey("base64"));
    }
  }

  /** Minimal subprocess entry point used only to verify the real stdio transport. */
  public static final class TestServerMain {
    private TestServerMain() {}

    public static void main(String[] args) throws InterruptedException {
      InputStream protocolInput = System.in;
      PrintStream protocolOutput = System.out;
      System.setOut(System.err);
      RecordingEngine engine = new RecordingEngine("content");
      McpArtifactGateway gateway = new McpArtifactGateway(engine, 100, 4);
      McpArtifactGateway.Session session =
          new McpArtifactGateway.Session(
              "principal-a",
              "tenant-a",
              Set.of(
                  "artifact.search",
                  "artifact.metadata",
                  "artifact.text",
                  "artifact.content"));
      new ArtifactMcpStdioServer(
          gateway, session, Duration.ofSeconds(10), protocolInput, protocolOutput);
      new CountDownLatch(1).await();
    }
  }
}
