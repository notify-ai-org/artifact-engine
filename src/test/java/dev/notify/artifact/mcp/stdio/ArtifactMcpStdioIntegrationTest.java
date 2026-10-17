package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;

import dev.notify.artifact.mcp.McpArtifactGateway;
import dev.notify.artifact.mcp.McpArtifactGatewayTest.RecordingEngine;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.BlobResourceContents;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.EmbeddedResource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

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
              CallToolRequest.builder()
                  .name(ArtifactMcpServerFeatures.METADATA_TOOL)
                  .arguments(Map.of("artifactId", "artifact-1"))
                  .build());

      assertFalse(Boolean.TRUE.equals(result.isError()));
      Map<String, Object> structured = (Map<String, Object>) result.structuredContent();
      assertEquals("artifact-1", structured.get("id"));

      CallToolResult content =
          client.callTool(
              CallToolRequest.builder()
                  .name(ArtifactMcpServerFeatures.CONTENT_TOOL)
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

  @Test
  @SuppressWarnings("unchecked")
  void serverAnswersConcurrentRequestsOnOnePipe() throws Exception {
    ServerParameters parameters =
        ServerParameters.builder(java())
            .args("-cp", classpath(), GatedServerMain.class.getName())
            .build();
    StdioClientTransport transport =
        new SerializedSendClientTransport(parameters, McpJsonDefaults.getMapper());
    transport.setStdErrorHandler(ignored -> {});
    ExecutorService callers = Executors.newFixedThreadPool(GatedServerMain.CONCURRENT_CALLS);

    try (McpSyncClient client =
        McpClient.sync(transport)
            .requestTimeout(Duration.ofSeconds(20))
            .initializationTimeout(Duration.ofSeconds(10))
            .build()) {
      client.initialize();
      List<Future<CallToolResult>> calls = new ArrayList<>();
      for (int index = 0; index < GatedServerMain.CONCURRENT_CALLS; index++) {
        String artifactId = "artifact-" + index;
        calls.add(callers.submit(() -> client.callTool(
            CallToolRequest.builder()
                .name(ArtifactMcpServerFeatures.METADATA_TOOL)
                .arguments(Map.of("artifactId", artifactId))
                .build())));
      }
      // Each call blocks until all of them are in flight, so a serial server would time out.
      for (int index = 0; index < calls.size(); index++) {
        CallToolResult result = calls.get(index).get(30, TimeUnit.SECONDS);
        assertFalse(Boolean.TRUE.equals(result.isError()), String.valueOf(result.structuredContent()));
        assertEquals(
            "artifact-" + index, ((Map<String, Object>) result.structuredContent()).get("id"));
      }
    } finally {
      callers.shutdownNow();
    }
  }

  @Test
  void mainExitsWithErrorStatusWhenTheEngineCannotStart() throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder(java(), "-cp", classpath(), ArtifactMcpStdioMain.class.getName());
    builder.environment().put("ARTIFACT_READ_ONLY", "not-a-boolean");
    builder.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
    Process process = builder.start();

    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "a failed start must not linger");
    assertEquals(1, process.exitValue());
  }

  @Test
  void mainExitsWhenTheHostClosesStdin() throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder(java(), "-cp", classpath(), ArtifactMcpStdioMain.class.getName());
    builder.environment().putAll(Map.of(
        "ARTIFACT_READ_ONLY", "true",
        "ARTIFACT_JDBC_URL", "",
        "JDBC_DATABASE_URL", "",
        "ARTIFACT_MCP_TENANT_ID", "tenant-a"));
    builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
    Process process = builder.start();
    try (BufferedReader stderr = new BufferedReader(
        new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = stderr.readLine()) != null && !line.contains("Artifact MCP stdio server ready")) {
        // Skip startup diagnostics.
      }
      assertNotNull(line, "the server did not report ready");
      process.getOutputStream().close();

      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "an orphaned server must exit");
      assertEquals(0, process.exitValue());
    } finally {
      process.destroyForcibly();
    }
  }

  private static String java() {
    return Path.of(System.getProperty("java.home"), "bin", "java").toString();
  }

  private static String classpath() {
    return System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
  }

  /** The SDK client transport is not safe for concurrent sends; the host serializes them too. */
  static final class SerializedSendClientTransport extends StdioClientTransport {
    SerializedSendClientTransport(ServerParameters parameters, McpJsonMapper mapper) {
      super(parameters, mapper);
    }

    @Override
    public synchronized Mono<Void> sendMessage(JSONRPCMessage message) {
      return super.sendMessage(message);
    }
  }

  /** Serves metadata only once {@link #CONCURRENT_CALLS} requests are being handled at once. */
  public static final class GatedServerMain {
    static final int CONCURRENT_CALLS = 8;

    private GatedServerMain() {}

    public static void main(String[] args) throws InterruptedException {
      InputStream protocolInput = System.in;
      PrintStream protocolOutput = System.out;
      System.setOut(System.err);
      McpArtifactGateway gateway = new McpArtifactGateway(new GatedEngine(), 100, 4);
      McpArtifactGateway.Session session =
          new McpArtifactGateway.Session("principal-a", "tenant-a", Set.of("artifact.metadata"));
      new ArtifactMcpStdioServer(
          gateway, session, Duration.ofSeconds(20), protocolInput, protocolOutput);
      new CountDownLatch(1).await();
    }
  }

  private static final class GatedEngine implements ArtifactEngine {
    private final RecordingEngine delegate = new RecordingEngine("content");
    private final CountDownLatch inFlight = new CountDownLatch(GatedServerMain.CONCURRENT_CALLS);

    @Override
    public Artifact metadata(String principalId, String tenantId, String artifactId) {
      inFlight.countDown();
      try {
        if (!inFlight.await(15, TimeUnit.SECONDS)) {
          throw new IllegalStateException("requests were not handled concurrently");
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(interrupted);
      }
      return delegate.metadata(principalId, tenantId, artifactId);
    }

    @Override
    public Artifact ingest(Requests.Ingest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream content(String principalId, String tenantId, String artifactId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String extractedText(
        String principalId, String tenantId, String artifactId, int maxCharacters) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Requests.SearchHit> search(Requests.Search request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String principalId, String tenantId, String artifactId) {
      throw new UnsupportedOperationException();
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
