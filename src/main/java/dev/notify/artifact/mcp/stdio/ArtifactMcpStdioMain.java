package dev.notify.artifact.mcp.stdio;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.mcp.McpArtifactGateway;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/** Standalone stdio entry point for deployment jars that provide an {@link ArtifactEngine}. */
public final class ArtifactMcpStdioMain {
  public static final String TENANT_ENV = "ARTIFACT_MCP_TENANT_ID";
  public static final String PRINCIPAL_ENV = "ARTIFACT_MCP_PRINCIPAL_ID";
  public static final String SCOPES_ENV = "ARTIFACT_MCP_SCOPES";
  public static final String MAX_TEXT_ENV = "ARTIFACT_MCP_MAX_TEXT_CHARACTERS";
  public static final String MAX_CONTENT_ENV = "ARTIFACT_MCP_MAX_CONTENT_BYTES";
  public static final String TIMEOUT_ENV = "ARTIFACT_MCP_REQUEST_TIMEOUT_SECONDS";

  private static final int DEFAULT_MAX_TEXT_CHARACTERS = 32_768;
  private static final int DEFAULT_MAX_CONTENT_BYTES = 256 * 1_024;
  private static final int DEFAULT_TIMEOUT_SECONDS = 30;

  private ArtifactMcpStdioMain() {}

  public static void main(String[] args) throws InterruptedException {
    InputStream protocolInput = System.in;
    PrintStream protocolOutput = System.out;
    // Reserve the original stdout stream for JSON-RPC before any provider can emit a log line.
    System.setOut(System.err);

    ArtifactMcpEngineProvider provider = loadProvider();
    ArtifactEngine engine = provider.createEngine();
    McpArtifactGateway gateway =
        new McpArtifactGateway(
            engine,
            positiveInt(MAX_TEXT_ENV, DEFAULT_MAX_TEXT_CHARACTERS),
            positiveInt(MAX_CONTENT_ENV, DEFAULT_MAX_CONTENT_BYTES));
    McpArtifactGateway.Session session =
        new McpArtifactGateway.Session(
            requiredEnvironment(PRINCIPAL_ENV),
            requiredEnvironment(TENANT_ENV),
            scopes(requiredEnvironment(SCOPES_ENV)));
    ArtifactMcpStdioServer server =
        new ArtifactMcpStdioServer(
            gateway,
            session,
            Duration.ofSeconds(positiveInt(TIMEOUT_ENV, DEFAULT_TIMEOUT_SECONDS)),
            protocolInput,
            protocolOutput);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close();
                  provider.close();
                },
                "artifact-mcp-shutdown"));

    // stdout belongs exclusively to MCP. Operational diagnostics must use stderr.
    System.err.println("Artifact MCP stdio server ready");
    new CountDownLatch(1).await();
  }

  private static ArtifactMcpEngineProvider loadProvider() {
    var providers = ServiceLoader.load(ArtifactMcpEngineProvider.class).stream().toList();
    if (providers.size() != 1) {
      throw new IllegalStateException(
          "Exactly one ArtifactMcpEngineProvider must be registered; found " + providers.size());
    }
    return providers.get(0).get();
  }

  private static String requiredEnvironment(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " is required");
    }
    return value.trim();
  }

  private static int positiveInt(String name, int fallback) {
    String raw = System.getenv(name);
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      int value = Integer.parseInt(raw);
      if (value < 1) {
        throw new IllegalArgumentException(name + " must be positive");
      }
      return value;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " must be an integer", exception);
    }
  }

  private static Set<String> scopes(String raw) {
    Set<String> scopes = new LinkedHashSet<>();
    Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(value -> !value.isBlank())
        .forEach(scopes::add);
    if (scopes.isEmpty()) {
      throw new IllegalArgumentException(SCOPES_ENV + " must contain at least one scope");
    }
    return Set.copyOf(scopes);
  }
}
