package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.notify.artifact.environment.Environment;
import java.io.InputStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ArtifactMcpStdioMainEnvironmentTest {

  @Test
  void classpathDefaultsDoNotShipPlaceholderEmbeddingCredentials() throws Exception {
    Properties defaults = new Properties();
    try (InputStream input =
        ArtifactMcpStdioMain.class.getResourceAsStream("/artifact-mcp.properties")) {
      defaults.load(input);
    }

    assertEquals("", defaults.getProperty("EMBEDDING_API_KEY"));
    assertEquals("", defaults.getProperty("OPENAI_API_KEY"));
  }

  @Test
  void commandLineOverridesClasspathProperties() {
    Environment environment =
        ArtifactMcpStdioMain.environment(
            new String[] {
              "--ARTIFACT_VECTOR_DIMENSIONS=42", "--ARTIFACT_MCP_REQUEST_TIMEOUT_SECONDS", "7"
            });

    assertEquals("42", environment.getProperty("ARTIFACT_VECTOR_DIMENSIONS"));
    assertEquals("7", environment.getProperty("ARTIFACT_MCP_REQUEST_TIMEOUT_SECONDS"));
    assertEquals("ap-south-1", environment.getProperty("ARTIFACT_S3_REGION"));
  }
}
