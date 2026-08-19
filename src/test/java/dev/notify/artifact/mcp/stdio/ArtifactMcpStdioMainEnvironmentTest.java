package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.notify.artifact.environment.Environment;
import org.junit.jupiter.api.Test;

class ArtifactMcpStdioMainEnvironmentTest {

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
