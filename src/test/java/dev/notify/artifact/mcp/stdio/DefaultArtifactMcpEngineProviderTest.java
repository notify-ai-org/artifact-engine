package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.auth.AuthorizationService.Permission;
import dev.notify.artifact.environment.MapEnvironmentSource;
import dev.notify.artifact.environment.StandardEnvironment;
import java.nio.file.Path;
import java.util.Map;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

class DefaultArtifactMcpEngineProviderTest {
  @Test
  void mapsProcessScopesToReadOnlyEnginePermissions() {
    var environment =
        new StandardEnvironment(
            new MapEnvironmentSource(
                "test",
                Map.of(
                    "ARTIFACT_MCP_PRINCIPAL_ID", "principal-a",
                    "ARTIFACT_MCP_TENANT_ID", "tenant-a",
                    "ARTIFACT_MCP_SCOPES",
                        "artifact.search,artifact.metadata,artifact.text,artifact.content")));

    var authorization = DefaultArtifactMcpEngineProvider.authorizationService(environment);

    assertDoesNotThrow(() -> authorization.require("principal-a", "tenant-a", Permission.SEARCH));
    assertDoesNotThrow(
        () -> authorization.require("principal-a", "tenant-a", Permission.READ_METADATA));
    assertDoesNotThrow(() -> authorization.require("principal-a", "tenant-a", Permission.READ_TEXT));
    assertDoesNotThrow(() -> authorization.require("principal-a", "tenant-a", Permission.DOWNLOAD));
    assertThrows(
        SecurityException.class,
        () -> authorization.require("principal-a", "tenant-a", Permission.INGEST));
  }

  @Test
  void registersExactlyOneDefaultProvider() {
    var providers = ServiceLoader.load(ArtifactMcpEngineProvider.class).stream().toList();

    assertEquals(1, providers.size());
    var provider = providers.get(0).get();
    assertInstanceOf(DefaultArtifactMcpEngineProvider.class, provider);
    var environment =
        new StandardEnvironment(
            new MapEnvironmentSource(
                "test",
                Map.of(
                    "ARTIFACT_S3_BUCKET", "artifact-test",
                    "ARTIFACT_S3_KMS_KEY_ID", "test-key",
                    "ARTIFACT_MCP_PRINCIPAL_ID", "principal-a",
                    "ARTIFACT_MCP_TENANT_ID", "tenant-a",
                    "ARTIFACT_MCP_SCOPES", "artifact.metadata",
                    "ARTIFACT_SPOOL_ROOT",
                        Path.of("target", "artifact-provider-test-spool").toString())));
    ArtifactEngine engine = provider.createEngine(environment);
    assertNotNull(engine);
    assertEquals(java.util.List.of(), engine.listMetadata("principal-a", "tenant-a", 10));
    assertThrows(
        SecurityException.class,
        () -> engine.listMetadata("different-principal", "tenant-a", 10));
    provider.close();
  }
}
