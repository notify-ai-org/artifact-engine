package dev.notify.artifact.mcp.stdio;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.notify.artifact.environment.MapEnvironmentSource;
import dev.notify.artifact.environment.StandardEnvironment;
import java.nio.file.Path;
import java.util.Map;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

class DefaultArtifactMcpEngineProviderTest {
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
                    "ARTIFACT_SPOOL_ROOT",
                        Path.of("target", "artifact-provider-test-spool").toString())));
    assertNotNull(provider.createEngine(environment));
    provider.close();
  }
}
