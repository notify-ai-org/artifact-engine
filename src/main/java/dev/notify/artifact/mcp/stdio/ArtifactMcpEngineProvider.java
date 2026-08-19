package dev.notify.artifact.mcp.stdio;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.environment.Environment;

/**
 * Service-provider hook used by the generic stdio launcher.
 *
 * <p>A deployment supplies the configured stores, workers, authentication policy, and engine by
 * registering an implementation in {@code META-INF/services}. The launcher never invents store
 * credentials or bypasses the application's normal authorization policy.
 */
public interface ArtifactMcpEngineProvider extends AutoCloseable {
  ArtifactEngine createEngine(Environment environment);

  @Override
  default void close() {}
}
