package dev.notify.artifact;

import dev.notify.artifact.DefaultArtifactEngineProvider.ArtifactEngineBuilder;
import dev.notify.artifact.environment.Environment;

/**
 * Service-provider hook used by the generic stdio launcher.
 *
 * <p>A deployment supplies the configured stores, workers, authentication policy, and engine by
 * registering an implementation in {@code META-INF/services}. The launcher never invents store
 * credentials or bypasses the application's normal authorization policy.
 */
public interface ArtifactEngineProvider extends AutoCloseable {

  /**
   * Creates a new engine instance. The provider may cache and return the same instance on repeated
   * calls, but it must not return a closed engine.
   *
   * @param environment the environment to use for configuration
   * @return a new or cached engine instance
   */
  ArtifactEngine createEngine(Environment environment); 

  ArtifactEngine createEngine(Environment environment,ArtifactEngineBuilder builder);

  @Override
  default void close() {}
}
