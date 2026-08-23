package dev.notify.artifact.embed;

import java.time.Duration;
import java.util.Optional;

public interface EmbeddingCache {
  Optional<float[]> get(String model, String version, String contentSha256);

  void put(String model, String version, String contentSha256, float[] vector);

  default void put(
      String model, String version, String contentSha256, float[] vector, Duration ttl) {
    put(model, version, contentSha256, vector);
  }
}
