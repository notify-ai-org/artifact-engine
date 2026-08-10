package dev.notify.artifact.embed;

import java.util.Optional;

public interface EmbeddingCache {
  Optional<float[]> get(String model, String version, String contentSha256);

  void put(String model, String version, String contentSha256, float[] vector);
}
