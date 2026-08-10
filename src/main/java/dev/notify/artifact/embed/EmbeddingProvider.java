package dev.notify.artifact.embed;

import java.util.List;

public interface EmbeddingProvider {
  String model();

  String version();

  List<float[]> embed(List<String> texts);
}
