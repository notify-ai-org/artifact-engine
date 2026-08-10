package dev.notify.artifact.embed;

import dev.notify.artifact.util.Checksum;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Batches provider calls and caches by model version plus normalized content checksum. */
public final class EmbeddingService {
  private final EmbeddingProvider provider;
  private final EmbeddingCache cache;
  private final int batchSize;

  public EmbeddingService(EmbeddingProvider provider, EmbeddingCache cache, int batchSize) {
    this.provider = provider;
    this.cache = cache;
    this.batchSize = Math.max(1, batchSize);
  }

  public List<float[]> embed(List<String> texts) {
    List<float[]> output = new ArrayList<>(Collections.nCopies(texts.size(), null));
    for (int start = 0; start < texts.size(); start += batchSize) {
      int end = Math.min(texts.size(), start + batchSize);
      List<Integer> misses = new ArrayList<>();
      List<String> missingText = new ArrayList<>();
      for (int i = start; i < end; i++) {
        String hash = Checksum.sha256(texts.get(i));
        var hit = cache.get(provider.model(), provider.version(), hash);
        if (hit.isPresent()) output.set(i, hit.get());
        else {
          misses.add(i);
          missingText.add(texts.get(i));
        }
      }
      if (!misses.isEmpty()) {
        List<float[]> vectors = provider.embed(List.copyOf(missingText));
        if (vectors.size() != misses.size())
          throw new IllegalStateException("Embedding provider returned wrong batch size");
        for (int j = 0; j < misses.size(); j++) {
          int i = misses.get(j);
          float[] vector = vectors.get(j);
          output.set(i, vector);
          cache.put(provider.model(), provider.version(), Checksum.sha256(texts.get(i)), vector);
        }
      }
    }
    return output;
  }

  public String model() {
    return provider.model();
  }

  public String version() {
    return provider.version();
  }
}
