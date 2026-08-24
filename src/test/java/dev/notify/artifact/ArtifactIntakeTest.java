package dev.notify.artifact;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.embed.EmbeddingCache;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.factory.DefaultArtifactJobFactory;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.ObjectStore;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

class ArtifactIntakeTest {
  @TempDir Path spoolRoot;

  private InMemoryStores.Metadata metadataStore;
  private DurableSpool spool;
  private ArtifactEngine engine;

  @BeforeEach
  void setUp() throws IOException {
    metadataStore = new InMemoryStores.Metadata();
    spool = new DurableSpool(spoolRoot, 1024, new ObjectMapper());
    var jobFactory =
        new DefaultArtifactJobFactory(
            metadataStore,
            new InMemoryStores.Vectors(),
            unusedObjectStore(),
            spool,
            new DataVerifier(),
            embeddingService(),
            (principal, tenant, permission) -> {},
            EngineOptions.defaults());
    engine =
        new DefaultArtifactEngine(
            jobFactory,
            new JobDispatcher() {
              @Override
              public <R> R dispatch(Job<R> job) throws Exception {
                return job.execute();
              }
            });
  }

  private static EmbeddingService embeddingService() {
    EmbeddingProvider provider =
        new EmbeddingProvider() {
          public String model() {
            return "test";
          }

          public String version() {
            return "1";
          }

          public List<float[]> embed(List<String> texts) {
            return texts.stream().map(ignored -> new float[] {1}).toList();
          }
        };
    Map<String, float[]> values = new HashMap<>();
    EmbeddingCache cache =
        new EmbeddingCache() {
          public Optional<float[]> get(String model, String version, String hash) {
            return Optional.ofNullable(values.get(model + version + hash));
          }

          public void put(String model, String version, String hash, float[] vector) {
            values.put(model + version + hash, vector);
          }
        };
    return new EmbeddingService(provider, cache, 8);
  }

  private static ObjectStore unusedObjectStore() {
    return new ObjectStore() {
      public void put(String tenant, String key, InputStream content, long length, String sha256) {
        throw new UnsupportedOperationException();
      }

      public InputStream get(String tenant, String key) {
        throw new UnsupportedOperationException();
      }

      public boolean verified(String tenant, String key, long length, String sha256) {
        return false;
      }

      public void delete(String tenant, String key) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
