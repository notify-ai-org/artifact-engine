package dev.notify.artifact.mcp.stdio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.DefaultArtifactEngine;
import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.auth.DefaultAuthorizationService;
import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.dispatcher.QueuingJobDispatcher;
import dev.notify.artifact.embed.EmbeddingCache;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.embed.InMemoryEmbeddingCache;
import dev.notify.artifact.embed.OkHttpEmbeddingProvider;
import dev.notify.artifact.environment.Environment;
import dev.notify.artifact.factory.DefaultArtifactJobFactory;
import dev.notify.artifact.queue.InMemoryJobQueue;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.jdbc.JdbiMetadataStore;
import dev.notify.artifact.jdbc.JdbiVectorStore;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.S3ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import org.jdbi.v3.core.Jdbi;

/**
 * Default provider for the standalone MCP launcher.
 *
 * <p>When a JDBC URL is configured, metadata and vectors use PostgreSQL through Jdbi. Without one,
 * the provider falls back to process-local development stores.
 */
public final class DefaultArtifactMcpEngineProvider implements ArtifactMcpEngineProvider {
  private ArtifactEngine engine;
  private HikariDataSource dataSource;
  private OkHttpClient embeddingHttpClient;
  private EmbeddingService embeddingService;
  private S3Client s3Client;
  private dev.notify.artifact.worker.DirectJobWorker directJobWorker;

  @Override
  public synchronized ArtifactEngine createEngine(Environment environment) {
    if (engine != null) return engine;
    ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    String jdbcUrl =
        firstNonBlank(property(environment, "ARTIFACT_JDBC_URL"), property(environment, "JDBC_DATABASE_URL"));
    int vectorDimensions = positiveInt(environment, "ARTIFACT_VECTOR_DIMENSIONS", 1536);
    MetadataStore metadata;
    VectorStore vectors;
    if (jdbcUrl == null) {
      dataSource = null;
      metadata = new InMemoryStores.Metadata();
      vectors = new InMemoryStores.Vectors();
    } else {
      dataSource = dataSource(environment, jdbcUrl);
      Jdbi jdbi = Jdbi.create(dataSource);
      metadata = new JdbiMetadataStore(jdbi, objectMapper);
      vectors = new JdbiVectorStore(jdbi, objectMapper, vectorDimensions);
    }
    s3Client = S3Client.builder()
        .region(Region.of(property(environment, "ARTIFACT_S3_REGION", "ap-south-1")))
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .build();
    ObjectStore objects = new S3ObjectStore(
        s3Client,
        new S3ObjectStore.Configuration(
            required(environment, "ARTIFACT_S3_BUCKET"),
            property(environment, "ARTIFACT_S3_ENVIRONMENT", "default"),
            required(environment, "ARTIFACT_S3_KMS_KEY_ID"),
            property(environment, "ARTIFACT_S3_EXPECTED_BUCKET_OWNER"),
            booleanProperty(environment, "ARTIFACT_S3_BUCKET_KEY_ENABLED", true)));
    EmbeddingRuntime embeddingRuntime = embeddingService(environment, objectMapper, vectorDimensions);
    embeddingService = embeddingRuntime.service();
    embeddingHttpClient = embeddingRuntime.client();
    DurableSpool spool;
    try {
      spool =
          new DurableSpool(
              Path.of(property(environment, "ARTIFACT_SPOOL_ROOT", "./data/artifact-spool")),
              positiveLong(
                  environment,
                  "ARTIFACT_SPOOL_MAX_ARTIFACT_BYTES",
                  128L * 1024 * 1024),
              objectMapper);
    } catch (IOException exception) {
      throw new IllegalStateException("Unable to initialize the artifact spool", exception);
    }

    var jobs =
        new DefaultArtifactJobFactory(
            metadata,
            vectors,
            objects,
            spool,
            new DataVerifier(),
            embeddingRuntime.service(),
            new DefaultAuthorizationService(),
            EngineOptions.defaults());
    directJobWorker = new dev.notify.artifact.worker.DirectJobWorker(4, 256);
    JobDispatcher directDispatcher =
        new dev.notify.artifact.dispatcher.DirectJobDispatcher(directJobWorker);
    engine = new DefaultArtifactEngine(jobs, directDispatcher);
    return engine;
  }

  @Override
  public void close() {
    if (embeddingService != null) {
      embeddingService.close();
    }
    if (dataSource != null) {
      dataSource.close();
    }
    if (embeddingHttpClient != null) {
      embeddingHttpClient.dispatcher().executorService().shutdown();
      embeddingHttpClient.connectionPool().evictAll();
    }
    if (s3Client != null) {
      s3Client.close();
    }
    if (directJobWorker != null) {
      directJobWorker.close();
    }
  }

  private static HikariDataSource dataSource(Environment environment, String jdbcUrl) {
    HikariConfig configuration = new HikariConfig();
    configuration.setJdbcUrl(jdbcUrl);
    String username =
        firstNonBlank(property(environment, "ARTIFACT_JDBC_USER"), property(environment, "DB_USER"));
    if (username != null) configuration.setUsername(username);
    configuration.setPassword(
        firstNonBlank(
            property(environment, "ARTIFACT_JDBC_PASSWORD"),
            property(environment, "DB_PASSWORD"),
            ""));
    configuration.setMaximumPoolSize(positiveInt(environment, "ARTIFACT_JDBC_MAX_POOL_SIZE", 8));
    configuration.setMinimumIdle(positiveInt(environment, "ARTIFACT_JDBC_MIN_IDLE", 1));
    configuration.setPoolName("artifact-mcp-jdbc");
    return new HikariDataSource(configuration);
  }

  private static String property(Environment environment, String name) {
    String value = environment.getProperty(name);
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static String property(Environment environment, String name, String fallback) {
    return firstNonBlank(property(environment, name), fallback);
  }

  private static String required(Environment environment, String name) {
    String value = property(environment, name);
    if (value == null) throw new IllegalStateException(name + " is required");
    return value;
  }

  private static int positiveInt(Environment environment, String name, int fallback) {
    String value = property(environment, name);
    if (value == null) return fallback;
    try {
      int parsed = Integer.parseInt(value);
      if (parsed < 1) throw new IllegalArgumentException(name + " must be positive");
      return parsed;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " must be an integer", exception);
    }
  }

  private static long positiveLong(Environment environment, String name, long fallback) {
    String value = property(environment, name);
    if (value == null) return fallback;
    try {
      long parsed = Long.parseLong(value);
      if (parsed < 1) throw new IllegalArgumentException(name + " must be positive");
      return parsed;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " must be an integer", exception);
    }
  }

  private static boolean booleanProperty(
      Environment environment, String name, boolean fallback) {
    String value = property(environment, name);
    if (value == null) return fallback;
    if ("true".equalsIgnoreCase(value)) return true;
    if ("false".equalsIgnoreCase(value)) return false;
    throw new IllegalArgumentException(name + " must be true or false");
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (value != null && !value.isBlank()) return value;
    }
    return null;
  }

  private static EmbeddingRuntime embeddingService(
      Environment environment, ObjectMapper objectMapper, int dimensions) {
    Duration cacheTtl =
        Duration.ofSeconds(positiveLong(environment, "EMBEDDING_CACHE_TTL_SECONDS", 3600));
    EmbeddingCache cache =
        new InMemoryEmbeddingCache(
            positiveInt(environment, "EMBEDDING_CACHE_MAX_ENTRIES", 10_000), cacheTtl);
    String baseUrl =
        firstNonBlank(property(environment, "EMBEDDING_BASE_URL"), "https://api.openai.com/v1");
    String apiKey =
        firstNonBlank(
            property(environment, "EMBEDDING_API_KEY"), property(environment, "OPENAI_API_KEY"));
    String path = firstNonBlank(property(environment, "EMBEDDING_API_PATH"), "/embeddings");
    int timeoutSeconds = positiveInt(environment, "EMBEDDING_TIMEOUT_SECONDS", 30);
    OkHttpClient client =
        new OkHttpClient.Builder()
            .connectTimeout(
                positiveInt(environment, "EMBEDDING_CONNECT_TIMEOUT_SECONDS", 10),
                TimeUnit.SECONDS)
            .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .writeTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .callTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .build();
    String queryModel =
        firstNonBlank(
            property(environment, "EMBEDDING_QUERY_MODEL"),
            firstModel(property(environment, "EMBEDDING_MODELS")),
            "text-embedding-3-small");
    LinkedHashSet<String> modelNames = new LinkedHashSet<>();
    modelNames.add(queryModel);
    modelNames.addAll(models(property(environment, "EMBEDDING_MODELS")));
    List<EmbeddingProvider> providers = new ArrayList<>();
    for (String model : modelNames) {
      providers.add(
          new OkHttpEmbeddingProvider(
              client,
              objectMapper,
              embeddingEndpoint(baseUrl, path),
              apiKey,
              model,
              model.equals(queryModel)
                  ? firstNonBlank(property(environment, "EMBEDDING_MODEL_VERSION"), model)
                  : model,
              dimensions));
    }
    return new EmbeddingRuntime(
        new EmbeddingService(
            providers,
            cache,
            positiveInt(environment, "EMBEDDING_MAX_BATCH_SIZE", 32),
            Duration.ofMillis(positiveLong(environment, "EMBEDDING_MAX_WAIT_MILLIS", 25)),
            cacheTtl,
            RetryPolicy.defaults()),
        client);
  }

  private static String embeddingEndpoint(String baseUrl, String path) {
    HttpUrl parsedBase = HttpUrl.get(baseUrl.endsWith("/") ? baseUrl : baseUrl + '/');
    HttpUrl resolved = parsedBase.resolve(path.startsWith("/") ? path.substring(1) : path);
    if (resolved == null) throw new IllegalArgumentException("Invalid embedding API path: " + path);
    return resolved.toString();
  }

  private static String firstModel(String models) {
    if (models == null) return null;
    for (String model : models.split(",")) {
      if (!model.isBlank()) return model.trim();
    }
    return null;
  }

  private static List<String> models(String configured) {
    if (configured == null) return List.of();
    List<String> models = new ArrayList<>();
    for (String model : configured.split(",")) {
      if (!model.isBlank()) models.add(model.trim());
    }
    return models;
  }

  private record EmbeddingRuntime(EmbeddingService service, OkHttpClient client) {}
}
