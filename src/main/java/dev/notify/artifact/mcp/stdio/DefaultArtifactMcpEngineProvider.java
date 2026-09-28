package dev.notify.artifact.mcp.stdio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.DefaultArtifactEngine;
import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.auth.DefaultAuthorizationService;
import dev.notify.artifact.auth.AuthorizationService.Permission;
import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.dispatcher.QueuingJobDispatcher;
import dev.notify.artifact.dispatcher.RoutingJobDispatcher;
import dev.notify.artifact.embed.EmbeddingCache;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.embed.InMemoryEmbeddingCache;
import dev.notify.artifact.embed.OkHttpEmbeddingProvider;
import dev.notify.artifact.environment.Environment;
import dev.notify.artifact.factory.DefaultArtifactJobFactory;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.jdbc.JdbiJobStore;
import dev.notify.artifact.jdbc.JdbiMetadataStore;
import dev.notify.artifact.jdbc.JdbiMultipartUploadStore;
import dev.notify.artifact.jdbc.JdbiVectorStore;
import dev.notify.artifact.jdbc.JdbiWorkflowStore;
import dev.notify.artifact.job.IndexJob;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReleaser;
import dev.notify.artifact.store.InMemoryJobStore;
import dev.notify.artifact.store.InMemoryMultipartUploadStore;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.JobStore;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.S3ObjectStore;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.worker.DefaultJobRecordExecutor;
import dev.notify.artifact.worker.JobRecordExecutor;
import dev.notify.artifact.worker.WorkerManager;
import dev.notify.artifact.workflow.InMemoryWorkflowStore;
import dev.notify.artifact.workflow.WorkflowManager;
import dev.notify.artifact.workflow.WorkflowStore;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  // System.Logger writes to stderr, which keeps stdout free for the MCP stdio protocol.
  private static final System.Logger LOGGER =
      System.getLogger(DefaultArtifactMcpEngineProvider.class.getName());

  private ArtifactEngine engine;
  private HikariDataSource dataSource;
  private OkHttpClient embeddingHttpClient;
  private EmbeddingService embeddingService;
  private S3Client s3Client;
  private dev.notify.artifact.worker.DirectJobWorker directJobWorker;
  private QueueManager queueManager;
  private WorkerManager workerManager;
  private WorkflowManager workflowManager;

  @Override
  public synchronized ArtifactEngine createEngine(Environment environment) {
    if (engine != null) return engine;
    ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    String jdbcUrl =
        firstNonBlank(property(environment, "ARTIFACT_JDBC_URL"), property(environment, "JDBC_DATABASE_URL"));
    int vectorDimensions = positiveInt(environment, "ARTIFACT_VECTOR_DIMENSIONS", 1536);
    MetadataStore metadata;
    VectorStore vectors;
    JobStore jobStore;
    WorkflowStore workflowStore;
    MultipartUploadStore multipartUploads;
    if (jdbcUrl == null) {
      dataSource = null;
      metadata = new InMemoryStores.Metadata();
      vectors = new InMemoryStores.Vectors();
      jobStore = new InMemoryJobStore();
      workflowStore = new InMemoryWorkflowStore();
      multipartUploads = new InMemoryMultipartUploadStore();
    } else {
      dataSource = dataSource(environment, jdbcUrl);
      Jdbi jdbi = Jdbi.create(dataSource);
      metadata = new JdbiMetadataStore(jdbi, objectMapper);
      vectors = new JdbiVectorStore(jdbi, objectMapper, vectorDimensions);
      JdbiJobStore jdbiJobs = new JdbiJobStore(jdbi, objectMapper);
      jobStore = jdbiJobs;
      workflowStore = new JdbiWorkflowStore(jdbi, objectMapper, jdbiJobs);
      multipartUploads = new JdbiMultipartUploadStore(jdbi);
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

    // Background processing: a durable queue per job type, workers that execute workflow steps,
    // and the workflow manager that submits them stage by stage.
    SpoolReleaser spoolReleaser = new SpoolReleaser(metadata, spool);
    JobRecordExecutor executor =
        new DefaultJobRecordExecutor(
            metadata,
            objects,
            spool,
            vectors,
            embeddingRuntime.service(),
            TextExtractorFactory.defaults(
                positiveInt(environment, "ARTIFACT_EXTRACT_MAX_INPUT_BYTES", 128 * 1024 * 1024),
                positiveInt(environment, "ARTIFACT_EXTRACT_MAX_OUTPUT_CHARACTERS", Integer.MAX_VALUE)),
            null,
            new Chunker(
                positiveInt(environment, "ARTIFACT_INDEX_WORDS_PER_CHUNK", 300),
                nonNegativeInt(environment, "ARTIFACT_INDEX_OVERLAP_WORDS", 30)),
            new IndexJob.Options(
                positiveInt(environment, "ARTIFACT_INDEX_COMMIT_BATCH_SIZE", 64),
                positiveInt(environment, "ARTIFACT_INDEX_MAX_CHUNKS_PER_ARTIFACT", 10_000),
                IndexJob.ChunkLimitPolicy.valueOf(
                    property(environment, "ARTIFACT_INDEX_CHUNK_LIMIT_POLICY", "FAIL"))),
            multipartUploads,
            spoolReleaser,
            new StorageKeyFactory(
                property(environment, "ARTIFACT_S3_ENVIRONMENT", "default"), Clock.systemUTC()));
    queueManager = new QueueManager();
    queueManager.start();
    boolean background = booleanProperty(environment, "ARTIFACT_BACKGROUND_WORKERS_ENABLED", true);
    workerManager =
        new WorkerManager(
            failure -> LOGGER.log(System.Logger.Level.WARNING, "Artifact job failed", failure.cause()),
            null,
            background ? initialWorkers(environment) : List.of(),
            positiveInt(environment, "ARTIFACT_WORKERS_MAX", WorkerManager.DEFAULT_MAX_WORKERS),
            queueManager,
            executor,
            jobStore,
            duration(environment, "ARTIFACT_WORKER_LEASE_SECONDS", WorkerManager.DEFAULT_LEASE_DURATION),
            WorkerManager.DEFAULT_POLL_INTERVAL);
    workflowManager =
        new WorkflowManager(
            workflowStore,
            queueManager,
            duration(environment, "ARTIFACT_WORKFLOW_POLL_SECONDS", Duration.ofSeconds(1)),
            failure -> LOGGER.log(System.Logger.Level.WARNING, "Artifact workflow dispatch failed", failure),
            workerManager);
    workflowManager.addCrashListener(spoolReleaser.onWorkflowCrashed());
    if (background) {
      workflowManager.start();
    }

    var jobs =
        new DefaultArtifactJobFactory(
            metadata,
            vectors,
            objects,
            spool,
            new DataVerifier(),
            embeddingRuntime.service(),
            authorizationService(environment),
            new EngineOptions(
                true,
                4,
                nonNegativeLong(environment, "ARTIFACT_STORE_MULTIPART_PART_BYTES", 64L * 1024 * 1024)),
            workflowManager);
    directJobWorker = new dev.notify.artifact.worker.DirectJobWorker(4, 256);
    JobDispatcher directDispatcher =
        new dev.notify.artifact.dispatcher.DirectJobDispatcher(directJobWorker);
    QueuingJobDispatcher queuingDispatcher = new QueuingJobDispatcher(queueManager, jobStore);
    JobDispatcher dispatcher = new RoutingJobDispatcher(directDispatcher, queuingDispatcher);
    engine = new DefaultArtifactEngine(jobs, dispatcher, workerManager);
    return engine;
  }

  /**
   * One worker per durable job type (several for multipart parts, which move the bytes). Counts are
   * configurable; a count of 0 leaves that job type unprocessed by this process.
   */
  private static List<WorkerManager.WorkerConfiguration> initialWorkers(Environment environment) {
    int queueCapacity = positiveInt(environment, "ARTIFACT_WORKERS_QUEUE_CAPACITY", 256);
    int batchSize = positiveInt(environment, "ARTIFACT_WORKERS_BATCH_SIZE", WorkerManager.DEFAULT_BATCH_SIZE);
    int partBatchSize = positiveInt(environment, "ARTIFACT_WORKERS_STORE_PART_BATCH_SIZE", 2);
    int storeCount = nonNegativeInt(environment, "ARTIFACT_WORKERS_STORE_COUNT", 1);
    List<WorkerManager.WorkerConfiguration> workers = new ArrayList<>();
    addWorkers(workers, "store", storeCount, queueCapacity, batchSize, JobRecord.JobType.STORE);
    addWorkers(workers, "store-init", storeCount, queueCapacity, batchSize, JobRecord.JobType.STORE_INIT);
    addWorkers(workers, "store-part",
        nonNegativeInt(environment, "ARTIFACT_WORKERS_STORE_PART_COUNT", 4), queueCapacity,
        partBatchSize, JobRecord.JobType.STORE_PART);
    addWorkers(workers, "store-complete", storeCount, queueCapacity, batchSize,
        JobRecord.JobType.STORE_COMPLETE);
    addWorkers(workers, "release-spool", storeCount, queueCapacity, batchSize,
        JobRecord.JobType.RELEASE_SPOOL);
    addWorkers(workers, "index", nonNegativeInt(environment, "ARTIFACT_WORKERS_INDEX_COUNT", 1),
        queueCapacity, batchSize, JobRecord.JobType.INDEX);
    return workers;
  }

  private static void addWorkers(
      List<WorkerManager.WorkerConfiguration> workers,
      String prefix,
      int count,
      int queueCapacity,
      int batchSize,
      JobRecord.JobType type) {
    for (int index = 1; index <= count; index++) {
      workers.add(
          new WorkerManager.WorkerConfiguration(
              "artifact-mcp-" + prefix + "-" + index,
              queueCapacity,
              batchSize,
              batchSize,
              WorkerManager.DEFAULT_FLUSH_INTERVAL,
              type));
    }
  }

  @Override
  public void close() {
    // Stop producers before consumers, and both before the stores they write to.
    if (workflowManager != null) {
      workflowManager.close();
    }
    if (workerManager != null) {
      workerManager.close();
    }
    if (queueManager != null) {
      queueManager.close();
    }
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

  static DefaultAuthorizationService authorizationService(Environment environment) {
    String principalId = required(environment, ArtifactMcpStdioMain.PRINCIPAL_ENV);
    String tenantId = required(environment, ArtifactMcpStdioMain.TENANT_ENV);
    Set<Permission> permissions = new LinkedHashSet<>();
    for (String configuredScope : required(environment, ArtifactMcpStdioMain.SCOPES_ENV).split(",")) {
      switch (configuredScope.trim()) {
        case "artifact.search" -> permissions.add(Permission.SEARCH);
        case "artifact.metadata" -> permissions.add(Permission.READ_METADATA);
        case "artifact.text" -> permissions.add(Permission.READ_TEXT);
        case "artifact.content" -> {
          permissions.add(Permission.READ_METADATA);
          permissions.add(Permission.DOWNLOAD);
        }
        case "artifact.*" -> permissions.addAll(
            Set.of(Permission.SEARCH, Permission.READ_METADATA, Permission.READ_TEXT, Permission.DOWNLOAD));
        default -> {
          // Unsupported scopes remain ungranted.
        }
      }
    }
    return new DefaultAuthorizationService(
        Map.of(new DefaultAuthorizationService.Subject(principalId, tenantId), permissions));
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

  private static int nonNegativeInt(Environment environment, String name, int fallback) {
    String value = property(environment, name);
    if (value == null) return fallback;
    try {
      int parsed = Integer.parseInt(value);
      if (parsed < 0) throw new IllegalArgumentException(name + " cannot be negative");
      return parsed;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " must be an integer", exception);
    }
  }

  private static long nonNegativeLong(Environment environment, String name, long fallback) {
    String value = property(environment, name);
    if (value == null) return fallback;
    try {
      long parsed = Long.parseLong(value);
      if (parsed < 0) throw new IllegalArgumentException(name + " cannot be negative");
      return parsed;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " must be an integer", exception);
    }
  }

  private static Duration duration(Environment environment, String secondsName, Duration fallback) {
    String value = property(environment, secondsName);
    return value == null ? fallback : Duration.ofSeconds(positiveLong(environment, secondsName, 1));
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
