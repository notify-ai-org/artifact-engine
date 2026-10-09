package dev.notify.artifact.factory;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.cache.RetrievalResultCache;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.environment.Environment;
import dev.notify.artifact.job.DeleteJob;
import dev.notify.artifact.job.FetchJob;
import dev.notify.artifact.job.IngestJob;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.ListMetadataJob;
import dev.notify.artifact.job.MetadataJob;
import dev.notify.artifact.job.RetrievalJob;
import dev.notify.artifact.job.SourceIngestJob;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import dev.notify.artifact.workflow.WorkflowManager;

/** Default factory that supplies workflow jobs with their required infrastructure collaborators. */
public final class DefaultArtifactJobFactory implements ArtifactJobFactory {
  private final MetadataStore metadataStore;
  private final VectorStore vectorStore;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final EmbeddingService embeddingService;
  private final ArtifactAccessVerifier accessVerifier;
  /** {@code true} (default) or {@code false}: reuse an existing artifact with the same content. */
  public static final String DEDUPLICATE_CONTENT = "ARTIFACT_DEDUPLICATE_CONTENT";

  /** 1 to 100, default 4: candidates fetched per requested search hit before filtering. */
  public static final String RETRIEVAL_CANDIDATE_MULTIPLIER =
      "ARTIFACT_RETRIEVAL_CANDIDATE_MULTIPLIER";

  /**
   * Part size for multipart object uploads, default 64 MiB. Artifacts larger than one part are
   * stored as a multipart upload with parts uploaded in parallel. {@code 0} disables it and every
   * artifact is stored with a single upload.
   */
  public static final String MULTIPART_PART_BYTES = "ARTIFACT_STORE_MULTIPART_PART_BYTES";

  /** S3 rejects non-final parts smaller than 5 MiB. */
  public static final long MIN_PART_BYTES = 5L * 1024 * 1024;

  private final boolean deduplicateContent;
  private final int retrievalCandidateMultiplier;
  private final long multipartPartBytes;
  private final WorkflowManager workflowManager;
  private final ChunkedIngestSupport chunkedIngest;
  private final RetrievalResultCache retrievalCache;

  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      Environment environment) {
    this(metadataStore, vectorStore, objectStore, durableSpool, dataVerifier, embeddingService,
        authorizationService, environment, null);
  }

  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      Environment environment,
      WorkflowManager workflowManager) {
    this(metadataStore, vectorStore, objectStore, durableSpool, dataVerifier, embeddingService,
        authorizationService, environment, workflowManager, null);
  }

  /**
   * @param chunkedIngest enables {@link #createSourceIngest}; requires a workflow manager
   */
  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      Environment environment,
      WorkflowManager workflowManager,
      ChunkedIngestSupport chunkedIngest) {
    this(metadataStore, vectorStore, objectStore, durableSpool, dataVerifier, embeddingService,
        authorizationService, environment, workflowManager, chunkedIngest, null);
  }

  /** @param retrievalCache optional search result cache used by {@link #createRetrieval} */
  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      Environment environment,
      WorkflowManager workflowManager,
      ChunkedIngestSupport chunkedIngest,
      RetrievalResultCache retrievalCache) {
    this.retrievalCache = retrievalCache;
    if (chunkedIngest != null && workflowManager == null) {
      throw new IllegalArgumentException("Chunked ingest requires a workflow manager");
    }
    this.chunkedIngest = chunkedIngest;
    this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
    this.vectorStore = Objects.requireNonNull(vectorStore, "vectorStore");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.durableSpool = Objects.requireNonNull(durableSpool, "durableSpool");
    Objects.requireNonNull(dataVerifier, "dataVerifier");
    this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService");
    this.accessVerifier =
        new ArtifactAccessVerifier(
            Objects.requireNonNull(authorizationService, "authorizationService"), dataVerifier);
    Objects.requireNonNull(environment, "environment");
    this.deduplicateContent = deduplicateContent(environment);
    this.retrievalCandidateMultiplier = retrievalCandidateMultiplier(environment);
    this.multipartPartBytes = multipartPartBytes(environment);
    this.workflowManager = workflowManager;
  }

  @Override
  public Job<Artifact> createIngest(Requests.Ingest request) {
    return new IngestJob(
        request,
        metadataStore,
        durableSpool,
        accessVerifier,
        deduplicateContent,
        multipartPartBytes,
        workflowManager);
  }

  @Override
  public Job<Artifact> createSourceIngest(Requests.IngestSource request) {
    if (chunkedIngest == null) {
      throw new UnsupportedOperationException("Source ingest is not configured");
    }
    return new SourceIngestJob(
        request, metadataStore, durableSpool, accessVerifier, chunkedIngest, workflowManager);
  }

  @Override
  public Job<Artifact> createMetadata(String principalId, String tenantId, String artifactId) {
    return new MetadataJob(
        principalId,
        tenantId,
        artifactId,
        metadataStore,
        accessVerifier);
  }

  @Override
  public Job<List<Artifact>> createListMetadata(
      String principalId, String tenantId, int limit) {
    return new ListMetadataJob(
        principalId, tenantId, limit, metadataStore, accessVerifier);
  }

  @Override
  public Job<InputStream> createFetch(String principalId, String tenantId, String artifactId) {
    return new FetchJob(
        principalId,
        tenantId,
        artifactId,
        metadataStore,
        objectStore,
        durableSpool,
        accessVerifier);
  }

  @Override
  public Job<String> createExtractedText(
      String principalId, String tenantId, String artifactId, int maxCharacters) {
    return new RetrievalJob.ExtractedText(
        principalId,
        tenantId,
        artifactId,
        maxCharacters,
        metadataStore,
        vectorStore,
        accessVerifier);
  }

  @Override
  public Job<List<Requests.SearchHit>> createRetrieval(Requests.Search request) {
    return new RetrievalJob(
        request,
        metadataStore,
        vectorStore,
        embeddingService,
        accessVerifier,
        retrievalCandidateMultiplier,
        retrievalCache);
  }

  @Override
  public Job<Void> createDelete(String principalId, String tenantId, String artifactId) {
    return new DeleteJob(
        principalId,
        tenantId,
        artifactId,
        metadataStore,
        vectorStore,
        objectStore,
        durableSpool,
        accessVerifier);
  }

  private static boolean deduplicateContent(Environment environment) {
    String value = property(environment, DEDUPLICATE_CONTENT);
    if (value == null || "true".equalsIgnoreCase(value)) return true;
    if ("false".equalsIgnoreCase(value)) return false;
    throw new IllegalArgumentException(DEDUPLICATE_CONTENT + " must be true or false");
  }

  private static int retrievalCandidateMultiplier(Environment environment) {
    long multiplier = number(environment, RETRIEVAL_CANDIDATE_MULTIPLIER, 4);
    if (multiplier < 1 || multiplier > 100) {
      throw new IllegalArgumentException(
          RETRIEVAL_CANDIDATE_MULTIPLIER + " must be between 1 and 100");
    }
    return (int) multiplier;
  }

  private static long multipartPartBytes(Environment environment) {
    long bytes = number(environment, MULTIPART_PART_BYTES, 64L * 1024 * 1024);
    if (bytes != 0 && bytes < MIN_PART_BYTES) {
      throw new IllegalArgumentException(MULTIPART_PART_BYTES + " must be 0 or at least 5 MiB");
    }
    return bytes;
  }

  private static long number(Environment environment, String name, long fallback) {
    String value = property(environment, name);
    if (value == null) return fallback;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(name + " must be an integer", exception);
    }
  }

  private static String property(Environment environment, String name) {
    String value = environment.getProperty(name);
    return value == null || value.isBlank() ? null : value.trim();
  }
}
