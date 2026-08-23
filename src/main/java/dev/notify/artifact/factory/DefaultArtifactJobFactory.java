package dev.notify.artifact.factory;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.job.DeleteJob;
import dev.notify.artifact.job.ExtractedTextJob;
import dev.notify.artifact.job.FetchJob;
import dev.notify.artifact.job.IngestJob;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.ListMetadataJob;
import dev.notify.artifact.job.MetadataJob;
import dev.notify.artifact.job.RetrievalJob;
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
  private final EngineOptions options;
  private final WorkflowManager workflowManager;

  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      EngineOptions options) {
    this(metadataStore, vectorStore, objectStore, durableSpool, dataVerifier, embeddingService,
        authorizationService, options, null);
  }

  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      EngineOptions options,
      WorkflowManager workflowManager) {
    this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
    this.vectorStore = Objects.requireNonNull(vectorStore, "vectorStore");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.durableSpool = Objects.requireNonNull(durableSpool, "durableSpool");
    Objects.requireNonNull(dataVerifier, "dataVerifier");
    this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService");
    this.accessVerifier =
        new ArtifactAccessVerifier(
            Objects.requireNonNull(authorizationService, "authorizationService"), dataVerifier);
    this.options = Objects.requireNonNull(options, "options");
    this.workflowManager = workflowManager;
  }

  @Override
  public Job<Artifact> createIngest(Requests.Ingest request) {
    return new IngestJob(
        request,
        metadataStore,
        durableSpool,
        accessVerifier,
        options,
        workflowManager);
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
    return new ExtractedTextJob(
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
        options);
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
}
