package dev.notify.artifact.factory;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.cache.RetrievalResultCache;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.environment.Environment;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.ListMetadataJob;
import dev.notify.artifact.job.MetadataJob;
import dev.notify.artifact.job.RetrievalJob;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.VectorStore;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * Creates only the retrieval jobs: search, metadata and extracted text. They read the metadata and
 * vector stores and run as direct jobs, so a read-only engine needs no object store, spool, queue
 * or workflow machinery. Every write, and fetching the original bytes, is rejected.
 */
public final class ReadOnlyArtifactJobFactory implements ArtifactJobFactory {
  private final MetadataStore metadataStore;
  private final VectorStore vectorStore;
  private final EmbeddingService embeddingService;
  private final ArtifactAccessVerifier accessVerifier;
  private final int retrievalCandidateMultiplier;
  private final double minSemanticScore;
  private final RetrievalResultCache retrievalCache;

  /** @param retrievalCache optional search result cache */
  public ReadOnlyArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      Environment environment,
      RetrievalResultCache retrievalCache) {
    this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
    this.vectorStore = Objects.requireNonNull(vectorStore, "vectorStore");
    this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService");
    this.accessVerifier =
        new ArtifactAccessVerifier(
            Objects.requireNonNull(authorizationService, "authorizationService"),
            new DataVerifier());
    this.retrievalCandidateMultiplier =
        DefaultArtifactJobFactory.retrievalCandidateMultiplier(
            Objects.requireNonNull(environment, "environment"));
    this.minSemanticScore = DefaultArtifactJobFactory.minSemanticScore(environment);
    this.retrievalCache = retrievalCache;
  }

  @Override
  public Job<Artifact> createIngest(Requests.Ingest request) {
    throw readOnly("ingest");
  }

  @Override
  public Job<Artifact> createSourceIngest(Requests.IngestSource request) {
    throw readOnly("source ingest");
  }

  @Override
  public Job<Artifact> createMetadata(String principalId, String tenantId, String artifactId) {
    return new MetadataJob(principalId, tenantId, artifactId, metadataStore, accessVerifier);
  }

  @Override
  public Job<List<Artifact>> createListMetadata(String principalId, String tenantId, int limit) {
    return new ListMetadataJob(principalId, tenantId, limit, metadataStore, accessVerifier);
  }

  @Override
  public Job<InputStream> createFetch(String principalId, String tenantId, String artifactId) {
    throw readOnly("content fetch");
  }

  @Override
  public Job<String> createExtractedText(
      String principalId, String tenantId, String artifactId, int maxCharacters) {
    return new RetrievalJob.ExtractedText(
        principalId, tenantId, artifactId, maxCharacters, metadataStore, vectorStore, accessVerifier);
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
        minSemanticScore,
        retrievalCache);
  }

  @Override
  public Job<Void> createDelete(String principalId, String tenantId, String artifactId) {
    throw readOnly("delete");
  }

  private static UnsupportedOperationException readOnly(String operation) {
    return new UnsupportedOperationException(
        "Cannot " + operation + ": this artifact engine is read-only");
  }
}
