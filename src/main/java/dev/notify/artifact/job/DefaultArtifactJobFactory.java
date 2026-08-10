package dev.notify.artifact.job;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.security.IngestionSecurityContext;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/** Default factory that supplies workflow jobs with their required infrastructure collaborators. */
public final class DefaultArtifactJobFactory implements ArtifactJobFactory {
  private final MetadataStore metadataStore;
  private final VectorStore vectorStore;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final DataVerifier dataVerifier;
  private final EmbeddingService embeddingService;
  private final AuthorizationService authorizationService;
  private final EngineOptions options;
  private final SecurityFilterChain<IngestionSecurityContext> ingestionSecurity;
  private final SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity;
  private final SecurityContextFactory securityContextFactory;

  public DefaultArtifactJobFactory(
      MetadataStore metadataStore,
      VectorStore vectorStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      DataVerifier dataVerifier,
      EmbeddingService embeddingService,
      AuthorizationService authorizationService,
      EngineOptions options,
      SecurityFilterChain<IngestionSecurityContext> ingestionSecurity,
      SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity,
      SecurityContextFactory securityContextFactory) {
    this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
    this.vectorStore = Objects.requireNonNull(vectorStore, "vectorStore");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.durableSpool = Objects.requireNonNull(durableSpool, "durableSpool");
    this.dataVerifier = Objects.requireNonNull(dataVerifier, "dataVerifier");
    this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService");
    this.authorizationService =
        Objects.requireNonNull(authorizationService, "authorizationService");
    this.options = Objects.requireNonNull(options, "options");
    this.ingestionSecurity = ingestionSecurity;
    this.retrievalSecurity = retrievalSecurity;
    this.securityContextFactory = securityContextFactory;
    if ((ingestionSecurity == null) != (securityContextFactory == null)
        || (retrievalSecurity == null) != (securityContextFactory == null)) {
      throw new IllegalArgumentException(
          "Security chains and context factory must be configured together");
    }
  }

  @Override
  public Job<Artifact> createIngest(Requests.Ingest request) {
    return new IngestJob(
        request,
        metadataStore,
        durableSpool,
        dataVerifier,
        authorizationService,
        options,
        ingestionSecurity,
        securityContextFactory);
  }

  @Override
  public Job<Artifact> createMetadata(String principalId, String tenantId, String artifactId) {
    return new MetadataJob(
        principalId,
        tenantId,
        artifactId,
        metadataStore,
        authorizationService,
        retrievalSecurity,
        securityContextFactory);
  }

  @Override
  public Job<List<Artifact>> createListMetadata(
      String principalId, String tenantId, int limit) {
    return new ListMetadataJob(principalId, tenantId, limit, metadataStore, authorizationService);
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
        authorizationService,
        retrievalSecurity,
        securityContextFactory);
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
        authorizationService,
        retrievalSecurity,
        securityContextFactory);
  }

  @Override
  public Job<List<Requests.SearchHit>> createRetrieval(Requests.Search request) {
    return new RetrievalJob(
        request,
        metadataStore,
        vectorStore,
        embeddingService,
        authorizationService,
        options,
        retrievalSecurity,
        securityContextFactory);
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
        authorizationService);
  }
}
