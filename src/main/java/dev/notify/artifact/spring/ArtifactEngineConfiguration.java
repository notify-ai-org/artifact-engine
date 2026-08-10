package dev.notify.artifact.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.DefaultArtifactEngine;
import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.job.ArtifactJobFactory;
import dev.notify.artifact.job.DefaultArtifactJobFactory;
import dev.notify.artifact.job.DirectJobDispatcher;
import dev.notify.artifact.job.JobDispatcher;
import dev.notify.artifact.security.ArtifactSecurity;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.IOException;
import java.nio.file.Path;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Minimal Spring edge configuration. Applications override store/provider/auth beans for
 * production.
 */
@Configuration(proxyBeanMethods = false)
public class ArtifactEngineConfiguration {
  @Bean
  @ConditionalOnMissingBean(MetadataStore.class)
  public MetadataStore artifactMetadataStore() {
    return new InMemoryStores.Metadata();
  }

  @Bean
  @ConditionalOnMissingBean(VectorStore.class)
  public VectorStore artifactVectorStore() {
    return new InMemoryStores.Vectors();
  }

  @Bean
  @ConditionalOnMissingBean(DataVerifier.class)
  public DataVerifier artifactDataVerifier() {
    return new DataVerifier();
  }

  @Bean
  @ConditionalOnMissingBean(DurableSpool.class)
  public DurableSpool artifactDurableSpool(ObjectMapper mapper) throws IOException {
    long maxArtifactBytes = Long.getLong("artifact.spool.max-artifact-bytes", 128L * 1024 * 1024);
    DurableSpool.Limits limits =
        new DurableSpool.Limits(
            maxArtifactBytes,
            Long.getLong("artifact.spool.max-bytes", 10L * 1024 * 1024 * 1024),
            Long.getLong("artifact.spool.max-files", 100_000),
            Long.getLong("artifact.spool.max-tenant-bytes", 1024L * 1024 * 1024),
            Long.getLong("artifact.spool.max-tenant-files", 10_000));
    return new DurableSpool(
        Path.of(System.getProperty("artifact.spool.root", "./data/artifact-spool")),
        limits,
        mapper);
  }

  @Bean
  @ConditionalOnMissingBean(EngineOptions.class)
  public EngineOptions artifactEngineOptions() {
    return EngineOptions.defaults();
  }

  @Bean
  @ConditionalOnMissingBean(JobDispatcher.class)
  public JobDispatcher artifactJobDispatcher() {
    return new DirectJobDispatcher();
  }

  @Bean
  @ConditionalOnMissingBean(ArtifactJobFactory.class)
  public ArtifactJobFactory artifactJobFactory(
      MetadataStore metadata,
      VectorStore vectors,
      ObjectStore objects,
      DurableSpool spool,
      DataVerifier verifier,
      EmbeddingService embeddings,
      AuthorizationService auth,
      EngineOptions options,
      ObjectProvider<ArtifactSecurity> securityProvider) {
    ArtifactSecurity security = securityProvider.getIfAvailable();
    if (security == null) {
      if (!Boolean.getBoolean("artifact.security.allow-insecure-local")) {
        throw new IllegalStateException(
            "Artifact security adapters are missing. Configure AuthenticationService, "
                + "MalwareScanner, and SecurityContextFactory, or explicitly enable the "
                + "insecure local-development mode.");
      }
      return new DefaultArtifactJobFactory(
          metadata, vectors, objects, spool, verifier, embeddings, auth, options, null, null, null);
    }
    return new DefaultArtifactJobFactory(
        metadata,
        vectors,
        objects,
        spool,
        verifier,
        embeddings,
        auth,
        options,
        security.ingestion(),
        security.retrieval(),
        security.contextFactory());
  }

  @Bean
  @ConditionalOnMissingBean(ArtifactEngine.class)
  public ArtifactEngine artifactEngine(ArtifactJobFactory jobFactory, JobDispatcher jobDispatcher) {
    return new DefaultArtifactEngine(jobFactory, jobDispatcher);
  }
}
