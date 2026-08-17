package dev.notify.artifact.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.DefaultArtifactEngine;
import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.dispatcher.QueuingJobDispatcher;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.AwsTextractOcr;
import dev.notify.artifact.extract.Ocr;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.factory.ArtifactJobFactory;
import dev.notify.artifact.factory.DefaultArtifactJobFactory;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.IOException;
import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.textract.TextractClient;

/**
 * Minimal Spring edge configuration. Applications override store/provider/auth beans for
 * production.
 */
@Configuration(proxyBeanMethods = false)
public class ArtifactEngineConfiguration {
  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean(TextractClient.class)
  @ConditionalOnProperty(
      prefix = "artifact.ocr.textract", name = "enabled", havingValue = "true")
  public TextractClient artifactTextractClient() {
    return TextractClient.create();
  }

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
  @ConditionalOnMissingBean(Ocr.class)
  @ConditionalOnBean(TextractClient.class)
  public Ocr artifactTextractOcr(
      TextractClient textractClient,
      @Value("${artifact.ocr.textract.max-input-bytes:10485760}") int maxInputBytes,
      @Value("${artifact.ocr.max-output-characters:1000000}") int maxOutputCharacters) {
    return new AwsTextractOcr(textractClient, maxInputBytes, maxOutputCharacters);
  }

  @Bean
  @ConditionalOnMissingBean(TextExtractorFactory.class)
  public TextExtractorFactory artifactTextExtractorFactory(
      @Value("${artifact.extract.max-input-bytes:134217728}") int maxInputBytes,
      @Value("${artifact.extract.max-output-characters:1000000}") int maxOutputCharacters) {
    return TextExtractorFactory.defaults(maxInputBytes, maxOutputCharacters);
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
  public JobDispatcher artifactJobDispatcher(QueueManager queueManager) {
    return new QueuingJobDispatcher(queueManager);
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
      EngineOptions options) {
    return new DefaultArtifactJobFactory(
        metadata, vectors, objects, spool, verifier, embeddings, auth, options);
  }

  @Bean
  @ConditionalOnMissingBean(ArtifactEngine.class)
  public ArtifactEngine artifactEngine(ArtifactJobFactory jobFactory, JobDispatcher jobDispatcher) {
    return new DefaultArtifactEngine(jobFactory, jobDispatcher);
  }
}
