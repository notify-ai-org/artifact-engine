package dev.notify.artifact.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.store.postgres.PostgresVectorStore;
import dev.notify.artifact.store.s3.S3ObjectStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.services.s3.S3Client;

/** Opt-in production store beans; application infrastructure owns the S3 client and data source. */
@Configuration(proxyBeanMethods = false)
@Import(ArtifactJpaStoreConfiguration.class)
public class ArtifactProductionStoreConfiguration {
  @Bean
  @Primary
  @ConditionalOnBean(S3Client.class)
  @ConditionalOnProperty(prefix = "artifact.s3", name = "enabled", havingValue = "true")
  public ObjectStore artifactS3ObjectStore(
      S3Client s3Client,
      @Value("${artifact.s3.bucket}") String bucket,
      @Value("${artifact.s3.environment}") String environment,
      @Value("${artifact.s3.kms-key-id}") String kmsKeyId,
      @Value("${artifact.s3.expected-bucket-owner:#{null}}") String expectedBucketOwner,
      @Value("${artifact.s3.bucket-key-enabled:true}") boolean bucketKeyEnabled) {
    return new S3ObjectStore(
        s3Client,
        new S3ObjectStore.Configuration(
            bucket, environment, kmsKeyId, expectedBucketOwner, bucketKeyEnabled));
  }

  @Bean
  @Primary
  @ConditionalOnBean(JdbcTemplate.class)
  @ConditionalOnProperty(
      prefix = "artifact.vector.postgres",
      name = "enabled",
      havingValue = "true")
  public VectorStore artifactPostgresVectorStore(
      JdbcTemplate jdbcTemplate,
      ObjectMapper objectMapper,
      @Value("${artifact.vector.postgres.dimensions:1536}") int dimensions) {
    return new PostgresVectorStore(jdbcTemplate, objectMapper, dimensions);
  }
}
