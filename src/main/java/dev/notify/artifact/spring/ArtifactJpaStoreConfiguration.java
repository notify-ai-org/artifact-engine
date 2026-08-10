package dev.notify.artifact.spring;

import dev.notify.artifact.spring.jpa.ArtifactEntity;
import dev.notify.artifact.spring.jpa.ArtifactJpaRepository;
import dev.notify.artifact.spring.jpa.JpaLogStore;
import dev.notify.artifact.spring.jpa.JpaMetadataStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Opt-in discovery and wiring for the JPA metadata, outbox, and append-only audit stores. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "artifact.jpa", name = "enabled", havingValue = "true")
@EntityScan(basePackageClasses = ArtifactEntity.class)
@EnableJpaRepositories(basePackageClasses = ArtifactJpaRepository.class)
@Import({JpaMetadataStore.class, JpaLogStore.class})
public class ArtifactJpaStoreConfiguration {}
