package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.DefaultArtifactMcpEngineProvider;
import dev.notify.artifact.auth.AuthorizationService.Permission;
import dev.notify.artifact.environment.MapEnvironmentSource;
import dev.notify.artifact.environment.StandardEnvironment;
import dev.notify.artifact.mcp.stdio.ArtifactMcpEngineProvider;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.InMemoryJobQueue;
import dev.notify.artifact.queue.JobQueue;
import dev.notify.artifact.worker.WorkerManager;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class DefaultArtifactMcpEngineProviderTest {
  @Test
  void mapsProcessScopesToReadOnlyEnginePermissions() {
    var environment =
        new StandardEnvironment(
            new MapEnvironmentSource(
                "test",
                Map.of(
                    "ARTIFACT_MCP_PRINCIPAL_ID", "principal-a",
                    "ARTIFACT_MCP_TENANT_ID", "tenant-a",
                    "ARTIFACT_MCP_SCOPES",
                        "artifact.search,artifact.metadata,artifact.text,artifact.content")));

    var authorization = DefaultArtifactMcpEngineProvider.authorizationService(environment);

    assertDoesNotThrow(() -> authorization.require("principal-a", "tenant-a", Permission.SEARCH));
    assertDoesNotThrow(
        () -> authorization.require("principal-a", "tenant-a", Permission.READ_METADATA));
    assertDoesNotThrow(() -> authorization.require("principal-a", "tenant-a", Permission.READ_TEXT));
    assertDoesNotThrow(() -> authorization.require("principal-a", "tenant-a", Permission.DOWNLOAD));
    assertThrows(
        SecurityException.class,
        () -> authorization.require("principal-a", "tenant-a", Permission.INGEST));
  }

  @Test
  void registersExactlyOneDefaultProvider() {
    var providers = ServiceLoader.load(ArtifactMcpEngineProvider.class).stream().toList();

    assertEquals(1, providers.size());
    var provider = providers.get(0).get();
    assertInstanceOf(DefaultArtifactMcpEngineProvider.class, provider);
    var environment =
        new StandardEnvironment(
            new MapEnvironmentSource(
                "test",
                Map.of(
                    "ARTIFACT_S3_BUCKET", "artifact-test",
                    "ARTIFACT_S3_KMS_KEY_ID", "test-key",
                    "ARTIFACT_MCP_PRINCIPAL_ID", "principal-a",
                    "ARTIFACT_MCP_TENANT_ID", "tenant-a",
                    "ARTIFACT_MCP_SCOPES", "artifact.metadata",
                    "ARTIFACT_SPOOL_ROOT",
                        Path.of("target", "artifact-provider-test-spool").toString())));
    ArtifactEngine engine = provider.createEngine(environment);
    assertNotNull(engine);
    assertEquals(java.util.List.of(), engine.listMetadata("principal-a", "tenant-a", 10));
    assertThrows(
        SecurityException.class,
        () -> engine.listMetadata("different-principal", "tenant-a", 10));
    provider.close();
  }

  @Test
  void startsOneWorkerPerDurableJobTypeAndExposesThemThroughTheFacade() {
    var provider = new DefaultArtifactMcpEngineProvider();
    try {
      ArtifactEngine engine = provider.createEngine(environment(Map.of()));

      Map<JobRecord.JobType, Long> perType =
          engine.workers().values().stream()
              .collect(Collectors.groupingBy(WorkerManager.WorkerSnapshot::type, Collectors.counting()));
      assertEquals(
          Map.of(
              JobRecord.JobType.STORE, 1L,
              JobRecord.JobType.STORE_INIT, 1L,
              JobRecord.JobType.STORE_PART, 4L,
              JobRecord.JobType.STORE_COMPLETE, 1L,
              JobRecord.JobType.RELEASE_SPOOL, 1L,
              JobRecord.JobType.INDEX, 1L),
          perType);

      WorkerManager.WorkerSnapshot added =
          engine.addWorker(new WorkerManager.WorkerConfiguration(
              "extra-index", 16, 4, 4, Duration.ofMillis(100), JobRecord.JobType.INDEX));
      assertEquals(JobRecord.JobType.INDEX, added.type());
      assertEquals(10, engine.workers().size());

      engine.removeWorker("extra-index");
      assertEquals(9, engine.workers().size());
    } finally {
      provider.close();
    }
  }

  @Test
  void backgroundWorkersCanBeDisabledForAReadOnlyReplica() {
    var provider = new DefaultArtifactMcpEngineProvider();
    try {
      ArtifactEngine engine =
          provider.createEngine(environment(Map.of("ARTIFACT_BACKGROUND_WORKERS_ENABLED", "false")));

      assertTrue(engine.workers().isEmpty());
      assertEquals(Map.of(), engine.jobStates());
    } finally {
      provider.close();
    }
  }

  @Test
  void exposesQueueRegistrationThroughTheFacade() {
    var provider = new DefaultArtifactMcpEngineProvider();
    try {
      ArtifactEngine engine =
          provider.createEngine(environment(Map.of("ARTIFACT_BACKGROUND_WORKERS_ENABLED", "false")));
      JobQueue queue = new InMemoryJobQueue();

      engine.addQueue(JobRecord.JobType.INDEX, queue);
      assertThrows(
          IllegalStateException.class,
          () -> engine.addQueue(JobRecord.JobType.INDEX, new InMemoryJobQueue()));
      assertEquals(queue, engine.removeQueue(JobRecord.JobType.INDEX).orElseThrow());
      assertTrue(engine.removeQueue(JobRecord.JobType.INDEX).isEmpty());
    } finally {
      provider.close();
    }
  }

  private static StandardEnvironment environment(Map<String, String> overrides) {
    Map<String, String> values = new HashMap<>(Map.of(
        "ARTIFACT_S3_BUCKET", "artifact-test",
        "ARTIFACT_S3_KMS_KEY_ID", "test-key",
        "ARTIFACT_MCP_PRINCIPAL_ID", "principal-a",
        "ARTIFACT_MCP_TENANT_ID", "tenant-a",
        "ARTIFACT_MCP_SCOPES", "artifact.metadata",
        "ARTIFACT_SPOOL_ROOT", Path.of("target", "artifact-provider-test-spool").toString()));
    values.putAll(overrides);
    return new StandardEnvironment(new MapEnvironmentSource("test", values));
  }
}
