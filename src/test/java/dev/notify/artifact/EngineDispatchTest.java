package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.factory.ArtifactJobFactory;
import dev.notify.artifact.job.DeleteJob;
import dev.notify.artifact.job.ExtractedTextJob;
import dev.notify.artifact.job.FetchJob;
import dev.notify.artifact.job.IngestJob;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.ListMetadataJob;
import dev.notify.artifact.job.MetadataJob;
import dev.notify.artifact.job.RetrievalJob;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.Requests;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineDispatchTest {
  @Test
  void facadeCreatesAndDispatchesOneTypedJobPerOperation() throws Exception {
    List<String> created = new ArrayList<>();
    List<String> dispatched = new ArrayList<>();
    Artifact artifact = artifact();
    Requests.Ingest ingest = ingestRequest();
    Requests.Search search =
        new Requests.Search("tenant", "principal", "query", 5, List.of(), List.of(), null);
    ArtifactJobFactory factory = new RecordingFactory(created, artifact);
    JobDispatcher dispatcher =
        new JobDispatcher() {
          @Override
          public <R> R dispatch(Job<R> job) throws Exception {
            dispatched.add(((TaggedJob<?>) job).tag());
            return job.execute();
          }
        };
    ArtifactEngine engine = new DefaultArtifactEngine(factory, dispatcher);

    assertSame(artifact, engine.ingest(ingest));
    assertSame(artifact, engine.metadata("principal", "tenant", "artifact"));
    assertEquals(List.of(artifact), engine.listMetadata("principal", "tenant", 10));
    try (InputStream content = engine.content("principal", "tenant", "artifact")) {
      assertEquals("content", new String(content.readAllBytes(), StandardCharsets.UTF_8));
    }
    assertEquals("text", engine.extractedText("principal", "tenant", "artifact", 100));
    assertEquals(List.of(), engine.search(search));
    engine.delete("principal", "tenant", "artifact");

    assertEquals(
        List.of("ingest", "metadata", "list-metadata", "fetch", "text", "retrieval", "delete"),
        created);
    assertEquals(created, dispatched);
  }

  @Test
  void engineHoldsOnlyFactoryAndDispatcherAndJobsNeverReferenceEngine() {
    assertEquals(
        java.util.Set.of(ArtifactJobFactory.class, JobDispatcher.class),
        java.util.Arrays.stream(DefaultArtifactEngine.class.getDeclaredFields())
            .map(java.lang.reflect.Field::getType)
            .collect(java.util.stream.Collectors.toSet()));

    for (Class<?> jobType :
        List.of(
            IngestJob.class,
            MetadataJob.class,
            ListMetadataJob.class,
            FetchJob.class,
            ExtractedTextJob.class,
            RetrievalJob.class,
            DeleteJob.class)) {
      boolean referencesEngine =
          java.util.Arrays.stream(jobType.getDeclaredFields())
              .anyMatch(field -> ArtifactEngine.class.isAssignableFrom(field.getType()));
      assertEquals(false, referencesEngine, jobType.getSimpleName());
    }
  }

  private static Artifact artifact() {
    Instant now = Instant.parse("2026-08-10T00:00:00Z");
    return new Artifact(
        "artifact",
        "tenant",
        "key",
        "fingerprint",
        "UPLOAD",
        null,
        "notes.txt",
        "text/plain",
        7,
        "sha256",
        null,
        Path.of("content"),
        ArtifactStatus.Storage.SPOOLED,
        ArtifactStatus.Index.PENDING,
        1,
        Map.of(),
        null,
        null,
        now,
        now);
  }

  private static Requests.Ingest ingestRequest() {
    byte[] content = "content".getBytes(StandardCharsets.UTF_8);
    return new Requests.Ingest(
        "tenant",
        "principal",
        "key",
        "notes.txt",
        "text/plain",
        new ByteArrayInputStream(content),
        content.length,
        Map.of());
  }

  private record TaggedJob<R>(String tag, R result) implements Job<R> {
    @Override
    public R execute() {
      return result;
    }
  }

  private static final class RecordingFactory implements ArtifactJobFactory {
    private final List<String> created;
    private final Artifact artifact;

    private RecordingFactory(List<String> created, Artifact artifact) {
      this.created = created;
      this.artifact = artifact;
    }

    @Override
    public Job<Artifact> createIngest(Requests.Ingest request) {
      return job("ingest", artifact);
    }

    @Override
    public Job<Artifact> createMetadata(String principalId, String tenantId, String artifactId) {
      return job("metadata", artifact);
    }

    @Override
    public Job<List<Artifact>> createListMetadata(
        String principalId, String tenantId, int limit) {
      return job("list-metadata", List.of(artifact));
    }

    @Override
    public Job<InputStream> createFetch(String principalId, String tenantId, String artifactId) {
      return job("fetch", new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public Job<String> createExtractedText(
        String principalId, String tenantId, String artifactId, int maxCharacters) {
      return job("text", "text");
    }

    @Override
    public Job<List<Requests.SearchHit>> createRetrieval(Requests.Search request) {
      return job("retrieval", List.of());
    }

    @Override
    public Job<Void> createDelete(String principalId, String tenantId, String artifactId) {
      return job("delete", null);
    }

    private <R> Job<R> job(String tag, R result) {
      created.add(tag);
      return new TaggedJob<>(tag, result);
    }
  }
}
