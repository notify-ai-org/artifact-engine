package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import dev.notify.artifact.DefaultArtifactEngine;
import dev.notify.artifact.environment.StandardEnvironment;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.chunk.ChunkBufferPool;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.chunk.SourcePolicy;
import dev.notify.artifact.chunk.UrlChunkSource;
import dev.notify.artifact.dispatcher.DirectJobDispatcher;
import dev.notify.artifact.dispatcher.QueuingJobDispatcher;
import dev.notify.artifact.dispatcher.RoutingJobDispatcher;
import dev.notify.artifact.embed.EmbeddingCache;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.factory.DefaultArtifactJobFactory;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReleaser;
import dev.notify.artifact.store.InMemoryIndexProgressStore;
import dev.notify.artifact.store.InMemoryJobStore;
import dev.notify.artifact.store.InMemoryMultipartUploadStore;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.testing.InMemoryMultipartObjectStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.chunk.UrlChunkSource.UnsupportedSourceException;
import dev.notify.artifact.worker.DefaultJobRecordExecutor;
import dev.notify.artifact.worker.DirectJobWorker;
import dev.notify.artifact.worker.WorkerManager;
import dev.notify.artifact.workflow.InMemoryWorkflowStore;
import dev.notify.artifact.workflow.Workflow;
import dev.notify.artifact.workflow.WorkflowManager;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChunkedSourceIngestTest {
  private static final int MIB = 1024 * 1024;
  private static final int CHUNK = 5 * MIB;
  private static final String TENANT = "tenant";

  @TempDir Path spoolRoot;
  @TempDir Path sources;

  private final Chunker chunker = new Chunker(2000, 100);
  private InMemoryStores.Metadata metadata;
  private InMemoryStores.Vectors vectors;
  private InMemoryMultipartObjectStore objects;
  private InMemoryWorkflowStore workflows;
  private InMemoryIndexProgressStore progress;
  private DurableSpool spool;
  private ChunkBufferPool pool;
  private QueueManager queues;
  private WorkerManager workers;
  private WorkflowManager workflowManager;
  private DirectJobWorker directWorker;
  private EmbeddingService embeddings;
  private ChunkedIngestSupport support;
  private DefaultArtifactEngine engine;
  private HttpServer server;
  private volatile byte[] served;
  private volatile boolean serveRanges = true;

  @BeforeEach
  void setUp() throws Exception {
    metadata = new InMemoryStores.Metadata();
    vectors = new InMemoryStores.Vectors();
    objects = new InMemoryMultipartObjectStore();
    workflows = new InMemoryWorkflowStore();
    progress = new InMemoryIndexProgressStore();
    spool = new DurableSpool(spoolRoot, 64L * MIB, new ObjectMapper());
    pool = new ChunkBufferPool(CHUNK, 2, Duration.ofMinutes(1));
    embeddings = new EmbeddingService(fakeProvider(), noCache(), 64);
    SourcePolicy policy = new SourcePolicy(List.of(sources), true,
        UrlChunkSource.defaultClient(Duration.ofSeconds(5)), Duration.ofSeconds(30));
    support = new ChunkedIngestSupport(pool, policy, progress, CHUNK, Duration.ofSeconds(30));
    InMemoryMultipartUploadStore uploads = new InMemoryMultipartUploadStore();
    DefaultJobRecordExecutor executor = new DefaultJobRecordExecutor(
        metadata, objects, spool, vectors, embeddings, TextExtractorFactory.defaults(64 * MIB,
            Integer.MAX_VALUE), null, chunker, IndexJob.Options.defaults(), uploads,
        new SpoolReleaser(metadata, spool), new StorageKeyFactory("test", Clock.systemUTC()),
        support);

    queues = new QueueManager();
    queues.start();
    List<WorkerManager.WorkerConfiguration> configs = new ArrayList<>();
    for (JobRecord.JobType type : List.of(JobRecord.JobType.STORE_INIT,
        JobRecord.JobType.BUFFER_CHUNK, JobRecord.JobType.SPOOL_CHUNK,
        JobRecord.JobType.STORE_CHUNK, JobRecord.JobType.INDEX_CHUNK,
        JobRecord.JobType.RELEASE_BUFFER, JobRecord.JobType.SPOOL_COMMIT,
        JobRecord.JobType.INDEX_FINAL, JobRecord.JobType.STORE_COMPLETE,
        JobRecord.JobType.RELEASE_SPOOL)) {
      configs.add(new WorkerManager.WorkerConfiguration(
          "w-" + type, 16, 2, 2, Duration.ofMillis(20), type));
    }
    workers = new WorkerManager(failure -> {}, null, configs, 32, queues, executor,
        new InMemoryJobStore(), Duration.ofMinutes(5), Duration.ofMillis(20));
    workflowManager = new WorkflowManager(
        workflows, queues, Duration.ofMillis(50), Throwable::printStackTrace, workers);
    workflowManager.start();

    directWorker = new DirectJobWorker(2, 16);
    DefaultArtifactJobFactory factory = new DefaultArtifactJobFactory(
        metadata, vectors, objects, spool, new DataVerifier(), embeddings,
        (principal, tenant, permission) -> {}, new StandardEnvironment(), workflowManager, support);
    engine = new DefaultArtifactEngine(factory,
        new RoutingJobDispatcher(new DirectJobDispatcher(directWorker),
            new QueuingJobDispatcher(queues)), workers);
  }

  @AfterEach
  void tearDown() {
    if (server != null) server.stop(0);
    workflowManager.close();
    workers.close();
    queues.close();
    directWorker.close();
    embeddings.close();
    pool.close();
  }

  @Test
  void ingestsATextUrlChunkByChunkWithTheSameChunksAsWholeFileIndexing() throws Exception {
    String text = text(12 * MIB + 12_345);
    served = text.getBytes(StandardCharsets.UTF_8);

    Artifact planned = engine.ingestFromSource(new Requests.IngestSource(
        TENANT, "principal", null, startServer(), null, "text/plain", Map.of()));

    assertEquals(ArtifactStatus.Storage.RECEIVING, planned.storageStatus());
    assertNull(planned.spoolPath(), "text ingests never touch the spool");
    assertEquals(0, spool.usage().bytes());
    Workflow done = await(planned.id());
    assertEquals(Workflow.WorkflowStatus.COMPLETED, done.status(), done.failureMessage());
    assertEquals(1 + 3 * 4 + 2, done.workflowSteps().size(),
        "STORE_INIT, 3 x (buffer, store, index, release), STORE_COMPLETE + INDEX_FINAL");
    assertTrue(done.workflowSteps().stream().map(step -> step.jobRecord().type())
        .noneMatch(type -> type == JobRecord.JobType.SPOOL_CHUNK
            || type == JobRecord.JobType.SPOOL_COMMIT
            || type == JobRecord.JobType.RELEASE_SPOOL));

    Artifact stored = current(planned.id());
    assertEquals(ArtifactStatus.Storage.STORED, stored.storageStatus());
    assertEquals(ArtifactStatus.Index.READY, stored.indexStatus());
    assertEquals(Checksum.sha256(new ByteArrayInputStream(served)), stored.sha256());
    assertTrue(stored.hasContentDigest());
    assertNull(stored.spoolPath(), "the spool copy is released after store and index");
    assertArrayEquals(served, objects.object(stored.storageKey()));

    List<String> indexed = vectors.chunks(TENANT, planned.id()).stream()
        .map(ArtifactChunk::text).toList();
    assertEquals(chunker.chunk(text), indexed,
        "chunk-by-chunk indexing must match indexing the whole text at once");
    assertEquals(0, pool.stats().cachedChunks(), "every chunk buffer was released");
    assertTrue(pool.stats().allocatedSlabs() <= 2);
    assertTrue(progress.find(TENANT, planned.id(), 1).isEmpty());
    assertEquals(0, spool.usage().bytes());
  }

  @Test
  void aRetriedChunkIsNeitherIndexedNorHashedTwice() throws Exception {
    String text = text(200_000);
    Path file = Files.writeString(sources.resolve("notes.txt"), text);
    String location = file.toUri().toString();
    String sourceVersion = support.sources().open(location).describe().version();
    int length = (int) Files.size(file);
    Instant now = Instant.now();
    Artifact artifact = metadata.save(new Artifact("retry-1", TENANT, "k", "f", "FILE", location,
        "notes.txt", "text/plain", length, Artifact.PENDING_DIGEST_PREFIX + "0".repeat(56), null,
        null, ArtifactStatus.Storage.RECEIVING, ArtifactStatus.Index.PENDING, 1, Map.of(), null,
        null, now, now));
    JobRecord indexChunk = JobRecord.pending("index-1", TENANT, artifact.id(),
        JobRecord.JobType.INDEX_CHUNK, Map.of("version", "1", "chunk", "1", "offset", "0",
            "length", Integer.toString(length), "sourceVersion", sourceVersion));
    JobRecord indexFinal = JobRecord.pending("final-1", TENANT, artifact.id(),
        JobRecord.JobType.INDEX_FINAL, Map.of("version", "1", "indexMode", "text-chunks"));

    IndexChunkJob first = new IndexChunkJob(indexChunk, metadata, embeddings, vectors, chunker,
        IndexJob.Options.defaults(), support);
    first.execute();
    first.execute(); // a worker retry after the progress was saved
    new IndexFinalJob(indexFinal, metadata, embeddings, vectors, chunker,
        IndexJob.Options.defaults(), support, () -> {
          throw new AssertionError("text is finished from chunk progress");
        }).execute();

    Artifact indexed = current(artifact.id());
    assertEquals(Checksum.sha256(new ByteArrayInputStream(Files.readAllBytes(file))),
        indexed.sha256());
    assertEquals(ArtifactStatus.Index.READY, indexed.indexStatus());
    assertEquals(chunker.chunk(text),
        vectors.chunks(TENANT, artifact.id()).stream().map(ArtifactChunk::text).toList());
  }

  @Test
  void indexesFormatsThatCannotBeSplitFromTheCommittedSpoolFile() throws Exception {
    Path pdf = sources.resolve("report.pdf");
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      document.addPage(page);
      try (PDPageContentStream content = new PDPageContentStream(document, page)) {
        content.beginText();
        content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
        content.newLineAtOffset(50, 700);
        content.showText("Quarterly revenue grew in every region");
        content.endText();
      }
      document.save(pdf.toFile());
    }

    Artifact planned = engine.ingestFromSource(new Requests.IngestSource(
        TENANT, "principal", null, pdf.toUri().toString(), null, null, Map.of()));
    Workflow done = await(planned.id());

    assertEquals(Workflow.WorkflowStatus.COMPLETED, done.status(), done.failureMessage());
    assertEquals("application/pdf", planned.mediaType());
    assertTrue(done.workflowSteps().stream()
        .noneMatch(step -> step.jobRecord().type() == JobRecord.JobType.INDEX_CHUNK));
    Artifact stored = current(planned.id());
    assertEquals(ArtifactStatus.Index.READY, stored.indexStatus());
    assertTrue(vectors.chunks(TENANT, planned.id()).get(0).text().contains("Quarterly revenue"));
    assertArrayEquals(Files.readAllBytes(pdf), objects.object(stored.storageKey()));
  }

  @Test
  void refusesSourcesThatCannotServeRangesWithoutLeavingASpoolReservation() throws Exception {
    served = "short text".getBytes(StandardCharsets.UTF_8);
    serveRanges = false;
    String url = startServer();

    assertThrows(UnsupportedSourceException.class, () -> engine.ingestFromSource(
        new Requests.IngestSource(TENANT, "principal", null, url, null, null, Map.of())));
    assertEquals(0, spool.usage().bytes());
  }

  // ---- helpers -------------------------------------------------------------------------------

  private Workflow await(String artifactId) throws InterruptedException {
    Instant deadline = Instant.now().plusSeconds(120);
    while (Instant.now().isBefore(deadline)) {
      Optional<Workflow> workflow = workflows.incomplete(10).stream()
          .filter(candidate -> artifactId.equals(candidate.attributes().get("artifactId")))
          .findFirst();
      if (workflow.isEmpty()) {
        Workflow finished = findAny(artifactId);
        if (finished != null) return finished;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("workflow did not finish");
  }

  private Workflow findAny(String artifactId) {
    for (Workflow candidate : workflows.retryCandidates(Instant.MAX, 10)) {
      if (artifactId.equals(candidate.attributes().get("artifactId"))) return candidate;
    }
    return workflows.findByJobRecordId(Checksum.sha256(
            TENANT + ":" + artifactId + ":1:" + JobRecord.JobType.STORE_INIT + ":0"))
        .orElse(null);
  }

  private Artifact current(String artifactId) {
    return metadata.find(TENANT, artifactId).orElseThrow();
  }

  /** UTF-8 text mixing ASCII and multi-byte characters, with irregular whitespace. */
  private static String text(int approximateBytes) {
    String[] pieces = {"alpha", "βeta", "gamma", "δelta", "中文", "emoji😀", "naïve", "x"};
    String[] separators = {" ", "  ", "\n", "\t"};
    Random random = new Random(3);
    StringBuilder text = new StringBuilder(approximateBytes);
    int bytes = 0;
    while (bytes < approximateBytes) {
      String piece = pieces[random.nextInt(pieces.length)] + random.nextInt(1000);
      text.append(piece).append(separators[random.nextInt(separators.length)]);
      bytes += piece.getBytes(StandardCharsets.UTF_8).length + 1;
    }
    return text.toString();
  }

  private String startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/doc.txt", exchange -> {
      byte[] body = served;
      exchange.getResponseHeaders().set("ETag", "\"" + body.length + "\"");
      if (serveRanges) exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
      String range = exchange.getRequestHeaders().getFirst("Range");
      if ("HEAD".equals(exchange.getRequestMethod())) {
        exchange.getResponseHeaders().set("Content-Length", Integer.toString(body.length));
        exchange.sendResponseHeaders(200, -1);
      } else if (range != null && serveRanges) {
        String[] bounds = range.substring("bytes=".length()).split("-");
        int start = Integer.parseInt(bounds[0]);
        int end = Integer.parseInt(bounds[1]);
        exchange.getResponseHeaders().set("Content-Range",
            "bytes " + start + "-" + end + "/" + body.length);
        exchange.sendResponseHeaders(206, end - start + 1);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(body, start, end - start + 1);
        }
      } else {
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(body);
        }
      }
      exchange.close();
    });
    server.start();
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/doc.txt";
  }

  private static EmbeddingProvider fakeProvider() {
    return new EmbeddingProvider() {
      public String model() {
        return "test-model";
      }

      public String version() {
        return "1";
      }

      public List<float[]> embed(List<String> texts) {
        return texts.stream().map(text -> new float[] {text.length(), text.hashCode(), 1}).toList();
      }
    };
  }

  private static EmbeddingCache noCache() {
    return new EmbeddingCache() {
      public Optional<float[]> get(String model, String version, String hash) {
        return Optional.empty();
      }

      public void put(String model, String version, String hash, float[] vector) {}
    };
  }
}
