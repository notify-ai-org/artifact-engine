package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.chunk.ChunkBufferPool;
import dev.notify.artifact.chunk.ChunkSource;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.chunk.SourcePolicy;
import dev.notify.artifact.chunk.UrlChunkSource;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.embed.InMemoryEmbeddingCache;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.multipart.MultipartPlan;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReleaser;
import dev.notify.artifact.store.InMemoryIndexProgressStore;
import dev.notify.artifact.store.InMemoryJobStore;
import dev.notify.artifact.store.InMemoryMultipartUploadStore;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.S3ObjectStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.worker.DefaultJobRecordExecutor;
import dev.notify.artifact.worker.JobRecordExecutor;
import dev.notify.artifact.worker.WorkerManager;
import dev.notify.artifact.workflow.InMemoryWorkflowStore;
import dev.notify.artifact.workflow.Workflow;
import dev.notify.artifact.workflow.WorkflowManager;
import dev.notify.artifact.workflow.WorkflowManager.PlannedStep;
import dev.notify.artifact.workflow.WorkflowStep;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * End-to-end chunked ingest of a large public file to S3: the workflow reads the source in
 * {@code Range} requests, one chunk at a time, into the buffer pool, then writes each chunk to the
 * reserved spool entry and uploads it as a multipart part straight from the buffer. The plan comes
 * from {@link SourceIngestJob#stages} (the non-text shape):
 *
 * <pre>
 * STORE_INIT
 * per chunk    BUFFER_CHUNK -> SPOOL_CHUNK -> STORE_CHUNK -> RELEASE_BUFFER
 * final        SPOOL_COMMIT, STORE_COMPLETE -> RELEASE_SPOOL
 * </pre>
 *
 * <p>Two deviations from production, both because public speed-test files are binary:
 * {@code SourceIngestJob.execute} is bypassed (its prefix check accepts only documents, images and
 * UTF-8 text), and SPOOL_COMMIT runs without the media-type re-check for the same reason; it still
 * checks the length, publishes the spool entry, and records the SHA-256. INDEX_FINAL is dropped:
 * binary content has no text to embed. Everything else is the production job code.
 *
 * <p>The source must answer HEAD with {@code Content-Length} and {@code Accept-Ranges: bytes},
 * honour {@code If-Range}, and not redirect. Needs AWS credentials from the default provider
 * chain, a bucket and KMS key, and free disk for the spool (the whole file is reserved up front).
 *
 * <pre>
 * mvn -o test -Dtest=LargeFileS3UploadIT -Dartifact.it.largeUpload=true \
 *   -Dartifact.it.bucket=my-bucket -Dartifact.it.kmsKeyId=alias/my-key \
 *   -Dartifact.it.region=ap-south-1 [-Dartifact.it.url=...] [-Dartifact.it.chunkBytes=...] \
 *   [-Dartifact.it.keepObject=true]
 * </pre>
 */
//@EnabledIfSystemProperty(named = "artifact.it.largeUpload", matches = "true")
class LargeFileS3UploadIT {
  /** 10 GiB of zeros; {@code https://proof.ovh.net/files/1Gb.dat} is a quicker 1 GiB run. */
  private static final String DEFAULT_URL = "https://proof.ovh.net/files/10Gb.dat";
  private static final String TENANT = "large-upload-it";
  private static final long MIB = 1024 * 1024;

  @Test
  void uploadsALargeSourceToS3ChunkByChunk() throws Exception {
    String url = setting("artifact.it.url", DEFAULT_URL);
    String bucket = setting("artifact.it.bucket", "notify-ai-artifact-v1");
    String kmsKeyId =
        setting("artifact.it.kmsKeyId", "arn:aws:kms:ap-south-1:563147319972:alias/aws/s3");
    String region = setting("artifact.it.region", "ap-south-1");
    String environment = setting("artifact.it.environment", "it");
    int chunkBytes = Integer.parseInt(setting("artifact.it.chunkBytes", Long.toString(64 * MIB)));
    int slabs = Integer.parseInt(setting("artifact.it.bufferSlabs", "4"));
    int partWorkers = Integer.parseInt(setting("artifact.it.partWorkers", "4"));
    Duration timeout = Duration.parse(setting("artifact.it.timeout", "PT3H"));
    boolean keepObject = Boolean.parseBoolean(setting("artifact.it.keepObject", "false"));
    Path spoolRoot = Path.of(setting("artifact.it.spoolRoot", "target/large-upload-spool"));

    InMemoryStores.Metadata metadata = new InMemoryStores.Metadata();
    InMemoryMultipartUploadStore uploads = new InMemoryMultipartUploadStore();
    InMemoryWorkflowStore workflows = new InMemoryWorkflowStore();
    DurableSpool spool = new DurableSpool(spoolRoot, Long.MAX_VALUE, new ObjectMapper());
    SourcePolicy sources = new SourcePolicy(List.of(), false,
        UrlChunkSource.defaultClient(Duration.ofSeconds(30)), Duration.ofMinutes(2));

    try (S3Client s3 =
            S3Client.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        ChunkBufferPool pool = new ChunkBufferPool(chunkBytes, slabs, Duration.ofMinutes(5));
        EmbeddingService unusedEmbeddings = unusedEmbeddings();
        QueueManager queues = new QueueManager()) {
      S3ObjectStore objects =
          new S3ObjectStore(
              s3,
              new S3ObjectStore.Configuration(
                  bucket, environment, kmsKeyId, setting("artifact.it.expectedBucketOwner", null),
                  true));
      ChunkedIngestSupport support = new ChunkedIngestSupport(
          pool, sources, new InMemoryIndexProgressStore(), chunkBytes, Duration.ofMinutes(10));

      // 0. Fail fast on credentials, region, or bucket access before reserving any disk.
      try {
        s3.headBucket(request -> request.bucket(bucket));
      } catch (RuntimeException unreachable) {
        throw new AssertionError("Cannot reach s3://" + bucket + " in " + region + ": "
            + causes(unreachable), unreachable);
      }

      // 1. Describe the source (HEAD) and plan its chunks, as SourceIngestJob does.
      ChunkSource.SourceInfo info = sources.open(url).describe();
      MultipartPlan plan = MultipartPlan.forSize(info.length(), chunkBytes);
      if (plan == null) plan = new MultipartPlan(info.length(), chunkBytes, 1);
      assertEquals(chunkBytes, plan.partSize(), "raise -Dartifact.it.chunkBytes for this source");
      long freeBytes = Files.getFileStore(Files.createDirectories(spoolRoot)).getUsableSpace();
      assertTrue(freeBytes > info.length(), String.format(
          "the spool needs %,d bytes but %s has %,d free", info.length(), spoolRoot, freeBytes));
      log("source %s: %,d bytes (version %s), %d chunks of %,d bytes",
          url, info.length(), info.version(), plan.partCount(), plan.partSize());

      // 2. Reserve the spool entry and register the artifact with a provisional digest.
      String artifactId = UUID.randomUUID().toString();
      Path spoolPath = spool.reserve(TENANT, artifactId, info.length(), Map.of("sourceUrl", url));
      Instant now = Instant.now();
      Artifact artifact =
          metadata.save(
              new Artifact(
                  artifactId, TENANT, "it-" + artifactId,
                  "it-" + artifactId, "URL", url, "large-upload.bin",
                  "application/octet-stream", info.length(),
                  Artifact.PENDING_DIGEST_PREFIX + "0".repeat(56), null, spoolPath,
                  ArtifactStatus.Storage.RECEIVING, ArtifactStatus.Index.PENDING, 1, Map.of(),
                  null, null, now, now));

      // 3. The production plan for non-text sources, minus INDEX_FINAL.
      List<List<PlannedStep>> stages =
          withoutType(SourceIngestJob.stages(artifact, plan, info.version(), false, slabs),
              JobRecord.JobType.INDEX_FINAL);

      // 4. Run it on real workers.
      DefaultJobRecordExecutor jobs =
          new DefaultJobRecordExecutor(
              metadata, objects, spool, new InMemoryStores.Vectors(), unusedEmbeddings,
              TextExtractorFactory.defaults(1, 1), null, new Chunker(300, 30),
              IndexJob.Options.defaults(), uploads, new SpoolReleaser(metadata, spool),
              new StorageKeyFactory(environment, Clock.systemUTC()), support);
      JobRecordExecutor executor = record -> record.type() == JobRecord.JobType.SPOOL_COMMIT
          ? () -> commitWithoutMediaCheck(metadata, spool, record)
          : jobs.toJob(record);
      queues.start();
      List<WorkerManager.WorkerConfiguration> workers = new ArrayList<>();
      workers.add(worker("store-init", 1, JobRecord.JobType.STORE_INIT));
      for (int index = 1; index <= partWorkers; index++) {
        workers.add(worker("buffer-" + index, 1, JobRecord.JobType.BUFFER_CHUNK));
        workers.add(worker("store-chunk-" + index, 1, JobRecord.JobType.STORE_CHUNK));
      }
      workers.add(worker("spool-chunk", 1, JobRecord.JobType.SPOOL_CHUNK));
      workers.add(worker("release-buffer", 4, JobRecord.JobType.RELEASE_BUFFER));
      workers.add(worker("spool-commit", 1, JobRecord.JobType.SPOOL_COMMIT));
      workers.add(worker("store-complete", 1, JobRecord.JobType.STORE_COMPLETE));
      workers.add(worker("release-spool", 1, JobRecord.JobType.RELEASE_SPOOL));

      Instant started = Instant.now();
      Workflow finished;
      try (WorkerManager workerManager =
              new WorkerManager(
                  failure -> log("job failed: %s", causes(failure.cause())), null, workers, 32, queues,
                  executor, new InMemoryJobStore(), Duration.ofMinutes(15), Duration.ofMillis(250));
          WorkflowManager workflowManager =
              new WorkflowManager(workflows, queues, Duration.ofSeconds(1), Throwable::printStackTrace,
                  workerManager)) {
        workflowManager.start();
        Workflow workflow =
            workflowManager.createPlan(
                SourceIngestJob.WORKFLOW_NAME,
                stages,
                Map.of("tenantId", TENANT, "artifactId", artifactId, "version", "1"));
        finished = awaitTerminal(workflows, workflow.id(), timeout);
      } catch (Throwable failure) {
        abortQuietly(objects, uploads, artifactId);
        spool.discardReserved(spoolPath);
        throw failure;
      }
      Duration elapsed = Duration.between(started, Instant.now());
      if (finished.status() == Workflow.WorkflowStatus.COMPLETED) {
        log("workflow COMPLETED in %s (%.1f MiB/s end to end)", elapsed,
            info.length() / (double) MIB / Math.max(1, elapsed.toSeconds()));
      } else {
        log("workflow %s after %s: %s", finished.status(), elapsed, finished.failureMessage());
      }

      // 5. Verify.
      if (finished.status() != Workflow.WorkflowStatus.COMPLETED) {
        abortQuietly(objects, uploads, artifactId);
        spool.discardReserved(spoolPath);
      }
      assertEquals(Workflow.WorkflowStatus.COMPLETED, finished.status(), finished.failureMessage());
      Artifact stored = metadata.find(TENANT, artifactId).orElseThrow();
      assertEquals(ArtifactStatus.Storage.STORED, stored.storageStatus());
      assertTrue(stored.hasContentDigest(), "SPOOL_COMMIT records the real SHA-256");
      assertNull(stored.spoolPath(), "the spool copy is released after storing");
      assertFalse(Files.exists(spoolPath));
      assertEquals(0, spool.usage().bytes(), "the spool reservation is released");
      assertEquals(0, pool.stats().cachedChunks(), "every chunk buffer was released");
      assertTrue(uploads.find(TENANT, artifactId, 1).isEmpty());

      HeadObjectResponse head = s3.headObject(request -> request.bucket(bucket).key(stored.storageKey()));
      assertEquals(info.length(), head.contentLength());
      assertNull(head.metadata().get("artifact-sha256"),
          "the upload starts before the whole-file digest is known");
      assertEquals(Checksum.sha256(TENANT), head.metadata().get("tenant-sha256"));
      assertTrue(head.checksumSHA256() == null
              || head.checksumSHA256().endsWith("-" + plan.partCount()),
          "multipart objects carry a composite checksum");
      log("verified s3://%s/%s (sha256 %s)", bucket, stored.storageKey(), stored.sha256());

      if (!keepObject) {
        objects.delete(TENANT, stored.storageKey());
        log("deleted the test object (pass -Dartifact.it.keepObject=true to keep it)");
      }
    }
  }

  /** Removes every step of {@code type} and the dependencies other steps had on it. */
  private static List<List<PlannedStep>> withoutType(
      List<List<PlannedStep>> stages, JobRecord.JobType type) {
    List<List<PlannedStep>> kept = new ArrayList<>();
    for (List<PlannedStep> stage : stages) {
      Set<String> removed = stage.stream()
          .filter(step -> step.job().type() == type)
          .map(step -> step.job().id())
          .collect(Collectors.toSet());
      kept.add(stage.stream()
          .filter(step -> !removed.contains(step.job().id()))
          .map(step -> new PlannedStep(step.job(), step.dependsOnJobIds().stream()
              .filter(id -> !removed.contains(id)).toList()))
          .toList());
    }
    return kept;
  }

  /** {@link SpoolCommitJob} without {@code DataVerifier}, which rejects binary content. */
  private static Artifact commitWithoutMediaCheck(
      InMemoryStores.Metadata metadata, DurableSpool spool, JobRecord record) throws Exception {
    Artifact artifact = metadata.find(record.tenantId(), record.artifactId()).orElseThrow();
    Instant started = Instant.now();
    DurableSpool.SpoolEntry entry = spool.commit(artifact.spoolPath(), artifact.sizeBytes());
    log("spool committed in %s (sha256 %s)", Duration.between(started, Instant.now()),
        entry.sha256());
    return metadata.update(record.tenantId(), record.artifactId(),
        current -> current.withContentDigest(entry.sha256()));
  }

  /** Polls until the workflow completes or crashes, logging chunk progress every 30 seconds. */
  private static Workflow awaitTerminal(InMemoryWorkflowStore workflows, String id, Duration timeout)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    Instant nextLog = Instant.now();
    while (Instant.now().isBefore(deadline)) {
      Workflow current = workflows.find(id).orElseThrow();
      if (current.status() == Workflow.WorkflowStatus.COMPLETED
          || current.status() == Workflow.WorkflowStatus.CRASHED) {
        return current;
      }
      if (!Instant.now().isBefore(nextLog)) {
        log("chunks buffered: %d, uploaded: %d of %d",
            completed(current, JobRecord.JobType.BUFFER_CHUNK),
            completed(current, JobRecord.JobType.STORE_CHUNK),
            current.workflowSteps().stream()
                .filter(step -> step.jobRecord().type() == JobRecord.JobType.STORE_CHUNK).count());
        nextLog = Instant.now().plusSeconds(30);
      }
      Thread.sleep(1_000);
    }
    throw new AssertionError("Upload did not finish within " + timeout);
  }

  private static long completed(Workflow workflow, JobRecord.JobType type) {
    return workflow.workflowSteps().stream()
        .filter(step -> step.jobRecord().type() == type)
        .filter(step -> step.status() == WorkflowStep.WorkflowStepStatus.COMPLETED)
        .count();
  }

  /** Discards parts of an unfinished upload so a failed run does not leave billable storage. */
  private static void abortQuietly(
      S3ObjectStore objects, InMemoryMultipartUploadStore uploads, String artifactId) {
    uploads.find(TENANT, artifactId, 1).ifPresent(upload -> {
      try {
        objects.abortMultipartUpload(TENANT, upload.storageKey(), upload.uploadId());
        log("aborted unfinished multipart upload %s", upload.uploadId());
      } catch (Exception failure) {
        log("could not abort multipart upload %s: %s", upload.uploadId(), failure);
      }
    });
  }

  private static WorkerManager.WorkerConfiguration worker(
      String id, int batchSize, JobRecord.JobType type) {
    return new WorkerManager.WorkerConfiguration(
        "it-" + id, 64, batchSize, batchSize, Duration.ofMillis(100), type);
  }

  /** Required by the executor's index path, which this test never runs. */
  private static EmbeddingService unusedEmbeddings() {
    EmbeddingProvider provider =
        new EmbeddingProvider() {
          public String model() {
            return "unused";
          }

          public String version() {
            return "0";
          }

          public List<float[]> embed(List<String> texts) {
            throw new UnsupportedOperationException("Indexing is not part of this test");
          }
        };
    return new EmbeddingService(provider, new InMemoryEmbeddingCache(1), 1);
  }

  /** "outer message <- cause <- root cause", so wrapped SDK errors are not hidden. */
  private static String causes(Throwable failure) {
    List<String> chain = new ArrayList<>();
    for (Throwable cause = failure; cause != null && chain.size() < 6; cause = cause.getCause()) {
      chain.add(cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }
    return String.join(" <- ", chain);
  }

  private static String setting(String name, String fallback) {
    String value = System.getProperty(name);
    return value == null || value.isBlank() ? fallback : value.trim();
  }

  private static void log(String format, Object... arguments) {
    System.out.printf("[LargeFileS3UploadIT] " + format + "%n", arguments);
  }
}
