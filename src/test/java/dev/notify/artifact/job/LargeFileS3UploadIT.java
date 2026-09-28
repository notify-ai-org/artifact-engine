package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.embed.InMemoryEmbeddingCache;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReleaser;
import dev.notify.artifact.store.InMemoryJobStore;
import dev.notify.artifact.store.InMemoryMultipartUploadStore;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.S3ObjectStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.worker.DefaultJobRecordExecutor;
import dev.notify.artifact.worker.WorkerManager;
import dev.notify.artifact.workflow.InMemoryWorkflowStore;
import dev.notify.artifact.workflow.Workflow;
import dev.notify.artifact.workflow.WorkflowManager;
import dev.notify.artifact.workflow.WorkflowStep;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * End-to-end upload of a real ~5 GB file to S3 through the artifact engine's large-file pipeline:
 * the download streams into the durable spool (hashed while copying), then the workflow manager
 * and workers run STORE_INIT, parallel STORE_PARTs, STORE_COMPLETE, and RELEASE_SPOOL against S3.
 *
 * <p>Intake ({@code engine.ingest}) is bypassed on purpose: its {@code DataVerifier} accepts only
 * documents, images, and UTF-8 text, and public speed-test files are binary. Everything intake
 * hands off to &mdash; spooling, workflow planning, multipart storage, verification, spool release
 * &mdash; is the production code path. Indexing is skipped: binary content has no text to embed.
 *
 * <p>Never runs in a normal build: the class name ends in {@code IT} (outside surefire's default
 * includes) and it also requires {@code -Dartifact.it.largeUpload=true}. Needs AWS credentials
 * from the default provider chain, a bucket and KMS key, and ~5 GB of free disk for the spool.
 *
 * <pre>
 * mvn -o test -Dtest=LargeFileS3UploadIT -Dartifact.it.largeUpload=true \
 *   -Dartifact.it.bucket=my-bucket -Dartifact.it.kmsKeyId=alias/my-key \
 *   -Dartifact.it.region=ap-south-1 [-Dartifact.it.url=...] [-Dartifact.it.keepObject=true]
 * </pre>
 */
@EnabledIfSystemProperty(named = "artifact.it.largeUpload", matches = "true")
class LargeFileS3UploadIT {
  private static final String DEFAULT_URL =
      "https://files.freetestfiles.com/Large/Sample-File-5GB.bin";
  private static final String TENANT = "large-upload-it";
  private static final long MIB = 1024 * 1024;

  @Test
  void uploadsAFiveGigabyteDownloadToS3WithParallelMultipartParts() throws Exception {
    String url = setting("artifact.it.url", DEFAULT_URL);
    String bucket = required("artifact.it.bucket");
    String kmsKeyId = required("artifact.it.kmsKeyId");
    String region = setting("artifact.it.region", "ap-south-1");
    String environment = setting("artifact.it.environment", "it");
    long partBytes = Long.parseLong(setting("artifact.it.partBytes", Long.toString(64 * MIB)));
    int partWorkers = Integer.parseInt(setting("artifact.it.partWorkers", "4"));
    Duration timeout = Duration.parse(setting("artifact.it.timeout", "PT3H"));
    boolean keepObject = Boolean.parseBoolean(setting("artifact.it.keepObject", "false"));
    Path spoolRoot = Path.of(setting("artifact.it.spoolRoot", "target/large-upload-spool"));

    InMemoryStores.Metadata metadata = new InMemoryStores.Metadata();
    DurableSpool spool = new DurableSpool(spoolRoot, 8L * 1024 * MIB, new ObjectMapper());
    InMemoryMultipartUploadStore uploads = new InMemoryMultipartUploadStore();
    InMemoryWorkflowStore workflows = new InMemoryWorkflowStore();

    try (S3Client s3 =
            S3Client.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        EmbeddingService unusedEmbeddings = unusedEmbeddings();
        QueueManager queues = new QueueManager()) {
      S3ObjectStore objects =
          new S3ObjectStore(
              s3,
              new S3ObjectStore.Configuration(
                  bucket, environment, kmsKeyId, setting("artifact.it.expectedBucketOwner", null),
                  true));

      // 1. Stream the download into the spool. Nothing is buffered in memory; the SHA-256 is
      //    computed while the bytes are written.
      String artifactId = UUID.randomUUID().toString();
      Instant downloadStarted = Instant.now();
      DurableSpool.SpoolEntry spooled;
      try (InputStream body = download(url)) {
        spooled = spool.write(TENANT, artifactId, body, Map.of("sourceUrl", url));
      }
      log("downloaded %,d bytes in %s (sha256 %s)",
          spooled.sizeBytes(), Duration.between(downloadStarted, Instant.now()), spooled.sha256());
      assertTrue(spooled.sizeBytes() > partBytes, "file must be larger than one part");

      // 2. Register the artifact as intake would after verification.
      Instant now = Instant.now();
      Artifact artifact =
          metadata.save(
              new Artifact(
                  artifactId, TENANT, "it-" + artifactId, spooled.sha256(), "URL", url,
                  "Sample-File-5GB.bin", "application/octet-stream", spooled.sizeBytes(),
                  spooled.sha256(), null, spooled.contentPath(), ArtifactStatus.Storage.SPOOLED,
                  ArtifactStatus.Index.PENDING, 1, Map.of(), null, null, now, now));

      // 3. Plan the workflow exactly as IngestJob does, minus INDEX (nothing to extract).
      List<List<JobRecord>> stages = new ArrayList<>();
      for (List<JobRecord> stage : IngestJob.initialStages(artifact, partBytes)) {
        if (stage.get(0).type() != JobRecord.JobType.INDEX) stages.add(stage);
      }
      int partCount = stages.get(1).size();
      log("planned %d parts of %,d bytes", partCount,
          Long.parseLong(stages.get(1).get(0).attributes().get("length")));

      // 4. Run it on real workers.
      SpoolReleaser releaser = new SpoolReleaser(metadata, spool);
      DefaultJobRecordExecutor executor =
          new DefaultJobRecordExecutor(
              metadata, objects, spool, new InMemoryStores.Vectors(), unusedEmbeddings,
              TextExtractorFactory.defaults(1, 1), null, new Chunker(300, 30),
              IndexJob.Options.defaults(), uploads, releaser,
              new StorageKeyFactory(environment, Clock.systemUTC()));
      queues.start();
      List<WorkerManager.WorkerConfiguration> workers = new ArrayList<>();
      workers.add(worker("store-init", 1, JobRecord.JobType.STORE_INIT));
      for (int index = 1; index <= partWorkers; index++) {
        workers.add(worker("store-part-" + index, 2, JobRecord.JobType.STORE_PART));
      }
      workers.add(worker("store-complete", 1, JobRecord.JobType.STORE_COMPLETE));
      workers.add(worker("release-spool", 1, JobRecord.JobType.RELEASE_SPOOL));

      Instant uploadStarted = Instant.now();
      Workflow finished;
      try (WorkerManager workerManager =
              new WorkerManager(
                  failure -> log("job failed: %s", failure.cause()), null, workers, 32, queues,
                  executor, new InMemoryJobStore(), Duration.ofMinutes(15), Duration.ofMillis(250));
          WorkflowManager workflowManager =
              new WorkflowManager(workflows, queues, Duration.ofSeconds(1), Throwable::printStackTrace,
                  workerManager)) {
        workflowManager.start();
        Workflow workflow =
            workflowManager.createStaged(
                "large-upload-it",
                stages,
                Map.of("tenantId", TENANT, "artifactId", artifactId, "version", "1"));
        finished = awaitTerminal(workflows, workflow.id(), timeout);
      } catch (Throwable failure) {
        abortQuietly(objects, uploads, artifactId);
        throw failure;
      }
      Duration uploadTime = Duration.between(uploadStarted, Instant.now());
      log("workflow %s in %s (%.1f MiB/s)", finished.status(), uploadTime,
          spooled.sizeBytes() / (double) MIB / Math.max(1, uploadTime.toSeconds()));

      // 5. Verify.
      if (finished.status() != Workflow.WorkflowStatus.COMPLETED) {
        abortQuietly(objects, uploads, artifactId);
      }
      assertEquals(Workflow.WorkflowStatus.COMPLETED, finished.status(), finished.failureMessage());
      Artifact stored = metadata.find(TENANT, artifactId).orElseThrow();
      assertEquals(ArtifactStatus.Storage.STORED, stored.storageStatus());
      assertNull(stored.spoolPath(), "the spool copy is released after storing");
      assertFalse(Files.exists(spooled.contentPath()));
      assertTrue(uploads.find(TENANT, artifactId, 1).isEmpty());

      HeadObjectResponse head = s3.headObject(request -> request.bucket(bucket).key(stored.storageKey()));
      assertEquals(spooled.sizeBytes(), head.contentLength());
      assertEquals(spooled.sha256(), head.metadata().get("artifact-sha256"));
      assertEquals(Checksum.sha256(TENANT), head.metadata().get("tenant-sha256"));
      assertTrue(head.checksumSHA256() == null || head.checksumSHA256().endsWith("-" + partCount),
          "multipart objects carry a composite checksum");
      log("verified s3://%s/%s", bucket, stored.storageKey());

      if (!keepObject) {
        objects.delete(TENANT, stored.storageKey());
        log("deleted the test object (pass -Dartifact.it.keepObject=true to keep it)");
      }
    }
  }

  private static InputStream download(String url) throws Exception {
    HttpClient client =
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    HttpResponse<InputStream> response =
        client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
            HttpResponse.BodyHandlers.ofInputStream());
    if (response.statusCode() != 200) {
      response.body().close();
      throw new IllegalStateException("Download failed with HTTP " + response.statusCode());
    }
    log("downloading %s (%s bytes)", url,
        response.headers().firstValue("content-length").orElse("unknown"));
    return response.body();
  }

  /** Polls until the workflow completes or crashes, logging part progress every 30 seconds. */
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
        long parts = current.workflowSteps().stream()
            .filter(step -> step.jobRecord().type() == JobRecord.JobType.STORE_PART).count();
        long done = current.workflowSteps().stream()
            .filter(step -> step.jobRecord().type() == JobRecord.JobType.STORE_PART)
            .filter(step -> step.status() == WorkflowStep.WorkflowStepStatus.COMPLETED).count();
        log("parts uploaded: %d/%d", done, parts);
        nextLog = Instant.now().plusSeconds(30);
      }
      Thread.sleep(1_000);
    }
    throw new AssertionError("Upload did not finish within " + timeout);
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

  private static String required(String name) {
    String value = setting(name, null);
    if (value == null) throw new IllegalStateException("Set -D" + name);
    return value;
  }

  private static String setting(String name, String fallback) {
    String value = System.getProperty(name);
    return value == null || value.isBlank() ? fallback : value.trim();
  }

  private static void log(String format, Object... arguments) {
    System.out.printf("[LargeFileS3UploadIT] " + format + "%n", arguments);
  }
}
