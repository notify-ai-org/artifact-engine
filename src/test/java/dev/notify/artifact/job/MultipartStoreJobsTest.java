package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.multipart.MultipartPlan;
import dev.notify.artifact.multipart.MultipartUploadCleaner;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryMultipartUploadStore;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.MultipartUploadStore.MultipartUpload;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.workflow.Workflow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MultipartStoreJobsTest {
  private static final long MIB = 1024 * 1024;
  private static final long PART = 5 * MIB;

  @TempDir Path spoolRoot;

  private InMemoryStores.Metadata metadata;
  private DurableSpool spool;
  private FakeMultipartStore objects;
  private InMemoryMultipartUploadStore uploads;
  private final StorageKeyFactory keys = new StorageKeyFactory("test", Clock.systemUTC());

  @BeforeEach
  void setUp() throws IOException {
    metadata = new InMemoryStores.Metadata();
    spool = new DurableSpool(spoolRoot, 64 * MIB, new ObjectMapper());
    objects = new FakeMultipartStore();
    uploads = new InMemoryMultipartUploadStore();
  }

  @Test
  void plansStoreThenIndexForSmallFilesAndParallelPartsForLargeOnes() throws Exception {
    Artifact small = ingest(text(4 * MIB));
    assertEquals(
        List.of(List.of(JobRecord.JobType.STORE), List.of(JobRecord.JobType.INDEX),
            List.of(JobRecord.JobType.RELEASE_SPOOL)),
        types(IngestJob.initialStages(small, PART)));

    Artifact large = ingest(text(12 * MIB));
    List<List<JobRecord>> stages = IngestJob.initialStages(large, PART);
    assertEquals(
        List.of(
            List.of(JobRecord.JobType.STORE_INIT),
            List.of(JobRecord.JobType.STORE_PART, JobRecord.JobType.STORE_PART,
                JobRecord.JobType.STORE_PART),
            List.of(JobRecord.JobType.STORE_COMPLETE),
            List.of(JobRecord.JobType.INDEX),
            List.of(JobRecord.JobType.RELEASE_SPOOL)),
        types(stages));
    assertEquals(
        List.of("5242880", "5242880", "2097152"),
        stages.get(1).stream().map(job -> job.attributes().get("length")).toList());
    assertEquals(7, stages.stream().flatMap(List::stream).map(JobRecord::id).distinct().count());
  }

  @Test
  void uploadsPartsInParallelAndStoresAVerifiedObject() throws Exception {
    byte[] content = text(12 * MIB);
    Artifact artifact = ingest(content);
    List<List<JobRecord>> stages = IngestJob.initialStages(artifact, PART);

    init(stages).execute();
    assertEquals(ArtifactStatus.Storage.UPLOADING, current(artifact).storageStatus());
    runPartsInParallel(stages);
    Artifact stored = complete(stages).execute();

    assertEquals(ArtifactStatus.Storage.STORED, stored.storageStatus());
    assertArrayEquals(content, objects.object(stored.storageKey()));
    assertTrue(uploads.find(artifact.tenantId(), artifact.id(), 1).isEmpty());
  }

  @Test
  void retriedInitReusesTheUploadAndRetriedPartsReplaceThemselves() throws Exception {
    byte[] content = text(12 * MIB);
    Artifact artifact = ingest(content);
    List<List<JobRecord>> stages = IngestJob.initialStages(artifact, PART);

    init(stages).execute();
    init(stages).execute();
    assertEquals(1, objects.created.get());

    runPartsInParallel(stages);
    part(stages, 2).execute();
    Artifact stored = complete(stages).execute();

    assertArrayEquals(content, objects.object(stored.storageKey()));
  }

  @Test
  void completionRetryAfterTheStoreFinishedVerifiesFromTheSavedChecksum() throws Exception {
    Artifact artifact = ingest(text(12 * MIB));
    List<List<JobRecord>> stages = IngestJob.initialStages(artifact, PART);
    init(stages).execute();
    runPartsInParallel(stages);
    complete(stages).execute();

    // Simulate a crash after S3 completed but before the row was deleted and STORED written.
    MultipartUpload finished = objects.lastCompletedUpload;
    uploads.putIfAbsent(finished);
    metadata.update(artifact.tenantId(), artifact.id(),
        current -> current.withStorage(ArtifactStatus.Storage.UPLOADING, finished.storageKey()));

    Artifact stored = complete(stages).execute();

    assertEquals(ArtifactStatus.Storage.STORED, stored.storageStatus());
    assertTrue(uploads.find(artifact.tenantId(), artifact.id(), 1).isEmpty());
  }

  @Test
  void completionFailsWhileAPartIsMissing() throws Exception {
    Artifact artifact = ingest(text(12 * MIB));
    List<List<JobRecord>> stages = IngestJob.initialStages(artifact, PART);
    init(stages).execute();
    part(stages, 1).execute();
    part(stages, 3).execute();

    IOException missing = assertThrows(IOException.class, () -> complete(stages).execute());

    assertTrue(missing.getMessage().contains("Expected 3 parts"), missing.getMessage());
    assertEquals(ArtifactStatus.Storage.RETRY_PENDING, current(artifact).storageStatus());
    assertEquals("OBJECT_STORE_MULTIPART_FAILED", current(artifact).failureCode());
  }

  @Test
  void aPartWhoseBytesDoNotMatchItsHashIsRejected() throws Exception {
    Artifact artifact = ingest(text(12 * MIB));
    List<List<JobRecord>> stages = IngestJob.initialStages(artifact, PART);
    init(stages).execute();
    objects.corruptNextPart = true;

    assertThrows(IOException.class, () -> part(stages, 1).execute());
  }

  @Test
  void crashCleanupAbortsTheUploadAndForgetsIt() throws Exception {
    Artifact artifact = ingest(text(12 * MIB));
    List<List<JobRecord>> stages = IngestJob.initialStages(artifact, PART);
    init(stages).execute();
    part(stages, 1).execute();
    String uploadId = uploads.find(artifact.tenantId(), artifact.id(), 1).orElseThrow().uploadId();

    new MultipartUploadCleaner(uploads, objects).accept(workflow(artifact));

    assertTrue(objects.aborted.contains(uploadId));
    assertTrue(uploads.find(artifact.tenantId(), artifact.id(), 1).isEmpty());
  }

  @Test
  void raisesThePartSizeWhenAFileWouldNeedMoreThanTenThousandParts() {
    long total = 100_000 * PART;
    MultipartPlan plan = MultipartPlan.forSize(total, PART);

    assertTrue(plan.partCount() <= MultipartPlan.MAX_PARTS);
    assertEquals(0, plan.partSize() % MIB);
    assertEquals(total, plan.parts().stream().mapToLong(MultipartPlan.Part::length).sum());
    assertNull(MultipartPlan.forSize(PART, PART));
  }

  // ---- helpers -------------------------------------------------------------------------------

  private Artifact ingest(byte[] content) throws Exception {
    return new IngestJob(
            new Requests.Ingest(
                "tenant", "principal", null, "notes.txt", "text/plain",
                new ByteArrayInputStream(content), content.length, Map.of()),
            metadata,
            spool,
            new ArtifactAccessVerifier((principal, tenant, permission) -> {}, new DataVerifier()),
            false,
            PART)
        .execute();
  }

  private StoreInitJob init(List<List<JobRecord>> stages) {
    return new StoreInitJob(stages.get(0).get(0), metadata, objects, uploads, keys);
  }

  private StorePartJob part(List<List<JobRecord>> stages, int number) {
    return new StorePartJob(stages.get(1).get(number - 1), metadata, objects, spool, uploads);
  }

  private StoreCompleteJob complete(List<List<JobRecord>> stages) {
    return new StoreCompleteJob(stages.get(2).get(0), metadata, objects, uploads);
  }

  private void runPartsInParallel(List<List<JobRecord>> stages) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      List<Future<ObjectStore.UploadedPart>> futures = new ArrayList<>();
      List<JobRecord> parts = new ArrayList<>(stages.get(1));
      Collections.reverse(parts);
      for (JobRecord record : parts) {
        futures.add(pool.submit(new StorePartJob(record, metadata, objects, spool, uploads)::execute));
      }
      for (Future<ObjectStore.UploadedPart> future : futures) future.get();
    } finally {
      pool.shutdownNow();
    }
  }

  private Artifact current(Artifact artifact) {
    return metadata.find(artifact.tenantId(), artifact.id()).orElseThrow();
  }

  private static Workflow workflow(Artifact artifact) {
    return new Workflow("wf", "ingest-store-index", Instant.now(), Instant.now(),
        Workflow.WorkflowStatus.CRASHED, null, null, List.of(),
        Map.of("tenantId", artifact.tenantId(), "artifactId", artifact.id(), "version", "1"),
        "boom");
  }

  private static List<List<JobRecord.JobType>> types(List<List<JobRecord>> stages) {
    return stages.stream().map(stage -> stage.stream().map(JobRecord::type).toList()).toList();
  }

  /** Printable ASCII so the intake verifier classifies it as text/plain. */
  private static byte[] text(long size) {
    byte[] bytes = new byte[(int) size];
    Random random = new Random(size);
    for (int index = 0; index < bytes.length; index++) {
      bytes[index] = (byte) (index % 80 == 79 ? '\n' : 'a' + random.nextInt(26));
    }
    return bytes;
  }

  /** Mimics the S3 behaviour the jobs depend on. */
  private final class FakeMultipartStore implements ObjectStore {
    private final Map<String, TreeMap<Integer, byte[]>> parts = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, String>> partChecksums = new ConcurrentHashMap<>();
    private final Map<String, String> uploadKeys = new ConcurrentHashMap<>();
    private final Map<String, String> uploadSha = new ConcurrentHashMap<>();
    private final Map<String, byte[]> objectsByKey = new ConcurrentHashMap<>();
    private final Map<String, String> compositeByKey = new ConcurrentHashMap<>();
    private final Map<String, String> shaByKey = new ConcurrentHashMap<>();
    final AtomicInteger created = new AtomicInteger();
    final List<String> aborted = Collections.synchronizedList(new ArrayList<>());
    volatile boolean corruptNextPart;
    volatile MultipartUpload lastCompletedUpload;

    byte[] object(String key) {
      return objectsByKey.get(key);
    }

    @Override
    public boolean supportsMultipart() {
      return true;
    }

    @Override
    public String createMultipartUpload(String tenantId, String key, String sha256) {
      created.incrementAndGet();
      String uploadId = "upload-" + created.get();
      parts.put(uploadId, new TreeMap<>());
      partChecksums.put(uploadId, new ConcurrentHashMap<>());
      uploadKeys.put(uploadId, key);
      uploadSha.put(uploadId, sha256);
      return uploadId;
    }

    @Override
    public UploadedPart uploadPart(String tenantId, String key, String uploadId, int partNumber,
        InputStream content, long length, String partSha256) throws IOException {
      byte[] bytes = content.readAllBytes();
      if (corruptNextPart) {
        corruptNextPart = false;
        bytes[0] ^= 1;
      }
      if (bytes.length != length || !Checksum.sha256(new ByteArrayInputStream(bytes)).equals(partSha256)) {
        throw new IOException("BadDigest");
      }
      synchronized (parts.get(uploadId)) {
        parts.get(uploadId).put(partNumber, bytes);
      }
      String base64 = Base64.getEncoder().encodeToString(HexFormat.of().parseHex(partSha256));
      partChecksums.get(uploadId).put(partNumber, base64);
      return new UploadedPart(partNumber, "etag-" + partNumber, length, base64);
    }

    @Override
    public List<UploadedPart> listParts(String tenantId, String key, String uploadId)
        throws IOException {
      TreeMap<Integer, byte[]> uploaded = parts.get(uploadId);
      if (uploaded == null) throw new IOException("NoSuchUpload");
      List<UploadedPart> result = new ArrayList<>();
      synchronized (uploaded) {
        uploaded.forEach((number, bytes) -> result.add(new UploadedPart(
            number, "etag-" + number, bytes.length, partChecksums.get(uploadId).get(number))));
      }
      return result;
    }

    @Override
    public void completeMultipartUpload(String tenantId, String key, String uploadId,
        List<UploadedPart> completed) throws IOException {
      TreeMap<Integer, byte[]> uploaded = parts.remove(uploadId);
      if (uploaded == null) throw new IOException("NoSuchUpload");
      ByteArrayOutputStream assembled = new ByteArrayOutputStream();
      for (UploadedPart part : completed) assembled.writeBytes(uploaded.get(part.partNumber()));
      objectsByKey.put(key, assembled.toByteArray());
      compositeByKey.put(key, ObjectStore.compositeSha256(completed));
      shaByKey.put(key, uploadSha.get(uploadId));
      MultipartUpload row = uploads.find(tenantId, uploadRowArtifact(key), 1).orElseThrow();
      lastCompletedUpload = row;
    }

    private String uploadRowArtifact(String key) {
      String[] segments = key.split("/");
      return segments[segments.length - 3];
    }

    @Override
    public void abortMultipartUpload(String tenantId, String key, String uploadId) {
      parts.remove(uploadId);
      aborted.add(uploadId);
    }

    @Override
    public boolean verifiedMultipart(String tenantId, String key, long length, String sha256,
        String compositeSha256) {
      byte[] object = objectsByKey.get(key);
      return object != null
          && object.length == length
          && sha256.equals(shaByKey.get(key))
          && compositeSha256.equals(compositeByKey.get(key));
    }

    @Override
    public void put(String tenantId, String key, InputStream content, long length, String sha256) {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream get(String tenantId, String key) {
      return new ByteArrayInputStream(objectsByKey.get(key));
    }

    @Override
    public boolean verified(String tenantId, String key, long length, String sha256) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String tenantId, String key) {
      objectsByKey.remove(key);
    }
  }
}
