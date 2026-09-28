package dev.notify.artifact.job;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.multipart.MultipartPlan;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.MultipartUploadStore.MultipartUpload;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.ObjectStore.UploadedPart;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Checks that every planned part is present with the planned size, completes the upload, and
 * verifies the assembled object before marking the artifact STORED.
 *
 * <p>The expected composite checksum is saved before completion. If a retry finds the upload
 * already completed (its part list is gone), it verifies the object against the saved checksum.
 */
public final class StoreCompleteJob extends AbstractJob<Artifact> {
  private static final StructuredLog LOG = StructuredLog.of(StoreCompleteJob.class);

  private final JobRecord record;
  private final ObjectStore objectStore;
  private final MultipartUploadStore uploads;

  public StoreCompleteJob(
      JobRecord record,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      MultipartUploadStore uploads) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.uploads = Objects.requireNonNull(uploads, "uploads");
  }

  @Override
  public Artifact execute() throws IOException {
    long version = version(record);
    Artifact artifact = artifactAtVersion(this, record, version);
    Optional<MultipartUpload> found = uploads.find(record.tenantId(), record.artifactId(), version);
    if (found.isEmpty()) {
      if (artifact.storageStatus() == ArtifactStatus.Storage.STORED) {
        LOG.info("multipart_already_stored", "artifact", record.artifactId(), "version", version);
        return artifact;
      }
      LOG.warn("multipart_not_initialized", "artifact", record.artifactId(), "version", version,
          "storage", artifact.storageStatus());
      throw new IllegalStateException("Multipart upload was not initialized");
    }
    MultipartUpload upload = found.get();
    String key = upload.storageKey();
    Instant started = Instant.now();
    try {
      if (upload.compositeSha256() != null && alreadyCompleted(artifact, upload)) {
        LOG.info("multipart_resumed_after_completion", "artifact", record.artifactId(),
            "upload", upload.uploadId(), "composite", upload.compositeSha256());
      } else {
        List<UploadedPart> parts =
            objectStore.listParts(record.tenantId(), key, upload.uploadId());
        LOG.info("multipart_completing", "artifact", record.artifactId(),
            "upload", upload.uploadId(), "partsFound", parts.size(),
            "partsExpected", upload.partCount(), "bytes", artifact.sizeBytes());
        requireAllParts(parts, artifact.sizeBytes(), upload);
        String composite = ObjectStore.compositeSha256(parts);
        uploads.recordComposite(record.tenantId(), record.artifactId(), version, composite);
        objectStore.completeMultipartUpload(record.tenantId(), key, upload.uploadId(), parts);
        LOG.debug("multipart_assembled", "artifact", record.artifactId(),
            "composite", composite, "duration", Duration.between(started, Instant.now()));
        if (!objectStore.verifiedMultipart(
            record.tenantId(), key, artifact.sizeBytes(), artifact.sha256(), composite)) {
          throw new IOException("Object store did not verify the assembled artifact");
        }
      }
    } catch (IOException | RuntimeException failure) {
      LOG.warn("multipart_complete_failed", "artifact", record.artifactId(),
          "upload", upload.uploadId(), "attempt", record.attempts(),
          "duration", Duration.between(started, Instant.now()),
          "error", failure.getClass().getSimpleName(), "reason", safeMessage(failure));
      metadataStore.update(
          record.tenantId(),
          record.artifactId(),
          current ->
              current
                  .withStorage(ArtifactStatus.Storage.RETRY_PENDING, key)
                  .withFailure(
                      ArtifactStatus.Storage.RETRY_PENDING,
                      current.indexStatus(),
                      "OBJECT_STORE_MULTIPART_FAILED",
                      safeMessage(failure)));
      throw failure;
    }

    uploads.delete(record.tenantId(), record.artifactId(), version);
    Artifact stored = metadataStore.update(
        record.tenantId(),
        record.artifactId(),
        current -> current.withStorage(ArtifactStatus.Storage.STORED, key));
    LOG.info("multipart_stored", "artifact", record.artifactId(), "version", version,
        "bytes", artifact.sizeBytes(), "parts", upload.partCount(),
        "uploadAge", Duration.between(upload.createdAt(), Instant.now()));
    return stored;
  }

  private boolean alreadyCompleted(Artifact artifact, MultipartUpload upload) throws IOException {
    return objectStore.verifiedMultipart(
        record.tenantId(), upload.storageKey(), artifact.sizeBytes(), artifact.sha256(),
        upload.compositeSha256());
  }

  private static void requireAllParts(
      List<UploadedPart> parts, long totalBytes, MultipartUpload upload) throws IOException {
    MultipartPlan plan = new MultipartPlan(totalBytes, upload.partSize(), upload.partCount());
    if (parts.size() != upload.partCount()) {
      throw new IOException(
          "Expected " + upload.partCount() + " parts but found " + parts.size());
    }
    for (int index = 0; index < parts.size(); index++) {
      UploadedPart part = parts.get(index);
      int expectedNumber = index + 1;
      if (part.partNumber() != expectedNumber
          || part.size() != plan.lengthOf(expectedNumber)
          || part.checksumSha256() == null) {
        throw new IOException("Part " + expectedNumber + " is missing or does not match the plan");
      }
    }
  }

  private static String safeMessage(Exception failure) {
    String message = failure.getMessage();
    return message == null
        ? failure.getClass().getSimpleName()
        : message.substring(0, Math.min(500, message.length()));
  }
}
