package dev.notify.artifact.job;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.MultipartUploadStore.MultipartUpload;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Uploads one byte range of the spooled artifact. The range is hashed first and the store
 * verifies the upload against that hash, so a part can only succeed with the exact spool bytes.
 * Re-uploading a part number replaces it, which makes retries safe.
 */
public final class StorePartJob extends AbstractJob<ObjectStore.UploadedPart> {
  private static final StructuredLog LOG = StructuredLog.of(StorePartJob.class);

  private final JobRecord record;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final MultipartUploadStore uploads;

  public StorePartJob(
      JobRecord record,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      MultipartUploadStore uploads) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.durableSpool = Objects.requireNonNull(durableSpool, "durableSpool");
    this.uploads = Objects.requireNonNull(uploads, "uploads");
  }

  @Override
  public ObjectStore.UploadedPart execute() throws IOException {
    long version = version(record);
    Artifact artifact = artifactAtVersion(this, record, version);
    MultipartUpload upload =
        uploads
            .find(record.tenantId(), record.artifactId(), version)
            .orElseThrow(() -> new IllegalStateException("Multipart upload was not initialized"));
    int partNumber = (int) longAttribute(record, "partNumber");
    long offset = longAttribute(record, "offset");
    long length = longAttribute(record, "length");

    Instant started = Instant.now();
    LOG.debug("part_started", "artifact", record.artifactId(), "part", partNumber,
        "of", upload.partCount(), "offset", offset, "bytes", length, "attempt", record.attempts(),
        "priority", record.priority());
    try {
      String partSha256;
      try (InputStream range = durableSpool.openRange(artifact.spoolPath(), offset, length)) {
        partSha256 = Checksum.sha256(range);
      }
      Instant hashed = Instant.now();
      ObjectStore.UploadedPart uploaded;
      try (InputStream range = durableSpool.openRange(artifact.spoolPath(), offset, length)) {
        uploaded = objectStore.uploadPart(
            record.tenantId(), upload.storageKey(), upload.uploadId(), partNumber, range, length,
            partSha256);
      }
      Duration uploadTime = Duration.between(hashed, Instant.now());
      LOG.info("part_uploaded", "artifact", record.artifactId(), "part", partNumber,
          "of", upload.partCount(), "bytes", length,
          "hashDuration", Duration.between(started, hashed), "uploadDuration", uploadTime,
          "mibPerSecond", StructuredLog.mibPerSecond(length, uploadTime),
          "attempt", record.attempts());
      return uploaded;
    } catch (IOException | RuntimeException failure) {
      LOG.warn("part_failed", "artifact", record.artifactId(), "part", partNumber,
          "of", upload.partCount(), "bytes", length, "attempt", record.attempts(),
          "duration", Duration.between(started, Instant.now()),
          "error", failure.getClass().getSimpleName(), "reason", failure.getMessage());
      throw failure;
    }
  }
}
