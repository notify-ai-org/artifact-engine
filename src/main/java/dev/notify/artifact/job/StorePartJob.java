package dev.notify.artifact.job;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.MultipartUploadStore.MultipartUpload;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.Checksum;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Uploads one byte range of the spooled artifact. The range is hashed first and the store
 * verifies the upload against that hash, so a part can only succeed with the exact spool bytes.
 * Re-uploading a part number replaces it, which makes retries safe.
 */
public final class StorePartJob extends AbstractJob<ObjectStore.UploadedPart> {
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

    String partSha256;
    try (InputStream range = durableSpool.openRange(artifact.spoolPath(), offset, length)) {
      partSha256 = Checksum.sha256(range);
    }
    try (InputStream range = durableSpool.openRange(artifact.spoolPath(), offset, length)) {
      return objectStore.uploadPart(
          record.tenantId(), upload.storageKey(), upload.uploadId(), partNumber, range, length,
          partSha256);
    }
  }
}
