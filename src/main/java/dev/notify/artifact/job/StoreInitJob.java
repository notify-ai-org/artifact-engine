package dev.notify.artifact.job;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.MultipartUploadStore.MultipartUpload;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Starts the multipart upload for an artifact version, once, even when retried or raced. */
public final class StoreInitJob extends AbstractJob<Artifact> {
  private static final StructuredLog LOG = StructuredLog.of(StoreInitJob.class);

  private final JobRecord record;
  private final ObjectStore objectStore;
  private final MultipartUploadStore uploads;
  private final StorageKeyFactory keyFactory;

  public StoreInitJob(
      JobRecord record,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      MultipartUploadStore uploads,
      StorageKeyFactory keyFactory) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.uploads = Objects.requireNonNull(uploads, "uploads");
    this.keyFactory = Objects.requireNonNull(keyFactory, "keyFactory");
  }

  @Override
  public Artifact execute() throws IOException {
    long version = version(record);
    Artifact artifact = artifactAtVersion(this, record, version);
    Optional<MultipartUpload> existing =
        uploads.find(record.tenantId(), record.artifactId(), version);
    MultipartUpload upload;
    if (existing.isPresent()) {
      upload = existing.get();
      LOG.info("multipart_reused", "artifact", record.artifactId(), "version", version,
          "upload", upload.uploadId(), "parts", upload.partCount());
    } else {
      String key = keyFactory.key(artifact);
      String uploadId =
          objectStore.createMultipartUpload(record.tenantId(), key, contentDigest(record, artifact));
      upload =
          uploads.putIfAbsent(
              new MultipartUpload(
                  record.tenantId(), record.artifactId(), version, key, uploadId,
                  longAttribute(record, "partSize"),
                  (int) longAttribute(record, "partCount"), Instant.now(), null));
      if (!upload.uploadId().equals(uploadId)) {
        // A concurrent attempt registered first; use its upload and discard ours.
        LOG.warn("multipart_race_lost", "artifact", record.artifactId(), "version", version,
            "kept", upload.uploadId(), "aborted", uploadId);
        objectStore.abortMultipartUpload(record.tenantId(), key, uploadId);
      } else {
        LOG.info("multipart_started", "artifact", record.artifactId(), "version", version,
            "upload", uploadId, "bytes", artifact.sizeBytes(), "parts", upload.partCount(),
            "partSize", upload.partSize());
      }
    }
    String key = upload.storageKey();
    return metadataStore.update(
        record.tenantId(),
        record.artifactId(),
        current -> current.withStorage(ArtifactStatus.Storage.UPLOADING, key));
  }

}
