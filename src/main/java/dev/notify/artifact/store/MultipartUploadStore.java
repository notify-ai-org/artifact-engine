package dev.notify.artifact.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Durable record of the one in-flight multipart upload per artifact version.
 *
 * <p>Workflow jobs are rebuilt from immutable {@code JobRecord}s, so the upload id that STORE_INIT
 * obtains from the object store is shared with the part and completion jobs through this store.
 */
public interface MultipartUploadStore {
  Optional<MultipartUpload> find(String tenantId, String artifactId, long version);

  /**
   * Inserts the upload unless one already exists for the artifact version, and returns whichever
   * is stored. A caller whose upload lost the race should abort its own.
   */
  MultipartUpload putIfAbsent(MultipartUpload upload);

  /**
   * Saves the expected composite checksum before the upload is completed, so a completion retry
   * can still verify the object after the store has discarded the part list.
   */
  void recordComposite(String tenantId, String artifactId, long version, String compositeSha256);

  void delete(String tenantId, String artifactId, long version);

  /** @param compositeSha256 expected composite checksum, set just before completion */
  record MultipartUpload(
      String tenantId,
      String artifactId,
      long version,
      String storageKey,
      String uploadId,
      long partSize,
      int partCount,
      Instant createdAt,
      String compositeSha256) {
    public MultipartUpload {
      Objects.requireNonNull(tenantId, "tenantId");
      Objects.requireNonNull(artifactId, "artifactId");
      Objects.requireNonNull(storageKey, "storageKey");
      Objects.requireNonNull(uploadId, "uploadId");
      Objects.requireNonNull(createdAt, "createdAt");
      if (partSize < 1 || partCount < 1) {
        throw new IllegalArgumentException("partSize and partCount must be positive");
      }
    }

    public MultipartUpload withComposite(String composite) {
      return new MultipartUpload(tenantId, artifactId, version, storageKey, uploadId, partSize,
          partCount, createdAt, composite);
    }
  }
}
