package dev.notify.artifact.job;

import dev.notify.artifact.chunk.ByteBufferInputStream;
import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.MultipartUploadStore.MultipartUpload;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Uploads a buffered chunk as one multipart part. The part is hashed from the same buffer it is
 * uploaded from, and the store rejects any part whose bytes do not match that hash.
 */
public final class StoreChunkJob extends AbstractJob<ObjectStore.UploadedPart> {
  private static final StructuredLog LOG = StructuredLog.of(StoreChunkJob.class);

  private final JobRecord record;
  private final ObjectStore objectStore;
  private final MultipartUploadStore uploads;
  private final ChunkedIngestSupport support;

  public StoreChunkJob(
      JobRecord record,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      MultipartUploadStore uploads,
      ChunkedIngestSupport support) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    this.uploads = Objects.requireNonNull(uploads, "uploads");
    this.support = Objects.requireNonNull(support, "support");
  }

  @Override
  public ObjectStore.UploadedPart execute() throws IOException {
    ChunkRef chunk = ChunkRef.of(record);
    Artifact artifact = artifactAtVersion(this, record, chunk.version());
    MultipartUpload upload =
        uploads
            .find(record.tenantId(), record.artifactId(), chunk.version())
            .orElseThrow(() -> new IllegalStateException("Multipart upload was not initialized"));
    Instant started = Instant.now();
    try (var lease = chunk.lease(support, artifact)) {
      String partSha256 = sha256(lease.buffer());
      ObjectStore.UploadedPart uploaded =
          objectStore.uploadPart(
              record.tenantId(), upload.storageKey(), upload.uploadId(), chunk.chunk(),
              new ByteBufferInputStream(lease.buffer()), lease.length(), partSha256);
      Duration elapsed = Duration.between(started, Instant.now());
      LOG.info("part_uploaded", "artifact", artifact.id(), "part", chunk.chunk(),
          "of", upload.partCount(), "bytes", lease.length(), "source", "buffer",
          "duration", elapsed, "mibPerSecond", StructuredLog.mibPerSecond(lease.length(), elapsed),
          "attempt", record.attempts());
      return uploaded;
    } catch (IOException | RuntimeException failure) {
      LOG.warn("part_failed", "artifact", artifact.id(), "part", chunk.chunk(),
          "attempt", record.attempts(), "error", failure.getClass().getSimpleName(),
          "reason", failure.getMessage());
      throw failure;
    }
  }

  private static String sha256(java.nio.ByteBuffer buffer) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(buffer.duplicate());
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
