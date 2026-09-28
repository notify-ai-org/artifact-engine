package dev.notify.artifact.job;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.StorageKeyFactory;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.util.Checksum;
import java.util.Map;

/** Uploads a spooled original with a stable key and verifies it before marking it stored. */
public final class StoreJob extends AbstractJob<Artifact> implements QueueableJob<Artifact> {
  private static final StructuredLog LOG = StructuredLog.of(StoreJob.class);

  private final String tenantId;
  private final String artifactId;
  private final ObjectStore objectStore;
  private final DurableSpool durableSpool;
  private final StorageKeyFactory keyFactory;

  public StoreJob(
      String tenantId,
      String artifactId,
      MetadataStore metadataStore,
      ObjectStore objectStore,
      DurableSpool durableSpool,
      StorageKeyFactory keyFactory) {
    super(null, metadataStore);
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.objectStore = objectStore;
    this.durableSpool = durableSpool;
    this.keyFactory = keyFactory;
  }

  @Override
  public Artifact execute() throws IOException {
    Artifact artifact =
        metadataStore
            .find(tenantId, artifactId)
            .orElseThrow(() -> new NoSuchElementException("Artifact not found: " + artifactId));

    String storageKey = keyFactory.key(artifact);
    metadataStore.update(
        tenantId,
        artifactId,
        current -> current.withStorage(ArtifactStatus.Storage.UPLOADING, storageKey));
    Instant started = Instant.now();
    LOG.info("store_started", "artifact", artifactId, "bytes", artifact.sizeBytes(),
        "version", artifact.version());
    try {
      try (InputStream content = durableSpool.open(artifact.spoolPath())) {
        objectStore.put(tenantId, storageKey, content, artifact.sizeBytes(), artifact.sha256());
      }
      Duration uploaded = Duration.between(started, Instant.now());
      LOG.debug("store_uploaded", "artifact", artifactId, "duration", uploaded);
      if (!objectStore.verified(tenantId, storageKey, artifact.sizeBytes(), artifact.sha256())) {
        throw new ObjectVerificationException("Object store did not verify the uploaded artifact");
      }
    } catch (IOException uploadFailure) {
      String failureCode =
          uploadFailure instanceof ObjectVerificationException
              ? "OBJECT_STORE_VERIFICATION_FAILED"
              : "OBJECT_STORE_UPLOAD_FAILED";
      LOG.warn("store_failed", "artifact", artifactId, "code", failureCode,
          "bytes", artifact.sizeBytes(), "duration", Duration.between(started, Instant.now()),
          "reason", safeMessage(uploadFailure));
      metadataStore.update(
          tenantId,
          artifactId,
          current ->
              current
                  .withStorage(ArtifactStatus.Storage.RETRY_PENDING, storageKey)
                  .withFailure(
                      ArtifactStatus.Storage.RETRY_PENDING,
                      current.indexStatus(),
                      failureCode,
                      safeMessage(uploadFailure)));
      throw uploadFailure;
    }

    Artifact stored = metadataStore.update(
        tenantId,
        artifactId,
        current -> current.withStorage(ArtifactStatus.Storage.STORED, storageKey));
    Duration elapsed = Duration.between(started, Instant.now());
    LOG.info("store_completed", "artifact", artifactId, "bytes", artifact.sizeBytes(),
        "duration", elapsed,
        "mibPerSecond", StructuredLog.mibPerSecond(artifact.sizeBytes(), elapsed));
    return stored;
  }

  @Override
  public JobRecord queueRecord() {
    Artifact artifact = required(tenantId, artifactId);
    return JobRecord.pending(
        Checksum.sha256(tenantId + ":" + artifactId + ":" + artifact.version() + ":STORE"),
        tenantId, artifactId, JobRecord.JobType.STORE,
        Map.of("version", Long.toString(artifact.version())));
  }

  @Override
  public Artifact queuedResult() {
    return required(tenantId, artifactId);
  }

  private static String safeMessage(Exception failure) {
    String message = failure.getMessage();
    return message == null
        ? failure.getClass().getSimpleName()
        : message.substring(0, Math.min(500, message.length()));
  }

  private static final class ObjectVerificationException extends IOException {
    private ObjectVerificationException(String message) {
      super(message);
    }
  }
}
