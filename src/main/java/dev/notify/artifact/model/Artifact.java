package dev.notify.artifact.model;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Immutable metadata for an original artifact. Tenant id is always part of its identity. */
public record Artifact(
    String id,
    String tenantId,
    String idempotencyKey,
    String idempotencyFingerprint,
    String sourceType,
    String sourceUri,
    String originalName,
    String mediaType,
    long sizeBytes,
    String sha256,
    String storageKey,
    Path spoolPath,
    ArtifactStatus.Storage storageStatus,
    ArtifactStatus.Index indexStatus,
    long version,
    Map<String, String> metadata,
    String failureCode,
    String failureMessage,
    Instant createdAt,
    Instant updatedAt) {
  public Artifact {
    Objects.requireNonNull(id);
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(originalName);
    Objects.requireNonNull(mediaType);
    Objects.requireNonNull(sha256);
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }

  public Artifact withStorage(ArtifactStatus.Storage status, String key) {
    return new Artifact(
        id,
        tenantId,
        idempotencyKey,
        idempotencyFingerprint,
        sourceType,
        sourceUri,
        originalName,
        mediaType,
        sizeBytes,
        sha256,
        key,
        spoolPath,
        status,
        indexStatus,
        version,
        metadata,
        failureCode,
        failureMessage,
        createdAt,
        Instant.now());
  }

  /** A provisional digest marks content that is still arriving; see {@link #hasContentDigest}. */
  public static final String PENDING_DIGEST_PREFIX = "pending-";

  /** False until a chunked ingest has committed its content and recorded the real SHA-256. */
  public boolean hasContentDigest() {
    return !sha256.startsWith(PENDING_DIGEST_PREFIX);
  }

  /** Records the SHA-256 of content that was committed after registration (chunked ingest). */
  public Artifact withContentDigest(String contentSha256) {
    return new Artifact(
        id,
        tenantId,
        idempotencyKey,
        idempotencyFingerprint,
        sourceType,
        sourceUri,
        originalName,
        mediaType,
        sizeBytes,
        java.util.Objects.requireNonNull(contentSha256, "contentSha256"),
        storageKey,
        spoolPath,
        storageStatus,
        indexStatus,
        version,
        metadata,
        failureCode,
        failureMessage,
        createdAt,
        Instant.now());
  }

  /** The local spool copy was released; the object store holds the only copy from now on. */
  public Artifact withoutSpool() {
    return new Artifact(
        id,
        tenantId,
        idempotencyKey,
        idempotencyFingerprint,
        sourceType,
        sourceUri,
        originalName,
        mediaType,
        sizeBytes,
        sha256,
        storageKey,
        null,
        storageStatus,
        indexStatus,
        version,
        metadata,
        failureCode,
        failureMessage,
        createdAt,
        Instant.now());
  }

  public Artifact withIndex(ArtifactStatus.Index status) {
    return new Artifact(
        id,
        tenantId,
        idempotencyKey,
        idempotencyFingerprint,
        sourceType,
        sourceUri,
        originalName,
        mediaType,
        sizeBytes,
        sha256,
        storageKey,
        spoolPath,
        storageStatus,
        status,
        version,
        metadata,
        failureCode,
        failureMessage,
        createdAt,
        Instant.now());
  }

  public Artifact withFailure(
      ArtifactStatus.Storage storage, ArtifactStatus.Index index, String code, String message) {
    return new Artifact(
        id,
        tenantId,
        idempotencyKey,
        idempotencyFingerprint,
        sourceType,
        sourceUri,
        originalName,
        mediaType,
        sizeBytes,
        sha256,
        storageKey,
        spoolPath,
        storage,
        index,
        version,
        metadata,
        code,
        message,
        createdAt,
        Instant.now());
  }
}
