package dev.notify.artifact.model;

import java.time.Instant;
import java.util.Map;

/**
 * @param priority claim order among ready jobs of one type: every ready {@link Priority#NORMAL}
 *     job is claimed before any ready {@link Priority#RETRY} job
 */
public record JobRecord(
    String id,
    String tenantId,
    String artifactId,
    JobType type,
    JobStatus status,
    int attempts,
    Instant nextAttemptAt,
    String leaseOwner,
    Instant leaseExpiresAt,
    Map<String, String> attributes,
    String lastError,
    Instant createdAt,
    Instant updatedAt,
    Priority priority) {
  public JobRecord {
    // Records serialized before priorities existed deserialize with a null priority.
    priority = priority == null ? Priority.NORMAL : priority;
  }

  /** A normal-priority record. */
  public JobRecord(
      String id,
      String tenantId,
      String artifactId,
      JobType type,
      JobStatus status,
      int attempts,
      Instant nextAttemptAt,
      String leaseOwner,
      Instant leaseExpiresAt,
      Map<String, String> attributes,
      String lastError,
      Instant createdAt,
      Instant updatedAt) {
    this(id, tenantId, artifactId, type, status, attempts, nextAttemptAt, leaseOwner,
        leaseExpiresAt, attributes, lastError, createdAt, updatedAt, Priority.NORMAL);
  }

  public enum Priority {
    /** First submission of a job. */
    NORMAL,
    /** Re-submission by the workflow retry scheduler; yields to fresh work. */
    RETRY
  }

  public enum JobType {
    INGEST,
    STORE,
    /** Starts an S3 multipart upload for a large artifact. */
    STORE_INIT,
    /** Uploads one byte range of the spooled artifact as a multipart part. */
    STORE_PART,
    /** Validates all parts, completes the multipart upload, and verifies the object. */
    STORE_COMPLETE,
    /** Deletes the local spool copy once the artifact is stored and indexed. */
    RELEASE_SPOOL,
    /** Chunked source ingest: reads one byte range of the source into the chunk buffer pool. */
    BUFFER_CHUNK,
    /** Chunked source ingest: writes a buffered chunk into the reserved spool entry. */
    SPOOL_CHUNK,
    /** Chunked source ingest: uploads a buffered chunk as one multipart part. */
    STORE_CHUNK,
    /** Chunked source ingest: extracts, chunks, and embeds a buffered text/plain chunk. */
    INDEX_CHUNK,
    /** Chunked source ingest: returns a chunk's buffer to the pool. */
    RELEASE_BUFFER,
    /** Chunked source ingest: verifies, hashes, and publishes the fully written spool entry. */
    SPOOL_COMMIT,
    /** Chunked source ingest: finishes text indexing, or indexes other formats from the spool. */
    INDEX_FINAL,
    FETCH,
    INDEX,
    RETRIEVAL
  }

  public enum JobStatus {
    PENDING,
    CLAIMED,
    RUNNING,
    RETRY_PENDING,
    COMPLETED,
    DEAD_LETTER
  }

  public JobRecord claimed(String owner, Instant expiry) {
    return new JobRecord(
        id,
        tenantId,
        artifactId,
        type,
        JobStatus.CLAIMED,
        attempts + 1,
        nextAttemptAt,
        owner,
        expiry,
        attributes,
        lastError,
        createdAt,
        Instant.now(),
        priority);
  }

  /**
   * A fresh, retry-priority copy of this job under a new id: same type, target, and attributes,
   * with a full attempt budget. Used when a failed workflow is retried from its failed step.
   */
  public JobRecord retryAs(String newId, Instant now) {
    return new JobRecord(
        newId, tenantId, artifactId, type, JobStatus.PENDING, 0, now, null, null, attributes, null,
        now, now, Priority.RETRY);
  }

  public static JobRecord pending(
      String id, String tenantId, String artifactId, JobType type, Map<String, String> attributes) {
    Instant now = Instant.now();
    return new JobRecord(
        id,
        tenantId,
        artifactId,
        type,
        JobStatus.PENDING,
        0,
        now,
        null,
        null,
        Map.copyOf(attributes),
        null,
        now,
        now);
  }
}
