package dev.notify.artifact.model;

import java.time.Instant;
import java.util.Map;

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
    Instant updatedAt) {
  public enum JobType {
    INGEST,
    STORE,
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
        Instant.now());
  }

  public JobRecord renewed(Instant expiry) {
    return new JobRecord(
        id,
        tenantId,
        artifactId,
        type,
        status,
        attempts,
        nextAttemptAt,
        leaseOwner,
        expiry,
        attributes,
        lastError,
        createdAt,
        Instant.now());
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
