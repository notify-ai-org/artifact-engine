package dev.notify.artifact.queue;

import dev.notify.artifact.model.JobRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class InMemoryJobQueue implements JobQueue {
  private final Map<String, JobRecord> jobs = new LinkedHashMap<>();

  public synchronized void enqueue(JobRecord j) {
    jobs.putIfAbsent(j.id(), j);
  }

  public synchronized Optional<JobRecord> claim(
      JobRecord.JobType type, String owner, Duration lease, Instant now) {
    return jobs.values().stream()
        .filter(
            j ->
                j.type() == type
                    && (j.status() == JobRecord.JobStatus.PENDING
                        || j.status() == JobRecord.JobStatus.RETRY_PENDING)
                    && (j.nextAttemptAt() == null || !j.nextAttemptAt().isAfter(now)))
        .findFirst()
        .map(
            j -> {
              var c = j.claimed(owner, now.plus(lease));
              jobs.put(c.id(), c);
              return c;
            });
  }

  public synchronized boolean complete(String id, String owner) {
    return updateOwned(id, owner, JobRecord.JobStatus.COMPLETED, null, null);
  }

  @Override
  public synchronized boolean renew(String id, String owner, Duration lease, Instant now) {
    JobRecord job = jobs.get(id);
    if (job == null
        || !Objects.equals(owner, job.leaseOwner())
        || job.leaseExpiresAt() == null
        || !job.leaseExpiresAt().isAfter(now)) {
      return false;
    }
    jobs.put(id, job.renewed(now.plus(lease)));
    return true;
  }

  public synchronized boolean retry(String id, String owner, Instant next, String error) {
    return updateOwned(id, owner, JobRecord.JobStatus.RETRY_PENDING, next, error);
  }

  public synchronized boolean deadLetter(String id, String owner, String error) {
    return updateOwned(id, owner, JobRecord.JobStatus.DEAD_LETTER, null, error);
  }

  private boolean updateOwned(
      String id, String owner, JobRecord.JobStatus state, Instant next, String error) {
    JobRecord j = jobs.get(id);
    if (j == null
        || !Objects.equals(owner, j.leaseOwner())
        || j.leaseExpiresAt() == null
        || !j.leaseExpiresAt().isAfter(Instant.now())) {
      return false;
    }
    jobs.put(
        id,
        new JobRecord(
            j.id(),
            j.tenantId(),
            j.artifactId(),
            j.type(),
            state,
            j.attempts(),
            next,
            null,
            null,
            j.attributes(),
            error,
            j.createdAt(),
            Instant.now()));
    return true;
  }

  public synchronized int recoverExpired(Instant now) {
    int count = 0;
    for (var entry : List.copyOf(jobs.entrySet())) {
      JobRecord j = entry.getValue();
      if (j.status() == JobRecord.JobStatus.CLAIMED && j.leaseExpiresAt().isBefore(now)) {
        jobs.put(
            j.id(),
            new JobRecord(
                j.id(),
                j.tenantId(),
                j.artifactId(),
                j.type(),
                JobRecord.JobStatus.RETRY_PENDING,
                j.attempts(),
                now,
                null,
                null,
                j.attributes(),
                "lease expired",
                j.createdAt(),
                now));
        count++;
      }
    }
    return count;
  }
}
