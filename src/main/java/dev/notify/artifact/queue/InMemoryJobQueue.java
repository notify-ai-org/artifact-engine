package dev.notify.artifact.queue;

import dev.notify.artifact.model.JobRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class InMemoryJobQueue implements JobQueue {
  private final Map<String, JobRecord> readyJobs = new LinkedHashMap<>();
  private final Map<String, JobRecord> claimedJobs = new LinkedHashMap<>();
  private final Set<String> knownJobIds = new HashSet<>();

  public synchronized void enqueue(JobRecord j) {
    if (knownJobIds.add(j.id())) {
      readyJobs.put(j.id(), j);
    }
  }

  @Override
  public synchronized void requeue(JobRecord job) {
    claimedJobs.remove(job.id());
    knownJobIds.add(job.id());
    readyJobs.put(job.id(), job);
  }

  public synchronized Optional<JobRecord> claim(
      JobRecord.JobType type, String owner, Duration lease, Instant now) {
    Optional<JobRecord> ready =
        readyJobs.values().stream()
            .filter(
                j ->
                    j.type() == type
                        && (j.status() == JobRecord.JobStatus.PENDING
                            || j.status() == JobRecord.JobStatus.RETRY_PENDING)
                        && (j.nextAttemptAt() == null || !j.nextAttemptAt().isAfter(now)))
            .findFirst();
    if (ready.isEmpty()) {
      return Optional.empty();
    }
    JobRecord claimed = ready.orElseThrow().claimed(owner, now.plus(lease));
    readyJobs.remove(claimed.id());
    claimedJobs.put(claimed.id(), claimed);
    return Optional.of(claimed);
  }

  public synchronized boolean complete(String id, String owner) {
    return updateOwned(id, owner, JobRecord.JobStatus.COMPLETED, null, null);
  }

  public synchronized boolean retry(String id, String owner, Instant next, String error) {
    return updateOwned(id, owner, JobRecord.JobStatus.RETRY_PENDING, next, error);
  }

  public synchronized boolean deadLetter(String id, String owner, String error) {
    return updateOwned(id, owner, JobRecord.JobStatus.DEAD_LETTER, null, error);
  }

  private boolean updateOwned(
      String id, String owner, JobRecord.JobStatus state, Instant next, String error) {
    JobRecord j = claimedJobs.get(id);
    if (j == null
        || !Objects.equals(owner, j.leaseOwner())
        || j.leaseExpiresAt() == null
        || !j.leaseExpiresAt().isAfter(Instant.now())) {
      return false;
    }
    claimedJobs.remove(id);
    if (state == JobRecord.JobStatus.RETRY_PENDING) {
      readyJobs.put(
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
    }
    return true;
  }

  public synchronized int recoverExpired(Instant now) {
    int count = 0;
    for (var entry : List.copyOf(claimedJobs.entrySet())) {
      JobRecord j = entry.getValue();
      if (j.status() == JobRecord.JobStatus.CLAIMED && j.leaseExpiresAt().isBefore(now)) {
        claimedJobs.remove(j.id());
        readyJobs.put(
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
