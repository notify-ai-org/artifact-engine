package dev.notify.artifact.queue;

import dev.notify.artifact.model.JobRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Durable leased operation queue; all terminal mutations are conditional on the owner token. */
public interface JobQueue {
  void enqueue(JobRecord job);

  Optional<JobRecord> claim(JobRecord.JobType type, String owner, Duration lease, Instant now);

  boolean complete(String jobId, String owner);

  boolean retry(String jobId, String owner, Instant nextAttempt, String error);

  boolean deadLetter(String jobId, String owner, String error);

  int recoverExpired(Instant now);
}
