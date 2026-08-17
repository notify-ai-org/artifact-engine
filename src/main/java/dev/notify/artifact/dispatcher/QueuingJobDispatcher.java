package dev.notify.artifact.dispatcher;

import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.QueueableJob;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.queue.QueueManager;
import java.util.Objects;

/**
 * Routes restart-safe jobs to the durable queue owned by {@link QueueManager}.
 *
 * <p>Only {@link QueueableJob} instances are persisted. Other jobs are delegated because an
 * arbitrary {@link Job} may close over an input stream, credentials, or process-local service
 * objects and therefore cannot safely be placed in Redis. The default delegate executes these
 * request/response jobs on the caller thread.
 */
public final class QueuingJobDispatcher implements JobDispatcher {
  private final QueueManager queueManager;
  
  public QueuingJobDispatcher(QueueManager queueManager) {
    this.queueManager = Objects.requireNonNull(queueManager, "queueManager");
  }

  @Override
  public <R> R dispatch(Job<R> job) throws Exception {
    Objects.requireNonNull(job, "job");
    QueueableJob<R> queueableJob = (QueueableJob<R>) job;
    JobRecord record = requirePending(queueableJob.queueRecord());
    queueManager.enqueue(record);
    return queueableJob.queuedResult();
  }

  private static JobRecord requirePending(JobRecord record) {
    Objects.requireNonNull(record, "queueableJob.queueRecord()");
    if (record.status() != JobRecord.JobStatus.PENDING) {
      throw new IllegalArgumentException("Only PENDING jobs may be newly enqueued");
    }
    if (record.id() == null || record.id().isBlank()) {
      throw new IllegalArgumentException("Queued job id is required");
    }
    if (record.tenantId() == null || record.tenantId().isBlank()) {
      throw new IllegalArgumentException("Queued job tenant id is required");
    }
    if (record.artifactId() == null || record.artifactId().isBlank()) {
      throw new IllegalArgumentException("Queued job artifact id is required");
    }
    if (record.type() == null) {
      throw new IllegalArgumentException("Queued job type is required");
    }
    return record;
  }
}
