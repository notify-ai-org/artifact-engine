package dev.notify.artifact.worker;

import dev.notify.artifact.job.Job;
import dev.notify.artifact.model.JobRecord;

/** Reconstructs restart-safe work from a durable job record. */
@FunctionalInterface
public interface JobRecordExecutor {
  Job<?> toJob(JobRecord job);
}
