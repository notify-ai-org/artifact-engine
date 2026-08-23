package dev.notify.artifact.worker;

import dev.notify.artifact.model.JobRecord;

/** Reconstructs and executes restart-safe work from a durable job record. */
@FunctionalInterface
public interface JobRecordExecutor {
  void execute(JobRecord job) throws Exception;
}
