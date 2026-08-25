package dev.notify.artifact.store;

import dev.notify.artifact.model.JobRecord;
import java.util.Optional;
import java.util.function.UnaryOperator;

/** Durable repository for job definitions and execution state. */
public interface JobStore {
  /** Registers a job without overwriting a record already advanced by a worker. */
  JobRecord create(JobRecord job);

  JobRecord save(JobRecord job);

  Optional<JobRecord> find(String jobId);

  /** Atomically updates the latest stored version of a job. */
  JobRecord update(String jobId, UnaryOperator<JobRecord> update);
}
