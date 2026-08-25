package dev.notify.artifact.store;

import dev.notify.artifact.model.JobRecord;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

/** Process-local job repository for tests and non-durable deployments. */
public final class InMemoryJobStore implements JobStore {
  private final Map<String, JobRecord> jobs = new LinkedHashMap<>();

  @Override
  public synchronized JobRecord create(JobRecord job) {
    Objects.requireNonNull(job, "job");
    return jobs.computeIfAbsent(job.id(), ignored -> job);
  }

  @Override
  public synchronized JobRecord save(JobRecord job) {
    Objects.requireNonNull(job, "job");
    jobs.put(job.id(), job);
    return job;
  }

  @Override
  public synchronized Optional<JobRecord> find(String jobId) {
    return Optional.ofNullable(jobs.get(jobId));
  }

  @Override
  public synchronized JobRecord update(String jobId, UnaryOperator<JobRecord> update) {
    JobRecord current =
        Optional.ofNullable(jobs.get(jobId))
            .orElseThrow(() -> new NoSuchElementException("Job not found: " + jobId));
    JobRecord changed = Objects.requireNonNull(update.apply(current), "updated job");
    if (!jobId.equals(changed.id())) {
      throw new IllegalArgumentException("Job update cannot change its id");
    }
    jobs.put(jobId, changed);
    return changed;
  }
}
