package dev.notify.artifact.worker;

import dev.notify.artifact.job.Job;
import dev.notify.artifact.model.JobRecord;
import java.util.Objects;
import java.util.function.Function;

/** Resolves a durable queue record to its workflow job and executes that job. */
public final class DefaultDurableJobHandler implements DurableJobWorker.Handler {
  private final Function<JobRecord, ? extends Job<?>> jobResolver;

  public DefaultDurableJobHandler(Function<JobRecord, ? extends Job<?>> jobResolver) {
    this.jobResolver = Objects.requireNonNull(jobResolver, "jobResolver");
  }

  @Override
  public void execute(JobRecord record) throws Exception {
    Job<?> job =
        Objects.requireNonNull(
            jobResolver.apply(Objects.requireNonNull(record, "record")),
            "jobResolver returned null");
    job.execute();
  }
}
