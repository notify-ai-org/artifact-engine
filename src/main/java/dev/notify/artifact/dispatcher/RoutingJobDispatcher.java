package dev.notify.artifact.dispatcher;

import dev.notify.artifact.job.DirectJob;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.QueueableJob;
import java.util.Objects;

/** Routes jobs by their explicit execution contract and rejects unclassified jobs. */
public final class RoutingJobDispatcher implements JobDispatcher {
  private final JobDispatcher direct;
  private final JobDispatcher queued;

  public RoutingJobDispatcher(JobDispatcher direct, JobDispatcher queued) {
    this.direct = Objects.requireNonNull(direct, "direct");
    this.queued = Objects.requireNonNull(queued, "queued");
  }

  @Override
  public <R> R dispatch(Job<R> job) throws Exception {
    Objects.requireNonNull(job, "job");
    if (job instanceof DirectJob<?>) return direct.dispatch(job);
    if (job instanceof QueueableJob<?>) return queued.dispatch(job);
    throw new IllegalArgumentException("Job must implement DirectJob or QueueableJob: " + job.getClass().getName());
  }
}
