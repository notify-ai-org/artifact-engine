package dev.notify.artifact.job;

import java.util.Objects;

/** Executes a job on the caller thread; useful for synchronous APIs and deterministic tests. */
public final class DirectJobDispatcher implements JobDispatcher {
  @Override
  public <R> R dispatch(Job<R> job) throws Exception {
    return Objects.requireNonNull(job, "job").execute();
  }
}
