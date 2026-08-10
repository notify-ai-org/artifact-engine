package dev.notify.artifact.job;

/** Dispatch boundary used by the facade to execute jobs without owning their workflows. */
@FunctionalInterface
public interface JobDispatcher {
  <R> R dispatch(Job<R> job) throws Exception;
}
