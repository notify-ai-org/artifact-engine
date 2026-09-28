package dev.notify.artifact.job;

@FunctionalInterface
public interface Job<R> {
  R execute() throws Exception;

  /**
   * Runs on the dispatching thread before the job is handed off (to the direct pool or the durable
   * queue). Use it for work paced by the caller's own input, such as draining a request body, so a
   * slow client cannot occupy threads shared with other jobs.
   */
  default void prepare() throws Exception {}

  /**
   * Called when a prepared job will not run to completion after all (the hand-off was rejected or
   * the caller was interrupted), so it can release what {@link #prepare()} acquired. Must be a
   * no-op if execution already started.
   */
  default void abandon() {}
}
