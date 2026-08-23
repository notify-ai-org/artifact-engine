package dev.notify.artifact.dispatcher;

import dev.notify.artifact.job.DirectJob;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.worker.DirectJobWorker;
import java.util.Objects;

/** Dispatches only explicitly process-local jobs to the dedicated direct worker. */
public final class DirectJobDispatcher implements JobDispatcher {
  private final DirectJobWorker worker;

  public DirectJobDispatcher(DirectJobWorker worker) {
    this.worker = Objects.requireNonNull(worker, "worker");
  }

  @Override
  public <R> R dispatch(Job<R> job) throws Exception {
    if (!(job instanceof DirectJob<?>)) {
      throw new IllegalArgumentException("Direct dispatcher requires DirectJob");
    }
    @SuppressWarnings("unchecked")
    DirectJob<R> direct = (DirectJob<R>) job;
    return worker.execute(direct);
  }
}
