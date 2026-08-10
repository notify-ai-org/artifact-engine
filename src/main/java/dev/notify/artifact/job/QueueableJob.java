package dev.notify.artifact.job;

import dev.notify.artifact.model.JobRecord;

/**
 * A job whose durable execution can be represented by a {@link JobRecord}.
 *
 * <p>The record, rather than this Java object, is written to the durable queue. This is important:
 * job implementations may contain streams, SDK clients, or other process-local collaborators that
 * cannot survive serialization or an application restart. A durable worker reconstructs the
 * executable job from the record's type, tenant, artifact, and attributes.
 *
 * <p>{@link #queuedResult()} is the immediate acknowledgement returned to the caller after the
 * queue accepts the record. It must not represent successful execution of the background work.
 */
public interface QueueableJob<R> extends Job<R> {
  /** Returns the complete, restart-safe description of the work to enqueue. */
  JobRecord queueRecord();

  /** Returns the acknowledgement value after a successful enqueue. */
  R queuedResult();
}
