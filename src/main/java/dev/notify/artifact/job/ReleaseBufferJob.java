package dev.notify.artifact.job;

import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.util.StructuredLog;
import java.util.Objects;

/**
 * Returns a chunk's buffer to the pool once spool, store, and index are done with it. Only this
 * process's pool is affected; a copy another instance loaded ages out through its idle TTL.
 */
public final class ReleaseBufferJob implements Job<Void> {
  private static final StructuredLog LOG = StructuredLog.of(ReleaseBufferJob.class);

  private final JobRecord record;
  private final ChunkedIngestSupport support;

  public ReleaseBufferJob(JobRecord record, ChunkedIngestSupport support) {
    this.record = Objects.requireNonNull(record, "record");
    this.support = Objects.requireNonNull(support, "support");
  }

  @Override
  public Void execute() {
    ChunkRef chunk = ChunkRef.of(record);
    support.pool().release(chunk.bufferKey(record.artifactId()));
    LOG.debug("chunk_released", "artifact", record.artifactId(), "chunk", chunk.chunk());
    return null;
  }
}
