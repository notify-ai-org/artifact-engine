package dev.notify.artifact.chunk;

import dev.notify.artifact.job.AbstractJob;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import java.io.IOException;
import java.io.InterruptedIOException;

/** One source chunk named by a chunk job's record. */
public record ChunkRef(long version, int chunk, long offset, int length, String sourceVersion) {
  public static final String CHUNK = "chunk";
  public static final String OFFSET = "offset";
  public static final String LENGTH = "length";
  public static final String SOURCE_VERSION = "sourceVersion";

  public static ChunkRef of(JobRecord record) {
    return new ChunkRef(
        AbstractJob.version(record),
        (int) AbstractJob.longAttribute(record, CHUNK),
        AbstractJob.longAttribute(record, OFFSET),
        (int) AbstractJob.longAttribute(record, LENGTH),
        record.attributes().get(SOURCE_VERSION));
  }

  public String bufferKey(String artifactId) {
    return artifactId + ":" + version + ":" + chunk;
  }

  /**
   * The chunk's bytes: from the buffer pool when already loaded, otherwise read from the source
   * now. The source is re-vetted by the policy on every read.
   */
  public ChunkBufferPool.Lease lease(ChunkedIngestSupport support, Artifact artifact) throws IOException {
    try {
      return support.pool().acquire(
          bufferKey(artifact.id()),
          length,
          target -> support.sources().open(artifact.sourceUri()).read(offset, target, sourceVersion),
          support.bufferWait());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while waiting for a chunk buffer");
    }
  }
}
