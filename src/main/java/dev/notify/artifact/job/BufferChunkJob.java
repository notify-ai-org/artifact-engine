package dev.notify.artifact.job;

import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Reads one byte range of the source into the chunk buffer pool for the chunk's other jobs. */
public final class BufferChunkJob extends AbstractJob<Integer> {
  private static final StructuredLog LOG = StructuredLog.of(BufferChunkJob.class);

  private final JobRecord record;
  private final ChunkedIngestSupport support;

  public BufferChunkJob(JobRecord record, MetadataStore metadataStore, ChunkedIngestSupport support) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.support = Objects.requireNonNull(support, "support");
  }

  @Override
  public Integer execute() throws IOException {
    ChunkRef chunk = ChunkRef.of(record);
    Artifact artifact = artifactAtVersion(this, record, chunk.version());
    Instant started = Instant.now();
    try (var lease = chunk.lease(support, artifact)) {
      Duration elapsed = Duration.between(started, Instant.now());
      LOG.info("chunk_buffered", "artifact", artifact.id(), "chunk", chunk.chunk(),
          "offset", chunk.offset(), "bytes", lease.length(), "duration", elapsed,
          "mibPerSecond", StructuredLog.mibPerSecond(lease.length(), elapsed));
      return lease.length();
    }
  }
}
