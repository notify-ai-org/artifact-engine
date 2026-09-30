package dev.notify.artifact.job;

import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Writes a buffered chunk at its offset in the reserved spool entry and flushes it to disk. */
public final class SpoolChunkJob extends AbstractJob<Integer> {
  private static final StructuredLog LOG = StructuredLog.of(SpoolChunkJob.class);

  private final JobRecord record;
  private final DurableSpool spool;
  private final ChunkedIngestSupport support;

  public SpoolChunkJob(
      JobRecord record,
      MetadataStore metadataStore,
      DurableSpool spool,
      ChunkedIngestSupport support) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.spool = Objects.requireNonNull(spool, "spool");
    this.support = Objects.requireNonNull(support, "support");
  }

  @Override
  public Integer execute() throws IOException {
    ChunkRef chunk = ChunkRef.of(record);
    Artifact artifact = artifactAtVersion(this, record, chunk.version());
    Instant started = Instant.now();
    try (var lease = chunk.lease(support, artifact)) {
      spool.writeChunk(artifact.spoolPath(), chunk.offset(), lease.buffer());
      LOG.debug("chunk_spooled", "artifact", artifact.id(), "chunk", chunk.chunk(),
          "bytes", lease.length(), "duration", Duration.between(started, Instant.now()));
      return lease.length();
    }
  }
}
