package dev.notify.artifact.job;

import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.StructuredLog;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Publishes the fully written spool entry: checks its length, re-verifies its media type against
 * the complete file (a zip is only confirmed as DOCX here), and records the real SHA-256.
 */
public final class SpoolCommitJob extends AbstractJob<Artifact> {
  private static final StructuredLog LOG = StructuredLog.of(SpoolCommitJob.class);

  private final JobRecord record;
  private final DurableSpool spool;
  private final DataVerifier dataVerifier;

  public SpoolCommitJob(
      JobRecord record,
      MetadataStore metadataStore,
      DurableSpool spool,
      DataVerifier dataVerifier) {
    super(null, metadataStore);
    this.record = Objects.requireNonNull(record, "record");
    this.spool = Objects.requireNonNull(spool, "spool");
    this.dataVerifier = Objects.requireNonNull(dataVerifier, "dataVerifier");
  }

  @Override
  public Artifact execute() throws IOException {
    Artifact artifact = artifactAtVersion(this, record, version(record));
    Instant started = Instant.now();
    DurableSpool.SpoolEntry entry = spool.commit(artifact.spoolPath(), artifact.sizeBytes());
    dataVerifier.verify(entry.contentPath(), artifact.mediaType());
    Artifact committed = metadataStore.update(
        record.tenantId(), record.artifactId(), current -> current.withContentDigest(entry.sha256()));
    LOG.info("spool_committed", "artifact", artifact.id(), "bytes", entry.sizeBytes(),
        "sha256", entry.sha256(), "duration", Duration.between(started, Instant.now()));
    return committed;
  }
}
