package dev.notify.artifact.job;

import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.spool.SpoolReleaser;

import java.util.Objects;

/** Final ingest workflow stage: frees the spool copy once store and index have both completed. */
public final class ReleaseSpoolJob implements Job<Boolean> {
  private final JobRecord record;
  private final SpoolReleaser releaser;

  public ReleaseSpoolJob(JobRecord record, SpoolReleaser releaser) {
    this.record = Objects.requireNonNull(record, "record");
    this.releaser = Objects.requireNonNull(releaser, "releaser");
  }

  @Override
  public Boolean execute() throws Exception {
    return releaser.release(record.tenantId(), record.artifactId());
  }
}
