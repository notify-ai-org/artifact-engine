package dev.notify.artifact.job;

import java.util.NoSuchElementException;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.store.MetadataStore;

public abstract class AbstractJob<R> implements Job<R> {

    protected final ArtifactAccessVerifier verifier;

    protected final MetadataStore metadataStore;

    protected AbstractJob(ArtifactAccessVerifier verifier, MetadataStore metadataStore) {
        this.verifier = verifier;
        this.metadataStore = metadataStore;
    }

    @Override
    public abstract R execute() throws Exception;

    protected Artifact required(String tenantId, String artifactId) {
        return metadataStore
            .find(tenantId, artifactId)
            .orElseThrow(() -> new NoSuchElementException("Artifact not found"));
    }

    protected Artifact requiredReadable(String tenantId, String artifactId) {
        Artifact artifact = required(tenantId, artifactId);
        if (artifact.storageStatus() == ArtifactStatus.Storage.DELETED
            || artifact.indexStatus() == ArtifactStatus.Index.DELETED) {
            throw new NoSuchElementException("Artifact not found");
        }
        return artifact;
    }

    protected String verify(
      String principalId,
      String tenantId,
      AuthorizationService.Permission permission,
      Artifact artifact,
      String extractedContent
    ) {
        if (artifact == null) {
            return extractedContent;
        }
        verifier.authenticate(principalId, tenantId, permission);
        verifier.verifyArtifact(tenantId, artifact);
        return verifier.verifyExtractedContent(artifact, extractedContent);
    }

     public static long version(JobRecord record) {
    return longAttribute(record, "version");
  }

  /** Attribute marking store jobs whose whole-file digest is not known when the upload starts. */
  static final String CONTENT_DIGEST = "contentDigest";
  static final String DEFERRED = "deferred";

  /**
   * The whole-file SHA-256 to record with the stored object, or null when the plan deferred it:
   * a chunked ingest starts uploading before the content has been read in full.
   */
  static String contentDigest(JobRecord record, Artifact artifact) {
    return DEFERRED.equals(record.attributes().get(CONTENT_DIGEST)) ? null : artifact.sha256();
  }

  public static long longAttribute(JobRecord record, String name) {
    String value = record.attributes().get(name);
    if (value == null) {
      throw new IllegalArgumentException("Job " + record.id() + " is missing attribute " + name);
    }
    return Long.parseLong(value);
  }

  /** The workflow was planned for one artifact version; a newer version gets its own workflow. */
  static Artifact artifactAtVersion(AbstractJob<?> job, JobRecord record, long version) {
    Artifact artifact = job.required(record.tenantId(), record.artifactId());
    if (artifact.version() != version) {
      throw new IllegalStateException(
          "Artifact " + record.artifactId() + " moved from version " + version + " to "
              + artifact.version());
    }
    return artifact;
  }
    
}
