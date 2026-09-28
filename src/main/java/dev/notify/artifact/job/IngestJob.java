package dev.notify.artifact.job;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.multipart.MultipartPlan;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Idempotency;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import dev.notify.artifact.workflow.WorkflowManager;

/**
 * Durable intake workflow: authorize, spool, verify, register metadata, and publish outbox jobs.
 */
public final class IngestJob extends AbstractJob<Artifact> implements DirectJob<Artifact> {
  private final Requests.Ingest request;
  private final DurableSpool durableSpool;
  private final ArtifactAccessVerifier accessVerifier;
  private final EngineOptions options;
  private final WorkflowManager workflowManager;
  private final AtomicBoolean claimed = new AtomicBoolean();
  private volatile DurableSpool.SpoolEntry spoolEntry;
  private volatile String artifactId;

  public IngestJob(
      Requests.Ingest request,
      MetadataStore metadataStore,
      DurableSpool durableSpool,
      ArtifactAccessVerifier accessVerifier,
      EngineOptions options) {
    this(request, metadataStore, durableSpool, accessVerifier, options, null);
  }

  public IngestJob(
      Requests.Ingest request,
      MetadataStore metadataStore,
      DurableSpool durableSpool,
      ArtifactAccessVerifier accessVerifier,
      EngineOptions options,
      WorkflowManager workflowManager) {
    super(accessVerifier, metadataStore);
    this.request = request;
    this.durableSpool = durableSpool;
    this.accessVerifier = accessVerifier;
    this.options = options;
    this.workflowManager = workflowManager;
  }

  /**
   * Authorizes, then drains the caller's stream into the spool on the calling thread. The request
   * body is paced by the client, so this must not run on the shared direct-job pool.
   */
  @Override
  public void prepare() throws IOException {
    if (spoolEntry != null) return;
    accessVerifier.authenticate(
        request.principalId(), request.tenantId(), AuthorizationService.Permission.INGEST);
    String id = UUID.randomUUID().toString();
    spoolEntry = durableSpool.write(request.tenantId(), id, request.content(), request.metadata());
    artifactId = id;
  }

  @Override
  public void abandon() {
    if (!claimed.compareAndSet(false, true) || spoolEntry == null) return;
    try {
      durableSpool.discard(spoolEntry.contentPath());
    } catch (IOException ignored) {
      // Unregistered spool entries are also reported by SpoolReconciler as orphans.
    }
  }

  @Override
  public Artifact execute() throws IOException {
    if (!claimed.compareAndSet(false, true)) {
      throw new IllegalStateException("Ingest job was abandoned");
    }
    // Dispatchers that do not call prepare() still get the full intake.
    prepare();
    String artifactId = this.artifactId;
    DurableSpool.SpoolEntry spoolEntry = this.spoolEntry;
    String detectedMediaType;
    String sanitizedFilename;
    Map<String, String> securedMetadata = new java.util.TreeMap<>(request.metadata());
    try {
      ArtifactAccessVerifier.VerifiedIngestion verified =
          accessVerifier.verifyIngestion(
              request.principalId(),
              request.tenantId(),
              spoolEntry.contentPath(),
              request.declaredMediaType(),
              request.originalName());
      detectedMediaType = verified.detectedMediaType();
      sanitizedFilename = verified.sanitizedFilename();
    } catch (IOException | RuntimeException verificationFailure) {
      discardFailedIntake(spoolEntry.contentPath(), verificationFailure);
      throw verificationFailure;
    }

    if (request.contentLength() >= 0 && request.contentLength() != spoolEntry.sizeBytes()) {
      IllegalArgumentException mismatch =
          new IllegalArgumentException(
              "Declared content length does not match the streamed artifact length");
      discardFailedIntake(spoolEntry.contentPath(), mismatch);
      throw mismatch;
    }

    String idempotencyKey =
        request.idempotencyKey() == null || request.idempotencyKey().isBlank()
            ? UUID.randomUUID().toString()
            : request.idempotencyKey();
    String idempotencyFingerprint =
        Idempotency.fingerprint(
            request.tenantId(),
            "INGEST",
            spoolEntry.sha256(),
            fingerprintMetadata(request, detectedMediaType, sanitizedFilename));
    Instant now = Instant.now();
    Artifact artifact =
        new Artifact(
            artifactId,
            request.tenantId(),
            idempotencyKey,
            idempotencyFingerprint,
            "UPLOAD",
            null,
            sanitizedFilename,
            detectedMediaType,
            spoolEntry.sizeBytes(),
            spoolEntry.sha256(),
            null,
            spoolEntry.contentPath(),
            ArtifactStatus.Storage.SPOOLED,
            ArtifactStatus.Index.PENDING,
            1,
            securedMetadata,
            null,
            null,
            now,
            now);
    try {
      List<List<JobRecord>> stages = initialStages(artifact, options.multipartPartBytes());
      MetadataStore.Registration registration =
          metadataStore.register(
              artifact, options.deduplicateContent());
      if (registration.outcome() == MetadataStore.Registration.Outcome.CREATED
          && workflowManager != null) {
        workflowManager.createStaged(
            "ingest-store-index",
            stages,
            Map.of(
                "tenantId", artifact.tenantId(),
                "artifactId", artifact.id(),
                "version", Long.toString(artifact.version())));
      }
      if (registration.outcome() != MetadataStore.Registration.Outcome.CREATED) {
        durableSpool.discard(spoolEntry.contentPath());
      }
      return registration.artifact();
    } catch (RuntimeException registrationFailure) {
      discardFailedIntake(spoolEntry.contentPath(), registrationFailure);
      throw registrationFailure;
    }
  }

  private void discardFailedIntake(java.nio.file.Path contentPath, Throwable intakeFailure) {
    try {
      durableSpool.discard(contentPath);
    } catch (IOException cleanupFailure) {
      intakeFailure.addSuppressed(cleanupFailure);
    }
  }

  /**
   * Store, then index, then release the spool copy. An artifact larger than one part is stored as a
   * multipart upload whose parts run in parallel: INIT, then every PART, then COMPLETE.
   */
  static List<List<JobRecord>> initialStages(Artifact artifact, long multipartPartBytes) {
    String version = Long.toString(artifact.version());
    JobRecord index = pending(artifact, JobRecord.JobType.INDEX, "", Map.of("version", version));
    JobRecord release =
        pending(artifact, JobRecord.JobType.RELEASE_SPOOL, "", Map.of("version", version));
    MultipartPlan plan = MultipartPlan.forSize(artifact.sizeBytes(), multipartPartBytes);
    if (plan == null) {
      return List.of(
          List.of(pending(artifact, JobRecord.JobType.STORE, "", Map.of("version", version))),
          List.of(index),
          List.of(release));
    }
    JobRecord init =
        pending(
            artifact,
            JobRecord.JobType.STORE_INIT,
            "",
            Map.of(
                "version", version,
                "partSize", Long.toString(plan.partSize()),
                "partCount", Integer.toString(plan.partCount())));
    List<JobRecord> parts =
        plan.parts().stream()
            .map(
                part ->
                    pending(
                        artifact,
                        JobRecord.JobType.STORE_PART,
                        ":" + part.number(),
                        Map.of(
                            "version", version,
                            "partNumber", Integer.toString(part.number()),
                            "offset", Long.toString(part.offset()),
                            "length", Long.toString(part.length()))))
            .toList();
    JobRecord complete =
        pending(artifact, JobRecord.JobType.STORE_COMPLETE, "", Map.of("version", version));
    return List.of(List.of(init), parts, List.of(complete), List.of(index), List.of(release));
  }

  private static JobRecord pending(
      Artifact artifact, JobRecord.JobType type, String suffix, Map<String, String> attributes) {
    return JobRecord.pending(
        // Part ids hash the part number in so they stay within the 64-character job id column.
        suffix.isEmpty()
            ? operationId(artifact, type)
            : Checksum.sha256(operationId(artifact, type) + suffix),
        artifact.tenantId(),
        artifact.id(),
        type,
        attributes);
  }

  private static String operationId(Artifact artifact, JobRecord.JobType type) {
    return Checksum.sha256(
        artifact.tenantId() + ":" + artifact.id() + ":" + artifact.version() + ":" + type);
  }

  private static Map<String, String> fingerprintMetadata(
      Requests.Ingest request, String detectedMediaType, String sanitizedFilename) {
    Map<String, String> fingerprint = new java.util.TreeMap<>(request.metadata());
    fingerprint.put("originalName", sanitizedFilename);
    fingerprint.put("mediaType", detectedMediaType);
    return fingerprint;
  }
}
