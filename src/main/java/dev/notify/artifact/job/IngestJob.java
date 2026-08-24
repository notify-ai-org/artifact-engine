package dev.notify.artifact.job;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Idempotency;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

  @Override
  public Artifact execute() throws IOException {
    String artifactId = UUID.randomUUID().toString();
    DurableSpool.SpoolEntry spoolEntry =
        durableSpool.write(request.tenantId(), artifactId, request.content(), request.metadata());
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
      List<JobRecord> operations = initialOperations(artifact);
      MetadataStore.Registration registration =
          metadataStore.register(
              artifact, options.deduplicateContent());
      if (registration.outcome() == MetadataStore.Registration.Outcome.CREATED
          && workflowManager != null) {
        workflowManager.create(
            "ingest-store-index",
            operations,
            Map.of("tenantId", artifact.tenantId(), "artifactId", artifact.id()));
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

  private static List<JobRecord> initialOperations(Artifact artifact) {
    return List.of(
        JobRecord.pending(
            operationId(artifact, JobRecord.JobType.STORE),
            artifact.tenantId(),
            artifact.id(),
            JobRecord.JobType.STORE,
            Map.of("version", Long.toString(artifact.version()))),
        JobRecord.pending(
            operationId(artifact, JobRecord.JobType.INDEX),
            artifact.tenantId(),
            artifact.id(),
            JobRecord.JobType.INDEX,
            Map.of("version", Long.toString(artifact.version()))));
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
