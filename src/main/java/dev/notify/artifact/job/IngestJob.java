package dev.notify.artifact.job;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.security.IngestionSecurityContext;
import dev.notify.artifact.security.RetrievalScanGateFilter;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Idempotency;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Durable intake workflow: authorize, spool, verify, register metadata, and publish outbox jobs.
 */
public record IngestJob(
    Requests.Ingest request,
    MetadataStore metadataStore,
    DurableSpool durableSpool,
    DataVerifier dataVerifier,
    AuthorizationService authorizationService,
    EngineOptions options,
    SecurityFilterChain<IngestionSecurityContext> ingestionSecurity,
    SecurityContextFactory securityContextFactory)
    implements Job<Artifact> {

  @Override
  public Artifact execute() throws IOException {
    authorizationService.require(
        request.principalId(), request.tenantId(), AuthorizationService.Permission.INGEST);

    String artifactId = UUID.randomUUID().toString();
    DurableSpool.SpoolEntry spoolEntry =
        durableSpool.write(request.tenantId(), artifactId, request.content(), request.metadata());
    String detectedMediaType;
    String sanitizedFilename;
    Map<String, String> securedMetadata = new java.util.TreeMap<>(request.metadata());
    try {
      if (ingestionSecurity != null) {
        IngestionSecurityContext securityContext =
            securityContextFactory.ingestion(request, spoolEntry.contentPath());
        ingestionSecurity.verify(securityContext);
        detectedMediaType = securityContext.detectedMediaType();
        sanitizedFilename = securityContext.sanitizedFilename();
        if (detectedMediaType == null
            || sanitizedFilename == null
            || securityContext.scanVerdict() == null) {
          throw new IllegalStateException(
              "Ingestion security chain did not produce all required decisions");
        }
        securedMetadata.put(
            RetrievalScanGateFilter.SCAN_STATUS_METADATA, securityContext.scanVerdict().name());
      } else {
        detectedMediaType =
            dataVerifier.verify(spoolEntry.contentPath(), request.declaredMediaType());
        sanitizedFilename = sanitizeName(request.originalName());
      }
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
      MetadataStore.Registration registration =
          metadataStore.register(
              artifact, initialOperations(artifact), options.deduplicateContent());
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

  private static String sanitizeName(String name) {
    if (name == null || name.isBlank()) {
      return "artifact";
    }
    String sanitizedName = name.replaceAll("[\\r\\n\\u0000]", "_");
    return sanitizedName.substring(0, Math.min(255, sanitizedName.length()));
  }
}
