package dev.notify.artifact.job;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.chunk.ChunkRef;
import dev.notify.artifact.chunk.ChunkSource;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.multipart.MultipartPlan;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.util.Checksum;
import dev.notify.artifact.util.Idempotency;
import dev.notify.artifact.util.StructuredLog;
import dev.notify.artifact.workflow.WorkflowManager;
import dev.notify.artifact.workflow.WorkflowManager.PlannedStep;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Plans a chunked ingest from a source location. Only the first 8 KB are read here (to detect the
 * media type); everything else is read by the workflow. All chunks share one stage and are
 * pipelined: while one chunk uploads, the next ones are already downloading.
 *
 * <p>text/plain is indexed chunk by chunk and never touches the local spool, so its size is not
 * bounded by the spool volume:
 *
 * <pre>
 * chunks       STORE_INIT
 *              BUFFER_i -> ( STORE_CHUNK_i (after STORE_INIT) || INDEX_CHUNK_i ) -> RELEASE_BUFFER_i
 * final        STORE_COMPLETE || INDEX_FINAL
 * </pre>
 *
 * Other formats cannot be parsed in byte chunks, so they are also spooled and indexed whole:
 *
 * <pre>
 * chunks       STORE_INIT
 *              BUFFER_i -> ( SPOOL_i || STORE_CHUNK_i (after STORE_INIT) ) -> RELEASE_BUFFER_i
 * final        SPOOL_COMMIT -> INDEX_FINAL,  STORE_COMPLETE  -> RELEASE_SPOOL
 * </pre>
 *
 * Two orderings cross chunks: INDEX_CHUNK_i waits for INDEX_CHUNK_(i-1), since it resumes that
 * chunk's chunker and digest state, and BUFFER_i waits for RELEASE_BUFFER_(i-w), where w is the
 * buffer pool's slab count, so an ingest never loads more chunks than the pool can hold.
 *
 * The content's SHA-256 is unknown when the artifact is registered, so it carries a provisional
 * digest until INDEX_FINAL (text, from the running digest kept by INDEX_CHUNK) or SPOOL_COMMIT
 * (other formats) records the real one. Content deduplication does not apply to source ingests.
 * A lost chunk buffer is always re-read from the source, which is also how text ingests recover
 * without a spool copy.
 */
public final class SourceIngestJob extends AbstractJob<Artifact> implements DirectJob<Artifact> {
  private static final StructuredLog LOG = StructuredLog.of(SourceIngestJob.class);
  static final String WORKFLOW_NAME = "ingest-source";

  private final Requests.IngestSource request;
  private final DurableSpool spool;
  private final ArtifactAccessVerifier accessVerifier;
  private final ChunkedIngestSupport support;
  private final WorkflowManager workflowManager;

  public SourceIngestJob(
      Requests.IngestSource request,
      MetadataStore metadataStore,
      DurableSpool spool,
      ArtifactAccessVerifier accessVerifier,
      ChunkedIngestSupport support,
      WorkflowManager workflowManager) {
    super(accessVerifier, metadataStore);
    this.request = Objects.requireNonNull(request, "request");
    this.spool = Objects.requireNonNull(spool, "spool");
    this.accessVerifier = Objects.requireNonNull(accessVerifier, "accessVerifier");
    this.support = Objects.requireNonNull(support, "support");
    this.workflowManager = Objects.requireNonNull(workflowManager, "workflowManager");
  }

  @Override
  public Artifact execute() throws IOException {
    accessVerifier.authenticate(
        request.principalId(), request.tenantId(), AuthorizationService.Permission.INGEST);
    ChunkSource source = support.sources().open(request.location());
    ChunkSource.SourceInfo info = source.describe();
    if (info.length() < 1) throw new IllegalArgumentException("Source is empty");
    MultipartPlan plan = MultipartPlan.forSize(info.length(), support.chunkBytes());
    if (plan == null) plan = new MultipartPlan(info.length(), support.chunkBytes(), 1);
    if (plan.partSize() > support.chunkBytes()) {
      throw new IllegalArgumentException("Source needs more than " + MultipartPlan.MAX_PARTS
          + " chunks of " + support.chunkBytes() + " bytes; raise the chunk size");
    }

    ByteBuffer prefix = ByteBuffer.allocate((int) Math.min(DataVerifier.sniffBytes(), info.length()));
    source.read(0, prefix, info.version());
    String mediaType = accessVerifier.verifyPrefix(prefix.array(), request.declaredMediaType());
    String name = accessVerifier.sanitizedFilename(
        request.originalName() != null ? request.originalName() : lastSegment(request.location()));

    String artifactId = UUID.randomUUID().toString();
    boolean textChunks = "text/plain".equals(mediaType);
    java.nio.file.Path spoolPath = textChunks
        ? null
        : spool.reserve(request.tenantId(), artifactId, info.length(), request.metadata());
    try {
      String sourceIdentity = Checksum.sha256(
          request.location() + "|" + info.version() + "|" + info.length());
      String idempotencyKey = request.idempotencyKey() == null || request.idempotencyKey().isBlank()
          ? UUID.randomUUID().toString()
          : request.idempotencyKey();
      Map<String, String> fingerprint = new TreeMap<>(request.metadata());
      fingerprint.put("originalName", name);
      fingerprint.put("mediaType", mediaType);
      Instant now = Instant.now();
      Artifact artifact = new Artifact(
          artifactId, request.tenantId(), idempotencyKey,
          Idempotency.fingerprint(request.tenantId(), "INGEST_SOURCE", sourceIdentity, fingerprint),
          sourceType(request.location()), request.location(), name, mediaType, info.length(),
          Artifact.PENDING_DIGEST_PREFIX + sourceIdentity.substring(0, 56), null, spoolPath,
          ArtifactStatus.Storage.RECEIVING, ArtifactStatus.Index.PENDING, 1,
          new TreeMap<>(request.metadata()), null, null, now, now);

      MetadataStore.Registration registration = metadataStore.register(artifact, false);
      if (registration.outcome() != MetadataStore.Registration.Outcome.CREATED) {
        if (spoolPath != null) spool.discardReserved(spoolPath);
        LOG.info("ingest_deduplicated", "artifact", artifactId,
            "existingArtifact", registration.artifact().id(), "outcome", registration.outcome());
        return registration.artifact();
      }
      String workflowId = workflowManager.createPlan(
          WORKFLOW_NAME,
          stages(artifact, plan, info.version(), textChunks,
              support.pool().stats().slabCount()),
          Map.of("tenantId", artifact.tenantId(), "artifactId", artifactId,
              "version", Long.toString(artifact.version()))).id();
      LOG.info("ingest_registered", "artifact", artifactId, "type", mediaType,
          "bytes", info.length(), "storage", "chunked", "chunks", plan.partCount(),
          "chunkBytes", plan.partSize(), "index", textChunks ? "per-chunk" : "whole-file",
          "spool", textChunks ? "none" : "reserved",
          "workflow", workflowId);
      return artifact;
    } catch (IOException | RuntimeException failure) {
      LOG.warn("ingest_source_failed", "artifact", artifactId,
          "error", failure.getClass().getSimpleName(), "reason", failure.getMessage());
      try {
        if (spoolPath != null) spool.discardReserved(spoolPath);
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  /**
   * The workflow described in the class comment.
   *
   * @param chunksInFlight how many chunks may hold a buffer at once; at most the pool's slab
   *     count, because the pool evicts a loaded chunk nobody is leasing when it needs the slab
   */
  static List<List<PlannedStep>> stages(
      Artifact artifact, MultipartPlan plan, String sourceVersion, boolean textChunks,
      int chunksInFlight) {
    if (chunksInFlight < 1) throw new IllegalArgumentException("chunksInFlight must be positive");
    String version = Long.toString(artifact.version());
    List<List<PlannedStep>> stages = new ArrayList<>();
    JobRecord init = job(artifact, JobRecord.JobType.STORE_INIT, 0,
        Map.of("version", version, "partSize", Long.toString(plan.partSize()),
            "partCount", Integer.toString(plan.partCount()),
            CONTENT_DIGEST, DEFERRED));
    List<PlannedStep> chunks = new ArrayList<>();
    chunks.add(PlannedStep.independent(init));
    List<JobRecord> releases = new ArrayList<>();
    JobRecord previousIndex = null;

    for (MultipartPlan.Part part : plan.parts()) {
      Map<String, String> chunk = new HashMap<>();
      chunk.put("version", version);
      chunk.put(ChunkRef.CHUNK, Integer.toString(part.number()));
      chunk.put(ChunkRef.OFFSET, Long.toString(part.offset()));
      chunk.put(ChunkRef.LENGTH, Long.toString(part.length()));
      if (sourceVersion != null) chunk.put(ChunkRef.SOURCE_VERSION, sourceVersion);
      int number = part.number();
      JobRecord buffer = job(artifact, JobRecord.JobType.BUFFER_CHUNK, number, chunk);
      JobRecord stored = job(artifact, JobRecord.JobType.STORE_CHUNK, number, chunk);
      JobRecord release = job(artifact, JobRecord.JobType.RELEASE_BUFFER, number, chunk);
      // The window: chunk i waits for chunk i - chunksInFlight to give its buffer back.
      chunks.add(number > chunksInFlight
          ? PlannedStep.after(buffer, releases.get(number - 1 - chunksInFlight))
          : PlannedStep.independent(buffer));
      chunks.add(PlannedStep.after(stored, buffer, init));
      if (textChunks) {
        // INDEX_CHUNK resumes the previous chunk's chunker, decoder, and digest state.
        JobRecord indexed = job(artifact, JobRecord.JobType.INDEX_CHUNK, number, chunk);
        chunks.add(previousIndex == null
            ? PlannedStep.after(indexed, buffer)
            : PlannedStep.after(indexed, buffer, previousIndex));
        chunks.add(PlannedStep.after(release, stored, indexed));
        previousIndex = indexed;
      } else {
        JobRecord spooled = job(artifact, JobRecord.JobType.SPOOL_CHUNK, number, chunk);
        chunks.add(PlannedStep.after(spooled, buffer));
        chunks.add(PlannedStep.after(release, stored, spooled));
      }
      releases.add(release);
    }
    stages.add(chunks);

    Map<String, String> finalIndex = new HashMap<>(Map.of("version", version));
    if (textChunks) finalIndex.put(IndexFinalJob.MODE, IndexFinalJob.TEXT_CHUNKS);
    JobRecord index = job(artifact, JobRecord.JobType.INDEX_FINAL, 0, finalIndex);
    JobRecord complete = job(artifact, JobRecord.JobType.STORE_COMPLETE, 0,
        Map.of("version", version, CONTENT_DIGEST, DEFERRED));
    if (textChunks) {
      stages.add(List.of(PlannedStep.independent(complete), PlannedStep.independent(index)));
      return stages;
    }
    JobRecord commit = job(artifact, JobRecord.JobType.SPOOL_COMMIT, 0, Map.of("version", version));
    JobRecord releaseSpool =
        job(artifact, JobRecord.JobType.RELEASE_SPOOL, 0, Map.of("version", version));
    stages.add(List.of(
        PlannedStep.independent(commit),
        PlannedStep.independent(complete),
        PlannedStep.after(index, commit),
        PlannedStep.after(releaseSpool, index, complete)));
    return stages;
  }

  private static JobRecord job(
      Artifact artifact, JobRecord.JobType type, int chunk, Map<String, String> attributes) {
    return JobRecord.pending(
        Checksum.sha256(artifact.tenantId() + ":" + artifact.id() + ":" + artifact.version() + ":"
            + type + ":" + chunk),
        artifact.tenantId(), artifact.id(), type, attributes);
  }

  private static String sourceType(String location) {
    return location.regionMatches(true, 0, "file:", 0, 5) ? "FILE" : "URL";
  }

  private static String lastSegment(String location) {
    String path = URI.create(location).getPath();
    if (path == null || path.isBlank() || path.endsWith("/")) return "artifact";
    return path.substring(path.lastIndexOf('/') + 1);
  }
}
