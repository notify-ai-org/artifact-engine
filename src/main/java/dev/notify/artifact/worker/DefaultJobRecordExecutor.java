package dev.notify.artifact.worker;

import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.chunk.ChunkedIngestSupport;
import dev.notify.artifact.job.BufferChunkJob;
import dev.notify.artifact.job.IndexChunkJob;
import dev.notify.artifact.job.IndexFinalJob;
import dev.notify.artifact.job.IndexJob;
import dev.notify.artifact.job.ReleaseBufferJob;
import dev.notify.artifact.job.SpoolChunkJob;
import dev.notify.artifact.job.SpoolCommitJob;
import dev.notify.artifact.job.StoreChunkJob;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.job.ReleaseSpoolJob;
import dev.notify.artifact.job.StoreCompleteJob;
import dev.notify.artifact.job.StoreInitJob;
import dev.notify.artifact.job.StoreJob;
import dev.notify.artifact.job.StorePartJob;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.ocr.Ocr;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.spool.SpoolReleaser;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.StorageKeyFactory;
import java.util.Objects;

/** Rebuilds every durable workflow job type from its {@link JobRecord}. */
public final class DefaultJobRecordExecutor implements JobRecordExecutor {
  private final MetadataStore metadata;
  private final ObjectStore objects;
  private final DurableSpool spool;
  private final VectorStore vectors;
  private final EmbeddingService embeddings;
  private final TextExtractorFactory extractors;
  private final Ocr ocr;
  private final Chunker chunker;
  private final IndexJob.Options indexOptions;
  private final MultipartUploadStore multipartUploads;
  private final SpoolReleaser spoolReleaser;
  private final StorageKeyFactory keys;
  private final ChunkedIngestSupport chunkedIngest;
  private final DataVerifier dataVerifier = new DataVerifier();

  /**
   * @param ocr optional; without it, images without extractable text fail indexing
   */
  public DefaultJobRecordExecutor(
      MetadataStore metadata,
      ObjectStore objects,
      DurableSpool spool,
      VectorStore vectors,
      EmbeddingService embeddings,
      TextExtractorFactory extractors,
      Ocr ocr,
      Chunker chunker,
      IndexJob.Options indexOptions,
      MultipartUploadStore multipartUploads,
      SpoolReleaser spoolReleaser,
      StorageKeyFactory keys) {
    this(metadata, objects, spool, vectors, embeddings, extractors, ocr, chunker, indexOptions,
        multipartUploads, spoolReleaser, keys, null);
  }

  /** @param chunkedIngest enables the chunked source-ingest job types; null disables them */
  public DefaultJobRecordExecutor(
      MetadataStore metadata,
      ObjectStore objects,
      DurableSpool spool,
      VectorStore vectors,
      EmbeddingService embeddings,
      TextExtractorFactory extractors,
      Ocr ocr,
      Chunker chunker,
      IndexJob.Options indexOptions,
      MultipartUploadStore multipartUploads,
      SpoolReleaser spoolReleaser,
      StorageKeyFactory keys,
      ChunkedIngestSupport chunkedIngest) {
    this.chunkedIngest = chunkedIngest;
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.objects = Objects.requireNonNull(objects, "objects");
    this.spool = Objects.requireNonNull(spool, "spool");
    this.vectors = Objects.requireNonNull(vectors, "vectors");
    this.embeddings = Objects.requireNonNull(embeddings, "embeddings");
    this.extractors = Objects.requireNonNull(extractors, "extractors");
    this.ocr = ocr;
    this.chunker = Objects.requireNonNull(chunker, "chunker");
    this.indexOptions = Objects.requireNonNull(indexOptions, "indexOptions");
    this.multipartUploads = Objects.requireNonNull(multipartUploads, "multipartUploads");
    this.spoolReleaser = Objects.requireNonNull(spoolReleaser, "spoolReleaser");
    this.keys = Objects.requireNonNull(keys, "keys");
  }

  private ChunkedIngestSupport chunked(JobRecord record) {
    if (chunkedIngest == null) {
      throw new IllegalStateException(
          "Chunked ingest is not configured; cannot run " + record.type());
    }
    return chunkedIngest;
  }

  @Override
  public Job<?> toJob(JobRecord record) {
    return switch (record.type()) {
      case STORE -> new StoreJob(
          record.tenantId(), record.artifactId(), metadata, objects, spool, keys);
      case STORE_INIT -> new StoreInitJob(record, metadata, objects, multipartUploads, keys);
      case STORE_PART -> new StorePartJob(record, metadata, objects, spool, multipartUploads);
      case STORE_COMPLETE -> new StoreCompleteJob(record, metadata, objects, multipartUploads);
      case RELEASE_SPOOL -> new ReleaseSpoolJob(record, spoolReleaser);
      case INDEX -> new IndexJob(
          record.tenantId(), record.artifactId(), metadata, objects, spool, extractors, ocr,
          chunker, embeddings, vectors, indexOptions);
      case BUFFER_CHUNK -> new BufferChunkJob(record, metadata, chunked(record));
      case SPOOL_CHUNK -> new SpoolChunkJob(record, metadata, spool, chunked(record));
      case STORE_CHUNK ->
          new StoreChunkJob(record, metadata, objects, multipartUploads, chunked(record));
      case INDEX_CHUNK -> new IndexChunkJob(
          record, metadata, embeddings, vectors, chunker, indexOptions, chunked(record));
      case RELEASE_BUFFER -> new ReleaseBufferJob(record, chunked(record));
      case SPOOL_COMMIT -> new SpoolCommitJob(record, metadata, spool, dataVerifier);
      case INDEX_FINAL -> new IndexFinalJob(
          record, metadata, embeddings, vectors, chunker, indexOptions, chunked(record),
          () -> new IndexJob(
              record.tenantId(), record.artifactId(), metadata, objects, spool, extractors, ocr,
              chunker, embeddings, vectors, indexOptions));
      case INGEST, FETCH, RETRIEVAL ->
          throw new IllegalArgumentException("No durable executor for job type " + record.type());
    };
  }
}
