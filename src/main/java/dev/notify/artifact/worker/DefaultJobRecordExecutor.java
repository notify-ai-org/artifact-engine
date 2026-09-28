package dev.notify.artifact.worker;

import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.job.IndexJob;
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
      case INGEST, FETCH, RETRIEVAL ->
          throw new IllegalArgumentException("No durable executor for job type " + record.type());
    };
  }
}
