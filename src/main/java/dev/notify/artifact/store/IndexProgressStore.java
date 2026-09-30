package dev.notify.artifact.store;

import dev.notify.artifact.util.Chunker;
import java.util.Objects;
import java.util.Optional;

/**
 * State carried between the per-chunk index jobs of a chunked ingest: chunk {@code i+1} resumes the
 * chunker and UTF-8 decoder exactly where chunk {@code i} stopped. Saved at the end of each chunk
 * job, after its embeddings are committed, so a retried job restarts from the previous chunk's
 * state and deterministic chunk ids make the replay free.
 */
public interface IndexProgressStore {
  Optional<IndexProgress> find(String tenantId, String artifactId, long version);

  /** Inserts or replaces the progress for the artifact version. */
  void save(IndexProgress progress);

  void delete(String tenantId, String artifactId, long version);

  /**
   * @param lastChunk the last source chunk (1-based) whose text has been committed
   * @param utf8Carry base64 of the bytes of a character split across the chunk boundary
   * @param truncated the chunk budget was reached under the TRUNCATE policy; later chunks are only
   *     hashed, not indexed
   * @param contentDigest {@link dev.notify.artifact.util.ResumableSha256} state over every source
   *     byte indexed so far; saved together with the chunker state so a retry never hashes twice
   */
  record IndexProgress(
      String tenantId,
      String artifactId,
      long version,
      int lastChunk,
      Chunker.State chunker,
      String utf8Carry,
      boolean truncated,
      int embedded,
      int reused,
      String contentDigest) {
    public IndexProgress {
      Objects.requireNonNull(tenantId, "tenantId");
      Objects.requireNonNull(artifactId, "artifactId");
      Objects.requireNonNull(chunker, "chunker");
      utf8Carry = utf8Carry == null ? "" : utf8Carry;
      contentDigest = contentDigest == null ? "" : contentDigest;
    }

    public static IndexProgress start(String tenantId, String artifactId, long version) {
      return new IndexProgress(
          tenantId, artifactId, version, 0, Chunker.State.INITIAL, "", false, 0, 0, "");
    }
  }
}
