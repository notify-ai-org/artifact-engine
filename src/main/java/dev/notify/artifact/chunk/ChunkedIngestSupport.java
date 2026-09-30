package dev.notify.artifact.chunk;

import dev.notify.artifact.store.IndexProgressStore;
import java.time.Duration;
import java.util.Objects;

/**
 * Collaborators of a chunked source ingest.
 *
 * @param chunkBytes chunk (and multipart part) size; at most the pool's slab size and at least
 *     5 MiB, S3's minimum for all but the last part
 * @param bufferWait how long a chunk job waits for a free buffer before failing (retryably)
 */
public record ChunkedIngestSupport(
    ChunkBufferPool pool,
    SourcePolicy sources,
    IndexProgressStore progress,
    int chunkBytes,
    Duration bufferWait) {
  public ChunkedIngestSupport {
    Objects.requireNonNull(pool, "pool");
    Objects.requireNonNull(sources, "sources");
    Objects.requireNonNull(progress, "progress");
    Objects.requireNonNull(bufferWait, "bufferWait");
    if (chunkBytes < 1 || chunkBytes > pool.stats().slabBytes()) {
      throw new IllegalArgumentException("chunkBytes must be positive and fit a buffer slab");
    }
  }
}
