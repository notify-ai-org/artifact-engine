package dev.notify.artifact.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Content that can be read in independent byte ranges, so chunks are fetched by separate jobs. */
public interface ChunkSource {
  /** Length and version of the content; called once when the ingest is planned. */
  SourceInfo describe() throws IOException;

  /**
   * Fills {@code target.remaining()} bytes starting at {@code offset}. Fails if the content no
   * longer matches {@code expectedVersion} (when the source can tell), so a changed file is never
   * stitched together from two versions.
   */
  void read(long offset, ByteBuffer target, String expectedVersion) throws IOException;

  /**
   * @param version an opaque token (ETag, last-modified + length) identifying this content
   */
  record SourceInfo(long length, String version) {
    public SourceInfo {
      if (length < 0) throw new IllegalArgumentException("length cannot be negative");
    }
  }

  /** The content changed between planning and reading a chunk. */
  final class SourceChangedException extends IOException {
    public SourceChangedException(String message) {
      super(message);
    }
  }
}
