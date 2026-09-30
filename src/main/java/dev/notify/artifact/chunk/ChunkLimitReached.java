package dev.notify.artifact.chunk;

import java.io.IOException;

/**
 * Internal stop signal raised when a document reaches its chunk budget. An IOException so it
 * passes unchanged through extractors, including SAX-based ones that only tunnel checked sink
 * failures.
 */
public final class ChunkLimitReached extends IOException {
  ChunkLimitReached() {
    super("chunk budget reached");
  }
}
