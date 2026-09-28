package dev.notify.artifact.util;

import java.io.IOException;

/**
 * Receives extracted text incrementally, in document order. Pieces are concatenated verbatim, so
 * an extractor must emit its own separators (for example a newline after a paragraph or page).
 */
@FunctionalInterface
public interface TextSink {
  void accept(CharSequence text) throws IOException;
}
