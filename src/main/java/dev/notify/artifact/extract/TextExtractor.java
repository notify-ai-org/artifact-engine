package dev.notify.artifact.extract;

import java.io.IOException;
import java.nio.file.Path;

import dev.notify.artifact.util.TextSink;

/**
 * Extracts text from a local file. Implementations push text to the sink as they read and must not
 * buffer the whole document unless the format requires it (and then only up to a byte bound).
 */
public interface TextExtractor {
  boolean supports(String mediaType);

  void extract(Path file, TextSink sink) throws IOException;

  /** Convenience for small inputs: collects the extracted text into one string. */
  default String extract(Path file) throws IOException {
    StringBuilder text = new StringBuilder();
    extract(file, text::append);
    return text.toString();
  }
}
