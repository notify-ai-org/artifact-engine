package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import dev.notify.artifact.util.TextSink;

final class BoundedContent {
  private BoundedContent() {}

  static void requireSize(Path file, long maxBytes) throws IOException {
    if (Files.size(file) > maxBytes) {
      throw new IOException("Extractor input exceeds the configured byte limit");
    }
  }

  /** Reads a whole file for formats whose parsers need the full input in memory. */
  static String utf8(Path file, int maxBytes) throws IOException {
    requireSize(file, maxBytes);
    try (InputStream input = Files.newInputStream(file)) {
      byte[] content = input.readNBytes(maxBytes);
      if (input.read() >= 0) {
        throw new IOException("Extractor input exceeds the configured byte limit");
      }
      return new String(content, StandardCharsets.UTF_8);
    }
  }

  /** Wraps a sink so the total extracted text cannot exceed {@code maxCharacters}. */
  static TextSink limit(TextSink sink, int maxCharacters) {
    return new TextSink() {
      private long emitted;

      @Override
      public void accept(CharSequence text) throws IOException {
        emitted += text.length();
        if (emitted > maxCharacters) {
          throw new IOException("Extracted text limit exceeded");
        }
        sink.accept(text);
      }
    };
  }
}
