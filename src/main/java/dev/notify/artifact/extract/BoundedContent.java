package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

final class BoundedContent {
  private BoundedContent() {}

  static byte[] bytes(InputStream input, int maxBytes) throws IOException {
    byte[] content = input.readNBytes(maxBytes);
    if (input.read() >= 0) {
      throw new IOException("Extractor input exceeds the configured byte limit");
    }
    return content;
  }

  static String utf8(InputStream input, int maxBytes) throws IOException {
    return new String(bytes(input, maxBytes), StandardCharsets.UTF_8);
  }

  static String text(String value, int maxCharacters) throws IOException {
    if (value.length() > maxCharacters) {
      throw new IOException("Extracted text limit exceeded");
    }
    return value;
  }
}
