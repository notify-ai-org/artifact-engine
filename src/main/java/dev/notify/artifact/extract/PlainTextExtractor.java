package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import dev.notify.artifact.util.TextSink;

public final class PlainTextExtractor implements TextExtractor {
  private final int maxCharacters;

  public PlainTextExtractor(int maxCharacters) {
    this.maxCharacters = maxCharacters;
  }

  public boolean supports(String type) {
    return type != null
        && type.startsWith("text/")
        && !"text/markdown".equalsIgnoreCase(type)
        && !"text/html".equalsIgnoreCase(type);
  }

  public void extract(Path file, TextSink sink) throws IOException {
    TextSink bounded = BoundedContent.limit(sink, maxCharacters);
    try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      char[] buffer = new char[8192];
      for (int n; (n = reader.read(buffer)) >= 0; ) {
        if (n > 0) bounded.accept(CharBuffer.wrap(buffer, 0, n));
      }
    }
  }
}
