package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public final class PlainTextExtractor implements TextExtractor {
  private final int maxCharacters;

  public PlainTextExtractor(int maxCharacters) {
    this.maxCharacters = maxCharacters;
  }

  public boolean supports(String type) {
    return type.startsWith("text/");
  }

  public String extract(InputStream content) throws IOException {
    StringBuilder result = new StringBuilder();
    try (Reader reader = new InputStreamReader(content, StandardCharsets.UTF_8)) {
      char[] buffer = new char[8192];
      for (int n; (n = reader.read(buffer)) >= 0; ) {
        if (result.length() + n > maxCharacters)
          throw new IOException("Extracted text limit exceeded");
        result.append(buffer, 0, n);
      }
    }
    return result.toString();
  }
}
