package dev.notify.artifact.extract;

import java.io.IOException;
import java.nio.file.Path;
import org.jsoup.Jsoup;

import dev.notify.artifact.util.TextSink;

/** Extracts visible text from UTF-8 HTML without retaining markup or executable content. */
public final class HtmlTextExtractor implements TextExtractor {
  private final int maxInputBytes;
  private final int maxCharacters;

  public HtmlTextExtractor(int maxInputBytes, int maxCharacters) {
    this.maxInputBytes = positive(maxInputBytes, "maxInputBytes");
    this.maxCharacters = positive(maxCharacters, "maxCharacters");
  }

  @Override
  public boolean supports(String mediaType) {
    return "text/html".equalsIgnoreCase(mediaType);
  }

  @Override
  public void extract(Path file, TextSink sink) throws IOException {
    // jsoup builds a full DOM; the byte bound keeps that proportional to a sane HTML page.
    String visibleText = Jsoup.parse(BoundedContent.utf8(file, maxInputBytes)).text();
    BoundedContent.limit(sink, maxCharacters).accept(visibleText);
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
