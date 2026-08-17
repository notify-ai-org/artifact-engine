package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import org.jsoup.Jsoup;

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
  public String extract(InputStream content) throws IOException {
    String visibleText = Jsoup.parse(BoundedContent.utf8(content, maxInputBytes)).text();
    return BoundedContent.text(visibleText, maxCharacters);
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
