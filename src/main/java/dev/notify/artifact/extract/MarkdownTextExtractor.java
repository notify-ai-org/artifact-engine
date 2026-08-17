package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.text.TextContentRenderer;

/** Converts CommonMark content to readable plain text. */
public final class MarkdownTextExtractor implements TextExtractor {
  private static final Parser PARSER = Parser.builder().build();
  private static final TextContentRenderer RENDERER = TextContentRenderer.builder().build();

  private final int maxInputBytes;
  private final int maxCharacters;

  public MarkdownTextExtractor(int maxInputBytes, int maxCharacters) {
    this.maxInputBytes = positive(maxInputBytes, "maxInputBytes");
    this.maxCharacters = positive(maxCharacters, "maxCharacters");
  }

  @Override
  public boolean supports(String mediaType) {
    return "text/markdown".equalsIgnoreCase(mediaType);
  }

  @Override
  public String extract(InputStream content) throws IOException {
    String markdown = BoundedContent.utf8(content, maxInputBytes);
    return BoundedContent.text(RENDERER.render(PARSER.parse(markdown)), maxCharacters);
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
