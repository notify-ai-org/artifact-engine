package dev.notify.artifact.extract;

import java.io.IOException;
import java.nio.file.Path;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.text.TextContentRenderer;

import dev.notify.artifact.util.TextSink;

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
  public void extract(Path file, TextSink sink) throws IOException {
    // CommonMark needs the whole AST; the byte bound keeps that in check.
    String markdown = BoundedContent.utf8(file, maxInputBytes);
    BoundedContent.limit(sink, maxCharacters).accept(RENDERER.render(PARSER.parse(markdown)));
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
