package dev.notify.artifact.extract;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/** Selects the native text extractor for a normalized artifact media type. */
public final class TextExtractorFactory {
  private final List<TextExtractor> extractors;

  public TextExtractorFactory(List<TextExtractor> extractors) {
    this.extractors = List.copyOf(Objects.requireNonNull(extractors, "extractors"));
    if (this.extractors.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("extractors must not contain null");
    }
  }

  public static TextExtractorFactory defaults(int maxInputBytes, int maxCharacters) {
    return new TextExtractorFactory(
        List.of(
            new PdfTextExtractor(maxInputBytes, maxCharacters),
            new DocxTextExtractor(maxInputBytes, maxCharacters),
            new MarkdownTextExtractor(maxInputBytes, maxCharacters),
            new HtmlTextExtractor(maxInputBytes, maxCharacters),
            new PlainTextExtractor(maxCharacters)));
  }

  public Optional<TextExtractor> find(String mediaType) {
    String normalized = normalize(mediaType);
    return extractors.stream().filter(extractor -> extractor.supports(normalized)).findFirst();
  }

  private static String normalize(String mediaType) {
    if (mediaType == null) return "";
    int parameters = mediaType.indexOf(';');
    return (parameters < 0 ? mediaType : mediaType.substring(0, parameters))
        .trim()
        .toLowerCase(Locale.ROOT);
  }
}
