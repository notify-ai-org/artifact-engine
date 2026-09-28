package dev.notify.artifact.extract;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.IOUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import dev.notify.artifact.util.TextSink;

/**
 * Streams embedded PDF text page by page. The document is read from disk with a temp-file-only
 * scratch cache, so heap use is bounded by the largest page rather than the file size.
 */
public final class PdfTextExtractor implements TextExtractor {
  private final int maxInputBytes;
  private final int maxCharacters;

  public PdfTextExtractor(int maxInputBytes, int maxCharacters) {
    this.maxInputBytes = positive(maxInputBytes, "maxInputBytes");
    this.maxCharacters = positive(maxCharacters, "maxCharacters");
  }

  @Override
  public boolean supports(String mediaType) {
    return "application/pdf".equalsIgnoreCase(mediaType);
  }

  @Override
  public void extract(Path file, TextSink sink) throws IOException {
    BoundedContent.requireSize(file, maxInputBytes);
    TextSink bounded = BoundedContent.limit(sink, maxCharacters);
    try (PDDocument document =
        Loader.loadPDF(file.toFile(), IOUtils.createTempFileOnlyStreamCache())) {
      PDFTextStripper stripper = new PDFTextStripper();
      for (int page = 1, pages = document.getNumberOfPages(); page <= pages; page++) {
        stripper.setStartPage(page);
        stripper.setEndPage(page);
        String text = stripper.getText(document);
        if (!text.isEmpty()) bounded.accept(text);
      }
    }
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
