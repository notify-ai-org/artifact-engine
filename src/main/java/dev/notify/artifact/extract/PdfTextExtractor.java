package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

/** Extracts embedded text from a bounded PDF document. */
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
  public String extract(InputStream content) throws IOException {
    byte[] pdf = BoundedContent.bytes(content, maxInputBytes);
    try (PDDocument document = Loader.loadPDF(pdf)) {
      return BoundedContent.text(new PDFTextStripper().getText(document), maxCharacters);
    }
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
