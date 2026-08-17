package dev.notify.artifact.extract;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;

/** Extracts text from a bounded Office Open XML Word document. */
public final class DocxTextExtractor implements TextExtractor {
  public static final String MEDIA_TYPE =
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

  private final int maxInputBytes;
  private final int maxCharacters;

  public DocxTextExtractor(int maxInputBytes, int maxCharacters) {
    this.maxInputBytes = positive(maxInputBytes, "maxInputBytes");
    this.maxCharacters = positive(maxCharacters, "maxCharacters");
  }

  @Override
  public boolean supports(String mediaType) {
    return MEDIA_TYPE.equalsIgnoreCase(mediaType);
  }

  @Override
  public String extract(InputStream content) throws IOException {
    byte[] docx = BoundedContent.bytes(content, maxInputBytes);
    try (XWPFDocument document = new XWPFDocument(new ByteArrayInputStream(docx));
        XWPFWordExtractor extractor = new XWPFWordExtractor(document)) {
      return BoundedContent.text(extractor.getText(), maxCharacters);
    }
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
