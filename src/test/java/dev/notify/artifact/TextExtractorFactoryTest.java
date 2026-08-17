package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.extract.DocxTextExtractor;
import dev.notify.artifact.extract.HtmlTextExtractor;
import dev.notify.artifact.extract.MarkdownTextExtractor;
import dev.notify.artifact.extract.PdfTextExtractor;
import dev.notify.artifact.extract.PlainTextExtractor;
import dev.notify.artifact.extract.TextExtractor;
import dev.notify.artifact.extract.TextExtractorFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.Test;

class TextExtractorFactoryTest {
  private final TextExtractorFactory factory = TextExtractorFactory.defaults(1_000_000, 10_000);

  @Test
  void routesNormalizedMediaTypes() {
    assertInstanceOf(PlainTextExtractor.class, required("text/plain; charset=UTF-8"));
    assertInstanceOf(MarkdownTextExtractor.class, required("text/markdown"));
    assertInstanceOf(HtmlTextExtractor.class, required("text/html"));
    assertInstanceOf(PdfTextExtractor.class, required("application/pdf"));
    assertInstanceOf(DocxTextExtractor.class, required(DocxTextExtractor.MEDIA_TYPE));
    assertTrue(factory.find("application/octet-stream").isEmpty());
  }

  @Test
  void extractsMarkdownAsPlainText() throws Exception {
    String text = extract("text/markdown", "# Heading\n\nHello **world**.");

    assertTrue(text.contains("Heading"));
    assertTrue(text.contains("Hello world."));
    assertTrue(!text.contains("**"));
  }

  @Test
  void extractsVisibleHtmlText() throws Exception {
    String text = extract("text/html", "<html><body><h1>Heading</h1><p>Hello</p></body></html>");

    assertEquals("Heading Hello", text);
  }

  @Test
  void extractsPdfText() throws Exception {
    byte[] pdf;
    try (PDDocument document = new PDDocument();
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      PDPage page = new PDPage();
      document.addPage(page);
      try (PDPageContentStream content = new PDPageContentStream(document, page)) {
        content.beginText();
        content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
        content.newLineAtOffset(50, 700);
        content.showText("PDF extraction works");
        content.endText();
      }
      document.save(output);
      pdf = output.toByteArray();
    }

    String text = required("application/pdf").extract(new ByteArrayInputStream(pdf));
    assertTrue(text.contains("PDF extraction works"));
  }

  @Test
  void extractsDocxText() throws Exception {
    byte[] docx;
    try (XWPFDocument document = new XWPFDocument();
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      document.createParagraph().createRun().setText("DOCX extraction works");
      document.write(output);
      docx = output.toByteArray();
    }

    String text = required(DocxTextExtractor.MEDIA_TYPE).extract(new ByteArrayInputStream(docx));
    assertTrue(text.contains("DOCX extraction works"));
  }

  private String extract(String mediaType, String value) throws Exception {
    return required(mediaType)
        .extract(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)));
  }

  private TextExtractor required(String mediaType) {
    return factory.find(mediaType).orElseThrow();
  }
}
