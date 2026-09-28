package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.extract.DocxTextExtractor;
import dev.notify.artifact.extract.HtmlTextExtractor;
import dev.notify.artifact.extract.MarkdownTextExtractor;
import dev.notify.artifact.extract.PdfTextExtractor;
import dev.notify.artifact.extract.PlainTextExtractor;
import dev.notify.artifact.extract.TextExtractor;
import dev.notify.artifact.extract.TextExtractorFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.poi.wp.usermodel.HeaderFooterType;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TextExtractorFactoryTest {
  private final TextExtractorFactory factory = TextExtractorFactory.defaults(1_000_000, 10_000);

  @TempDir Path directory;

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
  void streamsPlainTextInBoundedPieces() throws Exception {
    String value = "word ".repeat(5_000);
    List<String> pieces = new ArrayList<>();

    new PlainTextExtractor(1_000_000)
        .extract(file(value.getBytes(StandardCharsets.UTF_8)), text -> pieces.add(text.toString()));

    assertEquals(value, String.join("", pieces));
    assertTrue(pieces.size() > 1);
    assertTrue(pieces.stream().allMatch(piece -> piece.length() <= 8192));
  }

  @Test
  void streamsPdfTextOnePageAtATime() throws Exception {
    Path pdf = pdf("First page text", "Second page text", "Third page text");
    List<String> pieces = new ArrayList<>();

    required("application/pdf").extract(pdf, text -> pieces.add(text.toString()));

    assertEquals(3, pieces.size());
    assertTrue(pieces.get(0).contains("First page text"));
    assertTrue(pieces.get(2).contains("Third page text"));
  }

  @Test
  void streamsDocxPartsInReadingOrder() throws Exception {
    Path docx;
    try (XWPFDocument document = new XWPFDocument()) {
      document.createHeader(HeaderFooterType.DEFAULT).createParagraph().createRun().setText("Header text");
      XWPFRun run = document.createParagraph().createRun();
      run.setText("Before tab");
      run.addTab();
      run.setText("after tab");
      XWPFTable table = document.createTable(1, 2);
      table.getRow(0).getCell(0).setText("Cell one");
      table.getRow(0).getCell(1).setText("Cell two");
      document.createFooter(HeaderFooterType.DEFAULT).createParagraph().createRun().setText("Footer text");
      docx = directory.resolve("doc.docx");
      try (OutputStream output = Files.newOutputStream(docx)) {
        document.write(output);
      }
    }

    String text = required(DocxTextExtractor.MEDIA_TYPE).extract(docx);

    assertTrue(text.contains("Before tab\tafter tab\n"), text);
    assertOrdered(text, "Header text", "Before tab", "Cell one", "Cell two", "Footer text");
  }

  @Test
  void rejectsDocxThatIsNotAZipPackage() throws Exception {
    Path bogus = file("not a docx".getBytes(StandardCharsets.UTF_8));

    assertThrows(IOException.class, () -> required(DocxTextExtractor.MEDIA_TYPE).extract(bogus));
  }

  @Test
  void enforcesCharacterAndByteLimitsWhileStreaming() throws Exception {
    TextExtractorFactory tight = TextExtractorFactory.defaults(64, 20);
    Path longText = file("x ".repeat(50).getBytes(StandardCharsets.UTF_8));
    IOException characters =
        assertThrows(IOException.class, () -> tight.find("text/plain").orElseThrow().extract(longText));
    assertEquals("Extracted text limit exceeded", characters.getMessage());

    Path largePdf = pdf("Too big for the byte limit");
    IOException bytes =
        assertThrows(IOException.class, () -> tight.find("application/pdf").orElseThrow().extract(largePdf));
    assertEquals("Extractor input exceeds the configured byte limit", bytes.getMessage());
  }

  private Path pdf(String... pages) throws IOException {
    Path pdf = Files.createTempFile(directory, "doc", ".pdf");
    try (PDDocument document = new PDDocument()) {
      for (String pageText : pages) {
        PDPage page = new PDPage();
        document.addPage(page);
        try (PDPageContentStream content = new PDPageContentStream(document, page)) {
          content.beginText();
          content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
          content.newLineAtOffset(50, 700);
          content.showText(pageText);
          content.endText();
        }
      }
      document.save(pdf.toFile());
    }
    return pdf;
  }

  private static void assertOrdered(String text, String... fragments) {
    int position = -1;
    for (String fragment : fragments) {
      int next = text.indexOf(fragment);
      assertTrue(next > position, () -> fragment + " out of order in: " + text);
      position = next;
    }
  }

  private String extract(String mediaType, String value) throws Exception {
    return required(mediaType).extract(file(value.getBytes(StandardCharsets.UTF_8)));
  }

  private Path file(byte[] content) throws IOException {
    return Files.write(Files.createTempFile(directory, "content", ".bin"), content);
  }

  private TextExtractor required(String mediaType) {
    return factory.find(mediaType).orElseThrow();
  }
}
