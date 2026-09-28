package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.openxml4j.opc.PackageRelationship;
import org.apache.poi.openxml4j.opc.PackageRelationshipTypes;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xwpf.usermodel.XWPFRelation;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import dev.notify.artifact.util.TextSink;

/**
 * Streams text from an Office Open XML Word document. The package is read from disk and each XML
 * part is SAX-parsed, so memory stays flat regardless of document length.
 */
public final class DocxTextExtractor implements TextExtractor {
  public static final String MEDIA_TYPE =
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

  private static final Set<String> WORD_NAMESPACES =
      Set.of(
          "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
          "http://purl.oclc.org/ooxml/wordprocessingml/main");
  private static final int FLUSH_CHARACTERS = 8192;

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
  public void extract(Path file, TextSink sink) throws IOException {
    BoundedContent.requireSize(file, maxInputBytes);
    TextSink bounded = BoundedContent.limit(sink, maxCharacters);
    OPCPackage pkg;
    try {
      pkg = OPCPackage.open(file.toFile(), PackageAccess.READ);
    } catch (InvalidFormatException | RuntimeException invalid) {
      throw new IOException("Invalid DOCX package", invalid);
    }
    try {
      PackagePart document = mainDocument(pkg);
      // Same reading order as XWPFWordExtractor: headers, body, notes, footers.
      for (PackagePart part : related(document, XWPFRelation.HEADER)) parse(part, bounded);
      parse(document, bounded);
      for (PackagePart part : related(document, XWPFRelation.FOOTNOTE)) parse(part, bounded);
      for (PackagePart part : related(document, XWPFRelation.ENDNOTE)) parse(part, bounded);
      for (PackagePart part : related(document, XWPFRelation.FOOTER)) parse(part, bounded);
    } catch (InvalidFormatException invalid) {
      throw new IOException("Invalid DOCX package", invalid);
    } finally {
      pkg.revert();
    }
  }

  private static PackagePart mainDocument(OPCPackage pkg) throws IOException {
    for (String type :
        List.of(
            PackageRelationshipTypes.CORE_DOCUMENT,
            PackageRelationshipTypes.STRICT_CORE_DOCUMENT)) {
      List<PackagePart> parts = pkg.getPartsByRelationshipType(type);
      if (!parts.isEmpty()) return parts.get(0);
    }
    throw new IOException("DOCX package has no main document part");
  }

  private static List<PackagePart> related(PackagePart document, XWPFRelation relation)
      throws InvalidFormatException {
    List<PackagePart> parts = new ArrayList<>();
    for (PackageRelationship relationship :
        document.getRelationshipsByType(relation.getRelation())) {
      PackagePart part = document.getRelatedPart(relationship);
      if (part != null) parts.add(part);
    }
    return parts;
  }

  private static void parse(PackagePart part, TextSink sink) throws IOException {
    WordTextHandler handler = new WordTextHandler(sink);
    try (InputStream xml = part.getInputStream()) {
      XMLReader reader = XMLHelper.newXMLReader();
      reader.setContentHandler(handler);
      reader.parse(new InputSource(xml));
      handler.flush();
    } catch (SinkFailure failure) {
      throw failure.cause;
    } catch (SAXException | javax.xml.parsers.ParserConfigurationException invalid) {
      throw new IOException("Invalid DOCX XML part " + part.getPartName(), invalid);
    }
  }

  /** Collects run text; emits at paragraph ends or when the buffer fills. */
  private static final class WordTextHandler extends DefaultHandler {
    private final TextSink sink;
    private final StringBuilder buffer = new StringBuilder();
    private boolean inText;

    private WordTextHandler(TextSink sink) {
      this.sink = sink;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
        throws SAXException {
      if (!WORD_NAMESPACES.contains(uri)) return;
      switch (localName) {
        case "t" -> inText = true;
        case "tab" -> append("\t");
        case "br", "cr" -> append("\n");
        default -> {}
      }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      if (!WORD_NAMESPACES.contains(uri)) return;
      if ("t".equals(localName)) inText = false;
      else if ("p".equals(localName)) {
        buffer.append('\n');
        flushOrWrap();
      }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      if (!inText) return;
      buffer.append(ch, start, length);
      if (buffer.length() >= FLUSH_CHARACTERS) flushOrWrap();
    }

    private void append(String text) throws SAXException {
      buffer.append(text);
      if (buffer.length() >= FLUSH_CHARACTERS) flushOrWrap();
    }

    void flush() throws IOException {
      if (buffer.isEmpty()) return;
      String text = buffer.toString();
      buffer.setLength(0);
      sink.accept(text);
    }

    private void flushOrWrap() throws SAXException {
      try {
        flush();
      } catch (IOException failure) {
        throw new SinkFailure(failure);
      }
    }
  }

  /** Carries a sink failure (limit exceeded, embedding error) through the SAX parser unchanged. */
  private static final class SinkFailure extends SAXException {
    private final IOException cause;

    private SinkFailure(IOException cause) {
      super(cause);
      this.cause = cause;
    }
  }

  private static int positive(int value, String name) {
    if (value < 1) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
