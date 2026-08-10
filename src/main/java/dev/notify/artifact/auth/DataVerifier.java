package dev.notify.artifact.auth;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.zip.ZipFile;

/** Detects supported content from bytes and rejects conflicting caller-supplied MIME types. */
public final class DataVerifier {
  public static final String DOCX =
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

  private static final int SNIFF_BYTES = 8 * 1024;
  private static final Set<String> TEXT_TYPES = Set.of("text/plain", "text/markdown", "text/html");

  public String verify(Path path, String declaredMediaType) throws IOException {
    String normalizedDeclaration = normalize(declaredMediaType);
    byte[] prefix = readPrefix(path);
    String detectedMediaType = detect(path, prefix, normalizedDeclaration);

    if (normalizedDeclaration != null && !compatible(normalizedDeclaration, detectedMediaType)) {
      throw new IllegalArgumentException(
          "Declared media type "
              + normalizedDeclaration
              + " does not match detected type "
              + detectedMediaType);
    }
    return detectedMediaType;
  }

  private static String detect(Path path, byte[] prefix, String declaredMediaType)
      throws IOException {
    if (startsWith(prefix, 0, 0x89, 'P', 'N', 'G')) {
      return "image/png";
    }
    if (startsWith(prefix, 0, 0xff, 0xd8, 0xff)) {
      return "image/jpeg";
    }
    if (startsWith(prefix, 0, 'R', 'I', 'F', 'F') && startsWith(prefix, 8, 'W', 'E', 'B', 'P')) {
      return "image/webp";
    }
    if (startsWith(prefix, 0, '%', 'P', 'D', 'F')) {
      return "application/pdf";
    }
    if (startsWith(prefix, 0, 'P', 'K', 0x03, 0x04) && isDocx(path)) {
      return DOCX;
    }
    if (isUtf8Text(prefix)) {
      return detectTextType(prefix, declaredMediaType);
    }
    throw new IllegalArgumentException("Unsupported or unrecognized artifact content");
  }

  private static String detectTextType(byte[] prefix, String declaredMediaType) {
    String sample =
        new String(prefix, StandardCharsets.UTF_8).stripLeading().toLowerCase(Locale.ROOT);
    if (sample.startsWith("<!doctype html") || sample.startsWith("<html")) {
      return "text/html";
    }
    if ("text/markdown".equals(declaredMediaType)) {
      return "text/markdown";
    }
    return "text/plain";
  }

  private static boolean isDocx(Path path) throws IOException {
    try (ZipFile zip = new ZipFile(path.toFile())) {
      return zip.getEntry("[Content_Types].xml") != null
          && zip.getEntry("word/document.xml") != null;
    } catch (java.util.zip.ZipException notAZip) {
      return false;
    }
  }

  private static boolean isUtf8Text(byte[] bytes) {
    for (byte value : bytes) {
      if (value == 0) {
        return false;
      }
    }
    try {
      StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes));
      return true;
    } catch (java.nio.charset.CharacterCodingException invalidUtf8) {
      return false;
    }
  }

  private static boolean compatible(String declared, String detected) {
    if (declared.equals(detected)) {
      return true;
    }
    return TEXT_TYPES.contains(declared)
        && TEXT_TYPES.contains(detected)
        && !"text/html".equals(declared);
  }

  private static byte[] readPrefix(Path path) throws IOException {
    try (var input = Files.newInputStream(path)) {
      return input.readNBytes(SNIFF_BYTES);
    }
  }

  private static boolean startsWith(byte[] data, int offset, int... signature) {
    if (data.length < offset + signature.length) {
      return false;
    }
    for (int index = 0; index < signature.length; index++) {
      if (Byte.toUnsignedInt(data[offset + index]) != signature[index]) {
        return false;
      }
    }
    return true;
  }

  private static String normalize(String mediaType) {
    if (mediaType == null || mediaType.isBlank()) {
      return null;
    }
    return mediaType.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
  }
}
