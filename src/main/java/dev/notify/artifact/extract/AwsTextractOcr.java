package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.Block;
import software.amazon.awssdk.services.textract.model.BlockType;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextRequest;
import software.amazon.awssdk.services.textract.model.Document;

/** Synchronous Amazon Textract OCR for bounded, single-page image content. */
public final class AwsTextractOcr implements Ocr {
  public static final int TEXTRACT_SYNC_MAX_BYTES = 10 * 1024 * 1024;

  private static final Set<String> SUPPORTED_MEDIA_TYPES =
      Set.of("image/jpeg", "image/png", "image/tiff");

  private final TextractClient textract;
  private final int maxInputBytes;
  private final int maxOutputCharacters;

  public AwsTextractOcr(TextractClient textract, int maxInputBytes, int maxOutputCharacters) {
    this.textract = Objects.requireNonNull(textract, "textract");
    if (maxInputBytes < 1 || maxInputBytes > TEXTRACT_SYNC_MAX_BYTES) {
      throw new IllegalArgumentException("maxInputBytes must be between 1 and 10485760");
    }
    if (maxOutputCharacters < 1) {
      throw new IllegalArgumentException("maxOutputCharacters must be positive");
    }
    this.maxInputBytes = maxInputBytes;
    this.maxOutputCharacters = maxOutputCharacters;
  }

  @Override
  public String recognize(InputStream image, String mediaType) throws IOException {
    Objects.requireNonNull(image, "image");
    String normalizedMediaType = normalize(mediaType);
    if (!SUPPORTED_MEDIA_TYPES.contains(normalizedMediaType)) {
      throw new IOException("Amazon Textract OCR does not support " + normalizedMediaType);
    }

    byte[] bytes = image.readNBytes(maxInputBytes + 1);
    if (bytes.length > maxInputBytes) {
      throw new IOException("OCR input exceeds the configured limit");
    }
    if (bytes.length == 0) {
      throw new IOException("OCR input is empty");
    }

    try {
      var response =
          textract.detectDocumentText(
              DetectDocumentTextRequest.builder()
                  .document(Document.builder().bytes(SdkBytes.fromByteArray(bytes)).build())
                  .build());
      StringBuilder text = new StringBuilder();
      for (Block block : response.blocks()) {
        if (block.blockType() != BlockType.LINE || block.text() == null || block.text().isBlank()) {
          continue;
        }
        int separatorLength = text.isEmpty() ? 0 : 1;
        if (text.length() + separatorLength + block.text().length() > maxOutputCharacters) {
          throw new IOException("OCR output exceeds the configured character limit");
        }
        if (separatorLength == 1) {
          text.append('\n');
        }
        text.append(block.text());
      }
      return text.toString();
    } catch (SdkException failure) {
      throw new IOException("Amazon Textract OCR failed", failure);
    }
  }

  private static String normalize(String mediaType) throws IOException {
    if (mediaType == null || mediaType.isBlank()) {
      throw new IOException("OCR media type is required");
    }
    int parameters = mediaType.indexOf(';');
    return (parameters < 0 ? mediaType : mediaType.substring(0, parameters))
        .trim()
        .toLowerCase(Locale.ROOT);
  }
}
