package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** Fail-closed OCR adapter used until an OCR provider is explicitly configured. */
public final class DefaultOcr implements Ocr {
  @Override
  public String recognize(InputStream image, String mediaType) throws IOException {
    Objects.requireNonNull(image, "image");
    throw new IOException("OCR provider is not configured");
  }
}
