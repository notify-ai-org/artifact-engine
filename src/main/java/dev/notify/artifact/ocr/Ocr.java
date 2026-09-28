package dev.notify.artifact.ocr;

import java.io.IOException;
import java.io.InputStream;

@FunctionalInterface
public interface Ocr {
  String recognize(InputStream image, String mediaType) throws IOException;
}
