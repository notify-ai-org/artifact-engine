package dev.notify.artifact.extract;

import java.io.IOException;
import java.io.InputStream;

public interface TextExtractor {
  boolean supports(String mediaType);

  String extract(InputStream content) throws IOException;
}
