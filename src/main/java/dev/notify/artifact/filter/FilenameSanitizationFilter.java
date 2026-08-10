package dev.notify.artifact.security;

import java.text.Normalizer;

/**
 * Produces a display-only filename with path, control, and bidirectional override characters
 * removed.
 */
public final class FilenameSanitizationFilter implements SecurityFilter<IngestionSecurityContext> {
  private static final int MAX_FILENAME_CHARACTERS = 255;

  @Override
  public String name() {
    return "filename-sanitization";
  }

  @Override
  public void verify(IngestionSecurityContext context) {
    String original = context.request().originalName();
    if (original == null || original.isBlank()) {
      context.sanitizedFilename("artifact");
      return;
    }
    String normalized = Normalizer.normalize(original, Normalizer.Form.NFKC);
    String sanitized =
        normalized
            .replaceAll("[\\p{Cc}\\p{Cf}]", "_")
            .replace('/', '_')
            .replace('\\', '_')
            .replaceAll("\\.{2,}", "_")
            .strip();
    if (sanitized.isBlank() || sanitized.equals(".") || sanitized.equals("..")) {
      sanitized = "artifact";
    }
    int end =
        sanitized.offsetByCodePoints(
            0, Math.min(MAX_FILENAME_CHARACTERS, sanitized.codePointCount(0, sanitized.length())));
    context.sanitizedFilename(sanitized.substring(0, end));
  }
}
