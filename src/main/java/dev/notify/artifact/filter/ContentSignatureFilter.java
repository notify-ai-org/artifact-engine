package dev.notify.artifact.security;

import dev.notify.artifact.auth.DataVerifier;
import java.io.IOException;
import java.util.Objects;

/** Detects the actual MIME type from bytes and rejects conflicts with the declared type. */
public final class ContentSignatureFilter implements SecurityFilter<IngestionSecurityContext> {
  private final DataVerifier verifier;

  public ContentSignatureFilter(DataVerifier verifier) {
    this.verifier = Objects.requireNonNull(verifier, "verifier");
  }

  @Override
  public String name() {
    return "content-signature";
  }

  @Override
  public void verify(IngestionSecurityContext context) {
    try {
      context.detectedMediaType(
          verifier.verify(context.contentPath(), context.request().declaredMediaType()));
    } catch (IOException | IllegalArgumentException failure) {
      throw new SecurityFilterException(
          "CONTENT_SIGNATURE_REJECTED", name(), "Artifact content type is invalid", failure);
    }
  }
}
