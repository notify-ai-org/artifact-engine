package dev.notify.artifact.auth;

import dev.notify.artifact.model.Artifact;
import java.io.IOException;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.Objects;
import org.jsoup.Jsoup;

/** Single authentication, authorization, and artifact-verification boundary. */
public final class ArtifactAccessVerifier {
  private static final int MAX_FILENAME_CHARACTERS = 255;

  private final AuthorizationService authorization;
  private final DataVerifier dataVerifier;

  public ArtifactAccessVerifier(
      AuthorizationService authorization, DataVerifier dataVerifier) {
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.dataVerifier = Objects.requireNonNull(dataVerifier, "dataVerifier");
  }

  public void authenticate(
      String principalId, String tenantId, AuthorizationService.Permission permission) {
    requireText(principalId, "principalId");
    requireText(tenantId, "tenantId");
    authorization.require(principalId, tenantId, Objects.requireNonNull(permission, "permission"));
  }

  public VerifiedIngestion verifyIngestion(
      String principalId,
      String tenantId,
      Path contentPath,
      String declaredMediaType,
      String originalName)
      throws IOException {
    authenticate(principalId, tenantId, AuthorizationService.Permission.INGEST);
    String detectedMediaType = dataVerifier.verify(contentPath, declaredMediaType);
    return new VerifiedIngestion(detectedMediaType, sanitizeFilename(originalName));
  }

  public Artifact verifyArtifact(String tenantId, Artifact artifact) {
    Objects.requireNonNull(artifact, "artifact");
    if (!tenantId.equals(artifact.tenantId())) {
      throw new SecurityException("Artifact does not belong to the authenticated tenant");
    }
    return artifact;
  }

  public String verifyExtractedContent(Artifact artifact, String content) {
    if (content == null || !"text/html".equalsIgnoreCase(artifact.mediaType())) {
      return content;
    }
    return Jsoup.parse(content).text();
  }

  private static String sanitizeFilename(String originalName) {
    if (originalName == null || originalName.isBlank()) {
      return "artifact";
    }
    String sanitized =
        Normalizer.normalize(originalName, Normalizer.Form.NFKC)
            .replaceAll("[\\p{Cc}\\p{Cf}]", "_")
            .replace('/', '_')
            .replace('\\', '_')
            .replaceAll("\\.{2,}", "_")
            .strip();
    if (sanitized.isBlank() || sanitized.equals(".") || sanitized.equals("..")) {
      return "artifact";
    }
    int codePoints = Math.min(MAX_FILENAME_CHARACTERS, sanitized.codePointCount(0, sanitized.length()));
    return sanitized.substring(0, sanitized.offsetByCodePoints(0, codePoints));
  }

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new SecurityException(field + " is required");
    }
  }

  public record VerifiedIngestion(String detectedMediaType, String sanitizedFilename) {}
}
