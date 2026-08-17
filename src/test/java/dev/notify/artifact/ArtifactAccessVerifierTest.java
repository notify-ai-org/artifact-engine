package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtifactAccessVerifierTest {
  @TempDir Path directory;

  @Test
  void authenticatesAndVerifiesIngestionDirectly() throws Exception {
    Path content = directory.resolve("content.txt");
    Files.writeString(content, "hello");
    ArtifactAccessVerifier verifier =
        new ArtifactAccessVerifier((principal, tenant, permission) -> {}, new DataVerifier());

    ArtifactAccessVerifier.VerifiedIngestion verified =
        verifier.verifyIngestion(
            "principal-a", "tenant-a", content, "text/plain", "../unsafe.txt");

    assertEquals("text/plain", verified.detectedMediaType());
    assertEquals("__unsafe.txt", verified.sanitizedFilename());
  }

  @Test
  void rejectsWhenAuthenticationLayerDeniesPermission() throws Exception {
    Path content = directory.resolve("content.txt");
    Files.writeString(content, "hello");
    ArtifactAccessVerifier verifier =
        new ArtifactAccessVerifier(
            (principal, tenant, permission) -> {
              throw new SecurityException("denied");
            },
            new DataVerifier());

    assertThrows(
        SecurityException.class,
        () ->
            verifier.verifyIngestion(
                "principal-a", "tenant-a", content, "text/plain", "notes.txt"));
  }

  @Test
  void rejectsMissingAuthenticationCoordinates() {
    ArtifactAccessVerifier verifier =
        new ArtifactAccessVerifier((principal, tenant, permission) -> {}, new DataVerifier());

    assertThrows(
        SecurityException.class,
        () -> verifier.authenticate("", "tenant-a", AuthorizationService.Permission.SEARCH));
  }
}
