package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.auth.DataVerifier;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataVerifierTest {
  @TempDir Path directory;

  @Test
  void detectsWebpFromRiffContainerSignature() throws Exception {
    Path webp = directory.resolve("upload.bin");
    Files.write(
        webp, new byte[] {'R', 'I', 'F', 'F', 4, 0, 0, 0, 'W', 'E', 'B', 'P', 'V', 'P', '8', ' '});

    assertEquals("image/webp", new DataVerifier().verify(webp, "image/webp"));
  }

  @Test
  void rejectsDeclaredTypeThatConflictsWithMagicBytes() throws Exception {
    Path pdf = directory.resolve("not-an-image");
    Files.write(pdf, "%PDF-1.7".getBytes(java.nio.charset.StandardCharsets.US_ASCII));

    assertThrows(IllegalArgumentException.class, () -> new DataVerifier().verify(pdf, "image/png"));
  }
}
