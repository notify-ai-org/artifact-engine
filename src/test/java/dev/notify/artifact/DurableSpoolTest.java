package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.util.Checksum;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableSpoolTest {
  @TempDir Path root;

  @Test
  void streamsAndPublishesContentWithChecksum() throws Exception {
    var spool = new DurableSpool(root, 100, new ObjectMapper());
    var result =
        spool.write(
            "tenant/../../x",
            "generated-id",
            new ByteArrayInputStream("hello".getBytes()),
            Map.of("name", "x"));
    assertEquals(5, result.sizeBytes());
    assertTrue(Files.exists(result.contentPath()));
    assertTrue(result.contentPath().startsWith(root));
    assertFalse(result.contentPath().toString().contains("tenant/../../x"));
  }

  @Test
  void enforcesLimitAndRemovesTemporaryFile() throws Exception {
    var spool = new DurableSpool(root, 2, new ObjectMapper());
    assertThrows(
        java.io.IOException.class,
        () -> spool.write("t", "a", new ByteArrayInputStream("long".getBytes()), Map.of()));
    try (var files = Files.walk(root)) {
      assertFalse(files.anyMatch(p -> p.toString().endsWith(".tmp")));
    }
  }

  @Test
  void checksumIsComputedWhileCopyingAcrossManyBuffers() throws Exception {
    byte[] content = new byte[300_000];
    new java.util.Random(7).nextBytes(content);
    var spool = new DurableSpool(root, 1 << 20, new ObjectMapper());

    var result = spool.write("t", "a", trickle(content, 10_000, null), Map.of());

    assertEquals(Checksum.sha256(new ByteArrayInputStream(content)), result.sha256());
    assertEquals(content.length, result.sizeBytes());
  }

  @Test
  void releasesEveryReservedByteWhenTheStreamFailsMidway() throws Exception {
    var spool = new DurableSpool(root, 1 << 20, new ObjectMapper());
    IOException disconnect = new IOException("client disconnected");

    assertThrows(
        IOException.class,
        () -> spool.write("t", "a", trickle(new byte[200_000], 10_000, disconnect), Map.of()));

    assertEquals(0, spool.usage().bytes());
    assertEquals(0, spool.usage().files());
  }

  @Test
  void releasesEveryReservedByteWhenTheLimitIsCrossedMidway() throws Exception {
    var spool = new DurableSpool(root, 100_000, new ObjectMapper());

    assertThrows(
        DurableSpool.SpoolQuotaExceededException.class,
        () -> spool.write("t", "a", trickle(new byte[200_000], 10_000, null), Map.of()));

    assertEquals(0, spool.usage().bytes());
    assertEquals(0, spool.usage().files());
  }

  /** Delivers content in small reads, optionally failing once it is exhausted. */
  private static InputStream trickle(byte[] content, int chunk, IOException failAtEnd) {
    return new InputStream() {
      private int position;

      @Override
      public int read() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] target, int offset, int length) throws IOException {
        if (position == content.length) {
          if (failAtEnd != null) throw failAtEnd;
          return -1;
        }
        int count = Math.min(Math.min(length, chunk), content.length - position);
        System.arraycopy(content, position, target, offset, count);
        position += count;
        return count;
      }
    };
  }
}
