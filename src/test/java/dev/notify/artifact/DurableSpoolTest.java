package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

  @Test
  void chunkedEntriesReserveQuotaWriteOutOfOrderAndCommitOnce() throws Exception {
    byte[] content = "0123456789abcdefghij".getBytes();
    var spool = new DurableSpool(root, 100, new ObjectMapper());

    Path target = spool.reserve("t", "chunked", content.length, Map.of());
    assertEquals(content.length, spool.usage().bytes());
    assertFalse(Files.exists(target), "readers never see an uncommitted entry");

    spool.writeChunk(target, 10, java.nio.ByteBuffer.wrap(content, 10, 10));
    spool.writeChunk(target, 0, java.nio.ByteBuffer.wrap(content, 0, 10));
    var committed = spool.commit(target, content.length);
    var again = spool.commit(target, content.length);
    spool.writeChunk(target, 0, java.nio.ByteBuffer.wrap(new byte[10]));

    assertEquals(Checksum.sha256(new ByteArrayInputStream(content)), committed.sha256());
    assertEquals(committed, again);
    assertArrayEquals(content, Files.readAllBytes(target), "writes after commit are ignored");
    assertEquals(content.length, spool.usage().bytes());
  }

  @Test
  void reservedQuotaSurvivesARestartAndIsReleasedOnDiscard() throws Exception {
    var spool = new DurableSpool(root, 100, new ObjectMapper());
    Path target = spool.reserve("t", "chunked", 40, Map.of());

    var restarted = new DurableSpool(root, 100, new ObjectMapper());
    assertEquals(40, restarted.usage().bytes());

    restarted.discardReserved(target);
    assertEquals(0, restarted.usage().bytes());
    assertEquals(0, restarted.usage().files());
  }

  @Test
  void refusesToReserveMoreThanTheArtifactLimitOrWriteOutsideTheReservation() throws Exception {
    var spool = new DurableSpool(root, 10, new ObjectMapper());
    assertThrows(DurableSpool.SpoolQuotaExceededException.class,
        () -> spool.reserve("t", "big", 11, Map.of()));

    Path target = spool.reserve("t", "small", 10, Map.of());
    assertThrows(IOException.class,
        () -> spool.writeChunk(target, 8, java.nio.ByteBuffer.wrap(new byte[4])));
    assertThrows(IOException.class, () -> spool.commit(target, 9));
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
