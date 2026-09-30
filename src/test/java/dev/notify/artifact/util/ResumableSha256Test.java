package dev.notify.artifact.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ResumableSha256Test {
  @Test
  void matchesTheStandardTestVectors() {
    assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        digest(""));
    assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        digest("abc"));
    assertEquals("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        digest("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"));
  }

  @Test
  void matchesMessageDigestAcrossRandomSplitsAndSavedStates() throws Exception {
    Random random = new Random(5);
    for (int trial = 0; trial < 300; trial++) {
      byte[] content = new byte[random.nextInt(1000)];
      random.nextBytes(content);

      String state = "";
      for (int start = 0; start < content.length; ) {
        int end = Math.min(content.length, start + 1 + random.nextInt(130));
        ResumableSha256 digest = ResumableSha256.restore(state);
        digest.update(ByteBuffer.wrap(content, start, end - start).slice());
        state = digest.state();
        start = end;
      }

      String expected = HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256").digest(content));
      assertEquals(expected, ResumableSha256.restore(state).hexDigest());
    }
  }

  @Test
  void digestingDoesNotDisturbFurtherUpdates() {
    ResumableSha256 digest = new ResumableSha256();
    digest.update("ab".getBytes(StandardCharsets.US_ASCII));
    digest.hexDigest();
    digest.update("c".getBytes(StandardCharsets.US_ASCII));

    assertEquals(digest("abc"), digest.hexDigest());
    assertEquals(3, digest.totalBytes());
  }

  @Test
  void rejectsCorruptState() {
    assertThrows(IllegalArgumentException.class, () -> ResumableSha256.restore("zz:1:"));
  }

  private static String digest(String text) {
    ResumableSha256 digest = new ResumableSha256();
    digest.update(text.getBytes(StandardCharsets.US_ASCII));
    return digest.hexDigest();
  }
}
