package dev.notify.artifact.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;

class Utf8ChunkDecoderTest {
  private static final String[] SYMBOLS = {"a", "Z", " ", "\n", "é", "ß", "€", "中", "文", "😀", "𝄞"};

  @Test
  void reassemblesTextSplitAtEveryPossibleByteBoundary() throws Exception {
    Random random = new Random(11);
    for (int trial = 0; trial < 400; trial++) {
      StringBuilder original = new StringBuilder();
      for (int index = random.nextInt(60); index > 0; index--) {
        original.append(SYMBOLS[random.nextInt(SYMBOLS.length)]);
      }
      byte[] bytes = original.toString().getBytes(StandardCharsets.UTF_8);

      StringBuilder decoded = new StringBuilder();
      byte[] carry = new byte[0];
      for (int start = 0; start < bytes.length; ) {
        int end = Math.min(bytes.length, start + 1 + random.nextInt(7));
        carry = Utf8ChunkDecoder.decode(carry, ByteBuffer.wrap(bytes, start, end - start).slice(),
            decoded::append);
        start = end;
      }
      Utf8ChunkDecoder.finish(carry, decoded::append);

      assertEquals(original.toString(), decoded.toString());
    }
  }

  @Test
  void carriesAtMostTheIncompleteTrailingCharacter() throws Exception {
    byte[] emoji = "😀".getBytes(StandardCharsets.UTF_8);
    StringBuilder decoded = new StringBuilder();

    byte[] carry = Utf8ChunkDecoder.decode(new byte[0],
        ByteBuffer.wrap(new byte[] {'o', 'k', emoji[0], emoji[1]}), decoded::append);

    assertEquals("ok", decoded.toString());
    assertEquals(2, carry.length);
    carry = Utf8ChunkDecoder.decode(carry, ByteBuffer.wrap(new byte[] {emoji[2], emoji[3], '!'}),
        decoded::append);
    assertEquals("ok😀!", decoded.toString());
    assertEquals(0, carry.length);
  }

  @Test
  void replacesMalformedAndTruncatedInput() throws Exception {
    StringBuilder decoded = new StringBuilder();
    byte[] carry = Utf8ChunkDecoder.decode(new byte[0],
        ByteBuffer.wrap(new byte[] {'a', (byte) 0xff, 'b', (byte) 0xe4, (byte) 0xb8}),
        decoded::append);
    Utf8ChunkDecoder.finish(carry, decoded::append);

    assertEquals("a�b�", decoded.toString());
  }
}
