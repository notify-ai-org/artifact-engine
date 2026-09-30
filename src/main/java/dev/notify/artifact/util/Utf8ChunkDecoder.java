package dev.notify.artifact.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Decodes UTF-8 text that arrives as independent byte chunks.
 *
 * <p>A chunk boundary can split a multi-byte character. {@link #decode} returns the incomplete
 * trailing bytes (at most three) as a carry to prepend to the next chunk; {@link #finish} decodes
 * whatever is left at the end. Text is pushed to the sink in bounded pieces, and the chunk itself
 * is never copied. Malformed input becomes U+FFFD, as with any lenient UTF-8 reader.
 */
public final class Utf8ChunkDecoder {
  private static final int PIECE_CHARACTERS = 8192;

  private Utf8ChunkDecoder() {}

  /**
   * @param carry bytes left over from the previous chunk
   * @return bytes of an incomplete character at the end of this chunk, to carry into the next
   */
  public static byte[] decode(byte[] carry, ByteBuffer chunk, TextSink sink) throws IOException {
    CharsetDecoder decoder = decoder();
    CharBuffer out = CharBuffer.allocate(PIECE_CHARACTERS);
    ByteBuffer in = chunk.duplicate();
    if (carry.length > 0) {
      // A UTF-8 sequence is at most four bytes, so carry plus four more bytes always resolves it.
      int take = Math.min(4, in.remaining());
      ByteBuffer head = ByteBuffer.allocate(carry.length + take);
      head.put(carry).put(in.duplicate().limit(in.position() + take)).flip();
      decodeInto(decoder, head, out, sink, false);
      int consumedFromChunk = head.position() - carry.length;
      if (consumedFromChunk < 0 || (take == in.remaining() && head.hasRemaining()
          && consumedFromChunk < take)) {
        // Only possible when the chunk is shorter than the character it completes.
        flush(out, sink);
        byte[] rest = new byte[head.remaining()];
        head.get(rest);
        return rest;
      }
      in.position(in.position() + consumedFromChunk);
    }
    decodeInto(decoder, in, out, sink, false);
    flush(out, sink);
    byte[] rest = new byte[in.remaining()];
    in.get(rest);
    return rest;
  }

  /** Decodes a final carry; an incomplete character at the very end becomes U+FFFD. */
  public static void finish(byte[] carry, TextSink sink) throws IOException {
    if (carry.length == 0) return;
    CharsetDecoder decoder = decoder();
    CharBuffer out = CharBuffer.allocate(PIECE_CHARACTERS);
    decodeInto(decoder, ByteBuffer.wrap(carry), out, sink, true);
    decoder.flush(out);
    flush(out, sink);
  }

  private static void decodeInto(
      CharsetDecoder decoder, ByteBuffer in, CharBuffer out, TextSink sink, boolean endOfInput)
      throws IOException {
    while (true) {
      CoderResult result = decoder.decode(in, out, endOfInput);
      if (result.isOverflow()) {
        flush(out, sink);
      } else {
        return;
      }
    }
  }

  private static void flush(CharBuffer out, TextSink sink) throws IOException {
    if (out.position() == 0) return;
    out.flip();
    sink.accept(out.toString());
    out.clear();
  }

  private static CharsetDecoder decoder() {
    return StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE);
  }
}
