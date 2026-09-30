package dev.notify.artifact.chunk;

import java.io.InputStream;
import java.nio.ByteBuffer;

/** Streams a buffer's remaining bytes without copying the buffer. */
public final class ByteBufferInputStream extends InputStream {
  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer.duplicate();
  }

  @Override
  public int read() {
    return buffer.hasRemaining() ? buffer.get() & 0xff : -1;
  }

  @Override
  public int read(byte[] target, int offset, int length) {
    if (!buffer.hasRemaining()) return -1;
    int count = Math.min(length, buffer.remaining());
    buffer.get(target, offset, count);
    return count;
  }

  @Override
  public int available() {
    return buffer.remaining();
  }
}
