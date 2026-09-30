package dev.notify.artifact.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** A local file read with positioned reads; its version is its size and modification time. */
public final class FileChunkSource implements ChunkSource {
  private final Path file;

  public FileChunkSource(Path file) {
    this.file = Objects.requireNonNull(file, "file");
  }

  @Override
  public SourceInfo describe() throws IOException {
    return new SourceInfo(Files.size(file), version());
  }

  @Override
  public void read(long offset, ByteBuffer target, String expectedVersion) throws IOException {
    if (expectedVersion != null && !expectedVersion.equals(version())) {
      throw new SourceChangedException("File " + file.getFileName() + " changed during ingest");
    }
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      long position = offset;
      while (target.hasRemaining()) {
        int read = channel.read(target, position);
        if (read < 0) {
          throw new IOException("File ended at " + position + " before the chunk was read");
        }
        position += read;
      }
    }
  }

  private String version() throws IOException {
    return Files.size(file) + "@" + Files.getLastModifiedTime(file).toMillis();
  }
}
