package dev.notify.artifact.spool;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.util.Checksum;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Local recovery boundary for incoming artifacts.
 *
 * <p>Content and metadata are written to random temporary paths, flushed, and atomically renamed.
 * Quotas are reserved incrementally so an unknown-length stream cannot bypass configured limits.
 */
public final class DurableSpool {
  private static final int COPY_BUFFER_BYTES = 64 * 1024;
  private static final String CONTENT_FILE = "content.pending";
  private static final String METADATA_FILE = "metadata.json";
  private static final String SCRATCH_DIRECTORY = ".scratch";

  private final Path root;
  private final Limits limits;
  private final ObjectMapper json;
  private final Map<String, Usage> tenantUsage = new HashMap<>();

  private long totalBytes;
  private long totalFiles;

  public DurableSpool(Path root, long maxArtifactBytes, ObjectMapper json) throws IOException {
    this(root, Limits.withArtifactLimit(maxArtifactBytes), json);
  }

  public DurableSpool(Path root, Limits limits, ObjectMapper json) throws IOException {
    this.root = root.toAbsolutePath().normalize();
    this.limits = limits;
    this.json = json;
    Files.createDirectories(this.root);
    rebuildUsage();
  }

  public SpoolEntry write(
      String tenantId, String artifactId, InputStream source, Map<String, ?> metadata)
      throws IOException {
    String tenantHash = Checksum.sha256(tenantId);
    Path directory = safeDirectory(tenantHash, artifactId);
    Files.createDirectories(directory);

    Path temporaryContent = directory.resolve(UUID.randomUUID() + ".content.tmp");
    Path publishedContent = directory.resolve(CONTENT_FILE);
    Copy copy = new Copy();
    reserveFile(tenantHash);
    try {
      streamToFile(source, temporaryContent, tenantHash, copy);
      forceFile(temporaryContent);

      Files.move(
          temporaryContent,
          publishedContent,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      publishMetadata(directory, metadata);
      return new SpoolEntry(publishedContent, copy.bytes, copy.sha256());
    } catch (Exception failure) {
      Files.deleteIfExists(temporaryContent);
      Files.deleteIfExists(publishedContent);
      Files.deleteIfExists(directory.resolve(METADATA_FILE));
      // copy.bytes counts every byte reserved so far, including a stream that failed midway.
      release(tenantHash, copy.bytes, 1);
      deleteDirectoryIfEmpty(directory);
      throw failure;
    }
  }

  public InputStream open(Path path) throws IOException {
    return Files.newInputStream(requireContentPath(path));
  }

  /** Opens {@code length} bytes of a spool entry starting at {@code offset}. */
  public InputStream openRange(Path path, long offset, long length) throws IOException {
    if (offset < 0 || length < 0) {
      throw new IllegalArgumentException("offset and length cannot be negative");
    }
    FileChannel channel = FileChannel.open(requireContentPath(path), StandardOpenOption.READ);
    try {
      if (offset + length > channel.size()) {
        throw new IOException("Requested range exceeds the spooled content");
      }
      channel.position(offset);
      return new RangeInputStream(java.nio.channels.Channels.newInputStream(channel), length);
    } catch (IOException | RuntimeException failure) {
      channel.close();
      throw failure;
    }
  }

  /** Reads at most a fixed number of bytes from the underlying stream. */
  private static final class RangeInputStream extends java.io.FilterInputStream {
    private long remaining;

    private RangeInputStream(InputStream input, long length) {
      super(input);
      this.remaining = length;
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) return -1;
      int value = super.read();
      if (value >= 0) remaining--;
      return value;
    }

    @Override
    public int read(byte[] target, int offset, int length) throws IOException {
      if (remaining <= 0) return -1;
      int read = super.read(target, offset, (int) Math.min(length, remaining));
      if (read > 0) remaining -= read;
      return read;
    }

    @Override
    public long skip(long count) throws IOException {
      long skipped = super.skip(Math.min(count, remaining));
      remaining -= skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(super.available(), remaining);
    }

    @Override
    public boolean markSupported() {
      return false;
    }
  }

  /** Returns the validated spool content path if the entry is still present on disk. */
  public Optional<Path> existingContent(Path path) {
    Path content = requireContentPath(path);
    return Files.isRegularFile(content) ? Optional.of(content) : Optional.empty();
  }

  /**
   * Creates an empty scratch file on the spool volume for transient work such as staging an
   * object-store download for random-access parsing. The caller must delete it. Scratch files are
   * not spool entries and are not counted against quotas.
   */
  public Path scratchFile() throws IOException {
    Path directory = Files.createDirectories(root.resolve(SCRATCH_DIRECTORY));
    return Files.createTempFile(directory, "scratch-", ".tmp");
  }

  /** Removes a confirmed, unreferenced entry and releases its quota reservation. */
  public void discard(Path path) throws IOException {
    Path content = requireContentPath(path);
    long size = Files.exists(content) ? Files.size(content) : 0;
    String tenantHash = content.getParent().getParent().getFileName().toString();

    Files.deleteIfExists(content);
    Files.deleteIfExists(content.getParent().resolve(METADATA_FILE));
    deleteDirectoryIfEmpty(content.getParent());
    release(tenantHash, size, size > 0 ? 1 : 0);
  }

  /** Lists complete spool entries for startup/periodic reconciliation with metadata records. */
  public List<Path> entries() throws IOException {
    try (var paths = Files.find(root, 3, (path, attributes) -> attributes.isRegularFile())) {
      return paths.filter(path -> path.getFileName().toString().equals(CONTENT_FILE)).toList();
    }
  }

  public synchronized UsageSnapshot usage() {
    return new UsageSnapshot(totalBytes, totalFiles, Map.copyOf(tenantUsage));
  }

  /**
   * Copies, reserves quota, and hashes in one pass, so the content is never re-read to checksum it.
   * The digest covers exactly the bytes written; the object store later verifies its upload against
   * this digest while reading the spool file back, which also covers on-disk corruption.
   */
  private void streamToFile(InputStream source, Path target, String tenantHash, Copy copy)
      throws IOException {
    try (var raw = Files.newOutputStream(target, StandardOpenOption.CREATE_NEW);
        var output = new BufferedOutputStream(raw, COPY_BUFFER_BYTES)) {
      byte[] buffer = new byte[COPY_BUFFER_BYTES];
      for (int read; (read = source.read(buffer)) >= 0; ) {
        if (read == 0) {
          continue;
        }
        if (copy.bytes + read > limits.maxArtifactBytes()) {
          throw new SpoolQuotaExceededException("Artifact exceeds the configured byte limit");
        }
        reserveBytes(tenantHash, read);
        copy.bytes += read;
        copy.digest.update(buffer, 0, read);
        output.write(buffer, 0, read);
      }
      output.flush();
    }
  }

  /** Progress of one spool write, visible to the failure path for exact quota release. */
  private static final class Copy {
    private final MessageDigest digest = sha256Digest();
    private long bytes;

    private String sha256() {
      return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256Digest() {
      try {
        return MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException impossible) {
        throw new IllegalStateException(impossible);
      }
    }
  }

  private void publishMetadata(Path directory, Map<String, ?> metadata) throws IOException {
    Path temporaryMetadata = directory.resolve(UUID.randomUUID() + ".metadata.tmp");
    try {
      json.writeValue(temporaryMetadata.toFile(), metadata == null ? Map.of() : metadata);
      forceFile(temporaryMetadata);
      Files.move(
          temporaryMetadata,
          directory.resolve(METADATA_FILE),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporaryMetadata);
    }
  }

  private synchronized void reserveFile(String tenantHash) throws SpoolQuotaExceededException {
    Usage usage = tenantUsage.getOrDefault(tenantHash, Usage.EMPTY);
    if (totalFiles + 1 > limits.maxFiles()) {
      throw new SpoolQuotaExceededException("Global spool file quota exceeded");
    }
    if (usage.files() + 1 > limits.maxFilesPerTenant()) {
      throw new SpoolQuotaExceededException("Tenant spool file quota exceeded");
    }
    totalFiles++;
    tenantUsage.put(tenantHash, new Usage(usage.bytes(), usage.files() + 1));
  }

  private synchronized void reserveBytes(String tenantHash, long bytes)
      throws SpoolQuotaExceededException {
    Usage usage = tenantUsage.getOrDefault(tenantHash, Usage.EMPTY);
    if (totalBytes + bytes > limits.maxBytes()) {
      throw new SpoolQuotaExceededException("Global spool byte quota exceeded");
    }
    if (usage.bytes() + bytes > limits.maxBytesPerTenant()) {
      throw new SpoolQuotaExceededException("Tenant spool byte quota exceeded");
    }
    totalBytes += bytes;
    tenantUsage.put(tenantHash, new Usage(usage.bytes() + bytes, usage.files()));
  }

  private synchronized void release(String tenantHash, long bytes, long files) {
    Usage usage = tenantUsage.getOrDefault(tenantHash, Usage.EMPTY);
    totalBytes = Math.max(0, totalBytes - bytes);
    totalFiles = Math.max(0, totalFiles - files);
    long remainingBytes = Math.max(0, usage.bytes() - bytes);
    long remainingFiles = Math.max(0, usage.files() - files);
    if (remainingBytes == 0 && remainingFiles == 0) {
      tenantUsage.remove(tenantHash);
    } else {
      tenantUsage.put(tenantHash, new Usage(remainingBytes, remainingFiles));
    }
  }

  private void rebuildUsage() throws IOException {
    for (Path content : entries()) {
      String tenantHash = content.getParent().getParent().getFileName().toString();
      long size = Files.size(content);
      totalBytes += size;
      totalFiles++;
      Usage usage = tenantUsage.getOrDefault(tenantHash, Usage.EMPTY);
      tenantUsage.put(tenantHash, new Usage(usage.bytes() + size, usage.files() + 1));
    }
  }

  private Path safeDirectory(String tenantHash, String artifactId) {
    if (!artifactId.matches("[A-Za-z0-9-]{1,128}")) {
      throw new IllegalArgumentException("Artifact id must be generated and path-safe");
    }
    return root.resolve(tenantHash).resolve(artifactId).normalize();
  }

  private Path requireContentPath(Path path) {
    Path normalized = path.toAbsolutePath().normalize();
    if (!normalized.startsWith(root) || !normalized.getFileName().toString().equals(CONTENT_FILE)) {
      throw new SecurityException("Invalid spool content path");
    }
    return normalized;
  }

  private static void forceFile(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  private static void deleteDirectoryIfEmpty(Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return;
    }
    try (var children = Files.list(directory)) {
      if (children.findAny().isEmpty()) {
        Files.deleteIfExists(directory);
      }
    }
  }

  public record Limits(
      long maxArtifactBytes,
      long maxBytes,
      long maxFiles,
      long maxBytesPerTenant,
      long maxFilesPerTenant) {
    public Limits {
      if (maxArtifactBytes < 1
          || maxBytes < maxArtifactBytes
          || maxFiles < 1
          || maxBytesPerTenant < maxArtifactBytes
          || maxFilesPerTenant < 1) {
        throw new IllegalArgumentException("Invalid spool limits");
      }
    }

    public static Limits withArtifactLimit(long maxArtifactBytes) {
      return new Limits(
          maxArtifactBytes, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }
  }

  public record Usage(long bytes, long files) {
    private static final Usage EMPTY = new Usage(0, 0);
  }

  public record UsageSnapshot(long bytes, long files, Map<String, Usage> tenants) {}

  public record SpoolEntry(Path contentPath, long sizeBytes, String sha256) {}

  public static final class SpoolQuotaExceededException extends IOException {
    public SpoolQuotaExceededException(String message) {
      super(message);
    }
  }
}
