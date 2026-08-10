package dev.notify.artifact.security;

import dev.notify.artifact.auth.DataVerifier;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/** Bounds archive expansion and rejects unsafe entry paths before extraction or scanning. */
public final class ArchiveSafetyFilter implements SecurityFilter<IngestionSecurityContext> {
  private static final int BUFFER_BYTES = 8192;

  private final Limits limits;

  public ArchiveSafetyFilter(Limits limits) {
    this.limits = Objects.requireNonNull(limits, "limits");
  }

  @Override
  public String name() {
    return "archive-safety";
  }

  @Override
  public void verify(IngestionSecurityContext context) {
    if (!DataVerifier.DOCX.equals(context.detectedMediaType())) {
      return;
    }

    try (ZipFile archive = new ZipFile(context.contentPath().toFile())) {
      inspect(archive);
    } catch (SecurityFilterException rejection) {
      throw rejection;
    } catch (ZipException invalidArchive) {
      reject("ARCHIVE_INVALID", "Archive structure is invalid", invalidArchive);
    } catch (IOException readFailure) {
      reject("ARCHIVE_READ_FAILED", "Archive could not be inspected", readFailure);
    }
  }

  private void inspect(ZipFile archive) throws IOException {
    int entries = 0;
    long totalExpandedBytes = 0;
    Enumeration<? extends ZipEntry> contents = archive.entries();
    while (contents.hasMoreElements()) {
      ZipEntry entry = contents.nextElement();
      if (++entries > limits.maxEntries()) {
        reject("ARCHIVE_ENTRY_COUNT_LIMIT", "Archive contains too many entries");
      }
      verifyEntryName(entry.getName());
      if (entry.isDirectory()) {
        continue;
      }

      long expandedBytes = expandedBytes(archive, entry);
      if (expandedBytes > limits.maxEntryBytes()) {
        reject("ARCHIVE_ENTRY_SIZE_LIMIT", "Archive entry exceeds the expanded size limit");
      }
      totalExpandedBytes = Math.addExact(totalExpandedBytes, expandedBytes);
      if (totalExpandedBytes > limits.maxExpandedBytes()) {
        reject("ARCHIVE_EXPANDED_SIZE_LIMIT", "Archive exceeds the total expanded size limit");
      }

      long compressedBytes = entry.getCompressedSize();
      if (expandedBytes > 0 && compressedBytes >= 0) {
        double ratio = expandedBytes / (double) Math.max(1, compressedBytes);
        if (ratio > limits.maxCompressionRatio()) {
          reject("ARCHIVE_RATIO_LIMIT", "Archive entry compression ratio exceeds the safe limit");
        }
      }
    }
  }

  private long expandedBytes(ZipFile archive, ZipEntry entry) throws IOException {
    long count = 0;
    byte[] buffer = new byte[BUFFER_BYTES];
    try (InputStream input = archive.getInputStream(entry)) {
      for (int read; (read = input.read(buffer)) >= 0; ) {
        count += read;
        if (count > limits.maxEntryBytes() || count > limits.maxExpandedBytes()) {
          return count;
        }
      }
    }
    return count;
  }

  private void verifyEntryName(String name) {
    String normalized = name.replace('\\', '/');
    if (normalized.startsWith("/")
        || normalized.matches("^[A-Za-z]:/.*")
        || java.util.Arrays.asList(normalized.split("/")).contains("..")) {
      reject("ARCHIVE_ENTRY_PATH_REJECTED", "Archive contains an unsafe entry path");
    }
  }

  private void reject(String code, String message) {
    throw new SecurityFilterException(code, name(), message);
  }

  private void reject(String code, String message, Throwable cause) {
    throw new SecurityFilterException(code, name(), message, cause);
  }

  public record Limits(
      int maxEntries, long maxEntryBytes, long maxExpandedBytes, double maxCompressionRatio) {
    public Limits {
      if (maxEntries <= 0
          || maxEntryBytes <= 0
          || maxExpandedBytes <= 0
          || maxCompressionRatio <= 0) {
        throw new IllegalArgumentException("Archive limits must be positive");
      }
    }
  }
}
