package dev.notify.artifact.multipart;

import java.util.ArrayList;
import java.util.List;

/**
 * How an artifact is split into multipart parts, within S3's limits: at most 10,000 parts, each
 * at most 5 GiB, all but the last at least 5 MiB. The configured size is raised (in whole MiB)
 * when a file would otherwise need more than 10,000 parts.
 */
public record MultipartPlan(long totalBytes, long partSize, int partCount) {
  public static final int MAX_PARTS = 10_000;
  public static final long MAX_PART_BYTES = 5L * 1024 * 1024 * 1024;
  private static final long MIB = 1024 * 1024;

  /** Returns a plan, or null when the artifact fits in one part and should use a single put. */
  public static MultipartPlan forSize(long totalBytes, long configuredPartBytes) {
    if (configuredPartBytes <= 0 || totalBytes <= configuredPartBytes) return null;
    long minimumForPartLimit = ceilDiv(totalBytes, MAX_PARTS);
    long partSize = Math.max(configuredPartBytes, ceilDiv(minimumForPartLimit, MIB) * MIB);
    if (partSize > MAX_PART_BYTES) {
      throw new IllegalArgumentException("Artifact exceeds the maximum multipart object size");
    }
    return new MultipartPlan(totalBytes, partSize, (int) ceilDiv(totalBytes, partSize));
  }

  public List<Part> parts() {
    List<Part> parts = new ArrayList<>(partCount);
    for (int number = 1; number <= partCount; number++) {
      long offset = (number - 1) * partSize;
      parts.add(new Part(number, offset, Math.min(partSize, totalBytes - offset)));
    }
    return parts;
  }

  public long lengthOf(int partNumber) {
    return Math.min(partSize, totalBytes - (partNumber - 1) * partSize);
  }

  public record Part(int number, long offset, long length) {}

  private static long ceilDiv(long value, long divisor) {
    return (value + divisor - 1) / divisor;
  }
}
