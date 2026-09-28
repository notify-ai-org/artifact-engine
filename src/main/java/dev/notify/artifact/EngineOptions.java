package dev.notify.artifact;

/**
 * Core policy choices that must be explicit and stable for a deployment.
 *
 * @param multipartPartBytes part size for multipart object uploads; artifacts larger than one part
 *     are stored as a multipart upload with parts uploaded in parallel. {@code 0} disables it and
 *     every artifact is stored with a single upload.
 */
public record EngineOptions(
    boolean deduplicateContent, int retrievalCandidateMultiplier, long multipartPartBytes) {
  /** S3 rejects non-final parts smaller than 5 MiB. */
  public static final long MIN_PART_BYTES = 5L * 1024 * 1024;

  public EngineOptions {
    if (retrievalCandidateMultiplier < 1 || retrievalCandidateMultiplier > 100) {
      throw new IllegalArgumentException(
          "Retrieval candidate multiplier must be between 1 and 100");
    }
    if (multipartPartBytes != 0 && multipartPartBytes < MIN_PART_BYTES) {
      throw new IllegalArgumentException("multipartPartBytes must be 0 or at least 5 MiB");
    }
  }

  public EngineOptions(boolean deduplicateContent, int retrievalCandidateMultiplier) {
    this(deduplicateContent, retrievalCandidateMultiplier, 0);
  }

  public static EngineOptions defaults() {
    return new EngineOptions(true, 4);
  }
}
