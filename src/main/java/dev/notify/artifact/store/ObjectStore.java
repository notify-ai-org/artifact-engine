package dev.notify.artifact.store;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;

public interface ObjectStore extends Store<InputStream> {
  void put(String tenantId, String key, InputStream content, long length, String sha256)
      throws IOException;

  InputStream get(String tenantId, String key) throws IOException;

  boolean verified(String tenantId, String key, long length, String sha256) throws IOException;

  void delete(String tenantId, String key) throws IOException;

  // ---- Multipart uploads (optional capability) ----------------------------------------------

  /** Whether the multipart operations below are implemented. */
  default boolean supportsMultipart() {
    return false;
  }

  /**
   * Starts a multipart upload with the same encryption and tenant/checksum metadata as {@link
   * #put}. Each part is later verified by the store against its own SHA-256.
   *
   * @param sha256 lowercase hex SHA-256 of the whole object, recorded as object metadata; null
   *     when the content is still arriving (chunked ingest), in which case the per-part checksums
   *     and the composite checksum are the object's integrity proof
   * @return the store's upload id
   */
  default String createMultipartUpload(String tenantId, String key, String sha256)
      throws IOException {
    throw new UnsupportedOperationException("Multipart uploads are not supported");
  }

  /**
   * Uploads one part. Re-uploading a part number replaces it, so retries are safe.
   *
   * @param partSha256 lowercase hex SHA-256 of exactly these bytes; the store rejects a mismatch
   */
  default UploadedPart uploadPart(
      String tenantId,
      String key,
      String uploadId,
      int partNumber,
      InputStream content,
      long length,
      String partSha256)
      throws IOException {
    throw new UnsupportedOperationException("Multipart uploads are not supported");
  }

  /** Lists every part uploaded so far, ordered by part number. */
  default List<UploadedPart> listParts(String tenantId, String key, String uploadId)
      throws IOException {
    throw new UnsupportedOperationException("Multipart uploads are not supported");
  }

  /** Assembles the listed parts into the final object. */
  default void completeMultipartUpload(
      String tenantId, String key, String uploadId, List<UploadedPart> parts) throws IOException {
    throw new UnsupportedOperationException("Multipart uploads are not supported");
  }

  /** Discards an unfinished upload and its parts. Aborting an unknown upload is not an error. */
  default void abortMultipartUpload(String tenantId, String key, String uploadId)
      throws IOException {
    throw new UnsupportedOperationException("Multipart uploads are not supported");
  }

  /**
   * Verifies an object assembled from parts: same checks as {@link #verified}, except that the
   * stored checksum is the composite form {@link #compositeSha256} rather than a whole-object hash.
   * A null {@code sha256} skips the whole-object metadata check, for uploads created without one.
   */
  default boolean verifiedMultipart(
      String tenantId, String key, long length, String sha256, String compositeSha256)
      throws IOException {
    throw new UnsupportedOperationException("Multipart uploads are not supported");
  }

  /**
   * S3's composite checksum for a multipart object: base64 of SHA-256 over the concatenated binary
   * part digests, suffixed with {@code -<part count>}.
   */
  static String compositeSha256(List<UploadedPart> parts) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      parts.stream()
          .sorted(Comparator.comparingInt(UploadedPart::partNumber))
          .forEach(part -> digest.update(Base64.getDecoder().decode(part.checksumSha256())));
      return Base64.getEncoder().encodeToString(digest.digest()) + "-" + parts.size();
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  /**
   * @param checksumSha256 base64 SHA-256 of the part as recorded by the store
   */
  record UploadedPart(int partNumber, String etag, long size, String checksumSha256) {}
}
