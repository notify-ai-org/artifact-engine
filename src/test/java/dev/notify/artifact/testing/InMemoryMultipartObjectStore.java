package dev.notify.artifact.testing;

import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.Checksum;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Object store for tests that behaves like S3 where the engine depends on it: parts are rejected
 * when their bytes do not match the declared SHA-256, completion assembles parts in order and
 * forgets the part list, and verification checks length, composite checksum, and (when recorded)
 * the whole-object SHA-256 metadata.
 */
public final class InMemoryMultipartObjectStore implements ObjectStore {
  private final Map<String, TreeMap<Integer, byte[]>> parts = new ConcurrentHashMap<>();
  private final Map<String, String> uploadSha = new ConcurrentHashMap<>();
  private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
  private final Map<String, String> composites = new ConcurrentHashMap<>();
  private final Map<String, String> objectSha = new ConcurrentHashMap<>();
  private final AtomicInteger uploads = new AtomicInteger();

  public byte[] object(String key) {
    return objects.get(key);
  }

  @Override
  public boolean supportsMultipart() {
    return true;
  }

  @Override
  public String createMultipartUpload(String tenantId, String key, String sha256) {
    String uploadId = "upload-" + uploads.incrementAndGet();
    parts.put(uploadId, new TreeMap<>());
    if (sha256 != null) uploadSha.put(uploadId, sha256);
    return uploadId;
  }

  @Override
  public UploadedPart uploadPart(String tenantId, String key, String uploadId, int partNumber,
      InputStream content, long length, String partSha256) throws IOException {
    byte[] bytes = content.readAllBytes();
    if (bytes.length != length
        || !Checksum.sha256(new ByteArrayInputStream(bytes)).equals(partSha256)) {
      throw new IOException("BadDigest for part " + partNumber);
    }
    TreeMap<Integer, byte[]> uploaded = parts.get(uploadId);
    if (uploaded == null) throw new IOException("NoSuchUpload");
    synchronized (uploaded) {
      uploaded.put(partNumber, bytes);
    }
    return new UploadedPart(partNumber, "etag-" + partNumber, length, base64(partSha256));
  }

  @Override
  public List<UploadedPart> listParts(String tenantId, String key, String uploadId)
      throws IOException {
    TreeMap<Integer, byte[]> uploaded = parts.get(uploadId);
    if (uploaded == null) throw new IOException("NoSuchUpload");
    List<UploadedPart> result = new ArrayList<>();
    synchronized (uploaded) {
      uploaded.forEach((number, bytes) -> result.add(new UploadedPart(number, "etag-" + number,
          bytes.length, base64(sha256(bytes)))));
    }
    return result;
  }

  @Override
  public void completeMultipartUpload(String tenantId, String key, String uploadId,
      List<UploadedPart> completed) throws IOException {
    TreeMap<Integer, byte[]> uploaded = parts.remove(uploadId);
    if (uploaded == null) throw new IOException("NoSuchUpload");
    ByteArrayOutputStream assembled = new ByteArrayOutputStream();
    for (UploadedPart part : completed) assembled.writeBytes(uploaded.get(part.partNumber()));
    objects.put(key, assembled.toByteArray());
    composites.put(key, ObjectStore.compositeSha256(completed));
    String sha = uploadSha.remove(uploadId);
    if (sha != null) objectSha.put(key, sha);
  }

  @Override
  public void abortMultipartUpload(String tenantId, String key, String uploadId) {
    parts.remove(uploadId);
  }

  @Override
  public boolean verifiedMultipart(String tenantId, String key, long length, String sha256,
      String compositeSha256) {
    byte[] object = objects.get(key);
    return object != null
        && object.length == length
        && compositeSha256.equals(composites.get(key))
        && (sha256 == null || sha256.equals(objectSha.get(key)));
  }

  @Override
  public void put(String tenantId, String key, InputStream content, long length, String sha256)
      throws IOException {
    objects.put(key, content.readAllBytes());
    objectSha.put(key, sha256);
  }

  @Override
  public InputStream get(String tenantId, String key) throws IOException {
    byte[] object = objects.get(key);
    if (object == null) throw new IOException("NoSuchKey");
    return new ByteArrayInputStream(object);
  }

  @Override
  public boolean verified(String tenantId, String key, long length, String sha256) {
    byte[] object = objects.get(key);
    return object != null && object.length == length && sha256.equals(objectSha.get(key));
  }

  @Override
  public void delete(String tenantId, String key) {
    objects.remove(key);
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(
          java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static String base64(String hex) {
    return Base64.getEncoder().encodeToString(HexFormat.of().parseHex(hex));
  }
}
