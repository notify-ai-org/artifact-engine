package dev.notify.artifact.store;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Process-local multipart upload records for tests and development. */
public final class InMemoryMultipartUploadStore implements MultipartUploadStore {
  private final Map<String, MultipartUpload> uploads = new ConcurrentHashMap<>();

  @Override
  public Optional<MultipartUpload> find(String tenantId, String artifactId, long version) {
    return Optional.ofNullable(uploads.get(key(tenantId, artifactId, version)));
  }

  @Override
  public MultipartUpload putIfAbsent(MultipartUpload upload) {
    MultipartUpload existing =
        uploads.putIfAbsent(key(upload.tenantId(), upload.artifactId(), upload.version()), upload);
    return existing == null ? upload : existing;
  }

  @Override
  public void recordComposite(
      String tenantId, String artifactId, long version, String compositeSha256) {
    uploads.computeIfPresent(
        key(tenantId, artifactId, version), (ignored, upload) -> upload.withComposite(compositeSha256));
  }

  @Override
  public void delete(String tenantId, String artifactId, long version) {
    uploads.remove(key(tenantId, artifactId, version));
  }

  private static String key(String tenantId, String artifactId, long version) {
    return tenantId + "\u0000" + artifactId + "\u0000" + version;
  }
}
