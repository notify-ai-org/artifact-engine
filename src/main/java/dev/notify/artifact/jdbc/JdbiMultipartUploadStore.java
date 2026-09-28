package dev.notify.artifact.jdbc;

import dev.notify.artifact.store.MultipartUploadStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

/** PostgreSQL multipart upload records keyed by tenant, artifact, and version. */
public final class JdbiMultipartUploadStore implements MultipartUploadStore {
  private final Jdbi jdbi;

  public JdbiMultipartUploadStore(Jdbi jdbi) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
  }

  @Override
  public Optional<MultipartUpload> find(String tenantId, String artifactId, long version) {
    return jdbi.withHandle(handle -> find(handle, tenantId, artifactId, version));
  }

  @Override
  public MultipartUpload putIfAbsent(MultipartUpload upload) {
    return jdbi.inTransaction(
        handle -> {
          handle.createUpdate(
                  """
                  INSERT INTO artifact_multipart_upload
                    (tenant_id, artifact_id, version, storage_key, upload_id, part_size,
                     part_count, created_at)
                  VALUES (:tenantId, :artifactId, :version, :storageKey, :uploadId, :partSize,
                          :partCount, :createdAt)
                  ON CONFLICT (tenant_id, artifact_id, version) DO NOTHING
                  """)
              .bind("tenantId", upload.tenantId())
              .bind("artifactId", upload.artifactId())
              .bind("version", upload.version())
              .bind("storageKey", upload.storageKey())
              .bind("uploadId", upload.uploadId())
              .bind("partSize", upload.partSize())
              .bind("partCount", upload.partCount())
              .bind("createdAt", upload.createdAt())
              .execute();
          return find(handle, upload.tenantId(), upload.artifactId(), upload.version())
              .orElseThrow();
        });
  }

  @Override
  public void recordComposite(
      String tenantId, String artifactId, long version, String compositeSha256) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    UPDATE artifact_multipart_upload SET composite_sha256 = :composite
                    WHERE tenant_id = :tenantId AND artifact_id = :artifactId
                      AND version = :version
                    """)
                .bind("composite", compositeSha256)
                .bind("tenantId", tenantId)
                .bind("artifactId", artifactId)
                .bind("version", version)
                .execute());
  }

  @Override
  public void delete(String tenantId, String artifactId, long version) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    DELETE FROM artifact_multipart_upload
                    WHERE tenant_id = :tenantId AND artifact_id = :artifactId
                      AND version = :version
                    """)
                .bind("tenantId", tenantId)
                .bind("artifactId", artifactId)
                .bind("version", version)
                .execute());
  }

  private static Optional<MultipartUpload> find(
      Handle handle, String tenantId, String artifactId, long version) {
    return handle
        .createQuery(
            """
            SELECT * FROM artifact_multipart_upload
            WHERE tenant_id = :tenantId AND artifact_id = :artifactId AND version = :version
            """)
        .bind("tenantId", tenantId)
        .bind("artifactId", artifactId)
        .bind("version", version)
        .map((resultSet, context) -> map(resultSet))
        .findOne();
  }

  private static MultipartUpload map(ResultSet resultSet) throws SQLException {
    return new MultipartUpload(
        resultSet.getString("tenant_id"),
        resultSet.getString("artifact_id"),
        resultSet.getLong("version"),
        resultSet.getString("storage_key"),
        resultSet.getString("upload_id"),
        resultSet.getLong("part_size"),
        resultSet.getInt("part_count"),
        resultSet.getTimestamp("created_at").toInstant(),
        resultSet.getString("composite_sha256"));
  }
}
