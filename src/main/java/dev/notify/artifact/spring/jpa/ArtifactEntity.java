package dev.notify.artifact.spring.jpa;

import dev.notify.artifact.model.Artifact;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;
import java.time.Instant;

/** JPA persistence envelope; the JSON payload preserves the infrastructure-neutral core model. */
@Entity
@Table(
    name = "artifact",
    uniqueConstraints =
        @UniqueConstraint(
            name = "uk_artifact_tenant_idempotency",
            columnNames = {"tenant_id", "idempotency_key"}),
    indexes = {
      @Index(name = "ix_artifact_tenant_checksum", columnList = "tenant_id,sha256"),
      @Index(name = "ix_artifact_spool_path", columnList = "spool_path")
    })
public class ArtifactEntity {
  @Id
  @Column(length = 64)
  private String id;

  @Column(name = "tenant_id", nullable = false, length = 128)
  private String tenantId;

  @Column(name = "idempotency_key", nullable = false, length = 256)
  private String idempotencyKey;

  @Column(name = "idempotency_fingerprint", nullable = false, length = 64)
  private String idempotencyFingerprint;

  @Column(nullable = false, length = 64)
  private String sha256;

  @Column(name = "spool_path", length = 2048)
  private String spoolPath;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  @Column(name = "original_name", nullable = false, length = 255)
  private String originalName;

  @Column(name = "media_type", nullable = false, length = 160)
  private String mediaType;

  @Column(name = "size_bytes", nullable = false)
  private long sizeBytes;

  @Column(name = "storage_key", length = 2048)
  private String storageKey;

  @Column(name = "storage_status", nullable = false, length = 32)
  private String storageStatus;

  @Column(name = "index_status", nullable = false, length = 32)
  private String indexStatus;

  @Column(name = "artifact_version", nullable = false)
  private long artifactVersion;

  @Column(name = "tags_csv", length = 4096)
  private String tagsCsv;

  @Column(name = "payload_json", nullable = false, columnDefinition = "TEXT")
  private String payloadJson;

  @Version private long lockVersion;

  protected ArtifactEntity() {}

  ArtifactEntity(Artifact artifact, String payloadJson) {
    this.id = artifact.id();
    this.tenantId = artifact.tenantId();
    this.idempotencyKey = artifact.idempotencyKey();
    this.idempotencyFingerprint = artifact.idempotencyFingerprint();
    this.sha256 = artifact.sha256();
    this.createdAt = artifact.createdAt();
    replace(artifact, payloadJson);
  }

  public String getId() {
    return id;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getIdempotencyKey() {
    return idempotencyKey;
  }

  public String getIdempotencyFingerprint() {
    return idempotencyFingerprint;
  }

  public String getPayloadJson() {
    return payloadJson;
  }

  void replace(Artifact artifact, String payloadJson) {
    this.payloadJson = payloadJson;
    this.spoolPath =
        artifact.spoolPath() == null
            ? null
            : artifact.spoolPath().toAbsolutePath().normalize().toString();
    this.updatedAt = artifact.updatedAt();
    this.originalName = artifact.originalName();
    this.mediaType = artifact.mediaType();
    this.sizeBytes = artifact.sizeBytes();
    this.storageKey = artifact.storageKey();
    this.storageStatus = artifact.storageStatus().name();
    this.indexStatus = artifact.indexStatus().name();
    this.artifactVersion = artifact.version();
    this.tagsCsv = normalizeTags(artifact.metadata().get("tags"));
  }

  private static String normalizeTags(String tags) {
    if (tags == null || tags.isBlank()) {
      return null;
    }
    return java.util.Arrays.stream(tags.split(","))
        .map(String::trim)
        .filter(tag -> !tag.isBlank())
        .distinct()
        .sorted()
        .collect(java.util.stream.Collectors.joining(","));
  }
}
