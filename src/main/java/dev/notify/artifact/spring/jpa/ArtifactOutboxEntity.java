package dev.notify.artifact.spring.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(
    name = "artifact_outbox",
    indexes = @Index(name = "ix_artifact_outbox_created", columnList = "created_at"))
public class ArtifactOutboxEntity {
  @Id
  @Column(length = 64)
  private String id;

  @Column(name = "job_json", nullable = false, columnDefinition = "TEXT")
  private String jobJson;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  protected ArtifactOutboxEntity() {}

  ArtifactOutboxEntity(String id, String jobJson, Instant createdAt) {
    this.id = id;
    this.jobJson = jobJson;
    this.createdAt = createdAt;
  }

  public String getId() {
    return id;
  }

  public String getJobJson() {
    return jobJson;
  }
}
