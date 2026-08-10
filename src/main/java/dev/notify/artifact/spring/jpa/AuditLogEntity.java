package dev.notify.artifact.spring.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.Immutable;

/** Append-only audit row. Database grants/triggers must also prohibit update and delete. */
@Entity
@Immutable
@Table(
    name = "artifact_audit_log",
    indexes = {
      @Index(name = "ix_audit_tenant_time", columnList = "tenant_id,occurred_at"),
      @Index(name = "ix_audit_artifact_time", columnList = "artifact_id,occurred_at")
    })
public class AuditLogEntity {
  @Id
  @Column(length = 36)
  private String id;

  @Column(nullable = false, length = 80)
  private String operation;

  @Column(name = "principal_id", nullable = false, length = 256)
  private String principalId;

  @Column(name = "tenant_id", nullable = false, length = 128)
  private String tenantId;

  @Column(name = "artifact_id", length = 64)
  private String artifactId;

  @Column(nullable = false, length = 32)
  private String outcome;

  @Column(name = "latency_millis", nullable = false)
  private long latencyMillis;

  @Column(name = "occurred_at", nullable = false)
  private Instant occurredAt;

  @Column(name = "safe_details_json", nullable = false, columnDefinition = "TEXT")
  private String safeDetailsJson;

  protected AuditLogEntity() {}

  AuditLogEntity(
      String id,
      String operation,
      String principalId,
      String tenantId,
      String artifactId,
      String outcome,
      long latencyMillis,
      Instant occurredAt,
      String safeDetailsJson) {
    this.id = id;
    this.operation = operation;
    this.principalId = principalId;
    this.tenantId = tenantId;
    this.artifactId = artifactId;
    this.outcome = outcome;
    this.latencyMillis = latencyMillis;
    this.occurredAt = occurredAt;
    this.safeDetailsJson = safeDetailsJson;
  }

  public String getSafeDetailsJson() {
    return safeDetailsJson;
  }
}
