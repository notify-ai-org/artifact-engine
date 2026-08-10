package dev.notify.artifact.spring.jpa;

import org.springframework.data.repository.Repository;

/** Deliberately exposes insert only; audit deletion is a separate privileged retention process. */
public interface AuditLogJpaRepository extends Repository<AuditLogEntity, String> {
  AuditLogEntity save(AuditLogEntity event);
}
