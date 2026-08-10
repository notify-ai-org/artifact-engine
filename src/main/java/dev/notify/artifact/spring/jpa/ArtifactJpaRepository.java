package dev.notify.artifact.spring.jpa;

import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ArtifactJpaRepository extends JpaRepository<ArtifactEntity, String> {
  Optional<ArtifactEntity> findByTenantIdAndId(String tenantId, String id);

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query(
      "select artifact from ArtifactEntity artifact where artifact.tenantId = :tenantId and artifact.id = :id")
  Optional<ArtifactEntity> findLockedByTenantIdAndId(
      @Param("tenantId") String tenantId, @Param("id") String id);

  Optional<ArtifactEntity> findByTenantIdAndIdempotencyKey(String tenantId, String key);

  Optional<ArtifactEntity> findFirstByTenantIdAndSha256(String tenantId, String sha256);

  Optional<ArtifactEntity> findBySpoolPath(String spoolPath);

  List<ArtifactEntity> findByTenantIdOrderByCreatedAtDesc(String tenantId, Pageable pageable);
}
