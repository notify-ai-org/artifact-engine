package dev.notify.artifact.spring.jpa;

import jakarta.persistence.LockModeType;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

public interface ArtifactOutboxJpaRepository extends JpaRepository<ArtifactOutboxEntity, String> {
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  List<ArtifactOutboxEntity> findAllByOrderByCreatedAtAsc(Pageable pageable);
}
