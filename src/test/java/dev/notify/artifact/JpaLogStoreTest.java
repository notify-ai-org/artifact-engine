package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.spring.jpa.AuditLogEntity;
import dev.notify.artifact.spring.jpa.AuditLogJpaRepository;
import dev.notify.artifact.spring.jpa.JpaLogStore;
import dev.notify.artifact.store.LogStore;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JpaLogStoreTest {
  @Test
  void redactsSensitiveDetailsBeforePersistence() throws Exception {
    AuditLogJpaRepository repository = org.mockito.Mockito.mock(AuditLogJpaRepository.class);
    when(repository.save(any(AuditLogEntity.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));
    JpaLogStore store = new JpaLogStore(repository, new ObjectMapper());

    store.append(
        new LogStore.AuditEvent(
            "DOWNLOAD",
            "principal-a",
            "tenant-a",
            "artifact-a",
            "SUCCESS",
            12,
            Instant.now(),
            Map.of("accessToken", "secret-value", "status", "served")));

    ArgumentCaptor<AuditLogEntity> entity = ArgumentCaptor.forClass(AuditLogEntity.class);
    verify(repository).save(entity.capture());
    @SuppressWarnings("unchecked")
    Map<String, String> details =
        new ObjectMapper().readValue(entity.getValue().getSafeDetailsJson(), Map.class);
    assertEquals("[REDACTED]", details.get("accessToken"));
    assertEquals("served", details.get("status"));
  }
}
