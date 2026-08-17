package dev.notify.artifact.store;

import java.time.Instant;
import java.util.Map;

public interface LogStore extends Store<LogStore.AuditEvent> {
  void append(AuditEvent event);

  record AuditEvent(
      String operation,
      String principalId,
      String tenantId,
      String artifactId,
      String outcome,
      long latencyMillis,
      Instant occurredAt,
      Map<String, String> safeDetails) {}
}
