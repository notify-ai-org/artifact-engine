package dev.notify.artifact.spring.jpa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.store.LogStore;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** JPA-backed append-only audit store with mandatory sensitive-value redaction. */
@Repository
@Primary
public class JpaLogStore implements LogStore {
  private static final int MAX_DETAILS = 50;
  private static final int MAX_DETAIL_VALUE_LENGTH = 512;
  private static final Set<String> SENSITIVE_KEY_PARTS =
      Set.of(
          "token",
          "secret",
          "password",
          "authorization",
          "cookie",
          "presigned",
          "content",
          "document",
          "credential");

  private final AuditLogJpaRepository repository;
  private final ObjectMapper json;

  public JpaLogStore(AuditLogJpaRepository repository, ObjectMapper objectMapper) {
    this.repository = repository;
    this.json = objectMapper;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void append(AuditEvent event) {
    validate(event);
    repository.save(
        new AuditLogEntity(
            UUID.randomUUID().toString(),
            bounded(event.operation(), 80),
            bounded(event.principalId(), 256),
            bounded(event.tenantId(), 128),
            nullableBounded(event.artifactId(), 64),
            bounded(event.outcome(), 32),
            Math.max(0, event.latencyMillis()),
            event.occurredAt(),
            serialize(sanitize(event.safeDetails()))));
  }

  private static void validate(AuditEvent event) {
    if (event == null
        || blank(event.operation())
        || blank(event.principalId())
        || blank(event.tenantId())
        || blank(event.outcome())
        || event.occurredAt() == null
        || event.occurredAt().isAfter(Instant.now().plusSeconds(300))) {
      throw new IllegalArgumentException("Audit event is incomplete or has an invalid timestamp");
    }
  }

  private static Map<String, String> sanitize(Map<String, String> details) {
    if (details == null || details.isEmpty()) {
      return Map.of();
    }
    Map<String, String> safe = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : details.entrySet()) {
      if (safe.size() >= MAX_DETAILS) {
        break;
      }
      String key = bounded(entry.getKey(), 64);
      String lowerKey = key.toLowerCase(Locale.ROOT);
      if (SENSITIVE_KEY_PARTS.stream().anyMatch(lowerKey::contains)) {
        safe.put(key, "[REDACTED]");
        continue;
      }
      String value = entry.getValue() == null ? "" : entry.getValue();
      safe.put(key, looksSensitive(value) ? "[REDACTED]" : bounded(value, MAX_DETAIL_VALUE_LENGTH));
    }
    return Map.copyOf(safe);
  }

  private static boolean looksSensitive(String value) {
    String lower = value.toLowerCase(Locale.ROOT);
    return lower.startsWith("bearer ")
        || lower.contains("x-amz-signature=")
        || lower.contains("x-amz-credential=")
        || value.matches("^[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{20,}$");
  }

  private String serialize(Map<String, String> details) {
    try {
      return json.writeValueAsString(details);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Audit details cannot be serialized", failure);
    }
  }

  private static String bounded(String value, int maxLength) {
    if (value == null) {
      throw new IllegalArgumentException("Required audit value is missing");
    }
    String sanitized = value.replaceAll("[\\p{Cc}&&[^\\t]]", "_");
    return sanitized.substring(0, Math.min(maxLength, sanitized.length()));
  }

  private static String nullableBounded(String value, int maxLength) {
    return value == null ? null : bounded(value, maxLength);
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
