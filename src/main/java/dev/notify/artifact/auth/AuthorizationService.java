package dev.notify.artifact.auth;

/**
 * Implementations derive permissions from a trusted principal; callers never select another tenant.
 */
@FunctionalInterface
public interface AuthorizationService {
  void require(String principalId, String tenantId, Permission permission);

  enum Permission {
    INGEST,
    SEARCH,
    READ_METADATA,
    READ_TEXT,
    DOWNLOAD,
    DELETE,
    RETRY
  }
}
