package dev.notify.artifact.exception;

/** Raised when one tenant reuses an idempotency key for a different intake fingerprint. */
public final class IdempotencyConflictException extends RuntimeException {
  public IdempotencyConflictException(String idempotencyKey) {
    super("Idempotency key was already used with different artifact content: " + idempotencyKey);
  }
}
