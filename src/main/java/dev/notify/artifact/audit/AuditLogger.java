package dev.notify.artifact.audit;

import dev.notify.artifact.store.LogStore;
import java.time.Instant;
import java.util.Map;

/**
 * Records identifiers and outcomes only; document content, credentials and bearer tokens are
 * forbidden.
 */
public final class AuditLogger {
  private final LogStore store;

  public AuditLogger(LogStore store) {
    this.store = store;
  }

  public Scope begin(String operation, String principal, String tenant, String artifact) {
    return new Scope(operation, principal, tenant, artifact, System.nanoTime());
  }

  public final class Scope {
    private final String operation, principal, tenant, artifact;
    private final long start;

    private Scope(String o, String p, String t, String a, long s) {
      operation = o;
      principal = p;
      tenant = t;
      artifact = a;
      start = s;
    }

    public void success() {
      write("SUCCESS", Map.of());
    }

    public void failure(String code) {
      write("FAILURE", Map.of("code", code));
    }

    private void write(String outcome, Map<String, String> details) {
      store.append(
          new LogStore.AuditEvent(
              operation,
              principal,
              tenant,
              artifact,
              outcome,
              (System.nanoTime() - start) / 1_000_000,
              Instant.now(),
              details));
    }
  }
}
