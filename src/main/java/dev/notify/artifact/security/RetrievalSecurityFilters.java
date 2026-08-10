package dev.notify.artifact.security;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Canonical retrieval order, ending with model-facing content sanitization. */
public final class RetrievalSecurityFilters {
  private RetrievalSecurityFilters() {}

  public static SecurityFilterChain<RetrievalSecurityContext> create(
      AuthenticationService authentication, Policy policy, Clock clock) {
    return new SecurityFilterChain<>(
        List.of(
            new AuthenticationFilter<>(authentication, clock),
            new TenantIsolationFilter<>(),
            new EncryptionPolicyFilter<>(policy.allowedTlsProtocols(), false),
            new ExtractedHtmlSanitizationFilter<>()));
  }

  public record Policy(
      Set<String> allowedTlsProtocols,
      ProtocolAbuseFilter.Limits protocolLimits,
      Map<String, Set<String>> streamTransitions) {
    public Policy {
      allowedTlsProtocols = Set.copyOf(allowedTlsProtocols);
      streamTransitions = Map.copyOf(streamTransitions);
    }

    public static Policy defaults() {
      return new Policy(
          Set.of("TLSv1.3", "TLSv1.2"),
          new ProtocolAbuseFilter.Limits(1024 * 1024, 10_000, 128, 100),
          Map.of("READY", Set.of("READING"), "READING", Set.of("READING", "COMPLETED")));
    }
  }
}
