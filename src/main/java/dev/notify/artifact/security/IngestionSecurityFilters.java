package dev.notify.artifact.security;

import dev.notify.artifact.auth.DataVerifier;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Canonical ingestion order; changing it should be treated as a security-sensitive change. */
public final class IngestionSecurityFilters {
  private IngestionSecurityFilters() {}

  public static SecurityFilterChain<IngestionSecurityContext> create(
      AuthenticationService authentication,
      DataVerifier dataVerifier,
      MalwareScanner malwareScanner,
      UrlIngestionPolicyFilter urlPolicy,
      Policy policy,
      Clock clock) {
    return new SecurityFilterChain<>(
        List.of(
            new AuthenticationFilter<>(authentication, clock),
            new TenantIsolationFilter<>(),
            new EncryptionPolicyFilter<>(policy.allowedTlsProtocols(), policy.requireKms()),,
            urlPolicy,
            new ContentSignatureFilter(dataVerifier),
            new FilenameSanitizationFilter()));
  }

  public record Policy(
      Set<String> allowedTlsProtocols,
      boolean requireKms,
      ProtocolAbuseFilter.Limits protocolLimits,
      Map<String, Set<String>> streamTransitions,
      ArchiveSafetyFilter.Limits archiveLimits) {
    public Policy {
      allowedTlsProtocols = Set.copyOf(allowedTlsProtocols);
      streamTransitions = Map.copyOf(streamTransitions);
    }

    public static Policy defaults() {
      return new Policy(
          Set.of("TLSv1.3", "TLSv1.2"),
          true,
          new ProtocolAbuseFilter.Limits(1024 * 1024, 10_000, 128, 100),
          Map.of(
              "NEW", Set.of("RECEIVING"),
              "RECEIVING", Set.of("RECEIVING", "SPOOLED"),
              "SPOOLED", Set.of("COMPLETED")),
          new ArchiveSafetyFilter.Limits(10_000, 64L * 1024 * 1024, 256L * 1024 * 1024, 100));
    }
  }
}
