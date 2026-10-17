package dev.notify.artifact.mcp.stdio;

import dev.notify.artifact.mcp.McpArtifactGateway;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Resolves the gateway session for one MCP request.
 *
 * <p>The tenant and the scope ceiling are bound to the process when it starts. The trusted host
 * sends the calling principal, and optionally narrower scopes, in the request's {@code _meta};
 * tool arguments can never change either. A request without {@code _meta} uses the process
 * defaults.
 */
public final class McpRequestIdentity {
  public static final String PRINCIPAL_META = "dev.notify-ai/principalId";
  public static final String TENANT_META = "dev.notify-ai/tenantId";
  public static final String SCOPES_META = "dev.notify-ai/scopes";
  private static final String ALL_SCOPES = "artifact.*";

  private final McpArtifactGateway.Session process;

  public McpRequestIdentity(McpArtifactGateway.Session process) {
    this.process = Objects.requireNonNull(process, "process");
  }

  /** @throws SecurityException when the request names another tenant or an invalid principal */
  public McpArtifactGateway.Session resolve(Map<String, Object> meta) {
    if (meta == null || meta.isEmpty()) {
      return process;
    }
    Object tenant = meta.get(TENANT_META);
    if (tenant != null && !process.tenantId().equals(tenant)) {
      throw new SecurityException("The request is for a different tenant than this process");
    }
    Object principal = meta.get(PRINCIPAL_META);
    String principalId = process.principalId();
    if (principal != null) {
      if (!(principal instanceof String value) || !validIdentity(value)) {
        throw new SecurityException("The request principal is invalid");
      }
      principalId = value.trim();
    }
    Object requested = meta.get(SCOPES_META);
    Set<String> scopes = requested == null ? process.scopes() : narrow(requested);
    return new McpArtifactGateway.Session(principalId, process.tenantId(), scopes);
  }

  /** Keeps only the requested scopes that the process ceiling grants. */
  private Set<String> narrow(Object requested) {
    Collection<?> values =
        requested instanceof Collection<?> collection
            ? collection
            : requested instanceof String text ? Set.of(text.split(",")) : Set.of();
    boolean ceilingAll = process.scopes().contains(ALL_SCOPES);
    Set<String> granted = new LinkedHashSet<>();
    for (Object value : values) {
      if (!(value instanceof String raw)) continue;
      String scope = raw.trim();
      if (ALL_SCOPES.equals(scope)) {
        granted.addAll(process.scopes());
      } else if (ceilingAll || process.scopes().contains(scope)) {
        granted.add(scope);
      }
    }
    return granted;
  }

  private static boolean validIdentity(String value) {
    return !value.isBlank()
        && value.length() <= 256
        && value.chars().noneMatch(character -> character < 0x20 || character == 0x7f);
  }
}
