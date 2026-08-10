package dev.notify.artifact.security;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** SSRF defense for URL ingestion: scheme/host allowlists and public-address resolution. */
public final class UrlIngestionPolicyFilter implements SecurityFilter<IngestionSecurityContext> {
  private final Set<String> approvedSchemes;
  private final Set<String> approvedHosts;
  private final HostResolver resolver;

  public UrlIngestionPolicyFilter(
      Set<String> approvedSchemes, Set<String> approvedHosts, HostResolver resolver) {
    this.approvedSchemes = lowerCase(approvedSchemes);
    this.approvedHosts = lowerCase(approvedHosts);
    this.resolver = resolver;
  }

  @Override
  public String name() {
    return "url-ingestion-policy";
  }

  @Override
  public void verify(IngestionSecurityContext context) {
    URI uri = context.sourceUri();
    if (uri == null) {
      return;
    }
    String scheme = lower(uri.getScheme());
    String host = lower(uri.getHost());
    if (!approvedSchemes.contains(scheme) || host == null) {
      reject("URL_SCHEME_REJECTED", "URL scheme or host is not allowed");
    }
    if (approvedHosts.isEmpty() || !approvedHosts.contains(host)) {
      reject("URL_HOST_REJECTED", "URL destination is not approved");
    }
    if (uri.getUserInfo() != null || uri.getFragment() != null) {
      reject("URL_COMPONENT_REJECTED", "URL contains prohibited components");
    }

    List<InetAddress> addresses;
    try {
      addresses = resolver.resolve(host);
    } catch (UnknownHostException failure) {
      throw new SecurityFilterException(
          "URL_RESOLUTION_FAILED", name(), "URL destination could not be resolved", failure);
    }
    if (addresses.isEmpty() || addresses.stream().anyMatch(UrlIngestionPolicyFilter::notPublic)) {
      reject("URL_PRIVATE_DESTINATION", "URL destination resolves to a prohibited network");
    }
    context.approvedSourceAddresses(addresses);
  }

  private static boolean notPublic(InetAddress address) {
    byte[] bytes = address.getAddress();
    boolean carrierGradeNat =
        bytes.length == 4
            && Byte.toUnsignedInt(bytes[0]) == 100
            && (Byte.toUnsignedInt(bytes[1]) & 0xc0) == 64;
    boolean uniqueLocalV6 = bytes.length == 16 && (Byte.toUnsignedInt(bytes[0]) & 0xfe) == 0xfc;
    return address.isAnyLocalAddress()
        || address.isLoopbackAddress()
        || address.isLinkLocalAddress()
        || address.isSiteLocalAddress()
        || address.isMulticastAddress()
        || carrierGradeNat
        || uniqueLocalV6;
  }

  private static Set<String> lowerCase(Set<String> values) {
    return values.stream()
        .map(UrlIngestionPolicyFilter::lower)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static String lower(String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }

  private void reject(String code, String message) {
    throw new SecurityFilterException(code, name(), message);
  }

  @FunctionalInterface
  public interface HostResolver {
    List<InetAddress> resolve(String host) throws UnknownHostException;

    static HostResolver system() {
      return SystemHostResolver.INSTANCE;
    }
  }
}
