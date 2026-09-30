package dev.notify.artifact.chunk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Decides which ingest sources the server may read, and opens them.
 *
 * <p>Fetching caller-supplied locations from the server invites request forgery and local file
 * disclosure, so the defaults are strict: only {@code http}/{@code https} to publicly routable
 * addresses (no loopback, private, link-local, carrier-grade NAT, multicast, or unique-local
 * addresses, which also covers cloud metadata endpoints), no redirects, and {@code file:} only
 * beneath configured roots after resolving symbolic links.
 *
 * <p>The host is resolved when the source is vetted; a hostile DNS server could answer
 * differently when the HTTP client connects (DNS rebinding). Deployments that need a hard
 * guarantee should also route source fetches through an egress proxy that enforces the same rule.
 */
public final class SourcePolicy {
  private final List<Path> fileRoots;
  private final boolean allowPrivateNetworks;
  private final HttpClient httpClient;
  private final Duration requestTimeout;

  /**
   * @param fileRoots directories whose files may be ingested; empty disables {@code file:} sources
   * @param allowPrivateNetworks permit non-public addresses (tests, trusted internal sources)
   */
  public SourcePolicy(
      List<Path> fileRoots,
      boolean allowPrivateNetworks,
      HttpClient httpClient,
      Duration requestTimeout) {
    this.fileRoots = fileRoots.stream().map(root -> root.toAbsolutePath().normalize()).toList();
    this.allowPrivateNetworks = allowPrivateNetworks;
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
  }

  /** Vets the location and returns a source for it. */
  public ChunkSource open(String location) throws IOException {
    URI uri;
    try {
      uri = URI.create(Objects.requireNonNull(location, "location"));
    } catch (IllegalArgumentException invalid) {
      throw new SecurityException("Source location is not a valid URI");
    }
    String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
    return switch (scheme) {
      case "http", "https" -> {
        requirePublicHost(uri);
        if (uri.getUserInfo() != null) {
          throw new SecurityException("Source URLs must not embed credentials");
        }
        yield new UrlChunkSource(httpClient, uri, requestTimeout);
      }
      case "file" -> new FileChunkSource(requireAllowedFile(uri));
      default -> throw new SecurityException("Unsupported source scheme: " + scheme);
    };
  }

  private void requirePublicHost(URI uri) throws UnknownHostException {
    String host = uri.getHost();
    if (host == null || host.isBlank()) throw new SecurityException("Source URL has no host");
    if (allowPrivateNetworks) return;
    for (InetAddress address : InetAddress.getAllByName(host)) {
      if (!isPublic(address)) {
        throw new SecurityException("Source host resolves to a non-public address");
      }
    }
  }

  static boolean isPublic(InetAddress address) {
    if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
        || address.isSiteLocalAddress() || address.isMulticastAddress()) {
      return false;
    }
    byte[] bytes = address.getAddress();
    if (bytes.length == 4) {
      int first = bytes[0] & 0xff;
      int second = bytes[1] & 0xff;
      if (first == 0 || first >= 240) return false;          // "this network", reserved
      if (first == 100 && second >= 64 && second <= 127) return false; // carrier-grade NAT
      if (first == 192 && second == 0 && (bytes[2] & 0xff) == 0) return false; // IETF protocol
      if (first == 198 && (second == 18 || second == 19)) return false; // benchmarking
    } else if (bytes.length == 16) {
      if ((bytes[0] & 0xfe) == 0xfc) return false;           // unique local fc00::/7
    }
    return true;
  }

  private Path requireAllowedFile(URI uri) throws IOException {
    if (fileRoots.isEmpty()) throw new SecurityException("File sources are disabled");
    Path requested = Path.of(uri);
    if (!Files.isRegularFile(requested)) throw new SecurityException("Source file does not exist");
    Path real = requested.toRealPath();
    for (Path root : fileRoots) {
      if (Files.exists(root) && real.startsWith(root.toRealPath())) return real;
    }
    throw new SecurityException("Source file is outside the allowed roots");
  }
}
