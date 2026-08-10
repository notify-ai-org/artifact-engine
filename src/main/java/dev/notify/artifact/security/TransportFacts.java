package dev.notify.artifact.security;

/** Trusted, connection-bound transport measurements consumed by protocol security filters. */
public record TransportFacts(
    boolean tls,
    String tlsProtocol,
    String cipherSuite,
    long frameBytes,
    long bytesReceived,
    long elapsedMillis,
    long compressedBytes,
    long decompressedBytes,
    String previousStreamState,
    String requestedStreamState) {
  public TransportFacts {
    if (frameBytes < 0
        || bytesReceived < 0
        || elapsedMillis < 0
        || compressedBytes < 0
        || decompressedBytes < 0) {
      throw new IllegalArgumentException("Transport measurements must not be negative");
    }
  }

  /** Trusted facts for an in-process operation that does not cross a network boundary. */
  public static TransportFacts internal() {
    return new TransportFacts(
        true, "INTERNAL", "INTERNAL", 0, 0, 0, 0, 0, "INTERNAL", "INTERNAL");
  }
}
