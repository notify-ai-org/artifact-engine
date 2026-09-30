package dev.notify.artifact.chunk;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * An HTTP(S) resource read with {@code Range} requests.
 *
 * <p>{@link #describe()} requires a {@code Content-Length} and {@code Accept-Ranges: bytes}. Each
 * chunk read sends {@code If-Range} with the ETag (or Last-Modified) seen at planning: a server
 * whose content changed answers 200 with the whole new body instead of 206, and the read fails
 * with {@link SourceChangedException} rather than mixing versions. Redirects are not followed so
 * the address vetted by {@link SourcePolicy} is the one fetched. Throttled requests (429, 503)
 * are retried after the server's Retry-After, or with exponential backoff.
 */
public final class UrlChunkSource implements ChunkSource {
  private final HttpClient client;
  private final URI uri;
  private final Duration timeout;

  public UrlChunkSource(HttpClient client, URI uri, Duration timeout) {
    this.client = Objects.requireNonNull(client, "client");
    this.uri = Objects.requireNonNull(uri, "uri");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
  }

  /** A client that never follows redirects; share one per process. */
  public static HttpClient defaultClient(Duration connectTimeout) {
    return HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NEVER)
        .connectTimeout(connectTimeout)
        .build();
  }

  @Override
  public SourceInfo describe() throws IOException {
    HttpResponse<Void> response =
        send(HttpRequest.newBuilder(uri).timeout(timeout)
            .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
            HttpResponse.BodyHandlers.discarding());
    if (response.statusCode() != 200) {
      throw new IOException("Source answered HEAD with HTTP " + response.statusCode());
    }
    long length = response.headers().firstValueAsLong("content-length").orElse(-1);
    if (length < 0) {
      throw new UnsupportedSourceException("Source does not report a Content-Length");
    }
    boolean ranges = response.headers().firstValue("accept-ranges")
        .map(value -> value.toLowerCase(Locale.ROOT).contains("bytes"))
        .orElse(false);
    if (!ranges) {
      throw new UnsupportedSourceException("Source does not accept byte-range requests");
    }
    String version = validator(response).orElse(null);
    return new SourceInfo(length, version);
  }

  @Override
  public void read(long offset, ByteBuffer target, String expectedVersion) throws IOException {
    int length = target.remaining();
    if (length == 0) return;
    long last = offset + length - 1;
    HttpRequest.Builder request = HttpRequest.newBuilder(uri).timeout(timeout).GET()
        .header("Range", "bytes=" + offset + "-" + last);
    if (expectedVersion != null) request.header("If-Range", expectedVersion);
    HttpResponse<InputStream> response =
        send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
    try (InputStream body = response.body()) {
      if (response.statusCode() == 200) {
        throw new SourceChangedException("Source changed during ingest (range not honoured)");
      }
      if (response.statusCode() != 206) {
        throw new IOException("Source answered a range request with HTTP "
            + response.statusCode());
      }
      String contentRange = response.headers().firstValue("content-range").orElse("");
      if (!contentRange.startsWith("bytes " + offset + "-" + last + "/")) {
        throw new IOException("Source returned range '" + contentRange + "' for bytes "
            + offset + "-" + last);
      }
      byte[] copy = new byte[64 * 1024];
      while (target.hasRemaining()) {
        int read = body.read(copy, 0, Math.min(copy.length, target.remaining()));
        if (read < 0) {
          throw new IOException("Source ended the range after " + (length - target.remaining())
              + " of " + length + " bytes");
        }
        target.put(copy, 0, read);
      }
    }
  }

  private static Optional<String> validator(HttpResponse<?> response) {
    Optional<String> etag = response.headers().firstValue("etag")
        .filter(value -> !value.startsWith("W/"));
    return etag.isPresent() ? etag : response.headers().firstValue("last-modified");
  }

  /**
   * Sends the request, waiting out throttling (429, 503) a few times before giving up. Servers
   * that serve large files often cap concurrent connections per client, and parallel chunk reads
   * hit that cap; waiting here keeps a throttled chunk from using up its job's retry attempts.
   */
  private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
      throws IOException {
    try {
      for (int attempt = 1; ; attempt++) {
        HttpResponse<T> response = client.send(request, handler);
        int status = response.statusCode();
        if ((status != 429 && status != 503) || attempt == THROTTLE_ATTEMPTS) return response;
        if (response.body() instanceof InputStream body) body.close();
        Thread.sleep(throttleDelay(response, attempt).toMillis());
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new java.io.InterruptedIOException("Interrupted while reading " + uri.getHost());
    }
  }

  private static final int THROTTLE_ATTEMPTS = 6;
  private static final Duration MAX_THROTTLE_DELAY = Duration.ofSeconds(30);

  /** Retry-After in seconds when given, otherwise 1, 2, 4, ... seconds; capped either way. */
  private static Duration throttleDelay(HttpResponse<?> response, int attempt) {
    Duration delay = response.headers().firstValue("retry-after")
        .flatMap(value -> {
          try {
            return Optional.of(Duration.ofSeconds(Long.parseLong(value.trim())));
          } catch (NumberFormatException httpDate) {
            return Optional.empty();
          }
        })
        .orElse(Duration.ofSeconds(1L << (attempt - 1)));
    return delay.compareTo(MAX_THROTTLE_DELAY) > 0 ? MAX_THROTTLE_DELAY : delay;
  }

  /** The source cannot be read in ranges; ingest it through the upload path instead. */
  public static final class UnsupportedSourceException extends IOException {
    public UnsupportedSourceException(String message) {
      super(message);
    }
  }
}
