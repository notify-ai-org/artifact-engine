package dev.notify.artifact.chunk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChunkSourceTest {
  private static final byte[] BODY = "0123456789abcdefghij".getBytes(StandardCharsets.US_ASCII);

  @TempDir Path directory;

  private HttpServer server;
  private final AtomicReference<String> etag = new AtomicReference<>("\"v1\"");
  private final AtomicReference<Boolean> ranges = new AtomicReference<>(true);
  private final java.util.concurrent.atomic.AtomicInteger throttledReads =
      new java.util.concurrent.atomic.AtomicInteger();

  @AfterEach
  void stop() {
    if (server != null) server.stop(0);
  }

  @Test
  void readsUrlChunksWithRangeRequests() throws Exception {
    ChunkSource source = policy(true).open(startServer());

    ChunkSource.SourceInfo info = source.describe();
    ByteBuffer chunk = ByteBuffer.allocate(5);
    source.read(10, chunk, info.version());

    assertEquals(20, info.length());
    assertEquals("\"v1\"", info.version());
    assertEquals("abcde", new String(chunk.array(), StandardCharsets.US_ASCII));
  }

  @Test
  void waitsOutThrottlingInsteadOfFailingTheChunk() throws Exception {
    throttledReads.set(2);
    ChunkSource source = policy(true).open(startServer());
    String version = source.describe().version();

    ByteBuffer chunk = ByteBuffer.allocate(5);
    source.read(0, chunk, version);

    assertEquals("01234", new String(chunk.array(), StandardCharsets.US_ASCII));
    assertEquals(0, throttledReads.get(), "both throttled answers were retried");
  }

  @Test
  void refusesUrlsThatCannotServeRanges() throws Exception {
    ranges.set(false);
    ChunkSource source = policy(true).open(startServer());

    assertThrows(UrlChunkSource.UnsupportedSourceException.class, source::describe);
  }

  @Test
  void failsInsteadOfMixingVersionsWhenTheUrlChanges() throws Exception {
    ChunkSource source = policy(true).open(startServer());
    String planned = source.describe().version();
    etag.set("\"v2\"");

    assertThrows(ChunkSource.SourceChangedException.class,
        () -> source.read(0, ByteBuffer.allocate(5), planned));
  }

  @Test
  void readsFileChunksAndDetectsChanges() throws Exception {
    Path file = Files.write(directory.resolve("doc.txt"), BODY);
    ChunkSource source = policy(false).open(file.toUri().toString());
    String version = source.describe().version();

    ByteBuffer chunk = ByteBuffer.allocate(4);
    source.read(16, chunk, version);
    assertEquals("ghij", new String(chunk.array(), StandardCharsets.US_ASCII));

    Files.write(file, "changed".getBytes(StandardCharsets.US_ASCII));
    assertThrows(ChunkSource.SourceChangedException.class,
        () -> source.read(0, ByteBuffer.allocate(4), version));
  }

  @Test
  void blocksPrivateAddressesAndFilesOutsideTheRoots() throws Exception {
    SourcePolicy strict = policy(false);

    assertThrows(SecurityException.class, () -> strict.open("http://127.0.0.1/secret"));
    assertThrows(SecurityException.class, () -> strict.open("http://169.254.169.254/latest"));
    assertThrows(SecurityException.class, () -> strict.open("http://10.1.2.3/x"));
    assertThrows(SecurityException.class, () -> strict.open("http://user:pw@example.com/x"));
    assertThrows(SecurityException.class, () -> strict.open("ftp://example.com/x"));
    Path outside = Files.createTempFile("outside", ".txt");
    try {
      assertThrows(SecurityException.class, () -> strict.open(outside.toUri().toString()));
    } finally {
      Files.deleteIfExists(outside);
    }
  }

  @Test
  void classifiesAddresses() throws Exception {
    assertFalse(SourcePolicy.isPublic(InetAddress.getByName("192.168.1.1")));
    assertFalse(SourcePolicy.isPublic(InetAddress.getByName("100.64.0.1")));
    assertFalse(SourcePolicy.isPublic(InetAddress.getByName("fd00::1")));
    assertTrue(SourcePolicy.isPublic(InetAddress.getByName("93.184.216.34")));
  }

  private SourcePolicy policy(boolean allowPrivate) {
    return new SourcePolicy(List.of(directory), allowPrivate,
        UrlChunkSource.defaultClient(Duration.ofSeconds(5)), Duration.ofSeconds(5));
  }

  /** Minimal range-capable server honouring If-Range against the current ETag. */
  private String startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/file", exchange -> {
      exchange.getResponseHeaders().set("ETag", etag.get());
      if (ranges.get()) exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
      String range = exchange.getRequestHeaders().getFirst("Range");
      String ifRange = exchange.getRequestHeaders().getFirst("If-Range");
      boolean honourRange = range != null && (ifRange == null || ifRange.equals(etag.get()));
      if (!"HEAD".equals(exchange.getRequestMethod())
          && throttledReads.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
        exchange.getResponseHeaders().set("Retry-After", "0");
        exchange.sendResponseHeaders(429, -1);
      } else if ("HEAD".equals(exchange.getRequestMethod())) {
        exchange.getResponseHeaders().set("Content-Length", Integer.toString(BODY.length));
        exchange.sendResponseHeaders(200, -1);
      } else if (honourRange) {
        String[] bounds = range.substring("bytes=".length()).split("-");
        int start = Integer.parseInt(bounds[0]);
        int end = Integer.parseInt(bounds[1]);
        exchange.getResponseHeaders().set("Content-Range",
            "bytes " + start + "-" + end + "/" + BODY.length);
        exchange.sendResponseHeaders(206, end - start + 1);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(BODY, start, end - start + 1);
        }
      } else {
        exchange.sendResponseHeaders(200, BODY.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(BODY);
        }
      }
      exchange.close();
    });
    server.start();
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/file";
  }
}
