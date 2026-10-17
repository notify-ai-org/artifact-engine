package dev.notify.artifact.mcp.stdio;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpServerSession;
import io.modelcontextprotocol.spec.McpServerTransport;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Mono;

/**
 * Stdio transport that lets one process serve many JSON-RPC requests at once.
 *
 * <p>Each inbound message is handed to the session as soon as it is read, so slow tool calls do not
 * hold up the next request; the server runs tool handlers on its own pool. Responses are written
 * straight to stdout under a lock. The SDK's stdio transport routes them through a unicast sink
 * whose {@code tryEmitNext} fails when two handlers finish at the same moment, which would drop a
 * response and leave the caller waiting for its timeout.
 */
final class ConcurrentStdioServerTransportProvider implements McpServerTransportProvider {
  private static final System.Logger LOGGER =
      System.getLogger(ConcurrentStdioServerTransportProvider.class.getName());

  private final McpJsonMapper jsonMapper;
  private final InputStream input;
  private final OutputStream output;
  private final Runnable onInputClosed;
  private final AtomicBoolean closing = new AtomicBoolean(false);
  private volatile McpServerSession session;

  /** @param onInputClosed runs once stdin reaches end of file, normally because the host exited */
  ConcurrentStdioServerTransportProvider(
      McpJsonMapper jsonMapper, InputStream input, OutputStream output, Runnable onInputClosed) {
    this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
    this.input = Objects.requireNonNull(input, "input");
    this.output = Objects.requireNonNull(output, "output");
    this.onInputClosed = Objects.requireNonNull(onInputClosed, "onInputClosed");
  }

  @Override
  public void setSessionFactory(McpServerSession.Factory sessionFactory) {
    session = sessionFactory.create(new SessionTransport());
    Thread reader = new Thread(this::readLoop, "artifact-mcp-stdin");
    reader.setDaemon(true);
    reader.start();
  }

  private void readLoop() {
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      while (!closing.get() && (line = reader.readLine()) != null) {
        JSONRPCMessage message;
        try {
          message = McpSchema.deserializeJsonRpcMessage(jsonMapper, line);
        } catch (Exception malformed) {
          LOGGER.log(System.Logger.Level.WARNING, "Ignoring malformed MCP message", malformed);
          continue;
        }
        session
            .handle(message)
            .subscribe(
                null,
                failure ->
                    LOGGER.log(
                        System.Logger.Level.WARNING, "MCP message handling failed", failure));
      }
    } catch (IOException failure) {
      if (!closing.get()) {
        LOGGER.log(System.Logger.Level.WARNING, "Error reading MCP stdin", failure);
      }
    } finally {
      if (closing.compareAndSet(false, true)) {
        onInputClosed.run();
      }
    }
  }

  @Override
  public Mono<Void> notifyClients(String method, Object params) {
    McpServerSession current = session;
    return current == null
        ? Mono.error(new IllegalStateException("No session to notify"))
        : current.sendNotification(method, params);
  }

  @Override
  public Mono<Void> notifyClient(String sessionId, String method, Object params) {
    McpServerSession current = session;
    if (current == null || !current.getId().equals(sessionId)) {
      return Mono.error(new IllegalStateException("No session " + sessionId));
    }
    return current.sendNotification(method, params);
  }

  @Override
  public Mono<Void> closeGracefully() {
    closing.set(true);
    McpServerSession current = session;
    return current == null ? Mono.empty() : current.closeGracefully();
  }

  private final class SessionTransport implements McpServerTransport {
    @Override
    public Mono<Void> sendMessage(JSONRPCMessage message) {
      return Mono.fromRunnable(() -> write(message));
    }

    private void write(JSONRPCMessage message) {
      String json;
      try {
        // One message per line: escape any newline the serializer leaves in the payload.
        json =
            jsonMapper
                .writeValueAsString(message)
                .replace("\r\n", "\\n")
                .replace("\n", "\\n")
                .replace("\r", "\\n");
      } catch (IOException failure) {
        throw new IllegalStateException("Unable to serialize MCP message", failure);
      }
      byte[] line = (json + "\n").getBytes(StandardCharsets.UTF_8);
      synchronized (output) {
        try {
          output.write(line);
          output.flush();
        } catch (IOException failure) {
          if (!closing.get()) {
            throw new IllegalStateException("Unable to write MCP message", failure);
          }
        }
      }
    }

    @Override
    public <T> T unmarshalFrom(Object data, TypeRef<T> typeRef) {
      return jsonMapper.convertValue(data, typeRef);
    }

    @Override
    public Mono<Void> closeGracefully() {
      return Mono.fromRunnable(() -> closing.set(true));
    }
  }
}
