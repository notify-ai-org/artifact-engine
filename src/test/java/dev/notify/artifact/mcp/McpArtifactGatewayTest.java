package dev.notify.artifact.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.notify.artifact.ArtifactEngine;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.Requests;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class McpArtifactGatewayTest {

  @Test
  void contentReadsAreBoundedAndResumeAtTheReturnedOffset() throws Exception {
    RecordingEngine engine = new RecordingEngine("abcdefghij");
    McpArtifactGateway gateway = new McpArtifactGateway(engine, 100, 4);
    McpArtifactGateway.Session session =
        new McpArtifactGateway.Session("principal-a", "tenant-a", Set.of("artifact.content"));

    McpArtifactGateway.ContentChunkResult first =
        gateway.getArtifactContentChunk(session, "artifact-1", 2, 1_000);
    McpArtifactGateway.ContentChunkResult second =
        gateway.getArtifactContentChunk(session, "artifact-1", first.nextOffset(), 4);

    assertEquals("cdef", decode(first.base64()));
    assertEquals(6, first.nextOffset());
    assertFalse(first.endOfFile());
    assertEquals("ghij", decode(second.base64()));
    assertEquals(10, second.nextOffset());
    assertTrue(second.endOfFile());
    assertEquals("tenant-a", engine.lastTenant);
    assertEquals("principal-a", engine.lastPrincipal);
  }

  @Test
  void gatewayRequiresAnExplicitReadScope() {
    RecordingEngine engine = new RecordingEngine("content");
    McpArtifactGateway gateway = new McpArtifactGateway(engine, 100, 4);
    McpArtifactGateway.Session session =
        new McpArtifactGateway.Session("principal-a", "tenant-a", Set.of("artifact.search"));

    assertThrows(
        SecurityException.class,
        () -> gateway.getArtifactMetadata(session, "artifact-1"));
    assertEquals(null, engine.lastTenant);
  }

  private static String decode(String base64) {
    return new String(Base64.getDecoder().decode(base64), StandardCharsets.UTF_8);
  }

  public static final class RecordingEngine implements ArtifactEngine {
    private final byte[] content;
    public String lastPrincipal;
    public String lastTenant;

    public RecordingEngine(String content) {
      this.content = content.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Artifact ingest(Requests.Ingest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Artifact metadata(String principalId, String tenantId, String artifactId) {
      record(principalId, tenantId);
      Instant now = Instant.parse("2026-08-10T00:00:00Z");
      return new Artifact(
          artifactId,
          tenantId,
          "key",
          "fingerprint",
          "UPLOAD",
          null,
          "notes.txt",
          "text/plain",
          content.length,
          "sha256",
          "tenants/" + tenantId + "/artifacts/" + artifactId,
          Path.of("spool", artifactId),
          ArtifactStatus.Storage.STORED,
          ArtifactStatus.Index.READY,
          1,
          Map.of(),
          null,
          null,
          now,
          now);
    }

    @Override
    public InputStream content(String principalId, String tenantId, String artifactId) {
      record(principalId, tenantId);
      return new ByteArrayInputStream(content);
    }

    @Override
    public String extractedText(
        String principalId, String tenantId, String artifactId, int maxCharacters) {
      record(principalId, tenantId);
      String value = new String(content, StandardCharsets.UTF_8);
      return value.substring(0, Math.min(value.length(), maxCharacters));
    }

    @Override
    public List<Requests.SearchHit> search(Requests.Search request) {
      record(request.principalId(), request.tenantId());
      return List.of();
    }

    @Override
    public void delete(String principalId, String tenantId, String artifactId) throws IOException {
      throw new UnsupportedOperationException();
    }

    private void record(String principalId, String tenantId) {
      this.lastPrincipal = principalId;
      this.lastTenant = tenantId;
    }
  }
}
