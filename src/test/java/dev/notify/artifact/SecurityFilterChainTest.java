package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.security.ArchiveSafetyFilter;
import dev.notify.artifact.security.AuthenticationService;
import dev.notify.artifact.security.ContentSignatureFilter;
import dev.notify.artifact.security.IngestionSecurityContext;
import dev.notify.artifact.security.IngestionSecurityFilters;
import dev.notify.artifact.security.MalwareScanner;
import dev.notify.artifact.security.ProtocolAbuseFilter;
import dev.notify.artifact.security.RetrievalScanGateFilter;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.RetrievalSecurityFilters;
import dev.notify.artifact.security.SecurityFilterException;
import dev.notify.artifact.security.SecurityIdentity;
import dev.notify.artifact.security.StorageEncryption;
import dev.notify.artifact.security.TransportFacts;
import dev.notify.artifact.security.UrlIngestionPolicyFilter;
import dev.notify.artifact.util.Checksum;
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecurityFilterChainTest {
  private static final Instant NOW = Instant.parse("2026-08-10T00:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

  @TempDir Path directory;

  @Test
  void executesCanonicalIngestionFiltersAndDerivesSafeOutputs() throws Exception {
    Path content = directory.resolve("content.pending");
    Files.writeString(content, "<html><script>bad()</script><p>safe</p></html>");
    AtomicInteger scans = new AtomicInteger();
    var chain =
        IngestionSecurityFilters.create(
            authenticator("INGEST"),
            new DataVerifier(),
            path -> {
              scans.incrementAndGet();
              return new MalwareScanner.ScanResult(
                  MalwareScanner.Verdict.CLEAN, "test-scanner", null);
            },
            urlPolicy(),
            IngestionSecurityFilters.Policy.defaults(),
            CLOCK);
    IngestionSecurityContext context =
        new IngestionSecurityContext(
            ingestRequest("../dangerous\u202Etxt.exe", "text/html"),
            content,
            "session-1",
            transport("NEW", "RECEIVING"),
            new StorageEncryption(true, true, "alias/artifacts"),
            null);

    chain.verify(context);

    assertEquals(
        java.util.List.of(
            "authentication",
            "tenant-isolation",
            "encryption-policy",
            "protocol-abuse",
            "url-ingestion-policy",
            "content-signature",
            "archive-safety",
            "malware-scan",
            "filename-sanitization"),
        chain.filterNames());
    assertEquals("text/html", context.detectedMediaType());
    assertFalse(context.sanitizedFilename().contains(".."));
    assertEquals(MalwareScanner.Verdict.CLEAN, context.scanVerdict());
    assertEquals(1, scans.get());
  }

  @Test
  void authenticationRejectionShortCircuitsBeforeMalwareScanning() throws Exception {
    Path content = directory.resolve("content.pending");
    Files.writeString(content, "hello");
    AtomicInteger scans = new AtomicInteger();
    var chain =
        IngestionSecurityFilters.create(
            ignored -> null,
            new DataVerifier(),
            path -> {
              scans.incrementAndGet();
              return new MalwareScanner.ScanResult(MalwareScanner.Verdict.CLEAN, "test", null);
            },
            urlPolicy(),
            IngestionSecurityFilters.Policy.defaults(),
            CLOCK);
    IngestionSecurityContext context =
        new IngestionSecurityContext(
            ingestRequest("notes.txt", "text/plain"),
            content,
            "invalid",
            transport("NEW", "RECEIVING"),
            new StorageEncryption(true, true, "alias/artifacts"),
            null);

    SecurityFilterException rejection =
        assertThrows(SecurityFilterException.class, () -> chain.verify(context));

    assertEquals("AUTHENTICATION_EXPIRED", rejection.code());
    assertEquals(0, scans.get());
  }

  @Test
  void rejectsUrlThatResolvesToLoopback() throws Exception {
    Path content = directory.resolve("content.pending");
    Files.writeString(content, "hello");
    var chain =
        IngestionSecurityFilters.create(
            authenticator("INGEST"),
            new DataVerifier(),
            path -> new MalwareScanner.ScanResult(MalwareScanner.Verdict.CLEAN, "test", null),
            new UrlIngestionPolicyFilter(
                Set.of("https"),
                Set.of("approved.example"),
                host -> java.util.List.of(InetAddress.getByName("127.0.0.1"))),
            IngestionSecurityFilters.Policy.defaults(),
            CLOCK);
    IngestionSecurityContext context =
        new IngestionSecurityContext(
            ingestRequest("notes.txt", "text/plain"),
            content,
            "session-1",
            transport("NEW", "RECEIVING"),
            new StorageEncryption(true, true, "alias/artifacts"),
            URI.create("https://approved.example/artifact"));

    SecurityFilterException rejection =
        assertThrows(SecurityFilterException.class, () -> chain.verify(context));

    assertEquals("URL_PRIVATE_DESTINATION", rejection.code());
  }

  @Test
  void retrievalRequiresCleanTenantBoundArtifactAndSanitizesHtml() {
    Artifact artifact = htmlArtifact("tenant-a", MalwareScanner.Verdict.CLEAN.name());
    RetrievalSecurityContext context =
        new RetrievalSecurityContext(
            "principal-a",
            "tenant-a",
            "session-1",
            "READ_TEXT",
            artifact,
            transport("READY", "READING"),
            "<p>Hello</p><script>steal()</script>");
    var chain =
        RetrievalSecurityFilters.create(
            authenticator("READ_TEXT"), RetrievalSecurityFilters.Policy.defaults(), CLOCK);

    chain.verify(context);

    assertEquals("Hello", context.extractedContent());
  }

  @Test
  void rejectsDocxCompressionBombBeforeMalwareScanning() throws Exception {
    Path archive = directory.resolve("bomb.docx");
    try (var output = new ZipOutputStream(Files.newOutputStream(archive))) {
      writeEntry(output, "[Content_Types].xml", "<Types/>");
      writeEntry(output, "word/document.xml", "x".repeat(100_000));
    }
    IngestionSecurityContext context =
        new IngestionSecurityContext(
            ingestRequest("bomb.docx", DataVerifier.DOCX),
            archive,
            "session-1",
            transport("NEW", "RECEIVING"),
            new StorageEncryption(true, true, "alias/artifacts"),
            null);
    new ContentSignatureFilter(new DataVerifier()).verify(context);
    ArchiveSafetyFilter filter =
        new ArchiveSafetyFilter(new ArchiveSafetyFilter.Limits(10, 200_000, 200_000, 2));

    SecurityFilterException rejection =
        assertThrows(SecurityFilterException.class, () -> filter.verify(context));

    assertEquals("ARCHIVE_RATIO_LIMIT", rejection.code());
  }

  @Test
  void rejectsSlowlorisTransferFacts() throws Exception {
    Path content = directory.resolve("partial.pending");
    Files.writeString(content, "x");
    IngestionSecurityContext context =
        new IngestionSecurityContext(
            ingestRequest("notes.txt", "text/plain"),
            content,
            "session-1",
            new TransportFacts(
                true, "TLSv1.3", "TLS_AES_256_GCM_SHA384", 1, 10, 20_000, 0, 0, "NEW", "RECEIVING"),
            new StorageEncryption(true, true, "alias/artifacts"),
            null);
    ProtocolAbuseFilter<IngestionSecurityContext> filter =
        new ProtocolAbuseFilter<>(
            new ProtocolAbuseFilter.Limits(1024, 1000, 100, 10),
            Map.of("NEW", Set.of("RECEIVING")));

    SecurityFilterException rejection =
        assertThrows(SecurityFilterException.class, () -> filter.verify(context));

    assertEquals("SLOWLORIS_REJECTED", rejection.code());
  }

  private static AuthenticationService authenticator(String permission) {
    return handle ->
        new SecurityIdentity("principal-a", "tenant-a", Set.of(permission), NOW.plusSeconds(3600));
  }

  private static UrlIngestionPolicyFilter urlPolicy() {
    return new UrlIngestionPolicyFilter(
        Set.of("https"), Set.of(), host -> java.util.List.of(InetAddress.getByName("8.8.8.8")));
  }

  private static TransportFacts transport(String previous, String requested) {
    return new TransportFacts(
        true,
        "TLSv1.3",
        "TLS_AES_256_GCM_SHA384",
        1024,
        4096,
        1000,
        512,
        4096,
        previous,
        requested);
  }

  private static Requests.Ingest ingestRequest(String filename, String mediaType) {
    byte[] content = "placeholder".getBytes(StandardCharsets.UTF_8);
    return new Requests.Ingest(
        "tenant-a",
        "principal-a",
        "key",
        filename,
        mediaType,
        new ByteArrayInputStream(content),
        content.length,
        Map.of());
  }

  private static Artifact htmlArtifact(String tenantId, String scanStatus) {
    Instant now = NOW;
    return new Artifact(
        "artifact-id",
        tenantId,
        "key",
        "fingerprint",
        "UPLOAD",
        null,
        "page.html",
        "text/html",
        100,
        "checksum",
        "prod/" + Checksum.sha256(tenantId) + "/document/2026/08/artifact-id/1/content",
        Path.of("/tmp/content.pending"),
        ArtifactStatus.Storage.STORED,
        ArtifactStatus.Index.READY,
        1,
        Map.of(RetrievalScanGateFilter.SCAN_STATUS_METADATA, scanStatus),
        null,
        null,
        now,
        now);
  }

  private static void writeEntry(ZipOutputStream output, String name, String content)
      throws java.io.IOException {
    output.putNextEntry(new ZipEntry(name));
    output.write(content.getBytes(StandardCharsets.UTF_8));
    output.closeEntry();
  }
}
