package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryStores;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IngestJobTest {
  @TempDir Path spoolRoot;

  private InMemoryStores.Metadata metadata;
  private DurableSpool spool;

  @BeforeEach
  void setUp() throws IOException {
    metadata = new InMemoryStores.Metadata();
    spool = new DurableSpool(spoolRoot, 1 << 20, new ObjectMapper());
  }

  @Test
  void executeWithoutPrepareStillPerformsTheWholeIntake() throws Exception {
    Artifact artifact = job(text("hello world"), allowAll()).execute();

    assertEquals("text/plain", artifact.mediaType());
    assertEquals(1, spool.entries().size());
  }

  @Test
  void abandoningAPreparedJobDiscardsItsSpoolEntry() throws Exception {
    IngestJob job = job(text("hello world"), allowAll());
    job.prepare();
    assertEquals(1, spool.entries().size());

    job.abandon();

    assertTrue(spool.entries().isEmpty());
    assertEquals(0, spool.usage().bytes());
    assertThrows(IllegalStateException.class, job::execute);
  }

  @Test
  void abandonAfterExecuteStartedLeavesTheArtifactAlone() throws Exception {
    IngestJob job = job(text("hello world"), allowAll());
    job.prepare();
    job.execute();

    job.abandon();

    assertEquals(1, spool.entries().size());
  }

  @Test
  void rejectsUnauthorizedCallersBeforeReadingTheBody() {
    InputStream untouchable =
        new InputStream() {
          @Override
          public int read() {
            return fail("body must not be read before authorization");
          }
        };
    AuthorizationService denyAll =
        (principal, tenant, permission) -> {
          throw new SecurityException("denied");
        };

    assertThrows(SecurityException.class, () -> job(untouchable, denyAll).prepare());
  }

  private IngestJob job(InputStream content, AuthorizationService authorization) {
    return new IngestJob(
        new Requests.Ingest(
            "tenant", "principal", null, "notes.txt", "text/plain", content, -1, Map.of()),
        metadata,
        spool,
        new ArtifactAccessVerifier(authorization, new DataVerifier()),
        true,
        0);
  }

  private static AuthorizationService allowAll() {
    return (principal, tenant, permission) -> {};
  }

  private static InputStream text(String value) {
    return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
  }
}
