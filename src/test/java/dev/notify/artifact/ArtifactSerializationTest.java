package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArtifactSerializationTest {
  @Test
  void roundTripsJpaJsonEnvelope() throws Exception {
    Instant timestamp = Instant.parse("2026-08-10T00:00:00Z");
    Artifact source =
        new Artifact(
            "artifact-id",
            "tenant-id",
            "key",
            "fingerprint",
            "UPLOAD",
            null,
            "notes.txt",
            "text/plain",
            5,
            "checksum",
            null,
            Path.of("/tmp/spool/content.pending"),
            ArtifactStatus.Storage.SPOOLED,
            ArtifactStatus.Index.PENDING,
            1,
            Map.of("tag", "test"),
            null,
            null,
            timestamp,
            timestamp);
    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    Artifact restored = mapper.readValue(mapper.writeValueAsBytes(source), Artifact.class);

    assertEquals(source, restored);
  }
}
