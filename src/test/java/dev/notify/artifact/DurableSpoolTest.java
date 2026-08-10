package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.spool.DurableSpool;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableSpoolTest {
  @TempDir Path root;

  @Test
  void streamsAndPublishesContentWithChecksum() throws Exception {
    var spool = new DurableSpool(root, 100, new ObjectMapper());
    var result =
        spool.write(
            "tenant/../../x",
            "generated-id",
            new ByteArrayInputStream("hello".getBytes()),
            Map.of("name", "x"));
    assertEquals(5, result.sizeBytes());
    assertTrue(Files.exists(result.contentPath()));
    assertTrue(result.contentPath().startsWith(root));
    assertFalse(result.contentPath().toString().contains("tenant/../../x"));
  }

  @Test
  void enforcesLimitAndRemovesTemporaryFile() throws Exception {
    var spool = new DurableSpool(root, 2, new ObjectMapper());
    assertThrows(
        java.io.IOException.class,
        () -> spool.write("t", "a", new ByteArrayInputStream("long".getBytes()), Map.of()));
    try (var files = Files.walk(root)) {
      assertFalse(files.anyMatch(p -> p.toString().endsWith(".tmp")));
    }
  }
}
