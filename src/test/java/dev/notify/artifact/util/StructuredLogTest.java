package dev.notify.artifact.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StructuredLogTest {
  @Test
  void rendersKeyValuePairsWithoutLocaleFormatting() {
    assertEquals(
        "event=part_uploaded artifact=a-1 bytes=5368709120 duration=1532ms reason=-",
        StructuredLog.format("part_uploaded", "artifact", "a-1", "bytes", 5_368_709_120L,
            "duration", Duration.ofMillis(1532), "reason", null));
  }

  @Test
  void quotesAndFlattensFreeText() {
    assertEquals(
        "event=failed reason=\"S3 said \\\"slow down\\\" twice\"",
        StructuredLog.format("failed", "reason", "S3 said \"slow down\"\n twice"));
  }

  @Test
  void truncatesLongValuesAndFlagsAnUnpairedKey() {
    String line = StructuredLog.format("x", "reason", "a".repeat(1000), "dangling");

    assertTrue(line.contains("a".repeat(300) + "..."), line);
    assertTrue(line.endsWith(" dangling=?"), line);
  }

  @Test
  void reportsThroughputInMibPerSecond() {
    assertEquals("64.0", StructuredLog.mibPerSecond(64L * 1024 * 1024, Duration.ofSeconds(1)));
    assertEquals("0.0", StructuredLog.mibPerSecond(0, Duration.ZERO));
  }
}
