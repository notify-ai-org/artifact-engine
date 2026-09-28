package dev.notify.artifact.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeadLetterQueueTest {
  @Test
  void pushIsIdempotentPerWorkflowAndListsOldestFirst() {
    InMemoryDeadLetterQueue queue = new InMemoryDeadLetterQueue();
    DeadLetter first = letter("wf-1", "first failure");
    queue.push(first);
    queue.push(letter("wf-2", "other"));

    DeadLetter stored = queue.push(letter("wf-1", "pushed again"));

    assertSame(first, stored);
    assertEquals(2, queue.size());
    assertEquals(List.of("wf-1", "wf-2"), queue.list(10).stream().map(DeadLetter::id).toList());
    assertEquals(List.of("wf-1"), queue.list(1).stream().map(DeadLetter::id).toList());
  }

  @Test
  void removeTakesALetterOutForReplayOrDiscard() {
    InMemoryDeadLetterQueue queue = new InMemoryDeadLetterQueue();
    queue.push(letter("wf-1", "boom"));

    assertEquals("wf-1", queue.remove("wf-1").orElseThrow().id());
    assertTrue(queue.find("wf-1").isEmpty());
    assertTrue(queue.remove("wf-1").isEmpty());
  }

  @Test
  void compositeHandlerRunsEveryHandlerAndRethrowsTheFirstFailure() {
    List<String> ran = new ArrayList<>();
    DeadLetterHandler handler = DeadLetterHandler.composite(List.of(
        letter -> { ran.add("a"); throw new IllegalStateException("a failed"); },
        letter -> ran.add("b"),
        letter -> { ran.add("c"); throw new IllegalArgumentException("c failed"); }));

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> handler.handle(letter("wf", "boom")));

    assertEquals(List.of("a", "b", "c"), ran);
    assertEquals("c failed", failure.getSuppressed()[0].getMessage());
  }

  @Test
  void noOpHandlerDoesNothing() throws Exception {
    NoOpDeadLetterHandler.INSTANCE.handle(letter("wf", "boom"));
  }

  private static DeadLetter letter(String id, String failure) {
    return new DeadLetter(id, "ingest-store-index", Map.of("tenantId", "t"), List.of(), failure, 3,
        Instant.now(), Instant.now());
  }
}
