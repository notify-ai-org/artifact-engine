package dev.notify.artifact.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ChunkerTest {
  private static final String[] SEPARATORS = {" ", "  ", "\t", "\n", "\r\n", "\f", "\u000B"};

  @Test
  void streamingMatchesLegacyRegexChunkingForArbitraryPieceBoundaries() throws Exception {
    Random random = new Random(42);
    int[][] bounds = {{1, 0}, {3, 1}, {5, 0}, {5, 4}, {8, 3}, {300, 30}};
    for (int trial = 0; trial < 500; trial++) {
      String text = randomText(random, random.nextInt(60));
      for (int[] bound : bounds) {
        Chunker chunker = new Chunker(bound[0], bound[1]);
        List<String> expected = legacyChunk(text, bound[0], bound[1]);

        assertEquals(expected, chunker.chunk(text), () -> "chunk(" + text + ")");

        List<String> streamed = new ArrayList<>();
        Chunker.Session session =
            chunker.stream(
                (index, chunk) -> {
                  assertEquals(streamed.size(), index);
                  streamed.add(chunk);
                });
        for (int start = 0; start < text.length(); ) {
          int end = Math.min(text.length(), start + 1 + random.nextInt(7));
          session.accept(text.substring(start, end));
          start = end;
        }
        assertEquals(expected.size(), session.finish());
        assertEquals(expected, streamed, () -> "stream(" + text + ")");
      }
    }
  }

  @Test
  void snapshotAndResumeAcrossPiecesMatchesOneUninterruptedSession() throws Exception {
    Random random = new Random(7);
    for (int trial = 0; trial < 300; trial++) {
      String text = randomText(random, random.nextInt(80));
      Chunker chunker = new Chunker(5, 2);
      List<String> expected = chunker.chunk(text);

      List<String> streamed = new ArrayList<>();
      Chunker.ChunkConsumer collect = (index, chunk) -> {
        assertEquals(streamed.size(), index);
        streamed.add(chunk);
      };
      Chunker.State state = Chunker.State.INITIAL;
      for (int start = 0; start < text.length(); ) {
        int end = Math.min(text.length(), start + 1 + random.nextInt(9));
        Chunker.Session session = chunker.resume(state, collect);
        session.accept(text.substring(start, end));
        state = session.snapshot();
        start = end;
      }
      chunker.resume(state, collect).finish();

      assertEquals(expected, streamed, () -> "resumed(" + text + ")");
    }
  }

  @Test
  void keepsNonAsciiWhitespaceInsideWordsLikeTheLegacyRegex() {
    assertEquals(List.of("a b c"), new Chunker(5, 0).chunk("a b   c"));
  }

  @Test
  void rejectsUseAfterFinish() throws Exception {
    Chunker.Session session = new Chunker(2, 0).stream((index, chunk) -> {});
    session.finish();
    assertThrows(IllegalStateException.class, () -> session.accept("more"));
  }

  private static String randomText(Random random, int words) {
    StringBuilder text = new StringBuilder();
    if (random.nextBoolean()) text.append(SEPARATORS[random.nextInt(SEPARATORS.length)]);
    for (int i = 0; i < words; i++) {
      if (i > 0) text.append(SEPARATORS[random.nextInt(SEPARATORS.length)]);
      text.append("w").append(i).append("x".repeat(random.nextInt(4)));
    }
    if (random.nextBoolean()) text.append(SEPARATORS[random.nextInt(SEPARATORS.length)]);
    return text.toString();
  }

  /** Verbatim copy of the pre-streaming implementation, kept as the compatibility oracle. */
  private static List<String> legacyChunk(String text, int wordsPerChunk, int overlapWords) {
    String normalized = text == null ? "" : text.replaceAll("\\s+", " ").trim();
    if (normalized.isEmpty()) return List.of();
    String[] words = normalized.split(" ");
    List<String> result = new ArrayList<>();
    for (int start = 0; start < words.length; start += wordsPerChunk - overlapWords) {
      int end = Math.min(words.length, start + wordsPerChunk);
      result.add(String.join(" ", Arrays.copyOfRange(words, start, end)));
      if (end == words.length) break;
    }
    return result;
  }
}
