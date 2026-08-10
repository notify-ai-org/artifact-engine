package dev.notify.artifact.util;

import java.util.ArrayList;
import java.util.List;

/** Word-budget chunker with deterministic overlap and no unbounded intermediate buffers. */
public final class Chunker {
  private final int wordsPerChunk;
  private final int overlapWords;

  public Chunker(int wordsPerChunk, int overlapWords) {
    if (wordsPerChunk < 1 || overlapWords < 0 || overlapWords >= wordsPerChunk)
      throw new IllegalArgumentException("Invalid chunk bounds");
    this.wordsPerChunk = wordsPerChunk;
    this.overlapWords = overlapWords;
  }

  public List<String> chunk(String text) {
    String normalized = text == null ? "" : text.replaceAll("\\s+", " ").trim();
    if (normalized.isEmpty()) return List.of();
    String[] words = normalized.split(" ");
    List<String> result = new ArrayList<>();
    for (int start = 0; start < words.length; start += wordsPerChunk - overlapWords) {
      int end = Math.min(words.length, start + wordsPerChunk);
      result.add(String.join(" ", java.util.Arrays.copyOfRange(words, start, end)));
      if (end == words.length) break;
    }
    return result;
  }
}
