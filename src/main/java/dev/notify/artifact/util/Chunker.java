package dev.notify.artifact.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Word-budget chunker with deterministic overlap.
 *
 * <p>{@link #stream} accepts text incrementally and holds at most one chunk's worth of words, so
 * callers can chunk documents of any length without materializing the full text. {@link #chunk}
 * is the same algorithm applied to a single string.
 */
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
    List<String> result = new ArrayList<>();
    Session session = stream((index, chunk) -> result.add(chunk));
    try {
      if (text != null) session.accept(text);
      session.finish();
    } catch (IOException impossible) {
      throw new UncheckedIOException(impossible);
    }
    return result;
  }

  /** Starts an incremental chunking session that emits chunks to {@code consumer} in order. */
  public Session stream(ChunkConsumer consumer) {
    return new Session(java.util.Objects.requireNonNull(consumer, "consumer"));
  }

  @FunctionalInterface
  public interface ChunkConsumer {
    void accept(int index, String text) throws IOException;
  }

  /** Single-use, single-threaded chunking session. Words may span {@link #accept} calls. */
  public final class Session {
    private final ChunkConsumer consumer;
    private final ArrayDeque<String> window = new ArrayDeque<>(wordsPerChunk);
    private final StringBuilder word = new StringBuilder();
    private int nextIndex;
    private boolean finished;

    private Session(ChunkConsumer consumer) {
      this.consumer = consumer;
    }

    public void accept(CharSequence text) throws IOException {
      requireOpen();
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (isWhitespace(c)) endWord();
        else word.append(c);
      }
    }

    /** Flushes the trailing partial chunk. Returns the total number of chunks emitted. */
    public int finish() throws IOException {
      requireOpen();
      finished = true;
      endWord();
      // After a full chunk the window keeps only overlap words, which were already emitted.
      if (!window.isEmpty() && (nextIndex == 0 || window.size() > overlapWords)) emit();
      return nextIndex;
    }

    private void endWord() throws IOException {
      if (word.isEmpty()) return;
      window.addLast(word.toString());
      word.setLength(0);
      if (window.size() == wordsPerChunk) {
        emit();
        for (int k = 0; k < wordsPerChunk - overlapWords; k++) window.removeFirst();
      }
    }

    private void emit() throws IOException {
      consumer.accept(nextIndex++, String.join(" ", window));
    }

    private void requireOpen() {
      if (finished) throw new IllegalStateException("Chunking session is finished");
    }
  }

  /** Matches the ASCII whitespace class {@code \s} used by the original regex normalization. */
  private static boolean isWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\u000B' || c == '\f' || c == '\r';
  }
}
