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
    return new Session(java.util.Objects.requireNonNull(consumer, "consumer"), State.INITIAL);
  }

  /**
   * Continues a session from a {@link Session#snapshot()}, possibly in another job or process.
   * Feeding text across snapshot and resume yields exactly the chunks of one uninterrupted session.
   */
  public Session resume(State state, ChunkConsumer consumer) {
    return new Session(
        java.util.Objects.requireNonNull(consumer, "consumer"),
        java.util.Objects.requireNonNull(state, "state"));
  }

  @FunctionalInterface
  public interface ChunkConsumer {
    void accept(int index, String text) throws IOException;
  }

  /**
   * Everything a session carries between {@link Session#accept} calls: the overlap window (at most
   * one chunk of words), a word cut off at the end of the last input, and the next chunk index.
   */
  public record State(List<String> window, String partialWord, int nextIndex) {
    public static final State INITIAL = new State(List.of(), "", 0);

    public State {
      window = List.copyOf(window);
      partialWord = partialWord == null ? "" : partialWord;
      if (nextIndex < 0) throw new IllegalArgumentException("nextIndex cannot be negative");
    }
  }

  /** Single-use, single-threaded chunking session. Words may span {@link #accept} calls. */
  public final class Session {
    private final ChunkConsumer consumer;
    private final ArrayDeque<String> window = new ArrayDeque<>(wordsPerChunk);
    private final StringBuilder word = new StringBuilder();
    private int nextIndex;
    private boolean finished;

    private Session(ChunkConsumer consumer, State state) {
      if (state.window().size() >= wordsPerChunk) {
        throw new IllegalArgumentException("Saved window exceeds the chunk size");
      }
      this.consumer = consumer;
      this.window.addAll(state.window());
      this.word.append(state.partialWord());
      this.nextIndex = state.nextIndex();
    }

    /** The session's carried state; valid until the next {@link #accept} or {@link #finish}. */
    public State snapshot() {
      requireOpen();
      return new State(List.copyOf(window), word.toString(), nextIndex);
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
