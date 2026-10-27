package dev.notify.artifact.model;

public final class ArtifactStatus {
  private ArtifactStatus() {}

  public enum Storage {
    RECEIVING,
    SPOOLED,
    UPLOADING,
    STORED,
    RETRY_PENDING,
    DEAD_LETTER,
    DELETED
  }

  public enum Index {
    PENDING,
    EXTRACTING,
    CHUNKING,
    EMBEDDING,
    READY,
    /** Stored, but no text extractor or OCR handles its media type, so it is never indexed or searchable. */
    NOT_INDEXED,
    RETRY_PENDING,
    DEAD_LETTER,
    DELETED
  }
}
