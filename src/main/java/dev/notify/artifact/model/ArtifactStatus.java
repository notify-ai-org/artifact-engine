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
    RETRY_PENDING,
    DEAD_LETTER,
    DELETED
  }
}
