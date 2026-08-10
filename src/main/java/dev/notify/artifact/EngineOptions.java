package dev.notify.artifact;

/** Core policy choices that must be explicit and stable for a deployment. */
public record EngineOptions(boolean deduplicateContent, int retrievalCandidateMultiplier) {
  public EngineOptions {
    if (retrievalCandidateMultiplier < 1 || retrievalCandidateMultiplier > 100) {
      throw new IllegalArgumentException(
          "Retrieval candidate multiplier must be between 1 and 100");
    }
  }

  public static EngineOptions defaults() {
    return new EngineOptions(true, 4);
  }
}
