package dev.notify.artifact.security;

public final class IndexingContentContext implements ExtractedContentContext {
  private final String mediaType;
  private String extractedContent;

  public IndexingContentContext(String mediaType, String extractedContent) {
    this.mediaType = mediaType;
    this.extractedContent = extractedContent;
  }

  @Override
  public String mediaType() {
    return mediaType;
  }

  @Override
  public String extractedContent() {
    return extractedContent;
  }

  @Override
  public void extractedContent(String extractedContent) {
    this.extractedContent = extractedContent;
  }
}
