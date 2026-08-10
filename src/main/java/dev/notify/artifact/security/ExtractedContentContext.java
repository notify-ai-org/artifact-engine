package dev.notify.artifact.security;

public interface ExtractedContentContext {
  String mediaType();

  String extractedContent();

  void extractedContent(String extractedContent);
}
