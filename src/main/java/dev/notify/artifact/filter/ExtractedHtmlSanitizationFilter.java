package dev.notify.artifact.security;

import org.jsoup.Jsoup;

/** Converts extracted HTML to inert plain text before indexing or returning it to a model. */
public final class ExtractedHtmlSanitizationFilter<C extends ExtractedContentContext>
    implements SecurityFilter<C> {
  @Override
  public String name() {
    return "extracted-html-sanitization";
  }

  @Override
  public void verify(C context) {
    if (context.extractedContent() == null) {
      return;
    }
    if ("text/html".equals(context.mediaType())) {
      context.extractedContent(Jsoup.parse(context.extractedContent()).text());
    }
  }
}
