package dev.notify.artifact.cache;

import java.text.Normalizer;

/** Canonical form of query text for cache keys: NFKC, trimmed, whitespace runs collapsed. */
public final class QueryText {
  private QueryText() {}

  /** Case is kept: embedding models distinguish it, so folding it could change results. */
  public static String normalize(String query) {
    if (query == null) return "";
    return Normalizer.normalize(query, Normalizer.Form.NFKC).strip().replaceAll("\\s+", " ");
  }
}
