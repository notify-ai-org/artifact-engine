package dev.notify.artifact.security;

/** Safe rejection containing no tokens, document content, URLs, or sensitive metadata. */
public final class SecurityFilterException extends SecurityException {
  private final String code;
  private final String filter;

  public SecurityFilterException(String code, String filter, String safeMessage) {
    super(safeMessage);
    this.code = code;
    this.filter = filter;
  }

  public SecurityFilterException(String code, String filter, String safeMessage, Throwable cause) {
    super(safeMessage, cause);
    this.code = code;
    this.filter = filter;
  }

  public String code() {
    return code;
  }

  public String filter() {
    return filter;
  }
}
