package dev.notify.artifact.filter;

/** Default transformation filter that preserves the input unchanged. */
public final class IdentityFilter<T> implements FilterChain.Filter<T> {
  @Override
  public T apply(T value) {
    return value;
  }
}
