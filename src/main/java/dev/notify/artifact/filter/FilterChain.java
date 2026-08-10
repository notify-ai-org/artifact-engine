package dev.notify.artifact.filter;

import java.util.List;

/** Ordered transformation/validation chain used by worker-local jobs. */
public final class FilterChain<T> {
  private final List<Filter<T>> filters;

  public FilterChain(List<Filter<T>> filters) {
    this.filters = filters == null ? List.of() : List.copyOf(filters);
  }

  public T apply(T value) {
    T current = value;
    for (Filter<T> filter : filters) {
      current = filter.apply(current);
    }
    return current;
  }

  @FunctionalInterface
  public interface Filter<T> {
    T apply(T value);
  }
}
