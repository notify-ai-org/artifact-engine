package dev.notify.artifact.security;

import java.util.List;

/** Executes filters in declaration order and stops at the first rejection. */
public final class SecurityFilterChain<C> {
  private final List<SecurityFilter<C>> filters;

  public SecurityFilterChain(List<SecurityFilter<C>> filters) {
    if (filters == null || filters.isEmpty()) {
      throw new IllegalArgumentException("A security filter chain cannot be empty");
    }
    this.filters = List.copyOf(filters);
  }

  public C verify(C context) {
    for (SecurityFilter<C> filter : filters) {
      try {
        filter.verify(context);
      } catch (SecurityFilterException rejection) {
        throw rejection;
      } catch (RuntimeException failure) {
        throw new SecurityFilterException(
            "SECURITY_FILTER_FAILED", filter.name(), "Security validation failed", failure);
      }
    }
    return context;
  }

  public List<String> filterNames() {
    return filters.stream().map(SecurityFilter::name).toList();
  }
}
