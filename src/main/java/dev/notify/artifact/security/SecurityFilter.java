package dev.notify.artifact.security;

/** One fail-closed security decision in an ordered operation chain. */
public interface SecurityFilter<C> {
  String name();

  void verify(C context) throws SecurityFilterException;
}
