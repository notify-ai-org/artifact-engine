package dev.notify.artifact.job;

@FunctionalInterface
public interface Job<R> {
  R execute() throws Exception;
}
