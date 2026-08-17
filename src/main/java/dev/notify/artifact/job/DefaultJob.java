package dev.notify.artifact.job;

import java.util.Objects;
import java.util.concurrent.Callable;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.store.MetadataStore;

/** Reusable {@link Job} implementation backed by a checked Java {@link Callable}. */
public final class DefaultJob<R> extends AbstractJob<R> {
  private final Callable<? extends R> operation;

  public DefaultJob(
      ArtifactAccessVerifier verifier,
      MetadataStore metadataStore,
      Callable<? extends R> operation) {
    super(verifier, metadataStore);
    this.operation = Objects.requireNonNull(operation, "operation");
  }

  @Override
  public R execute() throws Exception {
    return operation.call();
  }
}
