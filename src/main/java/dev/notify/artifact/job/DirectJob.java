package dev.notify.artifact.job;

/** A process-local request/response job that must execute on the dedicated direct worker. */
public interface DirectJob<R> extends Job<R> {}
