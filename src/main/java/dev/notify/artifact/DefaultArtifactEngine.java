package dev.notify.artifact;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.dispatcher.JobDispatcher;
import dev.notify.artifact.dispatcher.QueuingJobDispatcher;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.factory.ArtifactJobFactory;
import dev.notify.artifact.factory.DefaultArtifactJobFactory;
import dev.notify.artifact.job.Job;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.queue.QueueManager;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.VectorStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * Thin public facade that converts incoming operations to typed jobs and dispatches them. Workflow
 * behavior belongs to job implementations, not to this engine.
 */
public final class DefaultArtifactEngine implements ArtifactEngine {
  private final ArtifactJobFactory jobFactory;
  private final JobDispatcher dispatcher;

  public DefaultArtifactEngine(ArtifactJobFactory jobFactory, JobDispatcher dispatcher) {
    this.jobFactory = Objects.requireNonNull(jobFactory, "jobFactory");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
  }

  public DefaultArtifactEngine(
      MetadataStore metadata,
      VectorStore vectors,
      ObjectStore objects,
      DurableSpool spool,
      DataVerifier verifier,
      EmbeddingService embeddings,
      QueueManager queueManager, 
      AuthorizationService authorization) {
    this(
        metadata,
        vectors,
        objects,
        spool,
        verifier,
        embeddings,
        authorization,
        queueManager, EngineOptions.defaults());
  }

  public DefaultArtifactEngine(
      MetadataStore metadata,
      VectorStore vectors,
      ObjectStore objects,
      DurableSpool spool,
      DataVerifier verifier,
      EmbeddingService embeddings,
      AuthorizationService authorization,
      QueueManager queueManager,
      EngineOptions options) {
    this(
        new DefaultArtifactJobFactory(
            metadata,
            vectors,
            objects,
            spool,
            verifier,
            embeddings,
            authorization,
            options),
        new QueuingJobDispatcher(queueManager));
  }

  @Override
  public Artifact ingest(Requests.Ingest request) throws IOException {
    return dispatchIo(jobFactory.createIngest(request), "ingest");
  }

  @Override
  public Artifact metadata(String principalId, String tenantId, String artifactId) {
    return dispatch(jobFactory.createMetadata(principalId, tenantId, artifactId), "metadata");
  }

  @Override
  public List<Artifact> listMetadata(String principalId, String tenantId, int limit) {
    return dispatch(
        jobFactory.createListMetadata(principalId, tenantId, limit), "metadata listing");
  }

  @Override
  public InputStream content(String principalId, String tenantId, String artifactId)
      throws IOException {
    return dispatchIo(jobFactory.createFetch(principalId, tenantId, artifactId), "fetch");
  }

  @Override
  public String extractedText(
      String principalId, String tenantId, String artifactId, int maxCharacters) {
    return dispatch(
        jobFactory.createExtractedText(principalId, tenantId, artifactId, maxCharacters),
        "extracted-text retrieval");
  }

  @Override
  public List<Requests.SearchHit> search(Requests.Search request) {
    return dispatch(jobFactory.createRetrieval(request), "search");
  }

  @Override
  public void delete(String principalId, String tenantId, String artifactId) throws IOException {
    dispatchIo(jobFactory.createDelete(principalId, tenantId, artifactId), "delete");
  }

  private <R> R dispatch(Job<R> job, String operation) {
    try {
      return dispatcher.dispatch(job);
    } catch (RuntimeException failure) {
      throw failure;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Artifact " + operation + " dispatch was interrupted", interrupted);
    } catch (Exception failure) {
      throw new IllegalStateException("Artifact " + operation + " job failed", failure);
    }
  }

  private <R> R dispatchIo(Job<R> job, String operation) throws IOException {
    try {
      return dispatcher.dispatch(job);
    } catch (IOException failure) {
      throw failure;
    } catch (RuntimeException failure) {
      throw failure;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Artifact " + operation + " dispatch was interrupted", interrupted);
    } catch (Exception failure) {
      throw new IOException("Artifact " + operation + " job failed", failure);
    }
  }
}
