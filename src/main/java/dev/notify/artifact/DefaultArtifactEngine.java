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
import dev.notify.artifact.worker.Worker;
import dev.notify.artifact.worker.WorkerManager;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Thin public facade that converts incoming operations to typed jobs and dispatches them. Workflow
 * behavior belongs to job implementations, not to this engine.
 */
public final class DefaultArtifactEngine implements ArtifactEngine {
  private final ArtifactJobFactory jobFactory;
  private final JobDispatcher dispatcher;
  private final WorkerManager workerManager;

  public DefaultArtifactEngine(ArtifactJobFactory jobFactory, JobDispatcher dispatcher) {
    this(jobFactory, dispatcher, null);
  }

  /**
   * @param workerManager optional; backs the worker administration methods. The engine does not
   *     own it and never closes it.
   */
  public DefaultArtifactEngine(
      ArtifactJobFactory jobFactory, JobDispatcher dispatcher, WorkerManager workerManager) {
    this.jobFactory = Objects.requireNonNull(jobFactory, "jobFactory");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.workerManager = workerManager;
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
  public Artifact ingestFromSource(Requests.IngestSource request) throws IOException {
    return dispatchIo(jobFactory.createSourceIngest(request), "source ingest");
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

  @Override
  public WorkerManager.WorkerSnapshot addWorker(WorkerManager.WorkerConfiguration configuration) {
    Objects.requireNonNull(configuration, "configuration");
    WorkerManager manager = workers("add a worker");
    manager.add(
        configuration.id(),
        configuration.queueCapacity(),
        configuration.batchSize(),
        configuration.batchBytes(),
        configuration.flushInterval(),
        configuration.type());
    return manager.snapshots().get(configuration.id());
  }

  @Override
  public void removeWorker(String workerId) {
    workers("remove a worker").remove(Objects.requireNonNull(workerId, "workerId"));
  }

  @Override
  public int removeIdleWorkers(Duration idle) {
    return workers("remove idle workers").removeIdle(Objects.requireNonNull(idle, "idle"));
  }

  @Override
  public Map<String, WorkerManager.WorkerSnapshot> workers() {
    return workers("list workers").snapshots();
  }

  @Override
  public int maxWorkers() {
    return workers("read the worker limit").maxWorkers();
  }

  @Override
  public int restoreWorkers() throws IOException {
    return workers("restore workers").restore();
  }

  @Override
  public void saveWorkerSnapshots() throws IOException {
    workers("save worker snapshots").flushSnapshots();
  }

  @Override
  public Map<String, Worker.StateChange> jobStates() {
    return workers("read job states").stateChanges();
  }

  @Override
  public void addJobStateListener(Consumer<Worker.StateChange> listener) {
    workers("add a job state listener").addStateChangeListener(listener);
  }

  @Override
  public void removeJobStateListener(Consumer<Worker.StateChange> listener) {
    workers("remove a job state listener").removeStateChangeListener(listener);
  }

  private WorkerManager workers(String operation) {
    if (workerManager == null) {
      throw new UnsupportedOperationException(
          "Cannot " + operation + ": this engine has no worker manager");
    }
    return workerManager;
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
