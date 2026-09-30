package dev.notify.artifact;

import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.worker.Worker;
import dev.notify.artifact.worker.WorkerManager;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Tenant-bound public facade. Implementations authorize every call before touching a store.
 *
 * <p>The worker administration methods at the end are process-level operator controls over the
 * background workers that store and index artifacts. They are not tenant-scoped and not
 * authorized per principal; hosts must not expose them to tenant callers. Engines built without a
 * {@link WorkerManager} throw {@link UnsupportedOperationException}.
 */
public interface ArtifactEngine {
  Artifact ingest(Requests.Ingest request) throws IOException;

  /**
   * Ingests content the engine reads itself from a URL or allowed file, chunk by chunk through a
   * bounded buffer pool. Returns once the artifact is registered and its workflow planned; the
   * content is read, spooled, stored, and indexed asynchronously.
   */
  default Artifact ingestFromSource(Requests.IngestSource request) throws IOException {
    throw new UnsupportedOperationException("Source ingest is not supported");
  }

  Artifact metadata(String principalId, String tenantId, String artifactId);

  default List<Artifact> listMetadata(String principalId, String tenantId, int limit) {
    throw new UnsupportedOperationException("Artifact metadata listing is not supported");
  }

  InputStream content(String principalId, String tenantId, String artifactId) throws IOException;

  String extractedText(String principalId, String tenantId, String artifactId, int maxCharacters);

  List<Requests.SearchHit> search(Requests.Search request);

  void delete(String principalId, String tenantId, String artifactId) throws IOException;

  // ---- Worker administration (operator-level, not tenant-scoped) ----------------------------

  /** Adds and starts a worker for one job type. */
  default WorkerManager.WorkerSnapshot addWorker(WorkerManager.WorkerConfiguration configuration) {
    throw workersNotConfigured();
  }

  /** Stops and removes a worker; its in-flight batch is cancelled and its jobs are re-leased. */
  default void removeWorker(String workerId) {
    throw workersNotConfigured();
  }

  /** Stops workers unused for at least {@code idle}. Returns how many were removed. */
  default int removeIdleWorkers(Duration idle) {
    throw workersNotConfigured();
  }

  /** Current workers by id, with their queue and batching configuration. */
  default Map<String, WorkerManager.WorkerSnapshot> workers() {
    throw workersNotConfigured();
  }

  default int maxWorkers() {
    throw workersNotConfigured();
  }

  /** Re-creates workers from the configured snapshot store. Returns how many were restored. */
  default int restoreWorkers() throws IOException {
    throw workersNotConfigured();
  }

  /** Persists the current worker topology to the configured snapshot store. */
  default void saveWorkerSnapshots() throws IOException {
    throw workersNotConfigured();
  }

  /** The latest state transition observed for each job, by job id. */
  default Map<String, Worker.StateChange> jobStates() {
    throw workersNotConfigured();
  }

  default void addJobStateListener(Consumer<Worker.StateChange> listener) {
    throw workersNotConfigured();
  }

  default void removeJobStateListener(Consumer<Worker.StateChange> listener) {
    throw workersNotConfigured();
  }

  private static UnsupportedOperationException workersNotConfigured() {
    return new UnsupportedOperationException("This engine has no worker manager");
  }
}
