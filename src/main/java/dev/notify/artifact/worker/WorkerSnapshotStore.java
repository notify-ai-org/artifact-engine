package dev.notify.artifact.worker;

import java.io.IOException;
import java.util.List;

/** Persists worker topology; durable jobs themselves remain in the queue. */
public interface WorkerSnapshotStore {
  List<WorkerManager.WorkerSnapshot> load() throws IOException;

  void save(List<WorkerManager.WorkerSnapshot> snapshots) throws IOException;
}
