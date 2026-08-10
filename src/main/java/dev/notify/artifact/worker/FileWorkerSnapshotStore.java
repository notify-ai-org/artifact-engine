package dev.notify.artifact.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;

/** Atomically publishes worker topology snapshots to a local recovery file. */
public final class FileWorkerSnapshotStore implements WorkerSnapshotStore {
  private final Path snapshotFile;
  private final ObjectMapper json;

  public FileWorkerSnapshotStore(Path snapshotFile, ObjectMapper json) throws IOException {
    this.snapshotFile = snapshotFile.toAbsolutePath().normalize();
    this.json = json.copy().findAndRegisterModules();
    Path parent = this.snapshotFile.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }

  @Override
  public List<WorkerManager.WorkerSnapshot> load() throws IOException {
    if (!Files.exists(snapshotFile)) {
      return List.of();
    }
    return json.readValue(
        snapshotFile.toFile(), new TypeReference<List<WorkerManager.WorkerSnapshot>>() {});
  }

  @Override
  public void save(List<WorkerManager.WorkerSnapshot> snapshots) throws IOException {
    Path temporary =
        snapshotFile.resolveSibling(snapshotFile.getFileName() + "." + UUID.randomUUID() + ".tmp");
    try {
      json.writeValue(temporary.toFile(), snapshots);
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        channel.force(true);
      }
      Files.move(
          temporary,
          snapshotFile,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }
}
