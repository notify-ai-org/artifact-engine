package dev.notify.artifact.multipart;

import dev.notify.artifact.dlq.DeadLetter;
import dev.notify.artifact.dlq.DeadLetterHandler;
import dev.notify.artifact.store.MultipartUploadStore;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.workflow.Workflow;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Aborts an unfinished multipart upload so its parts stop costing storage.
 *
 * <p>Register it as a {@link DeadLetterHandler}: a crashed workflow may still be retried from its
 * failed part, which needs the upload intact, so cleanup waits until the workflow is dead-lettered.
 * A bucket lifecycle rule (AbortIncompleteMultipartUpload) remains the backstop.
 */
public final class MultipartUploadCleaner implements DeadLetterHandler, Consumer<Workflow> {
  private final MultipartUploadStore uploads;
  private final ObjectStore objectStore;

  public MultipartUploadCleaner(MultipartUploadStore uploads, ObjectStore objectStore) {
    this.uploads = Objects.requireNonNull(uploads, "uploads");
    this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
  }

  @Override
  public void handle(DeadLetter letter) throws IOException {
    cleanUp(letter.attributes());
  }

  @Override
  public void accept(Workflow workflow) {
    try {
      cleanUp(workflow.attributes());
    } catch (IOException failure) {
      throw new IllegalStateException("Failed to abort multipart upload", failure);
    }
  }

  private void cleanUp(Map<String, String> attributes) throws IOException {
    String tenantId = attributes.get("tenantId");
    String artifactId = attributes.get("artifactId");
    String version = attributes.get("version");
    if (tenantId == null || artifactId == null || version == null) return;
    long artifactVersion = Long.parseLong(version);
    var upload = uploads.find(tenantId, artifactId, artifactVersion);
    if (upload.isEmpty()) return;
    objectStore.abortMultipartUpload(tenantId, upload.get().storageKey(), upload.get().uploadId());
    uploads.delete(tenantId, artifactId, artifactVersion);
  }
}
