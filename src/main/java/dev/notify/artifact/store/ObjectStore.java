package dev.notify.artifact.store;

import java.io.IOException;
import java.io.InputStream;

public interface ObjectStore {
  void put(String tenantId, String key, InputStream content, long length, String sha256)
      throws IOException;

  InputStream get(String tenantId, String key) throws IOException;

  boolean verified(String tenantId, String key, long length, String sha256) throws IOException;

  void delete(String tenantId, String key) throws IOException;
}
