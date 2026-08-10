package dev.notify.artifact.util;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class Checksum {
  private Checksum() {}

  public static String sha256(InputStream input) throws IOException {
    try {
      var digest = MessageDigest.getInstance("SHA-256");
      byte[] buffer = new byte[64 * 1024];
      for (int n; (n = input.read(buffer)) >= 0; ) if (n > 0) digest.update(buffer, 0, n);
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  public static String sha256(String value) {
    try {
      return sha256(
          new java.io.ByteArrayInputStream(
              value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (IOException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
