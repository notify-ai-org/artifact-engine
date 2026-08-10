package dev.notify.artifact.util;

public final class VectorUtils {
  private VectorUtils() {}

  public static double cosine(float[] a, float[] b) {
    if (a.length != b.length) throw new IllegalArgumentException("Vector dimensions differ");
    double dot = 0, aa = 0, bb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      aa += a[i] * a[i];
      bb += b[i] * b[i];
    }
    return aa == 0 || bb == 0 ? 0 : dot / (Math.sqrt(aa) * Math.sqrt(bb));
  }
}
