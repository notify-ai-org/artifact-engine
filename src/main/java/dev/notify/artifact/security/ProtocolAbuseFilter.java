package dev.notify.artifact.security;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Rejects oversized frames, invalid state changes, slow transfers, and compression bombs. */
public final class ProtocolAbuseFilter<C extends ProtectedOperationContext>
    implements SecurityFilter<C> {
  private final Limits limits;
  private final Map<String, Set<String>> streamTransitions;

  public ProtocolAbuseFilter(Limits limits, Map<String, Set<String>> streamTransitions) {
    this.limits = Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(streamTransitions, "streamTransitions");
    this.streamTransitions =
        streamTransitions.entrySet().stream()
            .collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey, entry -> Set.copyOf(entry.getValue())));
  }

  @Override
  public String name() {
    return "protocol-abuse";
  }

  @Override
  public void verify(C context) {
    TransportFacts facts = Objects.requireNonNull(context.transport(), "transport");
    if (facts.frameBytes() > limits.maxFrameBytes()) {
      reject("FRAME_SIZE_LIMIT", "Protocol frame exceeds the configured size limit");
    }

    Set<String> allowed = streamTransitions.get(facts.previousStreamState());
    if (allowed == null || !allowed.contains(facts.requestedStreamState())) {
      reject("INVALID_STREAM_TRANSITION", "Protocol stream transition is not allowed");
    }

    if (facts.elapsedMillis() > limits.slowlorisGraceMillis()) {
      double bytesPerSecond = facts.bytesReceived() * 1000.0 / facts.elapsedMillis();
      if (bytesPerSecond < limits.minimumBytesPerSecond()) {
        reject("SLOWLORIS_REJECTED", "Protocol transfer rate is below the configured minimum");
      }
    }

    if (facts.decompressedBytes() > 0) {
      double compressionRatio =
          facts.decompressedBytes() / (double) Math.max(1, facts.compressedBytes());
      if (compressionRatio > limits.maxCompressionRatio()) {
        reject("COMPRESSION_RATIO_LIMIT", "Protocol compression ratio exceeds the safe limit");
      }
    }
  }

  private void reject(String code, String message) {
    throw new SecurityFilterException(code, name(), message);
  }

  public record Limits(
      long maxFrameBytes,
      long slowlorisGraceMillis,
      long minimumBytesPerSecond,
      double maxCompressionRatio) {
    public Limits {
      if (maxFrameBytes <= 0
          || slowlorisGraceMillis < 0
          || minimumBytesPerSecond <= 0
          || maxCompressionRatio <= 0) {
        throw new IllegalArgumentException("Protocol limits must be positive");
      }
    }
  }
}
