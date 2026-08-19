package dev.notify.artifact.embed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/** OpenAI-compatible embedding provider backed by an injected OkHttp client. */
public final class OkHttpEmbeddingProvider implements EmbeddingProvider {
  private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private final OkHttpClient client;
  private final ObjectMapper json;
  private final HttpUrl endpoint;
  private final String apiKey;
  private final String model;
  private final String version;
  private final int dimensions;

  public OkHttpEmbeddingProvider(
      OkHttpClient client,
      ObjectMapper objectMapper,
      String endpoint,
      String apiKey,
      String model,
      String version,
      int dimensions) {
    this.client = Objects.requireNonNull(client, "client");
    this.json = Objects.requireNonNull(objectMapper, "objectMapper");
    this.endpoint = HttpUrl.get(requireText(endpoint, "endpoint"));
    this.apiKey = apiKey == null ? "" : apiKey.trim();
    this.model = requireText(model, "model");
    this.version = requireText(version, "version");
    if (dimensions < 1 || dimensions > 2_000) {
      throw new IllegalArgumentException("embedding dimensions must be between 1 and 2000");
    }
    this.dimensions = dimensions;
  }

  @Override
  public String model() {
    return model;
  }

  @Override
  public String version() {
    return version;
  }

  @Override
  public List<float[]> embed(List<String> texts) {
    Objects.requireNonNull(texts, "texts");
    if (texts.isEmpty()) return List.of();
    if (texts.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("embedding input must not contain null text");
    }

    RequestBody body;
    try {
      body = RequestBody.create(json.writeValueAsBytes(new EmbeddingRequest(model, texts)), JSON);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Embedding request cannot be serialized", exception);
    }
    Request.Builder request =
        new Request.Builder()
            .url(endpoint)
            .header("Accept", "application/json")
            .post(body);
    if (!apiKey.isBlank()) request.header("Authorization", "Bearer " + apiKey);

    try (Response response = client.newCall(request.build()).execute()) {
      if (!response.isSuccessful()) {
        throw new IllegalStateException(
            "Embedding request failed with HTTP "
                + response.code()
                + requestIdSuffix(response.header("x-request-id")));
      }
      ResponseBody responseBody = response.body();
      if (responseBody == null) {
        throw new IllegalStateException("Embedding response has no body");
      }
      return parse(json.readTree(responseBody.byteStream()), texts.size());
    } catch (IOException exception) {
      throw new IllegalStateException("Embedding request failed", exception);
    }
  }

  private List<float[]> parse(JsonNode root, int expectedCount) {
    JsonNode data = root.path("data");
    if (!data.isArray()) {
      throw new IllegalStateException("Embedding response does not contain a data array");
    }
    List<float[]> ordered = new ArrayList<>(Collections.nCopies(expectedCount, null));
    int fallbackIndex = 0;
    for (JsonNode item : data) {
      int index = item.has("index") ? item.path("index").asInt(-1) : fallbackIndex;
      fallbackIndex++;
      if (index < 0 || index >= expectedCount || ordered.get(index) != null) {
        throw new IllegalStateException("Embedding response contains an invalid index: " + index);
      }
      JsonNode embedding = item.path("embedding");
      if (!embedding.isArray() || embedding.size() != dimensions) {
        throw new IllegalStateException(
            "Embedding response dimensions do not match configured dimensions " + dimensions);
      }
      float[] vector = new float[dimensions];
      for (int component = 0; component < dimensions; component++) {
        JsonNode value = embedding.get(component);
        if (!value.isNumber()) {
          throw new IllegalStateException("Embedding response contains a non-numeric component");
        }
        vector[component] = value.floatValue();
        if (!Float.isFinite(vector[component])) {
          throw new IllegalStateException("Embedding response contains a non-finite component");
        }
      }
      ordered.set(index, vector);
    }
    if (ordered.stream().anyMatch(Objects::isNull)) {
      throw new IllegalStateException(
          "Embedding response count does not match request count " + expectedCount);
    }
    return List.copyOf(ordered);
  }

  private static String requestIdSuffix(String requestId) {
    return requestId == null || requestId.isBlank() ? "" : " (request-id " + requestId + ')';
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value.trim();
  }

  private record EmbeddingRequest(String model, List<String> input) {
    private EmbeddingRequest {
      input = List.copyOf(input);
    }
  }
}
