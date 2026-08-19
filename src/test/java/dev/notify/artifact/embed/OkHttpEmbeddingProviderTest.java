package dev.notify.artifact.embed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.junit.jupiter.api.Test;

class OkHttpEmbeddingProviderTest {
  private static final MediaType JSON = MediaType.get("application/json");
  private final ObjectMapper json = new ObjectMapper();

  @Test
  void postsOpenAiCompatibleRequestAndOrdersVectorsByIndex() throws Exception {
    AtomicReference<Request> captured = new AtomicReference<>();
    var provider =
        provider(
            3,
            200,
            """
            {"data":[
              {"index":1,"embedding":[4.0,5.0,6.0]},
              {"index":0,"embedding":[1.0,2.0,3.0]}
            ]}
            """,
            captured,
            null);

    List<float[]> vectors = provider.embed(List.of("first", "second"));

    assertArrayEquals(new float[] {1, 2, 3}, vectors.get(0));
    assertArrayEquals(new float[] {4, 5, 6}, vectors.get(1));
    Request request = captured.get();
    assertEquals("/v1/embeddings", request.url().encodedPath());
    assertEquals("Bearer test-key", request.header("Authorization"));
    Buffer requestBody = new Buffer();
    request.body().writeTo(requestBody);
    JsonNode body = json.readTree(requestBody.readUtf8());
    assertEquals("test-model", body.path("model").asText());
    assertEquals("first", body.path("input").get(0).asText());
    assertEquals("second", body.path("input").get(1).asText());
  }

  @Test
  void rejectsUnexpectedEmbeddingDimensions() {
    var provider =
        provider(
            3,
            200,
            "{\"data\":[{\"index\":0,\"embedding\":[1.0]}]}",
            new AtomicReference<>(),
            null);

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> provider.embed(List.of("text")));

    assertEquals(
        "Embedding response dimensions do not match configured dimensions 3",
        failure.getMessage());
  }

  @Test
  void doesNotExposeErrorResponseBody() {
    var provider =
        provider(
            3,
            401,
            "secret upstream diagnostic",
            new AtomicReference<>(),
            "request-123");

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> provider.embed(List.of("text")));

    assertEquals(
        "Embedding request failed with HTTP 401 (request-id request-123)", failure.getMessage());
  }

  private OkHttpEmbeddingProvider provider(
      int dimensions,
      int status,
      String responseBody,
      AtomicReference<Request> captured,
      String requestId) {
    Interceptor responseInterceptor =
        chain -> {
          captured.set(chain.request());
          Response.Builder response =
              new Response.Builder()
                  .request(chain.request())
                  .protocol(Protocol.HTTP_1_1)
                  .code(status)
                  .message("test response")
                  .body(ResponseBody.create(responseBody, JSON));
          if (requestId != null) response.header("x-request-id", requestId);
          return response.build();
        };
    return new OkHttpEmbeddingProvider(
        new OkHttpClient.Builder().addInterceptor(responseInterceptor).build(),
        json,
        "https://embedding.example/v1/embeddings",
        "test-key",
        "test-model",
        "test-version",
        dimensions);
  }
}
