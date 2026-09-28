package dev.notify.artifact.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.embed.EmbeddingCache;
import dev.notify.artifact.embed.EmbeddingProvider;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.extract.PlainTextExtractor;
import dev.notify.artifact.extract.TextExtractor;
import dev.notify.artifact.extract.TextExtractorFactory;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.retry.RetryPolicy;
import dev.notify.artifact.spool.DurableSpool;
import dev.notify.artifact.store.InMemoryStores;
import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.Chunker;
import dev.notify.artifact.util.TextSink;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexJobTest {
  private static final String TENANT = "tenant";
  private static final String ARTIFACT = "artifact-1";
  private static final int BATCH = 3;
  private static final TextExtractor PLAIN_TEXT = new PlainTextExtractor(1_000_000);

  @TempDir Path spoolRoot;

  private final Chunker chunker = new Chunker(4, 1);
  private final List<Integer> providerCallSizes = new CopyOnWriteArrayList<>();
  private volatile int failAfter = Integer.MAX_VALUE;
  private InMemoryStores.Metadata metadata;
  private InMemoryStores.Vectors vectors;
  private DurableSpool spool;
  private EmbeddingService embeddings;

  @BeforeEach
  void setUp() throws IOException {
    metadata = new InMemoryStores.Metadata();
    vectors = new InMemoryStores.Vectors();
    spool = new DurableSpool(spoolRoot, 1 << 20, new ObjectMapper());
    embeddings =
        new EmbeddingService(
            List.of(provider()),
            noCache(),
            100,
            Duration.ZERO,
            Duration.ofHours(1),
            new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1, 0, failure -> false));
  }

  @AfterEach
  void tearDown() {
    embeddings.close();
  }

  @Test
  void embedsAndCommitsInBoundedBatches() throws Exception {
    String text = words(0, 40);
    storeArtifact(1, text);

    int count = job().execute();

    List<String> expected = chunker.chunk(text);
    assertEquals(expected.size(), count);
    assertEquals(expected, vectors.chunks(TENANT, ARTIFACT).stream().map(ArtifactChunk::text).toList());
    assertTrue(providerCallSizes.stream().allMatch(size -> size <= BATCH), providerCallSizes::toString);
    assertEquals(count, providerCallSizes.stream().mapToInt(Integer::intValue).sum());
    assertEquals(ArtifactStatus.Index.READY, artifact().indexStatus());
  }

  @Test
  void retryResumesWithoutReEmbeddingCommittedBatches() throws Exception {
    String text = words(0, 40);
    storeArtifact(1, text);
    int total = chunker.chunk(text).size();

    // Fail on the second provider call: exactly one batch is committed before the failure.
    failAfter = 1;
    assertThrows(IllegalStateException.class, job()::execute);
    assertEquals(ArtifactStatus.Index.RETRY_PENDING, artifact().indexStatus());
    assertEquals(BATCH, vectors.chunks(TENANT, ARTIFACT).size());

    failAfter = Integer.MAX_VALUE;
    providerCallSizes.clear();
    assertEquals(total, job().execute());

    assertEquals(total - BATCH, providerCallSizes.stream().mapToInt(Integer::intValue).sum());
    assertEquals(total, vectors.chunks(TENANT, ARTIFACT).size());
    assertEquals(ArtifactStatus.Index.READY, artifact().indexStatus());
  }

  @Test
  void reindexingUnchangedContentMakesNoProviderCalls() throws Exception {
    storeArtifact(1, words(0, 25));
    job().execute();
    providerCallSizes.clear();

    job().execute();

    assertEquals(List.of(), providerCallSizes);
  }

  @Test
  void shorterNewVersionRemovesTrailingChunksFromTheLongerOne() throws Exception {
    storeArtifact(1, words(0, 40));
    job().execute();

    String shorter = words(100, 10);
    storeArtifact(2, shorter);
    int count = job().execute();

    List<ArtifactChunk> stored = vectors.chunks(TENANT, ARTIFACT);
    assertEquals(chunker.chunk(shorter), stored.stream().map(ArtifactChunk::text).toList());
    assertEquals(IntStream.range(0, count).boxed().toList(), stored.stream().map(ArtifactChunk::index).toList());
  }

  @Test
  void stagesObjectStoreContentWhenTheSpoolCopyIsGoneAndCleansUp() throws Exception {
    String text = words(0, 20);
    storeArtifact(1, text);
    spool.discard(artifact().spoolPath());
    metadata.update(
        TENANT, ARTIFACT, current -> current.withStorage(ArtifactStatus.Storage.STORED, "key-1"));
    ObjectStore objects = objectStore(Map.of("key-1", text.getBytes(StandardCharsets.UTF_8)));

    int count = job(objects).execute();

    assertEquals(chunker.chunk(text).size(), count);
    try (var scratch = Files.list(spoolRoot.resolve(".scratch"))) {
      assertEquals(0, scratch.count());
    }
  }

  @Test
  void failPolicyStopsAtTheChunkBudgetWithADistinctFailureCode() throws Exception {
    storeArtifact(1, words(0, 100));

    assertThrows(
        IndexJob.ChunkLimitExceededException.class,
        () -> job(new IndexJob.Options(BATCH, 5, IndexJob.ChunkLimitPolicy.FAIL)).execute());

    assertEquals(ArtifactStatus.Index.RETRY_PENDING, artifact().indexStatus());
    assertEquals("INDEX_CHUNK_LIMIT_EXCEEDED", artifact().failureCode());
    assertTrue(providerCallSizes.stream().mapToInt(Integer::intValue).sum() <= 5);
  }

  @Test
  void truncatePolicyIndexesTheFirstChunksAndFlagsTheArtifact() throws Exception {
    String text = words(0, 100);
    storeArtifact(1, text);

    int count = job(new IndexJob.Options(BATCH, 5, IndexJob.ChunkLimitPolicy.TRUNCATE)).execute();

    assertEquals(5, count);
    assertEquals(
        chunker.chunk(text).subList(0, 5),
        vectors.chunks(TENANT, ARTIFACT).stream().map(ArtifactChunk::text).toList());
    assertEquals(ArtifactStatus.Index.READY, artifact().indexStatus());
    assertEquals("INDEX_TRUNCATED", artifact().failureCode());

    // A later version that fits clears the flag.
    storeArtifact(2, words(0, 10));
    job(new IndexJob.Options(BATCH, 5, IndexJob.ChunkLimitPolicy.TRUNCATE)).execute();
    assertEquals(null, artifact().failureCode());
  }

  @Test
  void reachingTheBudgetStopsReadingTheDocument() throws Exception {
    storeArtifact(1, "ignored");
    int[] piecesEmitted = {0};
    TextExtractor endless =
        new TextExtractor() {
          public boolean supports(String mediaType) {
            return true;
          }

          public void extract(Path file, TextSink sink) throws IOException {
            for (int piece = 0; piece < 100_000; piece++) {
              piecesEmitted[0]++;
              sink.accept("w" + piece + " ");
            }
          }
        };

    job(null, endless, new IndexJob.Options(BATCH, 4, IndexJob.ChunkLimitPolicy.TRUNCATE))
        .execute();

    // 4 chunks of 4 words with 1 overlap need 13 words, plus the word that trips the budget.
    assertTrue(piecesEmitted[0] < 20, () -> "read " + piecesEmitted[0] + " pieces");
  }

  private IndexJob job() {
    return job((ObjectStore) null);
  }

  private IndexJob job(ObjectStore objects) {
    return job(objects, PLAIN_TEXT, new IndexJob.Options(BATCH, 10_000, IndexJob.ChunkLimitPolicy.FAIL));
  }

  private IndexJob job(IndexJob.Options options) {
    return job(null, PLAIN_TEXT, options);
  }

  private IndexJob job(ObjectStore objects, TextExtractor extractor, IndexJob.Options options) {
    return new IndexJob(
        TENANT,
        ARTIFACT,
        metadata,
        objects,
        spool,
        new TextExtractorFactory(List.of(extractor)),
        null,
        chunker,
        embeddings,
        vectors,
        options);
  }

  private void storeArtifact(long version, String text) throws IOException {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    Path spoolPath =
        spool
            .write(TENANT, ARTIFACT + "-v" + version, new ByteArrayInputStream(bytes), Map.of())
            .contentPath();
    Instant now = Instant.now();
    metadata.save(
        new Artifact(
            ARTIFACT,
            TENANT,
            "key",
            "fingerprint",
            "UPLOAD",
            null,
            "doc.txt",
            "text/plain",
            bytes.length,
            "sha",
            null,
            spoolPath,
            ArtifactStatus.Storage.SPOOLED,
            ArtifactStatus.Index.PENDING,
            version,
            Map.of(),
            null,
            null,
            now,
            now));
  }

  private Artifact artifact() {
    return metadata.find(TENANT, ARTIFACT).orElseThrow();
  }

  private static String words(int from, int count) {
    return IntStream.range(from, from + count)
        .mapToObj(index -> "w" + index)
        .collect(Collectors.joining(" "));
  }

  private static ObjectStore objectStore(Map<String, byte[]> objects) {
    return new ObjectStore() {
      public void put(String tenant, String key, InputStream content, long length, String sha256) {
        throw new UnsupportedOperationException();
      }

      public InputStream get(String tenant, String key) {
        return new ByteArrayInputStream(objects.get(key));
      }

      public boolean verified(String tenant, String key, long length, String sha256) {
        return true;
      }

      public void delete(String tenant, String key) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private EmbeddingProvider provider() {
    return new EmbeddingProvider() {
      public String model() {
        return "test-model";
      }

      public String version() {
        return "1";
      }

      public List<float[]> embed(List<String> texts) {
        if (providerCallSizes.size() >= failAfter) {
          throw new IllegalStateException("provider unavailable");
        }
        providerCallSizes.add(texts.size());
        return texts.stream().map(text -> new float[] {text.length()}).toList();
      }
    };
  }

  private static EmbeddingCache noCache() {
    return new EmbeddingCache() {
      public Optional<float[]> get(String model, String version, String hash) {
        return Optional.empty();
      }

      public void put(String model, String version, String hash, float[] vector) {}
    };
  }
}
