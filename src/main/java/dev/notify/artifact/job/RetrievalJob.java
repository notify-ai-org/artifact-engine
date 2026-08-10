package dev.notify.artifact.job;

import dev.notify.artifact.EngineOptions;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.embed.EmbeddingService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactChunk;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.model.Requests;
import dev.notify.artifact.security.RetrievalSecurityContext;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.SecurityFilterChain;
import dev.notify.artifact.store.MetadataStore;
import dev.notify.artifact.store.VectorStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Authorized hybrid retrieval workflow with reciprocal-rank fusion and content sanitization. */
public record RetrievalJob(
    Requests.Search request,
    MetadataStore metadataStore,
    VectorStore vectorStore,
    EmbeddingService embeddingService,
    AuthorizationService authorizationService,
    EngineOptions options,
    SecurityFilterChain<RetrievalSecurityContext> retrievalSecurity,
    SecurityContextFactory securityContextFactory)
    implements Job<List<Requests.SearchHit>> {

  @Override
  public List<Requests.SearchHit> execute() {
    authorizationService.require(
        request.principalId(), request.tenantId(), AuthorizationService.Permission.SEARCH);
    verify(null, null);

    float[] queryEmbedding = embeddingService.embed(List.of(request.query())).get(0);
    int candidateLimit = request.limit() * options.retrievalCandidateMultiplier();
    VectorStore.SearchFilter storeFilter =
        new VectorStore.SearchFilter(request.mediaTypes(), request.tags());
    List<VectorStore.ScoredChunk> semantic =
        vectorStore.search(request.tenantId(), queryEmbedding, candidateLimit, storeFilter);
    List<VectorStore.ScoredChunk> keyword =
        vectorStore.keywordSearch(request.tenantId(), request.query(), candidateLimit, storeFilter);
    return fuseAndFilter(semantic, keyword);
  }

  private List<Requests.SearchHit> fuseAndFilter(
      List<VectorStore.ScoredChunk> semantic, List<VectorStore.ScoredChunk> keyword) {
    Map<String, Double> fusedScores = new HashMap<>();
    Map<String, ArtifactChunk> chunksById = new HashMap<>();
    addReciprocalRanks(semantic, fusedScores, chunksById);
    addReciprocalRanks(keyword, fusedScores, chunksById);

    List<Requests.SearchHit> results = new ArrayList<>();
    fusedScores.entrySet().stream()
        .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder()))
        .forEach(
            entry -> {
              if (results.size() >= request.limit()) {
                return;
              }
              ArtifactChunk chunk = chunksById.get(entry.getKey());
              metadataStore
                  .find(request.tenantId(), chunk.artifactId())
                  .filter(this::matchesFilters)
                  .ifPresent(
                      artifact -> {
                        ArtifactChunk securedChunk =
                            withText(chunk, verify(artifact, chunk.text()).extractedContent());
                        results.add(
                            new Requests.SearchHit(
                                artifact,
                                securedChunk,
                                entry.getValue(),
                                "artifact://" + artifact.id() + "/chunks/" + chunk.id()));
                      });
            });
    return List.copyOf(results);
  }

  private RetrievalSecurityContext verify(Artifact artifact, String extractedContent) {
    return JobRetrievalAccess.verify(
        request.principalId(),
        request.tenantId(),
        AuthorizationService.Permission.SEARCH,
        artifact,
        extractedContent,
        retrievalSecurity,
        securityContextFactory);
  }

  private boolean matchesFilters(Artifact artifact) {
    if (artifact.indexStatus() != ArtifactStatus.Index.READY) {
      return false;
    }
    if (!request.mediaTypes().isEmpty() && !request.mediaTypes().contains(artifact.mediaType())) {
      return false;
    }
    if (request.createdAfter() != null && artifact.createdAt().isBefore(request.createdAfter())) {
      return false;
    }
    if (!request.tags().isEmpty()) {
      Set<String> artifactTags = parseTags(artifact.metadata().get("tags"));
      return artifactTags.containsAll(request.tags());
    }
    return true;
  }

  private static void addReciprocalRanks(
      List<VectorStore.ScoredChunk> ranked,
      Map<String, Double> scores,
      Map<String, ArtifactChunk> chunks) {
    final double rankConstant = 60.0;
    for (int index = 0; index < ranked.size(); index++) {
      ArtifactChunk chunk = ranked.get(index).chunk();
      chunks.putIfAbsent(chunk.id(), chunk);
      scores.merge(chunk.id(), 1.0 / (rankConstant + index + 1), Double::sum);
    }
  }

  private static ArtifactChunk withText(ArtifactChunk source, String text) {
    if (Objects.equals(source.text(), text)) {
      return source;
    }
    return new ArtifactChunk(
        source.id(),
        source.artifactId(),
        source.tenantId(),
        source.index(),
        text,
        source.tokenCount(),
        source.pageNumber(),
        source.section(),
        source.coordinates(),
        source.contentSha256(),
        source.embeddingModel(),
        source.embeddingVersion(),
        source.embedding());
  }

  private static Set<String> parseTags(String value) {
    if (value == null || value.isBlank()) {
      return Set.of();
    }
    Set<String> tags = new HashSet<>();
    for (String tag : value.split(",")) {
      if (!tag.isBlank()) {
        tags.add(tag.trim());
      }
    }
    return Set.copyOf(tags);
  }
}
