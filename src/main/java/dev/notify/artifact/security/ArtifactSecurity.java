package dev.notify.artifact.security;

/** Fully configured engine security layer; partial configuration is not representable. */
public record ArtifactSecurity(
    SecurityFilterChain<IngestionSecurityContext> ingestion,
    SecurityFilterChain<RetrievalSecurityContext> retrieval,
    SecurityContextFactory contextFactory) {}
