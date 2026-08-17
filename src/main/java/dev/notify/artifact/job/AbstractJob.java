package dev.notify.artifact.job;

import java.util.NoSuchElementException;

import dev.notify.artifact.auth.ArtifactAccessVerifier;
import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.model.Artifact;
import dev.notify.artifact.model.ArtifactStatus;
import dev.notify.artifact.store.MetadataStore;

abstract class AbstractJob<R> implements Job<R> {

    protected final ArtifactAccessVerifier verifier;

    protected final MetadataStore metadataStore;

    protected AbstractJob(ArtifactAccessVerifier verifier, MetadataStore metadataStore) {
        this.verifier = verifier;
        this.metadataStore = metadataStore;
    }

    @Override
    public abstract R execute() throws Exception;

    protected Artifact required(String tenantId, String artifactId) {
        return metadataStore
            .find(tenantId, artifactId)
            .orElseThrow(() -> new NoSuchElementException("Artifact not found"));
    }

    protected Artifact requiredReadable(String tenantId, String artifactId) {
        Artifact artifact = required(tenantId, artifactId);
        if (artifact.storageStatus() == ArtifactStatus.Storage.DELETED
            || artifact.indexStatus() == ArtifactStatus.Index.DELETED) {
            throw new NoSuchElementException("Artifact not found");
        }
        return artifact;
    }

    protected String verify(
      String principalId,
      String tenantId,
      AuthorizationService.Permission permission,
      Artifact artifact,
      String extractedContent
    ) {
        if (artifact == null) {
            return extractedContent;
        }
        verifier.authenticate(principalId, tenantId, permission);
        verifier.verifyArtifact(tenantId, artifact);
        return verifier.verifyExtractedContent(artifact, extractedContent);
    }
    
}
