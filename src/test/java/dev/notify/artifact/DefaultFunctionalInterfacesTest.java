package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.auth.AuthorizationService;
import dev.notify.artifact.auth.DefaultAuthorizationService;
import dev.notify.artifact.extract.DefaultOcr;
import dev.notify.artifact.filter.IdentityFilter;
import dev.notify.artifact.job.DefaultJob;
import dev.notify.artifact.job.DirectJobDispatcher;
import dev.notify.artifact.model.JobRecord;
import dev.notify.artifact.retry.DefaultCheckedSupplier;
import dev.notify.artifact.security.DefaultAuthenticationService;
import dev.notify.artifact.security.DefaultMalwareScanner;
import dev.notify.artifact.security.MalwareScanner;
import dev.notify.artifact.security.SecurityIdentity;
import dev.notify.artifact.security.SystemHostResolver;
import dev.notify.artifact.security.UrlIngestionPolicyFilter;
import dev.notify.artifact.util.AcknowledgeHandler;
import dev.notify.artifact.util.NoOpAcknowledgeHandler;
import dev.notify.artifact.worker.DefaultDurableJobHandler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultFunctionalInterfacesTest {
  @Test
  void authorizationUsesExactPrincipalTenantAndPermissionGrants() {
    var subject = new DefaultAuthorizationService.Subject("principal-a", "tenant-a");
    var authorization =
        new DefaultAuthorizationService(
            Map.of(subject, Set.of(AuthorizationService.Permission.INGEST)));

    authorization.require("principal-a", "tenant-a", AuthorizationService.Permission.INGEST);

    assertThrows(
        SecurityException.class,
        () ->
            authorization.require(
                "principal-a", "tenant-a", AuthorizationService.Permission.DOWNLOAD));
    assertThrows(
        SecurityException.class,
        () ->
            authorization.require(
                "principal-a", "tenant-b", AuthorizationService.Permission.INGEST));
    assertThrows(
        SecurityException.class,
        () ->
            new DefaultAuthorizationService()
                .require("principal-a", "tenant-a", AuthorizationService.Permission.INGEST));
  }

  @Test
  void authenticationReturnsOnlyRegisteredOpaqueHandles() {
    var identity =
        new SecurityIdentity(
            "principal-a", "tenant-a", Set.of("INGEST"), Instant.parse("2030-01-01T00:00:00Z"));
    var authentication = new DefaultAuthenticationService(Map.of("opaque-handle", identity));

    assertSame(identity, authentication.authenticate("opaque-handle"));
    assertThrows(SecurityException.class, () -> authentication.authenticate("unknown"));
    assertThrows(
        SecurityException.class,
        () -> new DefaultAuthenticationService().authenticate("opaque-handle"));
  }

  @Test
  void unconfiguredContentAdaptersFailClosed() {
    MalwareScanner.ScanResult scan = new DefaultMalwareScanner().scan(Path.of("artifact.bin"));
    assertEquals(MalwareScanner.Verdict.ERROR, scan.verdict());
    assertEquals("unconfigured", scan.scanner());

    assertThrows(
        IOException.class,
        () ->
            new DefaultOcr()
                .recognize(new ByteArrayInputStream(new byte[] {1, 2, 3}), "image/png"));
  }

  @Test
  void executionAdaptersDelegateWithoutChangingResultsOrFailures() throws Exception {
    var dispatcher = new DirectJobDispatcher();
    assertEquals("result", dispatcher.dispatch(new DefaultJob<>(() -> "result")));
    assertEquals(42, new DefaultCheckedSupplier<>(() -> 42).get());

    IOException failure = new IOException("expected");
    DefaultJob<Void> failingJob =
        new DefaultJob<>(
            () -> {
              throw failure;
            });
    assertSame(failure, assertThrows(IOException.class, failingJob::execute));
  }

  @Test
  void durableHandlerResolvesAndExecutesTheWorkflowJob() throws Exception {
    JobRecord record =
        JobRecord.pending("job-1", "tenant-a", "artifact-a", JobRecord.JobType.INDEX, Map.of());
    AtomicReference<JobRecord> executed = new AtomicReference<>();
    var handler =
        new DefaultDurableJobHandler(
            queuedRecord ->
                new DefaultJob<>(
                    () -> {
                      executed.set(queuedRecord);
                      return null;
                    }));

    handler.execute(record);

    assertSame(record, executed.get());
  }

  @Test
  void utilityDefaultsAreConcreteAndSideEffectFree() {
    Object value = new Object();
    assertSame(value, new IdentityFilter<>().apply(value));

    NoOpAcknowledgeHandler.INSTANCE.acknowledge(
        "operation-1", AcknowledgeHandler.Outcome.ACCEPTED, "not logged");

    assertInstanceOf(
        SystemHostResolver.class, UrlIngestionPolicyFilter.HostResolver.system());
  }
}
