package dev.notify.artifact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.JobRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JobPriorityTest {
  private static final Duration LEASE = Duration.ofMinutes(1);

  @Test
  void freshJobsAreClaimedBeforeReadyRetryJobs() {
    InMemoryJobQueue queue = new InMemoryJobQueue();
    Instant now = Instant.now();
    queue.enqueue(retry("retry-1", now));
    queue.enqueue(fresh("fresh-1"));
    queue.enqueue(retry("retry-2", now));
    queue.enqueue(fresh("fresh-2"));

    assertEquals("fresh-1", claim(queue).id());
    assertEquals("fresh-2", claim(queue).id());
    assertEquals("retry-1", claim(queue).id());
    assertEquals("retry-2", claim(queue).id());
  }

  @Test
  void aRetryJobKeepsItsPriorityThroughWorkerRetriesAndLeaseRecovery() {
    InMemoryJobQueue queue = new InMemoryJobQueue();
    queue.enqueue(retry("retry-1", Instant.now()));

    JobRecord claimed = claim(queue);
    assertEquals(JobRecord.Priority.RETRY, claimed.priority());
    queue.retry(claimed.id(), "worker", Instant.now(), "transient");
    queue.enqueue(fresh("fresh-1"));

    assertEquals("fresh-1", claim(queue).id());
    JobRecord again = claim(queue);
    assertEquals("retry-1", again.id());
    assertEquals(JobRecord.Priority.RETRY, again.priority());
  }

  @Test
  void recordsSerializedBeforePrioritiesExistedReadAsNormal() throws Exception {
    ObjectMapper json = new ObjectMapper().findAndRegisterModules();
    String legacy = json.writeValueAsString(fresh("legacy")).replace(",\"priority\":\"NORMAL\"", "");
    assertTrue(!legacy.contains("priority"));

    assertEquals(JobRecord.Priority.NORMAL, json.readValue(legacy, JobRecord.class).priority());
  }

  @Test
  void retryAsCopiesTheJobUnderANewIdWithAFullAttemptBudget() {
    JobRecord failed = fresh("original").claimed("worker", Instant.now().plus(LEASE));
    Instant now = Instant.now();

    JobRecord retry = failed.retryAs("retry-id", now);

    assertEquals("retry-id", retry.id());
    assertEquals(failed.type(), retry.type());
    assertEquals(failed.attributes(), retry.attributes());
    assertEquals(JobRecord.JobStatus.PENDING, retry.status());
    assertEquals(0, retry.attempts());
    assertEquals(JobRecord.Priority.RETRY, retry.priority());
  }

  private static JobRecord claim(InMemoryJobQueue queue) {
    return queue
        .claim(JobRecord.JobType.INDEX, "worker", LEASE, Instant.now().plusSeconds(1))
        .orElseThrow();
  }

  private static JobRecord fresh(String id) {
    return JobRecord.pending(id, "tenant", "artifact", JobRecord.JobType.INDEX, Map.of("k", "v"));
  }

  private static JobRecord retry(String id, Instant now) {
    return fresh("source-" + id).retryAs(id, now);
  }
}
