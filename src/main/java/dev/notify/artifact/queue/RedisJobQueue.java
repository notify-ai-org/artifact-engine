package dev.notify.artifact.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.model.JobRecord;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Redis sorted-set queue whose claim and owner-token completion transitions are atomic Lua scripts.
 * Job ids make enqueue idempotent, while expired leases return to their type-specific ready set.
 */
public final class RedisJobQueue implements JobQueue {
  private static final int RECOVERY_BATCH_SIZE = 100;

  private static final String ENQUEUE =
          """
          if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
          redis.call('SET', KEYS[1], ARGV[1])
          redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
          return 1
          """;

  private static final String CLAIM =
          """
          local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
          if #ids == 0 then return nil end
          local id = ids[1]
          local jobKey = ARGV[4] .. id
          local json = redis.call('GET', jobKey)
          if not json then
            redis.call('ZREM', KEYS[1], id)
            return nil
          end
          local job = cjson.decode(json)
          job.status = 'CLAIMED'
          job.attempts = (job.attempts or 0) + 1
          job.leaseOwner = ARGV[2]
          job.leaseExpiresAt = ARGV[3]
          job.updatedAt = ARGV[5]
          local updated = cjson.encode(job)
          redis.call('SET', jobKey, updated)
          redis.call('ZREM', KEYS[1], id)
          redis.call('ZADD', KEYS[2], ARGV[6], id)
          return updated
          """;

  private static final String REQUEUE =
          """
          redis.call('SET', KEYS[1], ARGV[1])
          redis.call('ZREM', KEYS[2], ARGV[2])
          redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
          return 1
          """;

  private static final String FINISH =
          """
          local json = redis.call('GET', KEYS[1])
          if not json then return 0 end
          local job = cjson.decode(json)
          if job.leaseOwner ~= ARGV[1] then return 0 end
          local leaseScore = redis.call('ZSCORE', KEYS[2], job.id)
          if not leaseScore or tonumber(leaseScore) <= tonumber(ARGV[7]) then return 0 end
          job.status = ARGV[2]
          if ARGV[3] == '' then job.lastError = cjson.null else job.lastError = ARGV[3] end
          if ARGV[4] == '' then job.nextAttemptAt = cjson.null else job.nextAttemptAt = ARGV[4] end
          job.leaseOwner = cjson.null
          job.leaseExpiresAt = cjson.null
          job.updatedAt = ARGV[5]
          redis.call('SET', KEYS[1], cjson.encode(job))
          redis.call('ZREM', KEYS[2], job.id)
          if ARGV[2] == 'RETRY_PENDING' then
            redis.call('ZADD', KEYS[3], ARGV[6], job.id)
          end
          return 1
          """;

  private static final String RECOVER =
          """
          local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
          local recovered = 0
          for _, id in ipairs(ids) do
            local jobKey = ARGV[3] .. id
            local json = redis.call('GET', jobKey)
            if json then
              local job = cjson.decode(json)
              job.status = 'RETRY_PENDING'
              job.nextAttemptAt = ARGV[4]
              job.leaseOwner = cjson.null
              job.leaseExpiresAt = cjson.null
              job.lastError = 'lease expired'
              job.updatedAt = ARGV[4]
              redis.call('SET', jobKey, cjson.encode(job))
              redis.call('ZADD', ARGV[5] .. job.type, ARGV[1], id)
              recovered = recovered + 1
            end
            redis.call('ZREM', KEYS[1], id)
          end
          return recovered
          """;

  private final RedisCommands<String, String> redis;
  private final ObjectMapper json;
  private final String namespace;

  public RedisJobQueue(
      StatefulRedisConnection<String, String> connection, ObjectMapper json, String namespace) {
    this(connection.sync(), json, namespace);
  }

  public RedisJobQueue(
      RedisCommands<String, String> redis, ObjectMapper json, String namespace) {
    this.redis = redis;
    this.json = json.copy().findAndRegisterModules();
    if (namespace == null || !namespace.matches("[A-Za-z0-9:_-]+")) {
      throw new IllegalArgumentException("Redis namespace contains unsafe characters");
    }
    this.namespace = namespace.endsWith(":") ? namespace : namespace + ":";
  }

  @Override
  public void enqueue(JobRecord job) {
    redis.eval(
        ENQUEUE,
        ScriptOutputType.INTEGER,
        new String[] {jobKey(job.id()), readyKey(job.type())},
        serialize(job),
        Long.toString(score(job.nextAttemptAt())),
        job.id());
  }

  @Override
  public void requeue(JobRecord job) {
    redis.eval(
        REQUEUE,
        ScriptOutputType.INTEGER,
        new String[] {jobKey(job.id()), leasedKey(), readyKey(job.type())},
        serialize(job),
        job.id(),
        Long.toString(score(job.nextAttemptAt())));
  }

  @Override
  public Optional<JobRecord> claim(
      JobRecord.JobType type, String owner, Duration lease, Instant now) {
    Instant expiry = now.plus(lease);
    String claimed =
        redis.eval(
            CLAIM,
            ScriptOutputType.VALUE,
            new String[] {readyKey(type), leasedKey()},
            Long.toString(now.toEpochMilli()),
            owner,
            expiry.toString(),
            jobPrefix(),
            now.toString(),
            Long.toString(expiry.toEpochMilli()));
    return claimed == null ? Optional.empty() : Optional.of(deserialize(claimed));
  }

  @Override
  public boolean complete(String jobId, String owner) {
    return finish(jobId, owner, JobRecord.JobStatus.COMPLETED, null, null);
  }

  @Override
  public boolean retry(String jobId, String owner, Instant nextAttempt, String error) {
    return finish(jobId, owner, JobRecord.JobStatus.RETRY_PENDING, nextAttempt, error);
  }

  @Override
  public boolean deadLetter(String jobId, String owner, String error) {
    return finish(jobId, owner, JobRecord.JobStatus.DEAD_LETTER, null, error);
  }

  @Override
  public int recoverExpired(Instant now) {
    Long recovered =
        redis.eval(
            RECOVER,
            ScriptOutputType.INTEGER,
            new String[] {leasedKey()},
            Long.toString(now.toEpochMilli()),
            Integer.toString(RECOVERY_BATCH_SIZE),
            jobPrefix(),
            now.toString(),
            readyPrefix());
    return recovered == null ? 0 : recovered.intValue();
  }

  private boolean finish(
      String jobId, String owner, JobRecord.JobStatus status, Instant nextAttempt, String error) {
    JobRecord existing = load(jobId);
    if (existing == null) {
      return false;
    }
    Instant now = Instant.now();
    Long updated =
        redis.eval(
            FINISH,
            ScriptOutputType.INTEGER,
            new String[] {jobKey(jobId), leasedKey(), readyKey(existing.type())},
            owner,
            status.name(),
            error == null ? "" : error,
            nextAttempt == null ? "" : nextAttempt.toString(),
            now.toString(),
            Long.toString(score(nextAttempt)),
            Long.toString(now.toEpochMilli()));
    return updated != null && updated == 1;
  }

  private JobRecord load(String jobId) {
    String value = redis.get(jobKey(jobId));
    return value == null ? null : deserialize(value);
  }

  private String serialize(JobRecord job) {
    try {
      return json.writeValueAsString(job);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Job cannot be serialized", failure);
    }
  }

  private JobRecord deserialize(String value) {
    try {
      return json.readValue(value, JobRecord.class);
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Stored job is not valid JSON", failure);
    }
  }

  private long score(Instant time) {
    return time == null ? 0 : time.toEpochMilli();
  }

  private String jobPrefix() {
    return namespace + "job:";
  }

  private String jobKey(String id) {
    return jobPrefix() + id;
  }

  private String readyPrefix() {
    return namespace + "ready:";
  }

  private String readyKey(JobRecord.JobType type) {
    return readyPrefix() + type.name();
  }

  private String leasedKey() {
    return namespace + "leased";
  }
}
