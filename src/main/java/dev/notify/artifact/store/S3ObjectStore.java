package dev.notify.artifact.store.s3;

import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.util.Checksum;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/** AWS SDK v2 object store with exact-length streaming, tenant keys, checksums, and SSE-KMS. */
public final class S3ObjectStore implements ObjectStore {
  private static final String SHA256_METADATA = "artifact-sha256";
  private static final String TENANT_HASH_METADATA = "tenant-sha256";

  private final S3Client s3;
  private final Configuration configuration;

  public S3ObjectStore(S3Client s3, Configuration configuration) {
    this.s3 = Objects.requireNonNull(s3, "s3");
    this.configuration = Objects.requireNonNull(configuration, "configuration");
  }

  @Override
  public void put(String tenantId, String key, InputStream content, long length, String sha256)
      throws IOException {
    validateKey(tenantId, key);
    validateChecksum(sha256);
    if (length < 0) {
      throw new IllegalArgumentException("S3 streaming upload requires an exact content length");
    }
    Objects.requireNonNull(content, "content");

    PutObjectRequest.Builder request =
        PutObjectRequest.builder()
            .bucket(configuration.bucket())
            .key(key)
            .serverSideEncryption(ServerSideEncryption.AWS_KMS)
            .ssekmsKeyId(configuration.kmsKeyId())
            .bucketKeyEnabled(configuration.bucketKeyEnabled())
            .checksumSHA256(base64Sha256(sha256))
            .metadata(
                Map.of(SHA256_METADATA, sha256, TENANT_HASH_METADATA, Checksum.sha256(tenantId)));
    expectedOwner(request::expectedBucketOwner);

    try {
      s3.putObject(request.build(), RequestBody.fromInputStream(content, length));
    } catch (SdkException failure) {
      throw storageFailure("S3 put failed", failure);
    }
  }

  @Override
  public InputStream get(String tenantId, String key) throws IOException {
    validateKey(tenantId, key);
    GetObjectRequest.Builder request =
        GetObjectRequest.builder().bucket(configuration.bucket()).key(key);
    expectedOwner(request::expectedBucketOwner);

    try {
      ResponseInputStream<GetObjectResponse> response = s3.getObject(request.build());
      if (!kmsEncrypted(response.response().serverSideEncryptionAsString())
          || !Checksum.sha256(tenantId)
              .equals(response.response().metadata().get(TENANT_HASH_METADATA))) {
        response.close();
        throw new IOException("S3 object failed encryption or tenant metadata verification");
      }
      return response;
    } catch (SdkException failure) {
      throw storageFailure("S3 get failed", failure);
    }
  }

  @Override
  public boolean verified(String tenantId, String key, long length, String sha256)
      throws IOException {
    validateKey(tenantId, key);
    validateChecksum(sha256);
    HeadObjectRequest.Builder request =
        HeadObjectRequest.builder()
            .bucket(configuration.bucket())
            .key(key)
            .checksumMode(ChecksumMode.ENABLED);
    expectedOwner(request::expectedBucketOwner);

    try {
      HeadObjectResponse response = s3.headObject(request.build());
      return response.contentLength() == length
          && sha256.equals(response.metadata().get(SHA256_METADATA))
          && Checksum.sha256(tenantId).equals(response.metadata().get(TENANT_HASH_METADATA))
          && base64Sha256(sha256).equals(response.checksumSHA256())
          && kmsEncrypted(response.serverSideEncryptionAsString())
          && response.ssekmsKeyId() != null
          && !response.ssekmsKeyId().isBlank();
    } catch (SdkException failure) {
      throw storageFailure("S3 head verification failed", failure);
    }
  }

  @Override
  public void delete(String tenantId, String key) throws IOException {
    validateKey(tenantId, key);
    DeleteObjectRequest.Builder request =
        DeleteObjectRequest.builder().bucket(configuration.bucket()).key(key);
    expectedOwner(request::expectedBucketOwner);
    try {
      s3.deleteObject(request.build());
    } catch (SdkException failure) {
      throw storageFailure("S3 delete failed", failure);
    }
  }

  private void validateKey(String tenantId, String key) {
    if (tenantId == null || tenantId.isBlank() || key == null || key.isBlank()) {
      throw new IllegalArgumentException("Tenant and object key are required");
    }
    String expectedPrefix = configuration.environment() + "/" + Checksum.sha256(tenantId) + "/";
    if (!key.startsWith(expectedPrefix)
        || key.contains("//")
        || java.util.Arrays.asList(key.split("/", -1)).contains("..")) {
      throw new SecurityException("S3 key is outside the tenant namespace");
    }
  }

  private void validateChecksum(String sha256) {
    if (sha256 == null || !sha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("A lowercase hexadecimal SHA-256 checksum is required");
    }
  }

  private static String base64Sha256(String hexadecimal) {
    return Base64.getEncoder().encodeToString(HexFormat.of().parseHex(hexadecimal));
  }

  private static boolean kmsEncrypted(String encryption) {
    return "aws:kms".equals(encryption) || "aws:kms:dsse".equals(encryption);
  }

  private void expectedOwner(java.util.function.Consumer<String> setter) {
    if (configuration.expectedBucketOwner() != null) {
      setter.accept(configuration.expectedBucketOwner());
    }
  }

  private static IOException storageFailure(String message, SdkException failure) {
    return new IOException(message, failure);
  }

  public record Configuration(
      String bucket,
      String environment,
      String kmsKeyId,
      String expectedBucketOwner,
      boolean bucketKeyEnabled) {
    public Configuration {
      require(bucket, "bucket");
      require(kmsKeyId, "kmsKeyId");
      if (environment == null || !environment.matches("[A-Za-z0-9_-]+")) {
        throw new IllegalArgumentException("environment must be path-safe");
      }
      if (expectedBucketOwner != null && !expectedBucketOwner.matches("[0-9]{12}")) {
        throw new IllegalArgumentException("expectedBucketOwner must be a 12-digit AWS account id");
      }
    }

    private static void require(String value, String field) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException(field + " is required");
      }
    }
  }
}
