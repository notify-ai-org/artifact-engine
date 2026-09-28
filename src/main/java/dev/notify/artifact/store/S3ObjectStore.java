package dev.notify.artifact.store;

import dev.notify.artifact.util.Checksum;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

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
    validateChecksum(sha256);
    return verifiedAgainst(tenantId, key, length, sha256, base64Sha256(sha256));
  }

  @Override
  public boolean verifiedMultipart(
      String tenantId, String key, long length, String sha256, String compositeSha256)
      throws IOException {
    validateChecksum(sha256);
    if (compositeSha256 == null || !compositeSha256.matches("[A-Za-z0-9+/]{43}=-[0-9]{1,5}")) {
      throw new IllegalArgumentException("A composite SHA-256 checksum is required");
    }
    return verifiedAgainst(tenantId, key, length, sha256, compositeSha256);
  }

  /**
   * HEAD with checksum mode: S3 reports a whole-object SHA-256 for single puts and the composite
   * {@code <base64>-<parts>} form for multipart objects. Everything else is checked identically.
   */
  private boolean verifiedAgainst(
      String tenantId, String key, long length, String sha256, String expectedChecksum)
      throws IOException {
    validateKey(tenantId, key);
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
          && expectedChecksum.equals(response.checksumSHA256())
          && kmsEncrypted(response.serverSideEncryptionAsString())
          && response.ssekmsKeyId() != null
          && !response.ssekmsKeyId().isBlank();
    } catch (NoSuchKeyException missing) {
      return false;
    } catch (S3Exception failure) {
      if (failure.statusCode() == 404) return false;
      throw storageFailure("S3 head verification failed", failure);
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

  @Override
  public boolean supportsMultipart() {
    return true;
  }

  @Override
  public String createMultipartUpload(String tenantId, String key, String sha256)
      throws IOException {
    validateKey(tenantId, key);
    validateChecksum(sha256);
    CreateMultipartUploadRequest.Builder request =
        CreateMultipartUploadRequest.builder()
            .bucket(configuration.bucket())
            .key(key)
            .serverSideEncryption(ServerSideEncryption.AWS_KMS)
            .ssekmsKeyId(configuration.kmsKeyId())
            .bucketKeyEnabled(configuration.bucketKeyEnabled())
            .checksumAlgorithm(ChecksumAlgorithm.SHA256)
            .checksumType(ChecksumType.COMPOSITE)
            .metadata(
                Map.of(SHA256_METADATA, sha256, TENANT_HASH_METADATA, Checksum.sha256(tenantId)));
    expectedOwner(request::expectedBucketOwner);
    try {
      return s3.createMultipartUpload(request.build()).uploadId();
    } catch (SdkException failure) {
      throw storageFailure("S3 multipart create failed", failure);
    }
  }

  @Override
  public UploadedPart uploadPart(
      String tenantId,
      String key,
      String uploadId,
      int partNumber,
      InputStream content,
      long length,
      String partSha256)
      throws IOException {
    validateKey(tenantId, key);
    validateChecksum(partSha256);
    validatePart(uploadId, partNumber);
    if (length < 1) {
      throw new IllegalArgumentException("Part length must be positive");
    }
    UploadPartRequest.Builder request =
        UploadPartRequest.builder()
            .bucket(configuration.bucket())
            .key(key)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength(length)
            .checksumSHA256(base64Sha256(partSha256));
    expectedOwner(request::expectedBucketOwner);
    try {
      String etag =
          s3.uploadPart(
                  request.build(), RequestBody.fromInputStream(Objects.requireNonNull(content), length))
              .eTag();
      return new UploadedPart(partNumber, etag, length, base64Sha256(partSha256));
    } catch (SdkException failure) {
      throw storageFailure("S3 multipart part upload failed", failure);
    }
  }

  @Override
  public List<UploadedPart> listParts(String tenantId, String key, String uploadId)
      throws IOException {
    validateKey(tenantId, key);
    validatePart(uploadId, 1);
    List<UploadedPart> parts = new ArrayList<>();
    Integer marker = null;
    try {
      while (true) {
        ListPartsRequest.Builder request =
            ListPartsRequest.builder()
                .bucket(configuration.bucket())
                .key(key)
                .uploadId(uploadId)
                .partNumberMarker(marker);
        expectedOwner(request::expectedBucketOwner);
        ListPartsResponse response = s3.listParts(request.build());
        for (Part part : response.parts()) {
          parts.add(
              new UploadedPart(
                  part.partNumber(), part.eTag(), part.size(), part.checksumSHA256()));
        }
        if (!Boolean.TRUE.equals(response.isTruncated())) break;
        marker = response.nextPartNumberMarker();
      }
    } catch (SdkException failure) {
      throw storageFailure("S3 multipart part listing failed", failure);
    }
    parts.sort(Comparator.comparingInt(UploadedPart::partNumber));
    return List.copyOf(parts);
  }

  @Override
  public void completeMultipartUpload(
      String tenantId, String key, String uploadId, List<UploadedPart> parts) throws IOException {
    validateKey(tenantId, key);
    validatePart(uploadId, 1);
    if (parts == null || parts.isEmpty()) {
      throw new IllegalArgumentException("At least one part is required");
    }
    List<CompletedPart> completed =
        parts.stream()
            .sorted(Comparator.comparingInt(UploadedPart::partNumber))
            .map(
                part ->
                    CompletedPart.builder()
                        .partNumber(part.partNumber())
                        .eTag(part.etag())
                        .checksumSHA256(part.checksumSha256())
                        .build())
            .toList();
    CompleteMultipartUploadRequest.Builder request =
        CompleteMultipartUploadRequest.builder()
            .bucket(configuration.bucket())
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build());
    expectedOwner(request::expectedBucketOwner);
    try {
      s3.completeMultipartUpload(request.build());
    } catch (SdkException failure) {
      throw storageFailure("S3 multipart complete failed", failure);
    }
  }

  @Override
  public void abortMultipartUpload(String tenantId, String key, String uploadId)
      throws IOException {
    validateKey(tenantId, key);
    validatePart(uploadId, 1);
    AbortMultipartUploadRequest.Builder request =
        AbortMultipartUploadRequest.builder()
            .bucket(configuration.bucket())
            .key(key)
            .uploadId(uploadId);
    expectedOwner(request::expectedBucketOwner);
    try {
      s3.abortMultipartUpload(request.build());
    } catch (NoSuchUploadException alreadyGone) {
      // Completed, aborted, or expired by a lifecycle rule; nothing left to discard.
    } catch (SdkException failure) {
      throw storageFailure("S3 multipart abort failed", failure);
    }
  }

  private static void validatePart(String uploadId, int partNumber) {
    if (uploadId == null || uploadId.isBlank()) {
      throw new IllegalArgumentException("Multipart upload id is required");
    }
    if (partNumber < 1 || partNumber > 10_000) {
      throw new IllegalArgumentException("S3 part numbers range from 1 to 10000");
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
