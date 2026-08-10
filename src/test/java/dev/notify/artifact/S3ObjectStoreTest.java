package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.notify.artifact.store.s3.S3ObjectStore;
import dev.notify.artifact.util.Checksum;
import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

class S3ObjectStoreTest {
  private S3Client client;
  private S3ObjectStore store;
  private String key;

  @BeforeEach
  void setUp() {
    client = org.mockito.Mockito.mock(S3Client.class);
    store =
        new S3ObjectStore(
            client,
            new S3ObjectStore.Configuration(
                "artifacts", "prod", "alias/artifacts", "123456789012", true));
    key = "prod/" + Checksum.sha256("tenant-a") + "/document/2026/08/id/1/content";
  }

  @Test
  void streamsWithTenantMetadataChecksumAndKmsEncryption() throws Exception {
    byte[] content = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    String sha256 = Checksum.sha256(new ByteArrayInputStream(content));
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().build());

    store.put("tenant-a", key, new ByteArrayInputStream(content), content.length, sha256);

    ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(client).putObject(request.capture(), any(RequestBody.class));
    assertEquals(ServerSideEncryption.AWS_KMS, request.getValue().serverSideEncryption());
    assertEquals("alias/artifacts", request.getValue().ssekmsKeyId());
    assertEquals(sha256, request.getValue().metadata().get("artifact-sha256"));
    assertEquals(
        Base64.getEncoder().encodeToString(HexFormat.of().parseHex(sha256)),
        request.getValue().checksumSHA256());
  }

  @Test
  void verifiesHeadLengthChecksumEncryptionAndTenant() throws Exception {
    String sha256 = Checksum.sha256("hello");
    String checksum = Base64.getEncoder().encodeToString(HexFormat.of().parseHex(sha256));
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            HeadObjectResponse.builder()
                .contentLength(5L)
                .checksumSHA256(checksum)
                .serverSideEncryption(ServerSideEncryption.AWS_KMS)
                .ssekmsKeyId("arn:aws:kms:region:123456789012:key/key-id")
                .metadata(
                    Map.of("artifact-sha256", sha256, "tenant-sha256", Checksum.sha256("tenant-a")))
                .build());

    assertTrue(store.verified("tenant-a", key, 5, sha256));
  }

  @Test
  void rejectsCrossTenantKeyBeforeCallingS3() {
    assertThrows(
        SecurityException.class,
        () -> store.put("tenant-b", key, new ByteArrayInputStream(new byte[0]), 0, "0".repeat(64)));
  }
}
