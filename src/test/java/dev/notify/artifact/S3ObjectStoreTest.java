package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.notify.artifact.store.ObjectStore;
import dev.notify.artifact.store.S3ObjectStore;
import dev.notify.artifact.util.Checksum;
import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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

  @Test
  void startsMultipartUploadsWithKmsCompositeSha256AndTenantMetadata() throws Exception {
    String sha256 = Checksum.sha256("whole object");
    when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());

    assertEquals("upload-1", store.createMultipartUpload("tenant-a", key, sha256));

    ArgumentCaptor<CreateMultipartUploadRequest> request =
        ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
    verify(client).createMultipartUpload(request.capture());
    CreateMultipartUploadRequest sent = request.getValue();
    assertEquals(ServerSideEncryption.AWS_KMS, sent.serverSideEncryption());
    assertEquals("alias/artifacts", sent.ssekmsKeyId());
    assertEquals(ChecksumAlgorithm.SHA256, sent.checksumAlgorithm());
    assertEquals(ChecksumType.COMPOSITE, sent.checksumType());
    assertEquals("123456789012", sent.expectedBucketOwner());
    assertEquals(sha256, sent.metadata().get("artifact-sha256"));
    assertEquals(Checksum.sha256("tenant-a"), sent.metadata().get("tenant-sha256"));
  }

  @Test
  void uploadsEachPartWithItsOwnBase64Checksum() throws Exception {
    byte[] bytes = "part bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    String partSha = Checksum.sha256(new ByteArrayInputStream(bytes));
    when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenReturn(UploadPartResponse.builder().eTag("\"etag-2\"").build());

    ObjectStore.UploadedPart part =
        store.uploadPart("tenant-a", key, "upload-1", 2, new ByteArrayInputStream(bytes),
            bytes.length, partSha);

    ArgumentCaptor<UploadPartRequest> request = ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(client).uploadPart(request.capture(), any(RequestBody.class));
    assertEquals(2, request.getValue().partNumber());
    assertEquals((long) bytes.length, request.getValue().contentLength());
    assertEquals(base64(partSha), request.getValue().checksumSHA256());
    assertEquals(new ObjectStore.UploadedPart(2, "\"etag-2\"", bytes.length, base64(partSha)), part);
  }

  @Test
  void listsEveryPartAcrossPages() throws Exception {
    when(client.listParts(any(ListPartsRequest.class)))
        .thenReturn(
            ListPartsResponse.builder().isTruncated(true).nextPartNumberMarker(1)
                .parts(Part.builder().partNumber(1).eTag("a").size(5L).checksumSHA256("x").build())
                .build(),
            ListPartsResponse.builder().isTruncated(false)
                .parts(Part.builder().partNumber(2).eTag("b").size(3L).checksumSHA256("y").build())
                .build());

    List<ObjectStore.UploadedPart> parts = store.listParts("tenant-a", key, "upload-1");

    assertEquals(List.of(1, 2), parts.stream().map(ObjectStore.UploadedPart::partNumber).toList());
    ArgumentCaptor<ListPartsRequest> requests = ArgumentCaptor.forClass(ListPartsRequest.class);
    verify(client, org.mockito.Mockito.times(2)).listParts(requests.capture());
    assertEquals(1, requests.getAllValues().get(1).partNumberMarker());
  }

  @Test
  void verifiesMultipartObjectsAgainstTheCompositeChecksum() throws Exception {
    String sha256 = Checksum.sha256("hello world");
    List<ObjectStore.UploadedPart> parts = List.of(
        new ObjectStore.UploadedPart(1, "a", 6, base64(Checksum.sha256("hello "))),
        new ObjectStore.UploadedPart(2, "b", 5, base64(Checksum.sha256("world"))));
    String composite = ObjectStore.compositeSha256(parts);
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            HeadObjectResponse.builder()
                .contentLength(11L)
                .checksumSHA256(composite)
                .serverSideEncryption(ServerSideEncryption.AWS_KMS)
                .ssekmsKeyId("arn:aws:kms:region:123456789012:key/key-id")
                .metadata(
                    Map.of("artifact-sha256", sha256, "tenant-sha256", Checksum.sha256("tenant-a")))
                .build());

    assertTrue(composite.endsWith("-2"));
    assertTrue(store.verifiedMultipart("tenant-a", key, 11, sha256, composite));
    // The whole-object check must not accept a composite checksum, and vice versa.
    assertFalse(store.verified("tenant-a", key, 11, sha256));
    String otherComposite = ObjectStore.compositeSha256(List.of(parts.get(1), parts.get(0)).stream()
        .map(part -> new ObjectStore.UploadedPart(3 - part.partNumber(), part.etag(), part.size(),
            part.checksumSha256()))
        .toList());
    assertFalse(store.verifiedMultipart("tenant-a", key, 11, sha256, otherComposite));
  }

  @Test
  void treatsAMissingObjectAsNotVerified() throws Exception {
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenThrow(NoSuchKeyException.builder().statusCode(404).message("missing").build());

    assertFalse(store.verified("tenant-a", key, 5, Checksum.sha256("hello")));
  }

  @Test
  void abortingAnUploadThatIsAlreadyGoneIsNotAnError() throws Exception {
    when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
        .thenThrow(NoSuchUploadException.builder().statusCode(404).message("gone").build());

    store.abortMultipartUpload("tenant-a", key, "upload-1");
  }

  private static String base64(String hexadecimal) {
    return Base64.getEncoder().encodeToString(HexFormat.of().parseHex(hexadecimal));
  }
}
