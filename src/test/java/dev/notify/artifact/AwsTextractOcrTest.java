package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.notify.artifact.extract.AwsTextractOcr;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.Block;
import software.amazon.awssdk.services.textract.model.BlockType;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextRequest;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextResponse;

class AwsTextractOcrTest {
  private TextractClient textract;

  @BeforeEach
  void setUp() {
    textract = org.mockito.Mockito.mock(TextractClient.class);
  }

  @Test
  void returnsOnlyDetectedLinesInResponseOrder() throws Exception {
    when(textract.detectDocumentText(any(DetectDocumentTextRequest.class)))
        .thenReturn(
            DetectDocumentTextResponse.builder()
                .blocks(
                    Block.builder().blockType(BlockType.PAGE).build(),
                    Block.builder().blockType(BlockType.LINE).text("First line").build(),
                    Block.builder().blockType(BlockType.WORD).text("First").build(),
                    Block.builder().blockType(BlockType.LINE).text("Second line").build())
                .build());

    AwsTextractOcr ocr = new AwsTextractOcr(textract, 100, 100);

    assertEquals(
        "First line\nSecond line",
        ocr.recognize(new ByteArrayInputStream(new byte[] {1, 2, 3}), "image/png"));
    verify(textract).detectDocumentText(any(DetectDocumentTextRequest.class));
  }

  @Test
  void rejectsOversizedInputBeforeCallingTextract() {
    AwsTextractOcr ocr = new AwsTextractOcr(textract, 2, 100);

    assertThrows(
        IOException.class,
        () -> ocr.recognize(new ByteArrayInputStream(new byte[] {1, 2, 3}), "image/jpeg"));
  }

  @Test
  void rejectsUnsupportedImageFormat() {
    AwsTextractOcr ocr = new AwsTextractOcr(textract, 100, 100);

    assertThrows(
        IOException.class,
        () -> ocr.recognize(new ByteArrayInputStream(new byte[] {1}), "image/webp"));
  }

  @Test
  void enforcesOutputLimit() {
    when(textract.detectDocumentText(any(DetectDocumentTextRequest.class)))
        .thenReturn(
            DetectDocumentTextResponse.builder()
                .blocks(Block.builder().blockType(BlockType.LINE).text("too long").build())
                .build());
    AwsTextractOcr ocr = new AwsTextractOcr(textract, 100, 4);

    assertThrows(
        IOException.class,
        () -> ocr.recognize(new ByteArrayInputStream(new byte[] {1}), "image/png"));
  }
}
