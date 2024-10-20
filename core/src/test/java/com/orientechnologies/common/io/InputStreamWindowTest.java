package com.orientechnologies.common.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.Test;

public class InputStreamWindowTest {

  @Test
  public void testSimpleRead() throws IOException {

    final var stream = new ByteArrayInputStream(ALL_BYTES);
    final var windowSize = 100;
    final var window = new InputStreamWindow(stream, windowSize);

    int pageIdx = 0;
    final var result  = new byte[ALL_BYTES.length];
    while (window.size() > 0) {
      for (int i = 0; i < window.size(); i++) {
        result[pageIdx * windowSize + i] = window.get()[i];
      }

      window.advance(windowSize);
      pageIdx++;
    }

    assertEquals(3, pageIdx);
    assertArrayEquals(ALL_BYTES, result);
  }

  @Test
  public void testSimpleAdvance() throws IOException {
    final var inputSize = 100;
    final var bytes = new byte[inputSize];
    for (int i = 0; i < inputSize; i++) {
      bytes[i] = (byte) i;
    }

    for (Integer windowSize : List.of(80, 100, 160, 200)) {

      final var stream = new ByteArrayInputStream(bytes);
      final var window = new InputStreamWindow(stream, windowSize);

      assertEquals(
          Math.min(windowSize, inputSize),
          window.availableBytes(0, inputSize)
      );

      window.advance(79);

      assertEquals(bytes[79], window.get()[0]);

      assertEquals(21, window.availableBytes(0, inputSize));
    }

  }

  private static final byte[] ALL_BYTES;

  static {
    ALL_BYTES = new byte[256];
    for (int i = 0; i < 256; i++) {
      ALL_BYTES[i] = (byte) (i - 128);
    }
  }
}
