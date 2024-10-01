package com.orientechnologies.common.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.Test;

public class InputStreamWindowTest {

  @Test
  public void testSimpleRead() throws IOException {

    final var stream = new ByteArrayInputStream(ALL_BYTES);
    final var pageSize = 100;
    final var window = new InputStreamWindow(stream, pageSize);

    int pageIdx = 0;
    final var result  = new byte[ALL_BYTES.length];
    while (window.size() > 0) {
      for (int i = 0; i < window.size(); i++) {
        result[pageIdx * pageSize + i] = window.get()[i];
      }

      window.advance();
      pageIdx++;
    }

    assertEquals(3, pageIdx);
    assertArrayEquals(ALL_BYTES, result);
  }

  private static final byte[] ALL_BYTES;

  static {
    ALL_BYTES = new byte[256];
    for (int i = 0; i < 256; i++) {
      ALL_BYTES[i] = (byte) (i - 128);
    }
  }
}
