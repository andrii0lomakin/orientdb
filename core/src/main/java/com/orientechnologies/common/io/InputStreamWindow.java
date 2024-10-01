package com.orientechnologies.common.io;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamWindow {

  private final InputStream inputStream;
  private final byte[] content;

  private int size = 0;

  public InputStreamWindow(InputStream inputStream, int windowSize) throws IOException {
    this.inputStream = inputStream;
    this.content = new byte[windowSize];
    advance(windowSize);
  }

  public byte[] get() {
    return content;
  }

  public int size() {
    return size;
  }

  public boolean advance() throws IOException {
    return advance(content.length);
  }

  public boolean advance(int advanceBytes) throws IOException {

    int newSize = 0;
    if (advanceBytes < size) {
      System.arraycopy(content, advanceBytes, content, 0, size - advanceBytes);
      newSize = size - advanceBytes;
    }

    final int bytesToRead = content.length - newSize;

    newSize += inputStream.read(content, newSize, bytesToRead);

    size = Math.max(newSize, 0);
    return size > 0;
  }
}
