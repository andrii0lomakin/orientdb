package com.orientechnologies.common.io;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamWindow {

  private final InputStream inputStream;
  private final byte[] buffer;

  private int bufferSize = 0;

  public InputStreamWindow(
      InputStream inputStream,
      int windowSize
  ) throws IOException {
    this.inputStream = inputStream;
    this.buffer = new byte[windowSize];
    advance(windowSize);
  }

  public byte[] get() {
    return buffer;
  }

  public int availableBytes(int offset, int limit) {
    return Math.max(Math.min(bufferSize - offset, limit), 0);
  }

  public int size() {
    return bufferSize;
  }

  public boolean hasContent(int offset) {
    return bufferSize > offset;
  }

  public boolean hasContent() {
    return hasContent(0);
  }

  public boolean advance(int advanceBytes) throws IOException {

    int newSize = 0;
    if (advanceBytes < bufferSize) {
      System.arraycopy(buffer, advanceBytes, buffer, 0, bufferSize - advanceBytes);
      newSize = bufferSize - advanceBytes;
    }

    final int bytesToRead = buffer.length - newSize;

    newSize += inputStream.read(buffer, newSize, bytesToRead);

    bufferSize = Math.max(newSize, 0);
    return bufferSize > 0;
  }
}
