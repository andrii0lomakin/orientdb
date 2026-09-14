/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.jetbrains.youtrackdb.ycsb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestUtils {

  @Test
  public void bytesToFromLong() throws Exception {
    byte[] bytes = new byte[8];
    assertEquals(0L, Utils.bytesToLong(bytes));
    assertArrayEquals(Utils.longToBytes(0), bytes);

    bytes[7] = 1;
    assertEquals(1L, Utils.bytesToLong(bytes));
    assertArrayEquals(Utils.longToBytes(1L), bytes);

    bytes = new byte[] {127, -1, -1, -1, -1, -1, -1, -1};
    assertEquals(Long.MAX_VALUE, Utils.bytesToLong(bytes));
    assertArrayEquals(Utils.longToBytes(Long.MAX_VALUE), bytes);

    bytes = new byte[] {-128, 0, 0, 0, 0, 0, 0, 0};
    assertEquals(Long.MIN_VALUE, Utils.bytesToLong(bytes));
    assertArrayEquals(Utils.longToBytes(Long.MIN_VALUE), bytes);

    bytes =
        new byte[] {
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF
        };
    assertEquals(-1L, Utils.bytesToLong(bytes));
    assertArrayEquals(Utils.longToBytes(-1L), bytes);

    // if the array is too long we just skip the remainder
    bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 42, 42, 42};
    assertEquals(1L, Utils.bytesToLong(bytes));
  }

  @Test
  public void bytesToFromDouble() throws Exception {
    byte[] bytes = new byte[8];
    assertEquals(0, Utils.bytesToDouble(bytes), 0.0001);
    assertArrayEquals(Utils.doubleToBytes(0), bytes);

    bytes = new byte[] {63, -16, 0, 0, 0, 0, 0, 0};
    assertEquals(1, Utils.bytesToDouble(bytes), 0.0001);
    assertArrayEquals(Utils.doubleToBytes(1), bytes);

    bytes = new byte[] {-65, -16, 0, 0, 0, 0, 0, 0};
    assertEquals(-1, Utils.bytesToDouble(bytes), 0.0001);
    assertArrayEquals(Utils.doubleToBytes(-1), bytes);

    bytes = new byte[] {127, -17, -1, -1, -1, -1, -1, -1};
    assertEquals(Double.MAX_VALUE, Utils.bytesToDouble(bytes), 0.0001);
    assertArrayEquals(Utils.doubleToBytes(Double.MAX_VALUE), bytes);

    bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 1};
    assertEquals(Double.MIN_VALUE, Utils.bytesToDouble(bytes), 0.0001);
    assertArrayEquals(Utils.doubleToBytes(Double.MIN_VALUE), bytes);

    bytes = new byte[] {127, -8, 0, 0, 0, 0, 0, 0};
    assertTrue(Double.isNaN(Utils.bytesToDouble(bytes)));
    assertArrayEquals(Utils.doubleToBytes(Double.NaN), bytes);

    bytes = new byte[] {63, -16, 0, 0, 0, 0, 0, 0, 42, 42, 42};
    assertEquals(1, Utils.bytesToDouble(bytes), 0.0001);
  }

  @Test(expected = NullPointerException.class)
  public void bytesToLongNull() throws Exception {
    Utils.bytesToLong(null);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void bytesToLongTooShort() throws Exception {
    Utils.bytesToLong(new byte[] {0, 0, 0, 0, 0, 0, 0});
  }

  @Test(expected = IllegalArgumentException.class)
  public void bytesToDoubleTooShort() throws Exception {
    Utils.bytesToDouble(new byte[] {0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void jvmUtils() throws Exception {
    // This should ALWAYS return at least one thread.
    assertTrue(Utils.getActiveThreadCount() > 0);
    // This should always be greater than 0 or something is goofed up in the JVM.
    assertTrue(Utils.getUsedMemoryBytes() > 0);
    // Some operating systems may not implement this so we don't have a good
    // test. Just make sure it doesn't throw an exception.
    Utils.getSystemLoadAverage();
    // This will probably be zero but should never be negative.
    assertTrue(Utils.getGCTotalCollectionCount() >= 0);
    // Could be zero similar to GC total collection count
    assertTrue(Utils.getGCTotalTime() >= 0);
    // Could be empty
    assertTrue(Utils.getGCStatst().size() >= 0);
  }
}
