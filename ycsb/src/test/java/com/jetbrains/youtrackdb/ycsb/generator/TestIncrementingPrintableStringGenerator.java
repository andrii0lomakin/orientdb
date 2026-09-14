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
package com.jetbrains.youtrackdb.ycsb.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.NoSuchElementException;
import org.junit.Test;

public class TestIncrementingPrintableStringGenerator {
  private static final int[] ATOC = new int[] {65, 66, 67};

  @Test
  public void rolloverOK() throws Exception {
    final IncrementingPrintableStringGenerator gen =
        new IncrementingPrintableStringGenerator(2, ATOC);

    assertNull(gen.lastValue());
    assertEquals("AA", gen.nextValue());
    assertEquals("AA", gen.lastValue());
    assertEquals("AB", gen.nextValue());
    assertEquals("AB", gen.lastValue());
    assertEquals("AC", gen.nextValue());
    assertEquals("AC", gen.lastValue());
    assertEquals("BA", gen.nextValue());
    assertEquals("BA", gen.lastValue());
    assertEquals("BB", gen.nextValue());
    assertEquals("BB", gen.lastValue());
    assertEquals("BC", gen.nextValue());
    assertEquals("BC", gen.lastValue());
    assertEquals("CA", gen.nextValue());
    assertEquals("CA", gen.lastValue());
    assertEquals("CB", gen.nextValue());
    assertEquals("CB", gen.lastValue());
    assertEquals("CC", gen.nextValue());
    assertEquals("CC", gen.lastValue());
    assertEquals("AA", gen.nextValue()); // <-- rollover
    assertEquals("AA", gen.lastValue());
  }

  @Test
  public void rolloverOneCharacterOK() throws Exception {
    // It would be silly to create a generator with one character.
    final IncrementingPrintableStringGenerator gen =
        new IncrementingPrintableStringGenerator(2, new int[] {65});
    for (int i = 0; i < 5; i++) {
      assertEquals("AA", gen.nextValue());
    }
  }

  @Test
  public void rolloverException() throws Exception {
    final IncrementingPrintableStringGenerator gen =
        new IncrementingPrintableStringGenerator(2, ATOC);
    gen.setThrowExceptionOnRollover(true);

    int i = 0;
    try {
      while (i < 11) {
        ++i;
        gen.nextValue();
      }
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      assertEquals(10, i);
    }
  }

  @Test
  public void rolloverOneCharacterException() throws Exception {
    // It would be silly to create a generator with one character.
    final IncrementingPrintableStringGenerator gen =
        new IncrementingPrintableStringGenerator(2, new int[] {65});
    gen.setThrowExceptionOnRollover(true);

    int i = 0;
    try {
      while (i < 3) {
        ++i;
        gen.nextValue();
      }
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      assertEquals(2, i);
    }
  }

  @Test
  public void invalidLengths() throws Exception {
    try {
      new IncrementingPrintableStringGenerator(0, ATOC);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    try {
      new IncrementingPrintableStringGenerator(-42, ATOC);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void invalidCharacterSets() throws Exception {
    try {
      new IncrementingPrintableStringGenerator(2, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    try {
      new IncrementingPrintableStringGenerator(2, new int[] {});
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }
}
