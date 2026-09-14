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
package com.jetbrains.youtrackdb.ycsb.workloads;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.ycsb.ByteIterator;
import com.jetbrains.youtrackdb.ycsb.Client;
import com.jetbrains.youtrackdb.ycsb.DB;
import com.jetbrains.youtrackdb.ycsb.Status;
import com.jetbrains.youtrackdb.ycsb.generator.DiscreteGenerator;
import com.jetbrains.youtrackdb.ycsb.measurements.Measurements;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import org.junit.Test;

public class TestCoreWorkload {

  @Test
  public void createOperationChooser() {
    final Properties p = new Properties();
    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.20");
    final DiscreteGenerator generator = CoreWorkload.createOperationGenerator(p);
    final int[] counts = new int[5];

    for (int i = 0; i < 100; ++i) {
      switch (generator.nextString()) {
        case "READ" :
          ++counts[0];
          break;
        case "UPDATE" :
          ++counts[1];
          break;
        case "INSERT" :
          ++counts[2];
          break;
        case "SCAN" :
          ++counts[3];
          break;
        default :
          ++counts[4];
      }
    }

    for (int i : counts) {
      // Doesn't do a wonderful job of equal distribution, but in a hundred, if we
      // don't see at least one of each operation then the generator is really broke.
      assertTrue(i > 1);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void createOperationChooserNullProperties() {
    CoreWorkload.createOperationGenerator(null);
  }

  /**
   * Regression test: {@code doInsert} must materialize field values into
   * Strings before entering the retry loop. {@code buildValues} produces
   * {@link com.jetbrains.youtrackdb.ycsb.RandomByteIterator} instances
   * whose {@code toString()} consumes the underlying byte stream. If the
   * same {@code values} map is reused across retries, the second call
   * sees exhausted iterators and writes empty strings to the database.
   *
   * <p>The test uses a stub DB that fails the first insert and succeeds
   * on the second. It captures both the first and second call's values
   * and asserts that retry values are identical to the first-call values
   * (same content, same length — not empty strings).
   */
  @Test
  public void testDoInsertMaterializesValuesBeforeRetry() throws Exception {
    Properties p = new Properties();
    // Drive buildValues() toward RandomByteIterator (consumable), not the
    // deterministic StringByteIterator path.
    p.setProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY, "false");
    p.setProperty("fieldcount", "3");
    p.setProperty("fieldlength", "16");
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "10");
    // Tell CoreWorkload to retry insertion once on failure.
    p.setProperty(CoreWorkload.INSERTION_RETRY_LIMIT, "1");
    p.setProperty(CoreWorkload.INSERTION_RETRY_INTERVAL, "0");
    // Stabilize the proportions/distributions required by init().
    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "1.0");
    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0.0");
    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0.0");
    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0.0");
    p.setProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.0");

    Measurements.setProperties(p);
    CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    RecordingRetryDB db = new RecordingRetryDB();
    boolean result = workload.doInsert(db, null);
    assertTrue("doInsert must eventually succeed via retry", result);

    assertEquals("DB should see exactly two insert attempts (1 fail + 1 retry)",
        2, db.attempts.size());

    Map<String, String> first = db.attempts.get(0);
    Map<String, String> second = db.attempts.get(1);
    assertEquals("Both attempts must carry the same field set",
        first.keySet(), second.keySet());
    for (Map.Entry<String, String> e : first.entrySet()) {
      String field = e.getKey();
      String firstValue = e.getValue();
      String secondValue = second.get(field);
      assertFalse("First attempt value for " + field + " must be non-empty",
          firstValue.isEmpty());
      assertFalse("Retry value for " + field + " must be non-empty."
          + " If empty, the ByteIterator was re-read from the shared"
          + " values map after being exhausted by the first attempt.",
          secondValue.isEmpty());
      assertEquals("Retry value for " + field
          + " must match the first attempt's value",
          firstValue, secondValue);
    }
  }

  /**
   * Stub {@link DB} that returns ERROR on the first insert and OK
   * thereafter. Snapshots each call's field values into strings so the
   * test can compare the first attempt against the retry without holding
   * references to the (already-consumed) ByteIterators.
   */
  private static final class RecordingRetryDB extends DB {
    final List<Map<String, String>> attempts = new ArrayList<>();

    @Override
    public Status insert(String table, String key,
        Map<String, ByteIterator> values) {
      Map<String, String> snapshot = new HashMap<>();
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        snapshot.put(e.getKey(), e.getValue().toString());
      }
      attempts.add(snapshot);
      return attempts.size() == 1 ? Status.ERROR : Status.OK;
    }

    @Override
    public Status read(String table, String key, Set<String> fields,
        Map<String, ByteIterator> result) {
      return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      return Status.OK;
    }

    @Override
    public Status update(String table, String key,
        Map<String, ByteIterator> values) {
      return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
      return Status.OK;
    }
  }
}
