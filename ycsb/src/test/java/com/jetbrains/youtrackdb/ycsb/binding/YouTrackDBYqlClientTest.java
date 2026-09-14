/**
 * Copyright (c) 2024 JetBrains s.r.o. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jetbrains.youtrackdb.ycsb.binding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.internal.core.id.RecordId;
import com.jetbrains.youtrackdb.ycsb.ByteIterator;
import com.jetbrains.youtrackdb.ycsb.Status;
import com.jetbrains.youtrackdb.ycsb.StringByteIterator;
import com.jetbrains.youtrackdb.ycsb.measurements.Measurements;
import com.jetbrains.youtrackdb.ycsb.workloads.CoreWorkload;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link YouTrackDBYqlClient}. Uses MEMORY database type
 * for speed — no disk I/O required.
 */
public class YouTrackDBYqlClientTest {

  private YouTrackDBYqlClient client;
  private Path tempDir;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("ycsb-test-" + System.nanoTime());
    client = new YouTrackDBYqlClient();
    Properties props = new Properties();
    props.setProperty(YouTrackDBYqlClient.URL_PROPERTY, tempDir.toString());
    props.setProperty(YouTrackDBYqlClient.DB_NAME_PROPERTY, "testdb");
    props.setProperty(YouTrackDBYqlClient.DB_TYPE_PROPERTY, "MEMORY");
    props.setProperty(YouTrackDBYqlClient.NEW_DB_PROPERTY, "true");
    client.setProperties(props);
    client.init();
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.cleanup();
    }
    deleteDirectory(tempDir);
  }

  /**
   * Insert a record with two fields and verify the data was actually
   * persisted by reading it back via a direct YQL query on the shared
   * traversal source.
   */
  @Test
  public void testInsertAndVerifyDataPersisted() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("value0"));
    values.put("field1", new StringByteIterator("value1"));

    Status insertStatus = client.insert("usertable", "key1", values);
    assertEquals("Insert should succeed", Status.OK, insertStatus);

    // Read back via direct YQL to verify data was actually written
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> results = (List<
        Map<String, Object>>) (List<?>) YouTrackDBYqlClient.getTraversalSource().computeInTx(tx -> {
          var g = (YTDBGraphTraversalSource) tx;
          return g.yql(
              "SELECT ycsb_key, field0, field1 FROM usertable WHERE ycsb_key = :key",
              "key", "key1").toList();
        });

    assertEquals("Exactly one record should be found", 1, results.size());
    assertEquals("value0", results.get(0).get("field0"));
    assertEquals("value1", results.get(0).get("field1"));
  }

  /**
   * Insert 10 records with distinct keys and all 10 fields each, then
   * verify the database contains exactly 10 records.
   */
  @Test
  public void testInsertMultipleRecords() {
    for (int i = 0; i < 10; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      for (int f = 0; f < 10; f++) {
        values.put("field" + f, new StringByteIterator("val" + i + "_" + f));
      }
      Status status = client.insert("usertable", "user" + i, values);
      assertEquals("Insert of user" + i + " should succeed", Status.OK, status);
    }

    // Verify the database actually contains 10 records
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> allRecords = (List<
        Map<String, Object>>) (List<?>) YouTrackDBYqlClient.getTraversalSource().computeInTx(tx -> {
          var g = (YTDBGraphTraversalSource) tx;
          return g.yql("SELECT count(*) as cnt FROM usertable").toList();
        });
    assertEquals("Database should contain exactly 10 records",
        10L, ((Number) allRecords.get(0).get("cnt")).longValue());
  }

  /**
   * Insert a record with all 10 standard YCSB fields and verify each
   * field value was stored correctly via direct YQL read-back.
   */
  @Test
  public void testInsertAllFieldsAndVerifyValues() {
    Map<String, ByteIterator> values = new HashMap<>();
    for (int f = 0; f < 10; f++) {
      values.put("field" + f, new StringByteIterator("data_" + f));
    }
    Status status = client.insert("usertable", "full-key", values);
    assertEquals("Insert with all 10 fields should succeed", Status.OK, status);

    // Read back and verify all field values using explicit projection
    // (SELECT FROM returns Vertex objects; SELECT <fields> returns Maps)
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> results = (List<
        Map<String, Object>>) (List<?>) YouTrackDBYqlClient.getTraversalSource().computeInTx(tx -> {
          var g = (YTDBGraphTraversalSource) tx;
          return g.yql(
              "SELECT field0, field1, field2, field3, field4,"
                  + " field5, field6, field7, field8, field9"
                  + " FROM usertable WHERE ycsb_key = :key",
              "key", "full-key").toList();
        });

    assertEquals(1, results.size());
    Map<String, Object> record = results.get(0);
    for (int f = 0; f < 10; f++) {
      assertEquals("field" + f + " should have correct value",
          "data_" + f, record.get("field" + f));
    }
  }

  /**
   * Insert with an empty values map — the vertex is created with only the
   * ycsb_key property. This exercises the boundary where the dynamic SET
   * clause has zero field entries.
   */
  @Test
  public void testInsertEmptyValuesMap() {
    Map<String, ByteIterator> empty = new HashMap<>();
    Status status = client.insert("usertable", "empty-key", empty);
    assertEquals("Insert with empty values should succeed", Status.OK, status);

    // Verify the record exists with only the key
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> results = (List<
        Map<String, Object>>) (List<?>) YouTrackDBYqlClient.getTraversalSource().computeInTx(tx -> {
          var g = (YTDBGraphTraversalSource) tx;
          return g.yql(
              "SELECT ycsb_key FROM usertable WHERE ycsb_key = :key",
              "key", "empty-key").toList();
        });
    assertEquals(1, results.size());
    assertEquals("empty-key", results.get(0).get("ycsb_key"));
  }

  /**
   * Insert a record then read it back via the driver's read() method with
   * fields=null (all fields). Verify all field values match.
   */
  @Test
  public void testReadAllFields() {
    Map<String, ByteIterator> values = new HashMap<>();
    for (int f = 0; f < 10; f++) {
      values.put("field" + f, new StringByteIterator("read_" + f));
    }
    assertEquals(Status.OK, client.insert("usertable", "rkey1", values));

    Map<String, ByteIterator> result = new HashMap<>();
    Status readStatus = client.read("usertable", "rkey1", null, result);
    assertEquals("Read should succeed", Status.OK, readStatus);

    // fields=null returns all properties including ycsb_key — exactly 11
    assertEquals("Result should contain exactly 11 properties",
        11, result.size());
    for (int f = 0; f < 10; f++) {
      assertEquals("read_" + f, result.get("field" + f).toString());
    }
    assertEquals("rkey1", result.get("ycsb_key").toString());
  }

  /**
   * Read with a specific field set — only the requested fields should
   * appear in the result map.
   */
  @Test
  public void testReadSpecificFields() {
    Map<String, ByteIterator> values = new HashMap<>();
    for (int f = 0; f < 10; f++) {
      values.put("field" + f, new StringByteIterator("sel_" + f));
    }
    assertEquals(Status.OK, client.insert("usertable", "rkey2", values));

    Set<String> fields = Set.of("field0", "field5");

    Map<String, ByteIterator> result = new HashMap<>();
    Status readStatus = client.read("usertable", "rkey2", fields, result);
    assertEquals(Status.OK, readStatus);
    assertEquals(2, result.size());
    assertEquals("sel_0", result.get("field0").toString());
    assertEquals("sel_5", result.get("field5").toString());
  }

  /**
   * Reading a non-existent key should return NOT_FOUND.
   */
  @Test
  public void testReadNonExistentKeyReturnsNotFound() {
    Map<String, ByteIterator> result = new HashMap<>();
    Status status = client.read("usertable", "no-such-key", null, result);
    assertEquals(Status.NOT_FOUND, status);
    assertEquals("Result map should be empty for NOT_FOUND", 0, result.size());
  }

  /**
   * Insert a record, update some fields, then read back and verify
   * the updated values while unchanged fields retain their original values.
   */
  @Test
  public void testUpdateAndReadBack() {
    // Insert with all 10 fields
    Map<String, ByteIterator> values = new HashMap<>();
    for (int f = 0; f < 10; f++) {
      values.put("field" + f, new StringByteIterator("orig_" + f));
    }
    assertEquals(Status.OK, client.insert("usertable", "ukey1", values));

    // Update only field0 and field1
    Map<String, ByteIterator> updates = new HashMap<>();
    updates.put("field0", new StringByteIterator("updated_0"));
    updates.put("field1", new StringByteIterator("updated_1"));
    Status updateStatus = client.update("usertable", "ukey1", updates);
    assertEquals("Update should succeed", Status.OK, updateStatus);

    // Read back all fields and verify
    Map<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, client.read("usertable", "ukey1", null, result));
    assertEquals("updated_0", result.get("field0").toString());
    assertEquals("updated_1", result.get("field1").toString());
    // Unchanged fields should retain original values
    for (int f = 2; f < 10; f++) {
      assertEquals("orig_" + f, result.get("field" + f).toString());
    }
  }

  /**
   * Update with an empty values map should be a no-op returning OK,
   * not produce malformed SQL. Verifies the early-return guard.
   */
  @Test
  public void testUpdateEmptyValuesMapReturnsOk() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("orig"));
    assertEquals(Status.OK, client.insert("usertable", "empty-upd", values));

    Status status = client.update("usertable", "empty-upd", new HashMap<>());
    assertEquals("Empty update should be a no-op returning OK",
        Status.OK, status);

    // Verify original data is unchanged
    Map<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, client.read("usertable", "empty-upd", null, result));
    assertEquals("orig", result.get("field0").toString());
  }

  /**
   * executeWithRetry succeeds after transient CME — verify retry count
   * and final OK result.
   */
  @Test
  public void testExecuteWithRetrySucceedsAfterTransientCme() {
    AtomicInteger callCount = new AtomicInteger(0);
    Status result = client.executeWithRetry(() -> {
      if (callCount.incrementAndGet() < 3) {
        throw new ConcurrentModificationException(
            "testdb", new RecordId(1, 0), 1, 0, 2);
      }
    }, "Test", "test-key");

    assertEquals(Status.OK, result);
    assertEquals("Should have been called 3 times (2 CME + 1 success)",
        3, callCount.get());
  }

  /**
   * executeWithRetry returns ERROR when all retry attempts are exhausted
   * by persistent CME.
   */
  @Test
  public void testExecuteWithRetryExhaustsRetries() {
    AtomicInteger callCount = new AtomicInteger(0);
    Status result = client.executeWithRetry(() -> {
      callCount.incrementAndGet();
      throw new ConcurrentModificationException(
          "testdb", new RecordId(1, 0), 1, 0, 2);
    }, "Test", "test-key");

    assertEquals(Status.ERROR, result);
    assertEquals("Should have been called 3 times (all failures)",
        3, callCount.get());
  }

  /**
   * Insert a record, delete it, then verify read returns NOT_FOUND.
   */
  @Test
  public void testDeleteAndVerifyNotFound() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("to-delete"));
    assertEquals(Status.OK, client.insert("usertable", "dkey1", values));

    // Verify it exists first with actual content
    Map<String, ByteIterator> readResult = new HashMap<>();
    assertEquals(Status.OK, client.read("usertable", "dkey1", null, readResult));
    assertEquals("to-delete", readResult.get("field0").toString());

    // Delete
    Status deleteStatus = client.delete("usertable", "dkey1");
    assertEquals("Delete should succeed", Status.OK, deleteStatus);

    // Verify it's gone
    Map<String, ByteIterator> afterDelete = new HashMap<>();
    assertEquals("Read after delete should return NOT_FOUND",
        Status.NOT_FOUND, client.read("usertable", "dkey1", null, afterDelete));
  }

  /**
   * Insert 10 records with sequential keys, scan from the middle, and
   * verify correct count and ascending key order.
   */
  @Test
  public void testScanFromMiddle() {
    for (int i = 0; i < 10; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("field0", new StringByteIterator("scan_" + i));
      // Use zero-padded keys for correct lexicographic ordering
      assertEquals(Status.OK,
          client.insert("usertable", String.format("skey%03d", i), values));
    }

    // Scan 5 records starting from skey005
    Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    Status scanStatus = client.scan("usertable", "skey005", 5, null, result);
    assertEquals("Scan should succeed", Status.OK, scanStatus);
    assertEquals("Should return 5 records", 5, result.size());

    // Verify ascending order and field values
    for (int i = 0; i < 5; i++) {
      String expectedKey = String.format("skey%03d", i + 5);
      assertEquals(expectedKey, result.get(i).get("ycsb_key").toString());
      assertEquals("scan_" + (i + 5),
          result.get(i).get("field0").toString());
    }
  }

  /**
   * Scan with specific fields — only requested fields should appear in
   * each result record.
   */
  @Test
  public void testScanWithSpecificFields() {
    for (int i = 0; i < 5; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("field0", new StringByteIterator("sf0_" + i));
      values.put("field1", new StringByteIterator("sf1_" + i));
      assertEquals(Status.OK,
          client.insert("usertable", String.format("sfkey%03d", i), values));
    }

    Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    Status scanStatus = client.scan("usertable", "sfkey000", 5,
        Set.of("field0"), result);
    assertEquals(Status.OK, scanStatus);
    assertEquals(5, result.size());

    // Each record should have only field0
    for (int i = 0; i < 5; i++) {
      assertEquals(1, result.get(i).size());
      assertEquals("sf0_" + i, result.get(i).get("field0").toString());
    }
  }

  /**
   * Scan beyond the end of the dataset — should return fewer records than
   * requested without error.
   */
  @Test
  public void testScanBeyondDataset() {
    for (int i = 0; i < 3; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("field0", new StringByteIterator("beyond_" + i));
      assertEquals(Status.OK,
          client.insert("usertable", String.format("bkey%03d", i), values));
    }

    // Request 10 records but only 3 exist
    Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    Status scanStatus = client.scan("usertable", "bkey000", 10, null, result);
    assertEquals(Status.OK, scanStatus);
    assertEquals("Should return only 3 records (all that exist)", 3, result.size());
    for (int i = 0; i < 3; i++) {
      assertEquals(String.format("bkey%03d", i),
          result.get(i).get("ycsb_key").toString());
      assertEquals("beyond_" + i,
          result.get(i).get("field0").toString());
    }
  }

  /**
   * Scan with startkey beyond all existing keys — should return OK with
   * an empty result set.
   */
  @Test
  public void testScanWithStartkeyBeyondAllKeys() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("val"));
    assertEquals(Status.OK, client.insert("usertable", "aaa", values));

    Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    Status scanStatus = client.scan("usertable", "zzz", 10, null, result);
    assertEquals(Status.OK, scanStatus);
    assertEquals("Scan beyond all keys should return empty result",
        0, result.size());
  }

  /**
   * After cleanup, shared resources should be closed (static fields null).
   * Re-initializing with a new database name should create a fresh database
   * that accepts inserts.
   */
  @Test
  public void testCleanupAndReinit() throws Exception {
    // Insert a record to confirm the DB is working
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("before-cleanup"));
    assertEquals(Status.OK, client.insert("usertable", "key1", values));

    // Cleanup closes everything — balances setUp()'s init().
    // Null out immediately to prevent tearDown double-cleanup if an
    // assertion below fails before we reassign client.
    client.cleanup();
    client = null;

    // Verify shared resources are released
    assertNull("traversalSource should be null after last cleanup",
        YouTrackDBYqlClient.getTraversalSource());

    // Re-init with a different database name using a fresh client
    // so that tearDown()'s cleanup() is balanced with this init()
    client = new YouTrackDBYqlClient();
    Properties props = new Properties();
    props.setProperty(YouTrackDBYqlClient.URL_PROPERTY, tempDir.toString());
    props.setProperty(YouTrackDBYqlClient.DB_NAME_PROPERTY, "testdb2");
    props.setProperty(YouTrackDBYqlClient.DB_TYPE_PROPERTY, "MEMORY");
    props.setProperty(YouTrackDBYqlClient.NEW_DB_PROPERTY, "true");
    client.setProperties(props);
    client.init();

    // Should be able to insert into the new database
    Map<String, ByteIterator> values2 = new HashMap<>();
    values2.put("field0", new StringByteIterator("after-reinit"));
    assertEquals(Status.OK, client.insert("usertable", "reinit-key", values2));
  }

  /**
   * Multiple threads call init() concurrently via CountDownLatch.
   * Exactly one database should be created. All threads should complete
   * without error and be able to insert records.
   */
  @Test
  public void testConcurrentInit() throws Exception {
    // Clean up the instance created by setUp(). Null out immediately to
    // prevent tearDown double-cleanup if an assertion below fails.
    client.cleanup();
    client = null;

    int threadCount = 4;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger errorCount = new AtomicInteger(0);
    YouTrackDBYqlClient[] clients = new YouTrackDBYqlClient[threadCount];

    for (int i = 0; i < threadCount; i++) {
      clients[i] = new YouTrackDBYqlClient();
      Properties props = new Properties();
      props.setProperty(YouTrackDBYqlClient.URL_PROPERTY, tempDir.toString());
      props.setProperty(YouTrackDBYqlClient.DB_NAME_PROPERTY, "concdb");
      props.setProperty(YouTrackDBYqlClient.DB_TYPE_PROPERTY, "MEMORY");
      props.setProperty(YouTrackDBYqlClient.NEW_DB_PROPERTY, "true");
      clients[i].setProperties(props);

      final int idx = i;
      new Thread(() -> {
        try {
          startLatch.await();
          clients[idx].init();
        } catch (Exception e) {
          errorCount.incrementAndGet();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue("All threads should complete within 30s",
        doneLatch.await(30, TimeUnit.SECONDS));
    assertEquals("No init errors", 0, errorCount.get());

    // All clients should be able to insert via the shared traversal source
    for (int i = 0; i < threadCount; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("field0", new StringByteIterator("concurrent_" + i));
      assertEquals("Client " + i + " should be able to insert",
          Status.OK, clients[i].insert("usertable", "ckey" + i, values));
    }

    // Cleanup all clients
    for (YouTrackDBYqlClient c : clients) {
      c.cleanup();
    }

    // After all cleanups, shared state should be released
    assertNull("traversalSource should be null after all cleanups",
        YouTrackDBYqlClient.getTraversalSource());
  }

  /**
   * Full workload round-trip: use CoreWorkload to load 100 records and
   * run 200 operations. Exercises the driver through the real YCSB
   * framework path including key generation, field selection, and
   * operation distribution.
   */
  @Test
  public void testFullWorkloadRoundTrip() throws Exception {
    // Clean up setUp's client since we need fresh state for the workload.
    // Null out immediately to prevent tearDown double-cleanup.
    client.cleanup();
    client = null;

    int recordCount = 100;
    int operationCount = 200;

    Properties props = new Properties();
    props.setProperty(YouTrackDBYqlClient.URL_PROPERTY, tempDir.toString());
    props.setProperty(YouTrackDBYqlClient.DB_NAME_PROPERTY, "workloaddb");
    props.setProperty(YouTrackDBYqlClient.DB_TYPE_PROPERTY, "MEMORY");
    props.setProperty(YouTrackDBYqlClient.NEW_DB_PROPERTY, "true");
    props.setProperty("recordcount", String.valueOf(recordCount));
    props.setProperty("operationcount", String.valueOf(operationCount));
    props.setProperty("fieldcount", "10");
    props.setProperty("fieldlength", "10");
    props.setProperty("workload",
        "com.jetbrains.youtrackdb.ycsb.workloads.CoreWorkload");
    props.setProperty("readproportion", "0.5");
    props.setProperty("updateproportion", "0.3");
    props.setProperty("scanproportion", "0.1");
    props.setProperty("insertproportion", "0.1");
    props.setProperty("requestdistribution", "uniform");

    // Initialize Measurements singleton (required by CoreWorkload)
    Measurements.setProperties(props);

    // Initialize workload
    var workload = new CoreWorkload();
    workload.init(props);

    // Create and init the DB client
    var dbClient = new YouTrackDBYqlClient();
    dbClient.setProperties(props);
    dbClient.init();

    try {
      // Load phase: insert 100 records
      for (int i = 0; i < recordCount; i++) {
        assertTrue("Load insert " + i + " should succeed",
            workload.doInsert(dbClient, null));
      }

      // Transaction phase: run 200 operations.
      // Note: CoreWorkload.doTransaction() always returns true regardless
      // of DB operation results, so we verify data integrity below instead
      // of checking return values.
      for (int i = 0; i < operationCount; i++) {
        workload.doTransaction(dbClient, null);
      }

      // Verify data integrity: the load phase inserted 100 records and the
      // transaction phase has a 10% insert proportion (~20 more inserts).
      // No deletes are configured, so total should be at least 100.
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> countResult = (List<
          Map<String, Object>>) (List<?>) YouTrackDBYqlClient
              .getTraversalSource().computeInTx(tx -> {
                var g = (YTDBGraphTraversalSource) tx;
                return g.yql("SELECT count(*) as cnt FROM usertable").toList();
              });
      long dbRecordCount =
          ((Number) countResult.get(0).get("cnt")).longValue();
      assertTrue("Database should contain at least " + recordCount
          + " records after workload, got " + dbRecordCount,
          dbRecordCount >= recordCount);

      // Spot-check: read back an arbitrary record via the driver to verify
      // the data is accessible (use a key from the database, not a predicted
      // key format, since CoreWorkload uses hashed key ordering by default)
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> sampleKey = (List<
          Map<String, Object>>) (List<?>) YouTrackDBYqlClient
              .getTraversalSource().computeInTx(tx -> {
                var g = (YTDBGraphTraversalSource) tx;
                return g.yql("SELECT ycsb_key FROM usertable LIMIT 1")
                    .toList();
              });
      assertEquals("Should find at least one record", 1, sampleKey.size());
      String firstKey = (String) sampleKey.get(0).get("ycsb_key");

      Map<String, ByteIterator> verifyResult = new HashMap<>();
      Status readStatus = dbClient.read("usertable", firstKey,
          null, verifyResult);
      assertEquals("Should be able to read back a loaded record",
          Status.OK, readStatus);
      assertTrue("Read-back record should have fields",
          verifyResult.size() > 0);
    } finally {
      dbClient.cleanup();
      workload.cleanup();
    }
  }

  /**
   * executeWithRetry should restore the interrupt flag and return ERROR
   * when Thread.sleep is interrupted during CME backoff. This verifies
   * the interrupt contract required for clean YCSB thread pool shutdown.
   */
  @Test
  public void testExecuteWithRetryRestoresInterruptOnInterruption() {
    // Pre-set the interrupt flag so Thread.sleep throws immediately
    Thread.currentThread().interrupt();
    AtomicInteger callCount = new AtomicInteger(0);
    Status result = client.executeWithRetry(() -> {
      callCount.incrementAndGet();
      throw new ConcurrentModificationException(
          "testdb", new RecordId(1, 0), 1, 0, 2);
    }, "Test", "test-key");

    assertEquals("Should return ERROR on interruption",
        Status.ERROR, result);
    assertTrue("Interrupt flag should be restored",
        Thread.currentThread().isInterrupted());
    assertEquals("Should have been called once before interrupt hit",
        1, callCount.get());
    // Clear the interrupt flag for test cleanup
    Thread.interrupted();
  }

  /**
   * Inserting a duplicate key with a UNIQUE index succeeds silently —
   * CREATE VERTEX does not enforce unique constraint violations as errors.
   * This documents the actual YouTrackDB behavior discovered during
   * implementation: the driver returns OK for duplicate key inserts.
   */
  @Test
  public void testInsertDuplicateKeySucceedsSilently() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("first"));
    assertEquals(Status.OK, client.insert("usertable", "dup-key", values));

    Map<String, ByteIterator> values2 = new HashMap<>();
    values2.put("field0", new StringByteIterator("second"));
    Status status = client.insert("usertable", "dup-key", values2);
    assertEquals("Duplicate key insert succeeds silently",
        Status.OK, status);
  }

  /**
   * executeWithRetry should return ERROR immediately (no retry) when
   * the operation throws a non-CME exception.
   */
  @Test
  public void testExecuteWithRetryNoRetryOnNonCmeException() {
    AtomicInteger callCount = new AtomicInteger(0);
    Status result = client.executeWithRetry(() -> {
      callCount.incrementAndGet();
      throw new RuntimeException("Not a CME");
    }, "Test", "test-key");

    assertEquals(Status.ERROR, result);
    assertEquals("Should have been called exactly once (no retry)",
        1, callCount.get());
  }

  /**
   * Regression test: {@code update()} must materialize ByteIterator values
   * into Strings <em>before</em> entering the CME retry loop. If the
   * lambda passed to {@code executeWithRetry} re-reads the iterators on
   * a retry, a RandomByteIterator-like iterator would be exhausted by
   * the first attempt and the retry would write an empty string.
   *
   * <p>We deterministically force a "retry" via a test-only subclass that
   * overrides {@code executeWithRetry} to invoke the operation twice in a
   * row (no CME required). With the fix in place, {@code toString()} is
   * invoked exactly once on the source iterator regardless of how many
   * times the lambda runs. Without the fix, each lambda execution would
   * call {@code toString()} again, and the second call would observe an
   * empty value.
   */
  @Test
  public void testUpdateMaterializesByteIteratorsBeforeRetry() {
    // Seed the target record.
    Map<String, ByteIterator> seed = new HashMap<>();
    seed.put("field0", new StringByteIterator("seed"));
    assertEquals(Status.OK, client.insert("usertable", "mat-key", seed));

    // Use a subclass that simulates a single CME retry deterministically
    // by invoking the retry operation twice before returning OK.
    RetrySimulatingClient retrying = new RetrySimulatingClient();
    Properties props = new Properties();
    props.setProperty(YouTrackDBYqlClient.URL_PROPERTY, tempDir.toString());
    props.setProperty(YouTrackDBYqlClient.DB_NAME_PROPERTY, "testdb");
    props.setProperty(YouTrackDBYqlClient.DB_TYPE_PROPERTY, "MEMORY");
    // Do not recreate — attach to the same shared db as setUp().
    props.setProperty(YouTrackDBYqlClient.NEW_DB_PROPERTY, "false");
    retrying.setProperties(props);
    try {
      retrying.init();

      AtomicInteger calls = new AtomicInteger();
      SingleUseByteIterator iter = new SingleUseByteIterator("payload", calls);
      Map<String, ByteIterator> updates = new HashMap<>();
      updates.put("field0", iter);

      Status status = retrying.update("usertable", "mat-key", updates);
      assertEquals("Update must succeed even when the retry runs the"
          + " operation twice", Status.OK, status);
      assertEquals("RetrySimulatingClient must have invoked the retryable"
          + " operation twice", 2, retrying.operationInvocations);
      assertEquals("toString() must be called exactly once per value,"
          + " regardless of retry count. If it is called more than once,"
          + " materialization was skipped and a real CME retry would"
          + " write an empty string to the database.",
          1, calls.get());

      // Verify the update actually wrote "payload" — sanity check that the
      // retry loop did not corrupt the data either.
      Map<String, ByteIterator> result = new HashMap<>();
      assertEquals(Status.OK,
          retrying.read("usertable", "mat-key", null, result));
      assertEquals("payload", result.get("field0").toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        retrying.cleanup();
      } catch (Exception ignored) {
        // best effort
      }
    }
  }

  /**
   * Subclass that replaces the CME-based retry loop with a deterministic
   * double-invocation of the operation. Used to verify that values
   * captured by the retryable lambda remain stable across invocations.
   */
  private static final class RetrySimulatingClient extends YouTrackDBYqlClient {
    int operationInvocations = 0;

    @Override
    Status executeWithRetry(RetryableOperation operation,
        String opName, String key) {
      try {
        operation.execute();
        operationInvocations++;
        operation.execute();
        operationInvocations++;
        return Status.OK;
      } catch (Exception e) {
        return Status.ERROR;
      }
    }
  }

  /**
   * Regression test: {@code read()} with a {@code fields} set that includes
   * a property not set on the target vertex must succeed without throwing
   * {@code NoSuchElementException}. {@code extractFields} must guard each
   * lookup with {@code VertexProperty.isPresent()} rather than calling
   * {@code vertex.value(field)} directly.
   */
  @Test
  public void testReadWithMissingFieldDoesNotThrow() {
    // Insert a vertex with only field0 populated; field5 and field9 are
    // declared in the schema but never assigned a value.
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("only-set"));
    assertEquals(Status.OK, client.insert("usertable", "partial-key", values));

    // Ask for a mix of present and missing fields.
    Set<String> fields = new HashSet<>();
    fields.add("field0");
    fields.add("field5");
    fields.add("field9");

    Map<String, ByteIterator> result = new HashMap<>();
    Status status = client.read("usertable", "partial-key", fields, result);
    assertEquals("Read must not crash on missing fields", Status.OK, status);

    assertNotNull("Present field should be returned", result.get("field0"));
    assertEquals("only-set", result.get("field0").toString());
    assertFalse("Missing field5 must be absent from result",
        result.containsKey("field5"));
    assertFalse("Missing field9 must be absent from result",
        result.containsKey("field9"));
  }

  /**
   * Test-only ByteIterator that counts {@code toString()} invocations.
   * Returns its value on the first call and an empty string thereafter,
   * simulating the exhaustion behavior of {@link
   * com.jetbrains.youtrackdb.ycsb.RandomByteIterator}.
   */
  private static final class SingleUseByteIterator extends ByteIterator {
    private final String value;
    private final AtomicInteger callCount;
    private boolean consumed;

    SingleUseByteIterator(String value, AtomicInteger callCount) {
      this.value = value;
      this.callCount = callCount;
    }

    @Override
    public boolean hasNext() {
      return !consumed;
    }

    @Override
    public byte nextByte() {
      return 0;
    }

    @Override
    public long bytesLeft() {
      return consumed ? 0L : value.length();
    }

    @Override
    public String toString() {
      callCount.incrementAndGet();
      if (consumed) {
        return "";
      }
      consumed = true;
      return value;
    }
  }

  private static void deleteDirectory(Path dir) throws IOException {
    if (dir != null && Files.exists(dir)) {
      try (var paths = Files.walk(dir)) {
        paths.sorted(Comparator.reverseOrder())
            .forEach(p -> {
              try {
                Files.deleteIfExists(p);
              } catch (IOException e) {
                // best effort cleanup
              }
            });
      }
    }
  }
}
