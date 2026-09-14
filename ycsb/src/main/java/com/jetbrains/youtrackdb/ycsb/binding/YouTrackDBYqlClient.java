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

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.ycsb.ByteIterator;
import com.jetbrains.youtrackdb.ycsb.DB;
import com.jetbrains.youtrackdb.ycsb.DBException;
import com.jetbrains.youtrackdb.ycsb.Status;
import com.jetbrains.youtrackdb.ycsb.StringByteIterator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YouTrackDB YCSB driver using YQL (YouTrackDB Query Language) for all
 * database operations. Each YCSB client thread gets its own instance of
 * this class, but all instances share a single {@link YouTrackDB} connection
 * and {@link YTDBGraphTraversalSource}.
 *
 * <p>The driver creates a "usertable" vertex class with a unique index on
 * the {@code ycsb_key} property and 10 string field properties
 * ({@code field0}..{@code field9}).
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code ytdb.url} — database directory path (default: {@code ./target/ycsb-db})</li>
 *   <li>{@code ytdb.dbname} — database name (default: {@code ycsb})</li>
 *   <li>{@code ytdb.user} — database user (default: {@code admin})</li>
 *   <li>{@code ytdb.password} — database password (default: {@code admin})</li>
 *   <li>{@code ytdb.newdb} — drop and recreate the database on init (default: {@code true})</li>
 *   <li>{@code ytdb.dbtype} — database type: DISK or MEMORY (default: {@code DISK})</li>
 * </ul>
 */
public class YouTrackDBYqlClient extends DB {

  private static final Logger logger = LoggerFactory.getLogger(YouTrackDBYqlClient.class);

  static final String URL_PROPERTY = "ytdb.url";
  static final String URL_DEFAULT = "./target/ycsb-db";

  static final String DB_NAME_PROPERTY = "ytdb.dbname";
  static final String DB_NAME_DEFAULT = "ycsb";

  static final String USER_PROPERTY = "ytdb.user";
  static final String USER_DEFAULT = "admin";

  static final String PASSWORD_PROPERTY = "ytdb.password";
  static final String PASSWORD_DEFAULT = "admin";

  static final String NEW_DB_PROPERTY = "ytdb.newdb";
  static final String NEW_DB_DEFAULT = "true";

  static final String DB_TYPE_PROPERTY = "ytdb.dbtype";
  static final String DB_TYPE_DEFAULT = "DISK";

  private static final int FIELD_COUNT = 10;
  private static final int MAX_RETRY_ATTEMPTS = 3;
  private static final int MAX_RETRY_BACKOFF_MS = 50;

  private static final ReentrantLock initLock = new ReentrantLock();
  private static final AtomicInteger clientCount = new AtomicInteger(0);

  private static volatile YouTrackDB dbInstance;
  private static volatile YTDBGraphTraversalSource traversalSource;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String url = props.getProperty(URL_PROPERTY, URL_DEFAULT);
    String dbName = props.getProperty(DB_NAME_PROPERTY, DB_NAME_DEFAULT);
    String user = props.getProperty(USER_PROPERTY, USER_DEFAULT);
    String password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_DEFAULT);
    boolean newDb = Boolean.parseBoolean(
        props.getProperty(NEW_DB_PROPERTY, NEW_DB_DEFAULT));
    DatabaseType dbType = DatabaseType.valueOf(
        props.getProperty(DB_TYPE_PROPERTY, DB_TYPE_DEFAULT));

    initLock.lock();
    try {
      if (dbInstance == null) {
        logger.info("Initializing YouTrackDB at {} with database '{}' (type={})",
            url, dbName, dbType);

        YouTrackDB db = YourTracks.instance(url);
        boolean success = false;
        YTDBGraphTraversalSource g = null;
        try {
          if (newDb && db.exists(dbName)) {
            logger.info("Dropping existing database '{}'", dbName);
            db.drop(dbName);
          }

          if (!db.exists(dbName)) {
            db.create(dbName, dbType, user, password, "admin");
            logger.info("Created database '{}'", dbName);
          }

          g = db.openTraversal(dbName, user, password);
          createSchema(g);

          dbInstance = db;
          traversalSource = g;
          success = true;
        } finally {
          if (!success) {
            if (g != null) {
              try {
                g.close();
              } catch (Exception ignored) {
              }
            }
            try {
              db.close();
            } catch (Exception ignored) {
            }
          }
        }
      }
      clientCount.incrementAndGet();
    } catch (Exception e) {
      throw new DBException("Failed to initialize YouTrackDB", e);
    } finally {
      initLock.unlock();
    }
  }

  @Override
  public void cleanup() throws DBException {
    initLock.lock();
    try {
      if (clientCount.decrementAndGet() == 0) {
        logger.info("Last client thread cleaning up — closing YouTrackDB");
        try {
          if (traversalSource != null) {
            traversalSource.close();
          }
        } finally {
          traversalSource = null;
          try {
            if (dbInstance != null) {
              dbInstance.close();
            }
          } finally {
            dbInstance = null;
          }
        }
      }
    } catch (Exception e) {
      throw new DBException("Failed to cleanup YouTrackDB", e);
    } finally {
      initLock.unlock();
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      traversalSource.executeInTx(tx -> {
        var g = (YTDBGraphTraversalSource) tx;

        // Build: CREATE VERTEX usertable SET ycsb_key = :key, field0 = :field0, ...
        // No CME retry: YCSB load phase is single-threaded.
        StringBuilder sql = new StringBuilder("CREATE VERTEX ");
        sql.append(table).append(" SET ycsb_key = :key");

        // 2 args per entry (name, value) + 2 for key
        Object[] args = new Object[2 + values.size() * 2];
        args[0] = "key";
        args[1] = key;

        int argIdx = 2;
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          String fieldName = entry.getKey();
          sql.append(", ").append(fieldName).append(" = :").append(fieldName);
          args[argIdx++] = fieldName;
          args[argIdx++] = entry.getValue().toString();
        }

        g.yql(sql.toString(), args).iterate();
      });
      return Status.OK;
    } catch (Exception e) {
      logger.error("Insert failed for key {}", key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      return traversalSource.computeInTx(tx -> {
        var g = (YTDBGraphTraversalSource) tx;
        Optional<Object> optResult = g.yql(
            "SELECT FROM " + table + " WHERE ycsb_key = :key",
            "key", key).tryNext();

        if (optResult.isEmpty()) {
          return Status.NOT_FOUND;
        }

        Vertex vertex = (Vertex) optResult.get();
        extractFields(vertex, fields, result);
        return Status.OK;
      });
    } catch (Exception e) {
      logger.error("Read failed for key {}", key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try {
      return traversalSource.computeInTx(tx -> {
        var g = (YTDBGraphTraversalSource) tx;
        var results = g.yql(
            "SELECT FROM " + table + " WHERE ycsb_key >= :startkey"
                + " ORDER BY ycsb_key ASC LIMIT :count",
            "startkey", startkey, "count", recordcount).toList();

        for (Object obj : results) {
          HashMap<String, ByteIterator> record = new HashMap<>();
          extractFields((Vertex) obj, fields, record);
          result.add(record);
        }
        return Status.OK;
      });
    } catch (Exception e) {
      logger.error("Scan failed for startkey {}", startkey, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (values.isEmpty()) {
      return Status.OK;
    }
    // Materialize ByteIterators up-front: on a CME retry, executeWithRetry
    // invokes the lambda again, and ByteIterator.toString() consumes the
    // iterator on its first call (e.g. RandomByteIterator). Reading the
    // values into strings here ensures retries see stable data.
    Map<String, String> materializedValues = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      materializedValues.put(entry.getKey(), entry.getValue().toString());
    }
    return executeWithRetry(() -> {
      traversalSource.executeInTx(tx -> {
        var g = (YTDBGraphTraversalSource) tx;

        // Build: UPDATE usertable SET field0 = :field0, ... WHERE ycsb_key = :key
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(table).append(" SET ");

        // 2 args per entry (name, value) + 2 for key
        Object[] args = new Object[2 + materializedValues.size() * 2];

        int argIdx = 0;
        boolean first = true;
        for (Map.Entry<String, String> entry : materializedValues.entrySet()) {
          String fieldName = entry.getKey();
          if (!first) {
            sql.append(", ");
          }
          sql.append(fieldName).append(" = :").append(fieldName);
          args[argIdx++] = fieldName;
          args[argIdx++] = entry.getValue();
          first = false;
        }

        sql.append(" WHERE ycsb_key = :key");
        args[argIdx++] = "key";
        args[argIdx] = key;

        g.yql(sql.toString(), args).iterate();
      });
    }, "Update", key);
  }

  @Override
  public Status delete(String table, String key) {
    return executeWithRetry(() -> {
      traversalSource.executeInTx(tx -> {
        var g = (YTDBGraphTraversalSource) tx;
        g.yql("DELETE VERTEX " + table + " WHERE ycsb_key = :key",
            "key", key).iterate();
      });
    }, "Delete", key);
  }

  /**
   * Executes an operation with retry on {@link ConcurrentModificationException}
   * (MVCC conflict). Uses random backoff between retries.
   *
   * @param operation the operation to execute
   * @param opName   operation name for logging
   * @param key      the record key for logging
   * @return Status.OK on success, Status.ERROR after max retries exhausted
   */
  Status executeWithRetry(RetryableOperation operation,
      String opName, String key) {
    for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        operation.execute();
        return Status.OK;
      } catch (ConcurrentModificationException e) {
        if (attempt < MAX_RETRY_ATTEMPTS) {
          logger.debug("{} CME retry {}/{} for key {}",
              opName, attempt, MAX_RETRY_ATTEMPTS, key);
          try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(1, MAX_RETRY_BACKOFF_MS));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Status.ERROR;
          }
        } else {
          logger.error("{} failed after {} retries for key {}",
              opName, MAX_RETRY_ATTEMPTS, key, e);
          return Status.ERROR;
        }
      } catch (Exception e) {
        logger.error("{} failed for key {}", opName, key, e);
        return Status.ERROR;
      }
    }
    return Status.ERROR;
  }

  @FunctionalInterface
  interface RetryableOperation {
    void execute() throws Exception;
  }

  /**
   * Extracts properties from a vertex into the result map. When {@code fields}
   * is null, all user-visible properties are returned (vertex.properties()
   * excludes internals by TinkerPop API contract). Otherwise, only the
   * requested fields are extracted.
   */
  private static void extractFields(Vertex vertex, Set<String> fields,
      Map<String, ByteIterator> record) {
    if (fields == null) {
      Iterator<VertexProperty<Object>> props = vertex.properties();
      while (props.hasNext()) {
        VertexProperty<Object> vp = props.next();
        record.put(vp.key(),
            new StringByteIterator(vp.value().toString()));
      }
    } else {
      // Use property(field) + isPresent() so a missing field does not throw
      // NoSuchElementException and kill the benchmark thread.
      for (String field : fields) {
        VertexProperty<Object> vp = vertex.property(field);
        if (vp.isPresent()) {
          record.put(field, new StringByteIterator(vp.value().toString()));
        }
      }
    }
  }

  /**
   * Returns the shared traversal source. Package-private for test access.
   */
  static YTDBGraphTraversalSource getTraversalSource() {
    return traversalSource;
  }

  /**
   * Creates the usertable schema: vertex class, key property with unique
   * index, and 10 string field properties.
   */
  private static void createSchema(YTDBGraphTraversalSource g) {
    g.executeInTx(tx -> {
      var tg = (YTDBGraphTraversalSource) tx;
      tg.yql("CREATE CLASS usertable IF NOT EXISTS EXTENDS V").iterate();
      tg.yql("CREATE PROPERTY usertable.ycsb_key IF NOT EXISTS STRING").iterate();
      for (int i = 0; i < FIELD_COUNT; i++) {
        tg.yql("CREATE PROPERTY usertable.field" + i + " IF NOT EXISTS STRING").iterate();
      }
      tg.yql(
          "CREATE INDEX usertable.ycsb_key IF NOT EXISTS ON usertable (ycsb_key) UNIQUE")
          .iterate();
    });
    logger.info("Schema created: usertable with {} field properties and unique key index",
        FIELD_COUNT);
  }
}
