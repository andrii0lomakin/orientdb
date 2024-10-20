package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestConcurrentSequenceGenerationIT {

  static final int THREADS = 20;
  static final int RECORDS = 100;
  private OServer server;
  private OrientDB orientDB;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        TestConcurrentSequenceGenerationIT.class.getSimpleName());
    ODatabaseSession databaseSession =
        orientDB.open(TestConcurrentSequenceGenerationIT.class.getSimpleName(), "admin", "admin");
    databaseSession.execute(
        "sql",
        """
            CREATE CLASS TestSequence EXTENDS V;
             CREATE SEQUENCE TestSequenceIdSequence TYPE ORDERED;
            CREATE PROPERTY TestSequence.id LONG (MANDATORY TRUE, default\
             "sequence('TestSequenceIdSequence').next()");
            CREATE INDEX TestSequence_id_index ON TestSequence (id BY VALUE) UNIQUE;""");
    databaseSession.close();
  }

  @Test
  public void test() throws Exception {
    try (ODatabasePool pool =
        new ODatabasePool(
            orientDB, TestConcurrentSequenceGenerationIT.class.getSimpleName(), "admin", "admin")) {

      var executorService = Executors.newFixedThreadPool(THREADS);
      var futures = new ArrayList<Future<Object>>();

      for (int i = 0; i < THREADS; i++) {
        var future =
            executorService.submit(
                () -> {
                  try (ODatabaseSession db = pool.acquire()) {
                    for (int j = 0; j < RECORDS; j++) {
                      db.begin();
                      OVertex vert = db.newVertex("TestSequence");
                      assertNotNull(vert.getProperty("id"));
                      db.save(vert);
                      db.commit();
                    }
                  }

                  return null;
                });
        futures.add(future);
      }

      for (var future : futures) {
        future.get();
      }

      executorService.shutdown();
    }
  }

  @After
  public void after() {
    orientDB.drop(TestConcurrentSequenceGenerationIT.class.getSimpleName());
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }
}
