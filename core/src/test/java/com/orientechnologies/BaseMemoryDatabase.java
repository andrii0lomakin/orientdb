package com.orientechnologies;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class BaseMemoryDatabase {

  protected ODatabaseSession db;
  protected OrientDB context;
  @Rule public TestName name = new TestName();
  private String databaseName;

  @Before
  public void beforeTest() {
    context = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    String dbName = name.getMethodName();
    dbName = dbName.replace('[', '_');
    dbName = dbName.replace(']', '_');
    this.databaseName = dbName;
    context.create(this.databaseName, ODatabaseType.MEMORY, "admin", "adminpwd", "admin");
    db = context.open(this.databaseName, "admin", "adminpwd");
  }

  protected void reOpen(String user, String password) {
    this.db.close();
    this.db = context.open(this.databaseName, user, password);
  }

  @After
  public void afterTest() throws Exception {
    db.close();
    context.drop(databaseName);
    context.close();
  }

  public static void assertWithTimeout(ODatabaseSession session, Runnable runnable)
      throws Exception {
    for (int i = 0; i < 300; i++) {
      try {
        session.begin();
        runnable.run();
        session.commit();
        return;
      } catch (AssertionError e) {
        session.rollback();
        Thread.sleep(100);
      } catch (Exception e) {
        session.rollback();
        throw e;
      }
    }

    runnable.run();
  }
}
