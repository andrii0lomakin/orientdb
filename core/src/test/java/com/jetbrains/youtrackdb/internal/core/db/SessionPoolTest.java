package com.jetbrains.youtrackdb.internal.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB.LocalUserCredential;
import com.jetbrains.youtrackdb.api.YouTrackDB.PredefinedLocalRole;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.Test;

public class SessionPoolTest {

  private static final String PASSWORD = "adminpwd";

  @Test
  public void testPool() {
    var config = new BaseConfiguration();
    config.setProperty(GlobalConfiguration.CREATE_DEFAULT_USERS.getKey(), false);

    final var youTrackDb =
        YourTracks.instance(DbTestBase.getBaseDirectoryPathStr(getClass()), config);
    youTrackDb.createIfNotExists("test", DatabaseType.MEMORY,
        new LocalUserCredential("admin", PASSWORD, PredefinedLocalRole.ADMIN));
    final SessionPool pool =
        new SessionPoolImpl((YouTrackDBImpl) youTrackDb, "test", "admin", PASSWORD);
    var db = pool.acquire();
    db.executeInTx(
        transaction -> db.newEntity());
    db.close();
    pool.close();
    youTrackDb.close();
  }

  @Test
  public void testPoolCloseTx() {
    var config = new BaseConfiguration();
    config.setProperty(GlobalConfiguration.CREATE_DEFAULT_USERS.getKey(), false);
    config.setProperty(GlobalConfiguration.DB_POOL_MAX.getKey(), 1);

    final var youTrackDb =
        (YouTrackDBImpl) YourTracks.instance(
            DbTestBase.getBaseDirectoryPathStr(getClass()),
            config);

    youTrackDb.createIfNotExists("test", DatabaseType.MEMORY,
        new LocalUserCredential("admin", PASSWORD, PredefinedLocalRole.ADMIN));
    final var pool =
        new SessionPoolImpl(youTrackDb, "test", "admin", PASSWORD);
    var db = pool.acquire();
    db.createClass("Test");
    db.begin();
    db.newEntity("Test");
    db.close();
    db = pool.acquire();
    db.begin();
    assertEquals(0, db.countClass("Test"));
    db.rollback();
    db.close();
    pool.close();
    youTrackDb.close();
  }

  /** A recycled session drops an unclosed placement scope before the next borrower uses it. */
  @Test
  public void pooledReuseResetsLeakedPlanNullPlacementScope() {
    var config = new BaseConfiguration();
    config.setProperty(GlobalConfiguration.CREATE_DEFAULT_USERS.getKey(), false);
    config.setProperty(GlobalConfiguration.DB_POOL_MAX.getKey(), 1);
    try (var youTrackDb =
        (YouTrackDBImpl) YourTracks.instance(
            DbTestBase.getBaseDirectoryPathStr(getClass()), config)) {
      youTrackDb.createIfNotExists(
          "placementReset", DatabaseType.MEMORY,
          new LocalUserCredential("admin", PASSWORD, PredefinedLocalRole.ADMIN));
      try (var pool = new SessionPoolImpl(youTrackDb, "placementReset", "admin", PASSWORD)) {
        var firstBorrow = pool.acquire();
        var storageConfig = firstBorrow.getStorage().getContextConfiguration();
        storageConfig.setValue(
            GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC,
            OrderByNullsPlacement.FIRST);
        firstBorrow.getPlanNullPlacements().open();
        assertEquals(
            OrderByNullsPlacement.FIRST,
            firstBorrow.getPlanNullPlacements().resolve().ascending());
        storageConfig.setValue(
            GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC,
            OrderByNullsPlacement.LAST);
        firstBorrow.close();

        var secondBorrow = pool.acquire();
        try {
          assertSame("the pool must recycle the same session", firstBorrow, secondBorrow);
          assertEquals(
              "the new borrower must resolve the current placement",
              OrderByNullsPlacement.LAST,
              secondBorrow.getPlanNullPlacements().resolve().ascending());
        } finally {
          secondBorrow.close();
        }
      }
    }
  }

  @Test
  public void testPoolDoubleClose() {
    var config = new BaseConfiguration();
    config.setProperty(GlobalConfiguration.CREATE_DEFAULT_USERS.getKey(), false);
    config.setProperty(GlobalConfiguration.DB_POOL_MAX.getKey(), 1);
    final var youTrackDb =
        (YouTrackDBImpl) YourTracks.instance(
            DbTestBase.getBaseDirectoryPathStr(getClass()),
            config);

    youTrackDb.createIfNotExists("test", DatabaseType.MEMORY,
        new LocalUserCredential("admin", PASSWORD, PredefinedLocalRole.ADMIN));
    final SessionPool pool =
        new SessionPoolImpl(youTrackDb, "test", "admin", PASSWORD);
    var db = pool.acquire();
    db.close();
    pool.close();
    youTrackDb.close();
  }
}
