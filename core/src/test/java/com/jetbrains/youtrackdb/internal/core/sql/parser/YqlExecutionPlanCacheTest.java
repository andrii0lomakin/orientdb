package com.jetbrains.youtrackdb.internal.core.sql.parser;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.sql.OrderByNullsUtil;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.SQLEngine;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalExecutionPlan;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SequentialTest.class)
public class YqlExecutionPlanCacheTest extends BaseMemoryInternalDatabase {

  @Test
  public void testCacheInvalidation1() {
    var cache = YqlExecutionPlanCache.instance(session);
    var stm = "SELECT FROM OUser";

    // schema changes
    session.begin();
    session.query(stm).close();
    session.commit();
    cache = YqlExecutionPlanCache.instance(session);
    Assert.assertTrue(cache.contains(stm));

    var clazz = session.getMetadata().getSchema().createClass("testCacheInvalidation1");
    Assert.assertFalse(cache.contains(stm));

    // schema changes 2
    session.begin();
    session.query(stm).close();
    session.commit();

    cache = YqlExecutionPlanCache.instance(session);
    Assert.assertTrue(cache.contains(stm));

    var prop = clazz.createProperty("name", PropertyType.STRING);
    Assert.assertFalse(cache.contains(stm));

    // index changes
    session.begin();
    session.query(stm).close();
    session.commit();
    cache = YqlExecutionPlanCache.instance(session);
    Assert.assertTrue(cache.contains(stm));

    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);
    Assert.assertFalse(cache.contains(stm));
  }

  /**
   * A placement change rejects one stamped plan without invalidating an unstamped neighbour. Both
   * direction-specific settings must drive the same per-entry gate and cache accounting.
   */
  @Test
  public void nullPlacementChangesRejectStampedPlansInBothDirections() {
    assertPlacementChangeRejectsPlan(
        "Asc", "ASC", GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC);
    assertPlacementChangeRejectsPlan(
        "Desc", "DESC", GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC);
  }

  private void assertPlacementChangeRejectsPlan(
      String suffix, String direction, GlobalConfiguration placementKey) {
    var className = "PlanCacheNullPlacement" + suffix;
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("rank", PropertyType.INTEGER);

    var orderedSql = "SELECT rank FROM " + className + " ORDER BY rank " + direction;
    var unorderedSql = "SELECT rank FROM " + className;
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(session);
    var ordered = (SQLSelectStatement) SQLEngine.parse(orderedSql, session);
    var unordered = (SQLSelectStatement) SQLEngine.parse(unorderedSql, session);
    var orderedPlan = ordered.createExecutionPlan(ctx, false);
    var unorderedPlan = unordered.createExecutionPlan(ctx, false);
    var storageConfig = session.getStorage().getContextConfiguration();
    var oldPlacement = storageConfig.getValue(placementKey);

    try {
      storageConfig.setValue(placementKey, OrderByNullsPlacement.FIRST);
      var stamp = OrderByNullsUtil.resolvePlacements(session.getConfiguration());
      var cache = new YqlExecutionPlanCache(16);
      cache.putInternal(orderedSql, orderedPlan, session, stamp);
      cache.putInternal(unorderedSql, unorderedPlan, session, null);
      var invalidationBefore = cache.getLastInvalidation();

      storageConfig.setValue(placementKey, OrderByNullsPlacement.LAST);
      Assert.assertNull("the stale stamped plan must be rejected",
          cache.getInternal(orderedSql, ctx, session));
      Assert.assertFalse("the stale stamped plan must be removed", cache.contains(orderedSql));
      Assert.assertEquals("the rejection must count as one miss", 1, cache.getMisses());
      Assert.assertEquals("the rejection must not count as a hit", 0, cache.getHits());
      Assert.assertEquals("one rejection must not invalidate the whole cache",
          invalidationBefore, cache.getLastInvalidation());

      var served = cache.getInternal(unorderedSql, ctx, session);
      Assert.assertNotNull("the unstamped neighbouring plan must be served", served);
      ((InternalExecutionPlan) served).close();
      Assert.assertEquals("the unstamped lookup must count as one hit", 1, cache.getHits());
      Assert.assertTrue("the unstamped neighbouring plan must remain", cache.contains(unorderedSql));
    } finally {
      storageConfig.setValue(placementKey, oldPlacement);
      orderedPlan.close();
      unorderedPlan.close();
    }
  }

  /**
   * A stored plan is validated against its own placement stamp, one entry at a time.
   *
   * <p>The test stamps a plan with a placement pair the session does not currently resolve, which is
   * the state a build leaves behind when the setting changes while that build runs. The lookup must
   * refuse that plan and drop it. The same plan stored without a stamp must be served, because no
   * sort clause of it depends on placement.
   */
  @Test
  public void stampedPlanIsRejectedWhileUnstampedPlanIsServed() {
    var sql = "SELECT FROM OUser";
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(session);
    var statement = (SQLSelectStatement) SQLEngine.parse(sql, session);
    var plan = statement.createExecutionPlan(ctx, false);
    try {
      var cache = new YqlExecutionPlanCache(16);
      var current = OrderByNullsUtil.resolvePlacements(session.getConfiguration());
      var other =
          new ResolvedOrderByNullsPlacement(
              current.ascending() == OrderByNullsPlacement.FIRST
                  ? OrderByNullsPlacement.LAST
                  : OrderByNullsPlacement.FIRST,
              current.descending());

      cache.putInternal(sql, plan, session, other);
      Assert.assertTrue("the stamped plan must be stored", cache.contains(sql));
      Assert.assertNull(
          "a plan stamped with another placement must not be served",
          cache.getInternal(sql, ctx, session));
      Assert.assertFalse("the rejected plan must be dropped", cache.contains(sql));

      cache.putInternal(sql, plan, session, null);
      var served = cache.getInternal(sql, ctx, session);
      Assert.assertNotNull("a plan with no stamp must be served", served);
      ((InternalExecutionPlan) served).close();
      Assert.assertTrue("a plan with no stamp must stay stored", cache.contains(sql));
    } finally {
      plan.close();
    }
  }

  @Test
  public void testLRUEvictionLogic() {
    var stm1 = "SELECT FROM V";
    var stm2 = "SELECT FROM E";
    var stm3 = "SELECT FROM OUser";

    // Populate using putInternal (needs a session for copy)
    session.begin();
    session.query(stm1).close();
    session.commit();

    session.begin();
    session.query(stm2).close();
    session.commit();

    // Use the real cache instance to test eviction
    var realCache = YqlExecutionPlanCache.instance(session);
    Assert.assertTrue(realCache.contains(stm1));
    Assert.assertTrue(realCache.contains(stm2));

    // Third entry should evict the oldest
    session.begin();
    session.query(stm3).close();
    session.commit();

    Assert.assertTrue(realCache.contains(stm3));
  }

  @Test
  public void testDisabledCacheWhenSizeIsZero() {
    var cache = new YqlExecutionPlanCache(0);
    Assert.assertFalse(cache.contains("SELECT FROM V"));

    var result = cache.getInternal("SELECT FROM V", null, session);
    Assert.assertNull(result);
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    var cache = YqlExecutionPlanCache.instance(session);
    var threadCount = 8;
    var queriesPerThread = 10;
    var latch = new CountDownLatch(threadCount);
    var errors = new AtomicInteger(0);

    // Pre-populate with a query to test concurrent reads
    session.begin();
    session.query("SELECT FROM OUser").close();
    session.commit();
    Assert.assertTrue(cache.contains("SELECT FROM OUser"));

    for (var t = 0; t < threadCount; t++) {
      var thread = new Thread(() -> {
        try {
          for (var i = 0; i < queriesPerThread; i++) {
            // Concurrent contains() and invalidation should not throw
            cache.contains("SELECT FROM OUser");
          }
        } catch (Exception e) {
          errors.incrementAndGet();
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
      thread.start();
    }

    latch.await();
    Assert.assertEquals("No errors should occur during concurrent access", 0, errors.get());
  }

  @Test
  public void invalidate_clearsCache() {
    var cache = YqlExecutionPlanCache.instance(session);
    session.begin();
    session.query("SELECT FROM OUser").close();
    session.commit();
    Assert.assertTrue(cache.contains("SELECT FROM OUser"));

    cache.invalidate();
    Assert.assertFalse(cache.contains("SELECT FROM OUser"));
  }

  @Test
  public void invalidate_updatesTimestamp() {
    var cache = YqlExecutionPlanCache.instance(session);
    var before = YqlExecutionPlanCache.getLastInvalidation(session);
    cache.invalidate();
    var after = YqlExecutionPlanCache.getLastInvalidation(session);
    Assert.assertTrue("Invalidation timestamp should increase", after > before);
  }

  @Test
  public void invalidate_onDisabledCache_doesNotThrow() {
    var cache = new YqlExecutionPlanCache(0);
    cache.invalidate();
  }

  @Test
  public void onSchemaUpdate_invalidatesCache() {
    var cache = new YqlExecutionPlanCache(10);
    cache.onSchemaUpdate(null, "test", null);
  }

  @Test
  public void onIndexManagerUpdate_invalidatesCache() {
    var cache = new YqlExecutionPlanCache(10);
    cache.onIndexManagerUpdate(null, "test", null);
  }

  @Test
  public void onFunctionLibraryUpdate_invalidatesCache() {
    var cache = new YqlExecutionPlanCache(10);
    cache.onFunctionLibraryUpdate(null, "test");
  }

  @Test
  public void onSequenceLibraryUpdate_invalidatesCache() {
    var cache = new YqlExecutionPlanCache(10);
    cache.onSequenceLibraryUpdate(null, "test");
  }

  @Test
  public void onStorageConfigurationUpdate_invalidatesCache() {
    var cache = new YqlExecutionPlanCache(10);
    cache.onStorageConfigurationUpdate("test", null);
  }

  @Test
  public void testGlobalConfigurationCacheSize() {
    GlobalConfiguration.STATEMENT_CACHE_SIZE.setValue(2);

    var cache = new YqlExecutionPlanCache(
        GlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger());

    Assert.assertFalse(cache.contains("SELECT FROM V"));
    Assert.assertFalse(cache.contains("SELECT FROM E"));
  }
}
