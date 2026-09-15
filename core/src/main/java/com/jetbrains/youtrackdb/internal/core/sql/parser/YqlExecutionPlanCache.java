package com.jetbrains.youtrackdb.internal.core.sql.parser;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.AbstractMetadataUpdateCache;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalExecutionPlan;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * LRU cache for already prepared YQL/SQL execution plans using Guava Cache. Stores itself in
 * SharedContext as a resource and acts as an entry point for the SQL executor.
 *
 * <p><b>Null placement stamp.</b> A plan build can bake the position of null sort keys into a step,
 * so a plan prepared under one placement must never be served under another. Each stored plan
 * therefore carries the placement pair its own build resolved, and a lookup compares that stamp
 * against the pair the looking-up session resolves. A mismatch drops that one plan and reports a
 * miss, which leaves every other prepared plan in place. A build that resolved no placement stores
 * no stamp. A lookup of such a plan skips placement configuration resolution. Every lookup still
 * reads the command timeout configuration.
 */
public class YqlExecutionPlanCache
    extends AbstractMetadataUpdateCache<String, YqlExecutionPlanCache.StampedPlan> {

  private volatile long lastGlobalTimeout =
      GlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();

  /**
   * A prepared plan together with the null placement pair its build resolved.
   *
   * <p>{@code placements} is {@code null} when the build resolved none, which marks the plan as one
   * no placement change can invalidate.
   */
  record StampedPlan(
      @Nonnull InternalExecutionPlan plan,
      @Nullable ResolvedOrderByNullsPlacement placements) {

  }

  /**
   * @param size the size of the cache; 0 means cache disabled
   */
  public YqlExecutionPlanCache(int size) {
    super(size);
  }

  public static long getLastInvalidation(@Nonnull DatabaseSessionEmbedded db) {
    return instance(db).getLastInvalidation();
  }

  /**
   * @param statement an SQL statement
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(String statement) {
    return containsKey(statement);
  }

  /**
   * Returns an already prepared SQL execution plan, taking it from the cache if it exists or
   * creating a new one if it doesn't.
   *
   * @param statement the SQL statement
   * @param ctx       the command context
   * @param db        the current DB instance
   * @return a statement executor from the cache
   */
  @Nullable
  public static ExecutionPlan get(
      String statement, CommandContext ctx, DatabaseSessionEmbedded db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }
    if (statement == null) {
      return null;
    }

    var resource = db.getSharedContext().getYqlExecutionPlanCache();
    return resource.getInternal(statement, ctx, db);
  }

  /**
   * Publishes a freshly built plan.
   *
   * @param placements the null placement pair the enclosing scope resolved, or {@code null} when
   *                   the scope resolved none. Read it inside the scope so lookup can validate any
   *                   placement-sensitive work covered by that scope.
   */
  public static void put(
      String statement,
      ExecutionPlan plan,
      DatabaseSessionEmbedded db,
      @Nullable ResolvedOrderByNullsPlacement placements) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }
    if (statement == null) {
      return;
    }

    var resource = db.getSharedContext().getYqlExecutionPlanCache();
    resource.putInternal(statement, plan, db, placements);
  }

  public void putInternal(
      String statement,
      ExecutionPlan plan,
      DatabaseSessionEmbedded db,
      @Nullable ResolvedOrderByNullsPlacement placements) {
    if (statement == null || !cacheEnabled()) {
      return;
    }
    if (db.getTxSchemaState() != null) {
      // The mirror of the getInternal bypass: a plan built during a schema- or index-changing
      // transaction is shaped by the tx-local schema view and must not be published to other
      // sessions through the shared cache.
      return;
    }

    // Copy the plan outside the cache data structure — no lock contention on copy()
    var internal = (InternalExecutionPlan) plan;
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(db);
    internal = internal.copy(ctx);
    // this copy is never used, so it has to be closed to free resources
    internal.close();
    putCached(statement, new StampedPlan(internal, placements));
  }

  /**
   * Returns the cached execution plan for the given SQL statement, or null if not found.
   *
   * @param statement an SQL statement
   * @param ctx       the command context
   * @param db        the database session
   * @return the corresponding execution plan from cache, or null if not found
   */
  @Nullable
  public ExecutionPlan getInternal(
      String statement, CommandContext ctx, DatabaseSessionEmbedded db) {
    var currentGlobalTimeout =
        db.getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT);
    if (currentGlobalTimeout != this.lastGlobalTimeout) {
      // The timeout shapes every prepared plan, so a change drops the whole cache. Checked before
      // Guava is touched, so no plan prepared under the previous timeout can become a hit.
      invalidate();
      this.lastGlobalTimeout = currentGlobalTimeout;
    }

    if (statement == null || !cacheEnabled()) {
      return null;
    }
    if (db.getTxSchemaState() != null) {
      // An open schema- or index-changing transaction plans against its tx-local schema view
      // (tx-created classes with provisional collection ids, tx-dropped classes and indexes).
      // A plan cached from committed state can be stale against that view (for example a
      // pre-cached polymorphic scan that misses a tx-created subclass), so the shared cache is
      // bypassed for the transaction's duration. Schema transactions are rare, so the extra
      // planning cost is negligible.
      return null;
    }

    // Guava Cache handles LRU eviction and concurrent access internally
    var result = getCached(statement);
    if (result != null) {
      var stamp = result.placements();
      if (stamp != null && !stamp.equals(db.getPlanNullPlacements().resolve())) {
        // This plan was prepared under another null placement, so its baked-in order no longer
        // matches what the statement must return. Drop this one plan and report a miss so the
        // caller prepares it again. A plan with no stamp skips this branch and its configuration
        // read entirely.
        invalidateCached(statement);
        recordMiss();
        return null;
      }
      recordHit();
      return result.plan().copy(ctx);
    }
    recordMiss();
    return null;
  }

  public static @Nonnull YqlExecutionPlanCache instance(@Nonnull DatabaseSessionEmbedded db) {
    return db.getSharedContext().getYqlExecutionPlanCache();
  }
}
