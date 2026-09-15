package com.jetbrains.youtrackdb.internal.core.sql.executor.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.query.ResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End-to-end wiring tests for the tx-result cache on the live {@code query()} path. They prove the
 * session/transaction integration actually serves repeated identical SELECTs from the cache when the
 * feature flag is on, that a mutation between two identical queries is reflected in the second view,
 * that every transaction-end path clears the cache, that the cache field stays null and behaviour is
 * unchanged when the flag is off (the compatibility floor), and that a re-entrant {@code query()}
 * issued from inside the cache code path bypasses the cache.
 *
 * <p>The exhaustive invariant and cache-vs-fresh equivalence matrix (every invariant plus the RECORD
 * equivalence across the four mutation patterns) lands in the dedicated suite; this class is the
 * wiring smoke plus the flag-off and re-entrancy proofs. Run with {@code -ea} so the schema-DDL
 * canary and the owner-thread asserts are live.
 */
@Category(SequentialTest.class)
public class TxResultCacheWiringTest extends DbTestBase {

  private static final String CLASS_NAME = "WireRec";
  private static final String FIELD = "n";
  private static final String MATCH_NODE = "WireMatchNode";
  private static final String MATCH_EDGE = "WireMatchLink";

  private boolean previousEnabled;

  @Before
  public void enableCacheAndSchema() {
    // Capture the prior flag value so the global is restored in tearDown and cannot leak the
    // enabled state into the next test in the surefire sequence. Individual tests flip it as needed.
    previousEnabled = GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.getValueAsBoolean();
    var cls = session.createClass(CLASS_NAME);
    cls.createProperty(FIELD, PropertyType.INTEGER);
  }

  @After
  public void restoreFlag() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(previousEnabled);
  }

  private FrontendTransactionImpl tx() {
    return (FrontendTransactionImpl) session.getTransactionInternal();
  }

  /** Creates and persists {@code count} records of CLASS_NAME with ascending FIELD values. */
  private void seed(int count) {
    session.begin();
    for (var i = 0; i < count; i++) {
      var e = session.newEntity(CLASS_NAME);
      e.setProperty(FIELD, i);
    }
    session.commit();
  }

  /** Runs a SELECT and returns its row count, fully consuming and closing the result set. */
  private long countQuery(String sql) {
    try (var rs = session.query(sql)) {
      return rs.stream().count();
    }
  }

  // ===========================================================================
  // Flag ON — wiring smoke
  // ===========================================================================

  /**
   * With the flag on, the same SELECT issued twice inside one transaction misses on the first call
   * (driving real execution and populating one entry) and hits on the second (served from the
   * cache), so the metrics show exactly one miss and one hit and the cache holds a single entry.
   */
  @Test
  public void flagOn_repeatedSelectHitsCacheSecondTime() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(3);

    session.begin();
    var sql = "SELECT FROM " + CLASS_NAME;

    var first = countQuery(sql);
    assertEquals("first query returns all seeded rows", 3, first);

    var cache = tx().getQueryResultCache();
    assertNotNull("cache must exist with the flag on", cache);
    assertEquals("one entry populated after the first query", 1, cache.size());
    assertEquals("first query is a miss", 1, cache.getMetrics().getMisses());
    assertEquals("first query is not a hit", 0, cache.getMetrics().getHits());

    var second = countQuery(sql);
    assertEquals("second query returns the same rows", 3, second);
    assertEquals("second query is served from cache (a hit)", 1, cache.getMetrics().getHits());
    assertEquals("no further miss on the second query", 1, cache.getMetrics().getMisses());
    assertEquals("still one entry", 1, cache.size());

    session.rollback();
  }

  /**
   * A parameterised SELECT (named {@code :param}, passed as a Map) issued twice in one transaction
   * misses then hits, exactly like the positional-arg path. The first query runs against an empty
   * cache, so it exercises the empty-cache short-circuit and the Map-args arm of the lazy cache-key
   * build (the positional/no-arg arm is covered by the other wiring tests); the miss must still be
   * counted even though {@code lookup} was short-circuited, and the second query must hit.
   */
  @Test
  public void flagOn_parameterisedSelectHitsCacheSecondTime() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(3);

    session.begin();
    var sql = "SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " >= :min";
    Map<String, Object> params = Map.of("min", 1);

    long first;
    try (var rs = session.query(sql, params)) {
      first = rs.stream().count();
    }
    assertEquals("first parameterised query returns the matching rows (n=1,2)", 2, first);

    var cache = tx().getQueryResultCache();
    assertNotNull("cache must exist with the flag on", cache);
    assertEquals("one entry populated after the first parameterised query", 1, cache.size());
    assertEquals("first parameterised query is a miss (short-circuit still counts it)",
        1, cache.getMetrics().getMisses());
    assertEquals("first parameterised query is not a hit", 0, cache.getMetrics().getHits());

    long second;
    try (var rs = session.query(sql, params)) {
      second = rs.stream().count();
    }
    assertEquals("second parameterised query returns the same rows", 2, second);
    assertEquals("second parameterised query is served from cache (a hit)",
        1, cache.getMetrics().getHits());
    assertEquals("no further miss on the second parameterised query",
        1, cache.getMetrics().getMisses());
    assertEquals("still one entry", 1, cache.size());

    session.rollback();
  }

  /**
   * A {@code newEntity} that adds a matching record between two identical queries is reflected in the
   * second view: the second query returns one more row than the first, proving the delta build
   * merges the in-transaction CREATE into the cached result rather than replaying the stale frozen
   * set.
   */
  @Test
  public void flagOn_mutationBetweenQueriesReflectedInSecondView() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(2);

    session.begin();
    var sql = "SELECT FROM " + CLASS_NAME;

    var before = countQuery(sql);
    assertEquals(2, before);

    // Add a new matching record inside the same transaction, after the entry was populated.
    var added = session.newEntity(CLASS_NAME);
    added.setProperty(FIELD, 99);

    var after = countQuery(sql);
    assertEquals("second view reflects the in-tx CREATE via the delta build", 3, after);

    session.rollback();
  }

  /**
   * Commit runs the transaction-end clear sink, which wipes the cache. The cache reference is
   * captured before commit because the transaction's state is reset by the end path.
   */
  @Test
  public void flagOn_commitClearsCache() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(1);

    session.begin();
    countQuery("SELECT FROM " + CLASS_NAME);
    var cache = tx().getQueryResultCache();
    assertNotNull(cache);
    assertEquals("entry present before commit", 1, cache.size());

    session.commit();
    assertEquals("commit clears the cache", 0, cache.size());
  }

  /** Rollback runs the same transaction-end clear sink and wipes the cache. */
  @Test
  public void flagOn_rollbackClearsCache() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(1);

    session.begin();
    countQuery("SELECT FROM " + CLASS_NAME);
    var cache = tx().getQueryResultCache();
    assertNotNull(cache);
    assertEquals("entry present before rollback", 1, cache.size());

    session.rollback();
    assertEquals("rollback clears the cache", 0, cache.size());
  }

  /**
   * A re-entrant {@code query()} issued while a cache code path is already on the stack (simulated by
   * entering the cache scope directly) bypasses the cache: no entry is created and no lookup is
   * counted, because the re-entrancy depth guard short-circuits the gate to uncached execution.
   */
  @Test
  public void flagOn_reentrantQueryBypassesCache() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(2);

    session.begin();
    // Force the re-entrancy condition the session's own bracketing would create around a UDF in a
    // WHERE clause: depth > 0 means any query() must bypass the cache entirely.
    tx().enterCacheCode();
    try {
      var rows = countQuery("SELECT FROM " + CLASS_NAME);
      assertEquals("re-entrant query still returns correct rows uncached", 2, rows);
    } finally {
      tx().exitCacheCode();
    }

    var cache = tx().getQueryResultCache();
    assertNotNull(cache);
    assertEquals("re-entrant query created no cache entry", 0, cache.size());
    assertEquals("re-entrant query recorded no miss", 0, cache.getMetrics().getMisses());
    assertEquals("re-entrant query recorded no hit", 0, cache.getMetrics().getHits());

    session.rollback();
  }

  /**
   * Abandoned-view guard-release regression (the narrowed-guard fix). A {@code query()} that returns a
   * live view the caller neither drains nor closes must leave the tx cache-code depth at zero, so the
   * cache stays usable for the rest of the transaction. Before the fix the view held the guard for its
   * whole lifetime, so an abandoned-but-strongly-held view pinned depth &gt; 0 and silently routed every
   * later {@code query()} onto the re-entrant bypass, leaving the cache dead for the rest of the tx.
   * Here the first query's view is deliberately left open and unconsumed; the depth must be balanced
   * once {@code query()} returns, and a second identical query in the same tx must hit the cache.
   */
  @Test
  public void abandonedViewDoesNotDisableCacheForRestOfTx() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(2);

    session.begin();
    var cache = tx().getQueryResultCache();
    assertNotNull(cache);
    var sql = "SELECT FROM " + CLASS_NAME;

    // First query: keep the returned view open and unconsumed (the abandoned-view scenario). The
    // populate path puts the entry before returning, so it exists even though nothing was iterated.
    var abandoned = session.query(sql);
    assertEquals("the returned view must leave the cache-code guard balanced (depth 0)", 0,
        tx().getCacheCodeDepth());
    assertEquals("the first query populated exactly one entry", 1, cache.size());

    // Second identical query in the same tx must hit the cache, proving the abandoned view did not
    // keep the re-entrancy guard held. A leaked guard would route this onto the uncached bypass
    // (no hit counted).
    try (var rs = session.query(sql)) {
      var rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertEquals("the second query returns the seeded rows", 2, rows);
    }
    assertEquals("the second identical query was served from cache (a hit), not bypassed", 1,
        cache.getMetrics().getHits());

    abandoned.close();
    session.rollback();
  }

  // ===========================================================================
  // Flag OFF — zero behaviour change
  // ===========================================================================

  /**
   * With the flag off the transaction never allocates a cache: {@code getQueryResultCache()} stays
   * null and two identical queries behave exactly as they did before the feature existed, each
   * returning the full result independently.
   */
  @Test
  public void flagOff_cacheFieldStaysNullAndBehaviourUnchanged() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(false);
    seed(4);

    session.begin();
    var sql = "SELECT FROM " + CLASS_NAME;

    var first = countQuery(sql);
    var second = countQuery(sql);

    assertEquals("both queries return all rows", 4, first);
    assertEquals("repeated query is identical with the flag off", first, second);
    assertNull("no cache is allocated when the flag is off", tx().getQueryResultCache());

    session.rollback();
  }

  // ===========================================================================
  // Re-entrancy guard balance on the populate no-view branch
  // ===========================================================================

  /**
   * Fail-fast-and-guard-balance test for the populate branch that handles a non-{@code LocalResultSet}
   * execution result. That branch is an invariant breach, not a routine fallback: the cache gate routes
   * only RECORD/K0_NONE SELECT/MATCH into populate, and every such shape executes to a {@code
   * LocalResultSet} today, so reaching the branch means a query classified cacheable produced an
   * unexpected result type. Production rides a WARN and returns the unwrapped result (the consumer still
   * gets the right rows); under {@code -ea} (this suite) the paired {@code assert} makes it fail fast.
   *
   * <p>SELECT/MATCH always return a {@code LocalResultSet} today, so the branch is unreachable through
   * normal SQL; the test reaches it by invoking the private {@code serveThroughCache} with a
   * deterministic, RECORD-classified SELECT statement whose execution is overridden to return a plain
   * non-{@code LocalResultSet} result set. It pins two things: the breach fails fast via a Java {@code
   * assert} (an {@link AssertionError}, surfaced through reflection as an {@link
   * InvocationTargetException}); and — critically — the tx re-entrancy guard is still released on that
   * throw path, because {@code serveThroughCache} brackets the populate in try/finally and {@code
   * exitCacheCode()} runs on the exception. A leaked guard would pin depth &gt; 0 and route every later
   * {@code query()} onto the re-entrant bypass, leaving the cache dead for the rest of the tx.
   */
  @Test
  public void populateNonLocalResultBranchAssertsButStillReleasesGuard() throws Exception {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    seed(1);

    session.begin();
    var tx = tx();
    assertNotNull("cache must exist with the flag on", tx.getQueryResultCache());
    assertEquals("guard starts balanced before the gated call", 0, tx.getCacheCodeDepth());

    // A bare SELECT statement with no projection/group-by/skip/limit classifies as RECORD and is
    // deterministic (empty AST), so it passes every gate check and reaches the populate path. Its
    // execution is overridden to return an InternalResultSet (a concrete ResultSet that is NOT a
    // LocalResultSet), forcing the populate non-LocalResultSet branch.
    var nonLocalResult = new InternalResultSet(session);
    var statement = new NonLocalResultSelectStatement(nonLocalResult);

    try {
      // Under -ea the branch's assert throws; reflection wraps it in an InvocationTargetException.
      var thrown = assertThrows(
          "a cacheable statement returning a non-LocalResultSet must fail fast under -ea",
          InvocationTargetException.class,
          () -> invokeServeThroughCache(session, statement, null));
      assertTrue(
          "the breach must fail fast via a Java assertion, not some other error",
          thrown.getCause() instanceof AssertionError);

      assertEquals(
          "the re-entrancy guard is released on the assert-throw path (finally ran, no leak)",
          0,
          tx.getCacheCodeDepth());

      // A subsequent real query in the same transaction must still reach the cache: a leaked guard
      // would force every later query() onto the depth > 0 re-entrant bypass, leaving the cache dead.
      var cache = tx.getQueryResultCache();
      countQuery("SELECT FROM " + CLASS_NAME);
      assertEquals(
          "follow-up query still populates the cache, proving the guard did not leak",
          1,
          cache.size());
    } finally {
      // The throw path never returns or closes the synthetic result (in production it would be
      // returned to the caller, which closes it); close it here so the test leaves nothing open.
      nonLocalResult.close();
    }

    session.rollback();
  }

  // ===========================================================================
  // Null placement is fixed per served entry and stale entries are rejected
  // ===========================================================================

  /**
   * A populated ordered entry freezes its placement. A database-local placement change makes the
   * next identical query reject that entry as a miss, execute again, and publish a new frozen entry.
   */
  @Test
  public void flagOn_placementChangeRejectsAndRepopulatesOrderedEntry() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    var storageConfig = session.getStorage().getContextConfiguration();
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
    try {
      seed(2);
      session.begin();
      var sql = "SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC";

      assertEquals("the populating query returns the seeded rows", 2, countQuery(sql));

      var cache = tx().getQueryResultCache();
      assertNotNull("cache must exist with the flag on", cache);
      var oldEntry = CacheTestSupport.onlyEntry(cache);
      assertEquals(
          "populate must freeze the placement in force at that moment",
          new ResolvedOrderByNullsPlacement(
              OrderByNullsPlacement.LAST, OrderByNullsPlacement.LAST),
          oldEntry.fixedNullsDefault());

      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.FIRST);
      var withoutSortKey = session.newEntity(CLASS_NAME);
      assertNull("the injected row must carry an absent sort key",
          withoutSortKey.getProperty(FIELD));

      List<Object> values;
      try (var rs = session.query(sql)) {
        values = rs.stream().map(r -> r.<Object>getProperty(FIELD)).toList();
      }

      assertEquals("the fresh execution follows the new placement", Arrays.asList(null, 0, 1),
          values);
      var newEntry = CacheTestSupport.onlyEntry(cache);
      assertTrue("the stale entry must be replaced, not served", oldEntry != newEntry);
      assertEquals(
          "the replacement entry freezes the current placement",
          new ResolvedOrderByNullsPlacement(
              OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST),
          newEntry.fixedNullsDefault());
      assertEquals("populate and stale-placement rejection are both misses", 2,
          cache.getMetrics().getMisses());
      assertEquals("the stale placement is never counted as a hit", 0,
          cache.getMetrics().getHits());
      session.rollback();
    } finally {
      storageConfig.setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, null);
    }
  }

  /**
   * A descending ordered entry must also become stale when its direction-specific placement changes.
   * The replacement query puts an injected null after both values and publishes a new frozen pair.
   */
  @Test
  public void flagOn_descendingPlacementChangeRejectsAndRepopulatesOrderedEntry() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    var storageConfig = session.getStorage().getContextConfiguration();
    var oldDescending =
        storageConfig.getValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC);
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, OrderByNullsPlacement.FIRST);
    try {
      seed(2);
      session.begin();
      var sql = "SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " DESC";

      assertEquals("the populating query returns the seeded rows", 2, countQuery(sql));
      var cache = tx().getQueryResultCache();
      assertNotNull("cache must exist with the flag on", cache);
      var oldEntry = CacheTestSupport.onlyEntry(cache);
      assertEquals("populate must freeze descending nulls first",
          OrderByNullsPlacement.FIRST, oldEntry.fixedNullsDefault().descending());

      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, OrderByNullsPlacement.LAST);
      var withoutSortKey = session.newEntity(CLASS_NAME);
      assertNull("the injected row must carry an absent sort key",
          withoutSortKey.getProperty(FIELD));

      List<Object> values;
      try (var rs = session.query(sql)) {
        values = rs.stream().map(r -> r.<Object>getProperty(FIELD)).toList();
      }

      assertEquals("the fresh descending execution puts null last",
          Arrays.asList(1, 0, null), values);
      var newEntry = CacheTestSupport.onlyEntry(cache);
      assertTrue("the stale descending entry must be replaced", oldEntry != newEntry);
      assertEquals("the replacement must freeze descending nulls last",
          OrderByNullsPlacement.LAST, newEntry.fixedNullsDefault().descending());
      assertEquals("populate and stale-placement rejection are both misses", 2,
          cache.getMetrics().getMisses());
      assertEquals("the stale placement is never counted as a hit", 0,
          cache.getMetrics().getHits());
      session.rollback();
    } finally {
      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, oldDescending);
    }
  }

  /**
   * A multi-alias MATCH keeps its ORDER BY on the statement, so its cache entry carries no ORDER BY
   * of its own. The placement gate must still reject the frozen tuple set after a placement change.
   *
   * <p>The pattern binds two aliases through a traversal edge, which makes the entry a tuple entry
   * that replays verbatim. The populating query freezes the tuples under nulls first. A
   * mid-transaction change to nulls last must make the next identical query execute again and return
   * the null sort key at the end. A served entry would return the frozen nulls-first order.
   */
  @Test
  public void flagOn_placementChangeRejectsOrderedMultiAliasMatchEntry() {
    GlobalConfiguration.QUERY_TX_RESULT_CACHE_ENABLED.setValue(true);
    var storageConfig = session.getStorage().getContextConfiguration();
    var oldAscending =
        storageConfig.getValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC);
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.FIRST);
    try {
      seedMatchGraph();
      session.begin();
      var sql =
          "MATCH {class:" + MATCH_NODE + ", as:a}.out('" + MATCH_EDGE + "')"
              + "{class:" + MATCH_NODE + ", as:b} RETURN a.name AS an, b.n AS bn ORDER BY bn ASC";

      assertEquals(
          "the populating query orders the null tuple first",
          Arrays.asList(null, 20),
          matchSortKeys(sql));

      var cache = tx().getQueryResultCache();
      assertNotNull("cache must exist with the flag on", cache);
      var oldEntry = CacheTestSupport.onlyEntry(cache);
      assertEquals(
          "a two-alias pattern caches as a tuple entry",
          CacheableShape.MATCH_TUPLE_MULTI,
          oldEntry.getShape());
      assertEquals(
          "populate must freeze the placement even though the entry carries no ORDER BY",
          new ResolvedOrderByNullsPlacement(
              OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST),
          oldEntry.fixedNullsDefault());

      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);

      assertEquals(
          "the fresh execution follows the new placement",
          Arrays.asList(20, null),
          matchSortKeys(sql));
      var newEntry = CacheTestSupport.onlyEntry(cache);
      assertTrue("the stale tuple entry must be replaced, not served", oldEntry != newEntry);
      assertEquals(
          "populate and stale-placement rejection are both misses",
          2,
          cache.getMetrics().getMisses());
      assertEquals(
          "the stale placement is never counted as a hit", 0, cache.getMetrics().getHits());
      session.rollback();
    } finally {
      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, oldAscending);
    }
  }

  /** Runs the multi-alias MATCH and returns its sort-key column in row order. */
  private List<Object> matchSortKeys(String sql) {
    try (var rs = session.query(sql)) {
      return rs.stream().map(r -> r.<Object>getProperty("bn")).toList();
    }
  }

  /**
   * Creates a two-vertex-deep graph for the multi-alias MATCH test. One target vertex carries the
   * sort property and the other leaves it absent, so the pattern yields one null sort key.
   */
  private void seedMatchGraph() {
    session.execute("CREATE CLASS " + MATCH_NODE + " EXTENDS V").close();
    session.execute("CREATE CLASS " + MATCH_EDGE + " EXTENDS E").close();
    session.execute("CREATE PROPERTY " + MATCH_NODE + ".name STRING").close();
    session.execute("CREATE PROPERTY " + MATCH_NODE + ".n INTEGER").close();

    session.begin();
    session.execute("CREATE VERTEX " + MATCH_NODE + " SET name = 'root'").close();
    session.execute("CREATE VERTEX " + MATCH_NODE + " SET name = 'valued', n = 20").close();
    session.execute("CREATE VERTEX " + MATCH_NODE + " SET name = 'absent'").close();
    session.execute(
        "CREATE EDGE " + MATCH_EDGE
            + " FROM (SELECT FROM " + MATCH_NODE + " WHERE name = 'root')"
            + " TO (SELECT FROM " + MATCH_NODE + " WHERE name = 'valued')")
        .close();
    session.execute(
        "CREATE EDGE " + MATCH_EDGE
            + " FROM (SELECT FROM " + MATCH_NODE + " WHERE name = 'root')"
            + " TO (SELECT FROM " + MATCH_NODE + " WHERE name = 'absent')")
        .close();
    session.commit();
  }

  /** Reflectively invokes the private {@code serveThroughCache(SQLStatement, Object)} gate. */
  private static ResultSet invokeServeThroughCache(
      DatabaseSessionEmbedded session, SQLStatement statement, Object args) throws Exception {
    Method m =
        DatabaseSessionEmbedded.class.getDeclaredMethod(
            "serveThroughCache", SQLStatement.class, Object.class);
    m.setAccessible(true);
    return (ResultSet) m.invoke(session, statement, args);
  }

  /**
   * A SELECT statement whose execution returns a caller-supplied non-{@code LocalResultSet} result.
   * Empty AST keeps it RECORD-classified and deterministic so it passes the cache gate, while the
   * overridden {@code execute} drives the populate non-LocalResultSet branch under test.
   */
  private static final class NonLocalResultSelectStatement extends SQLSelectStatement {

    private final ResultSet result;

    private NonLocalResultSelectStatement(ResultSet result) {
      super(-1);
      this.result = result;
    }

    @Override
    public boolean isIdempotent() {
      return true;
    }

    @Override
    public ResultSet execute(
        DatabaseSessionEmbedded session,
        Object[] args,
        CommandContext parentContext,
        boolean usePlanCache) {
      return result;
    }
  }
}
