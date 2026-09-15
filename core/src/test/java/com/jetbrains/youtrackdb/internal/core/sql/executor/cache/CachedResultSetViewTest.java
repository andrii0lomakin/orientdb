package com.jetbrains.youtrackdb.internal.core.sql.executor.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Entity;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.record.RecordAbstract;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YouTrackDBSql;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies {@link CachedResultSetView} — the consumer-facing {@code ResultSet} that reconstructs the
 * rows a fresh uncached execution would return by sorted-merging a {@link CachedEntry}'s frozen
 * output with a {@link TxDeltaCursor}, and by pinning the entry against eviction while it iterates.
 * The view is unit-tested directly against synthetic entries and cursors (the session wiring
 * lands in a later step), so each test stages a known {@code (cache rows, skipSet, injectList)} shape
 * and asserts the merged emission order, the pin refcount, and the idempotent close.
 *
 * <p>Records are created in a live in-memory database so RIDs and ORDER BY comparisons are genuine, but
 * no query runs: the "cached" rows are wrapped {@link ResultInternal} instances seeded directly into
 * the entry, and the "stream tail" is a synthetic {@link ExecutionStream} the test controls.
 */
public class CachedResultSetViewTest {

  private static final String CLASS_NAME = "ViewRec";
  private static final String FIELD = "n";

  private YouTrackDBImpl youTrackDB;
  private DatabaseSessionEmbedded db;

  @Before
  public void before() {
    youTrackDB = DbTestBase.createYTDBManagerAndDb(getClass().getSimpleName(), DatabaseType.MEMORY,
        getClass());
    db = youTrackDB.open(getClass().getSimpleName(), "admin", DbTestBase.ADMIN_PASSWORD);
    var cls = db.createClass(CLASS_NAME);
    cls.createProperty(FIELD, PropertyType.INTEGER);
    db.begin();
  }

  @After
  public void after() {
    if (db.getTransactionInternal().isActive()) {
      db.rollback();
    }
    db.close();
    youTrackDB.drop(getClass().getSimpleName());
    youTrackDB.close();
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /** Creates a record of CLASS_NAME with FIELD=value and returns it as an Entity. */
  private Entity newRec(int value) {
    var e = db.newEntity(CLASS_NAME);
    e.setProperty(FIELD, value);
    return e;
  }

  private static RID ridOf(Entity e) {
    return ((RecordAbstract) e).getIdentity();
  }

  private Result resultOf(Entity e) {
    return new ResultInternal(db, e);
  }

  private CommandContext ctx() {
    return new BasicCommandContext(db);
  }

  /**
   * The live transaction backing {@code db}. The view enters and exits the transaction's cache-code
   * re-entrancy guard around each {@code computeNext()} (not for its whole iteration lifetime), so
   * iterating a view here leaves the depth balanced after every row; these unit tests run on the owning
   * thread, so the per-row enter/exit is balanced and does not perturb the pin-count assertions below.
   */
  private FrontendTransactionImpl tx() {
    return (FrontendTransactionImpl) db.getTransactionInternal();
  }

  private static SQLOrderBy parseOrderBy(String selectSql) {
    try {
      var parser = new YouTrackDBSql(new ByteArrayInputStream(selectSql.getBytes()));
      return ((SQLSelectStatement) parser.parse()).getOrderBy();
    } catch (Exception e) {
      throw new AssertionError("Failed to parse: " + selectSql, e);
    }
  }

  /**
   * Builds a RECORD entry whose frozen result is exactly {@code cachedRows} (seeded into
   * {@code results} and {@code cachedRids}), with the entry already exhausted (no live stream). Used by
   * the cases that exercise the merge without a stream tail.
   */
  private CachedEntry recordEntry(SQLOrderBy orderBy, List<Entity> cachedRows) {
    var entry = new CachedEntry(
        CacheableShape.RECORD, Set.of(CLASS_NAME), null, orderBy, null, null, null, 0L);
    for (var e : cachedRows) {
      entry.getResults().add(resultOf(e));
      entry.getCachedRids().add(ridOf(e));
    }
    entry.setExhausted(true);
    return entry;
  }

  /**
   * Builds a RECORD entry with NO pre-cached rows and a live synthetic stream over {@code streamRows},
   * so the view must lazy-pull every row from the stream. Mirrors a cache-miss whose populate has not
   * yet pulled anything.
   */
  private CachedEntry streamEntry(SQLOrderBy orderBy, List<Entity> streamRows) {
    var rows = new ArrayList<Result>();
    for (var e : streamRows) {
      rows.add(resultOf(e));
    }
    var entry = new CachedEntry(
        CacheableShape.RECORD, Set.of(CLASS_NAME), null, orderBy,
        new ListExecutionStream(rows), null, ctx(), 0L);
    return entry;
  }

  private TxDeltaCursor cursor(Set<RID> skipSet, List<Result> injectList) {
    return new TxDeltaCursor(skipSet, injectList);
  }

  private static List<Integer> drainValues(CachedResultSetView view) {
    var out = new ArrayList<Integer>();
    while (view.hasNext()) {
      out.add(view.next().getProperty(FIELD));
    }
    return out;
  }

  /** A minimal {@link ExecutionStream} that replays a fixed list and counts its closes. */
  private static final class ListExecutionStream implements ExecutionStream {

    private final List<Result> rows;
    private int pos;
    int closeCount;

    ListExecutionStream(List<Result> rows) {
      this.rows = rows;
    }

    @Override
    public boolean hasNext(CommandContext ctx) {
      return pos < rows.size();
    }

    @Override
    public Result next(CommandContext ctx) {
      return rows.get(pos++);
    }

    @Override
    public void close(CommandContext ctx) {
      closeCount++;
    }
  }

  // ===========================================================================
  // RECORD sorted-merge
  // ===========================================================================

  /**
   * A post-populate CREATE that matches is delivered as an inject row with no skip; the merged view
   * must emit it sorted into place among the cached rows. Cached {10, 30}, inject {20} under ORDER BY
   * ASC must come back {10, 20, 30}.
   */
  @Test
  public void createInjectIsSortedIntoCachedRows() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var cached = List.of(newRec(10), newRec(30));
    var entry = recordEntry(orderBy, cached);
    var inject = List.<Result>of(resultOf(newRec(20)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), inject), db, tx(), null, ctx());

    assertEquals(List.of(10, 20, 30), drainValues(view));
  }

  /**
   * A post-populate DELETE skips its cached row: the RID is in the skip-set and there is no inject, so
   * the view must drop it. Cached {10, 20, 30} with 20 skipped must come back {10, 30}.
   */
  @Test
  public void deleteSkipDropsCachedRow() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var ten = newRec(10);
    var twenty = newRec(20);
    var thirty = newRec(30);
    var entry = recordEntry(orderBy, List.of(ten, twenty, thirty));
    var view = new CachedResultSetView(
        entry, cursor(Set.of(ridOf(twenty)), List.of()), db, tx(), null, ctx());

    assertEquals(List.of(10, 30), drainValues(view));
  }

  /**
   * A post-populate UPDATE that moves a row's ORDER BY key re-positions it: the cached copy is skipped
   * and the post-mutation copy is injected at its new sort position. Cached {10, 20, 30}; row 10 is
   * updated to 25 (skip the old RID, inject a row with value 25). The view must come back {20, 25, 30}.
   */
  @Test
  public void updateRepositionsRowViaSkipPlusInject() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var moving = newRec(10);
    var entry = recordEntry(orderBy, List.of(moving, newRec(20), newRec(30)));

    // The post-mutation copy carries the new value; it re-uses the same RID, but the skip-set drops the
    // stale cached copy so only the injected copy survives.
    moving.setProperty(FIELD, 25);
    var inject = List.<Result>of(resultOf(moving));
    var view = new CachedResultSetView(
        entry, cursor(Set.of(ridOf(moving)), inject), db, tx(), null, ctx());

    assertEquals(List.of(20, 25, 30), drainValues(view));
  }

  /**
   * With no ORDER BY the inject list keeps mutation-iteration order and the merge drains injects ahead
   * of equally-unordered cached rows (the {@code orderBy == null} branch treats every inject as
   * sorting at-or-before the cache head). Cached {1, 2}, inject {9} must yield all three rows with no
   * loss or duplication; the unordered contract only fixes the set, not a total order.
   */
  @Test
  public void noOrderByEmitsAllRowsWithoutLoss() {
    var entry = recordEntry(null, List.of(newRec(1), newRec(2)));
    var inject = List.<Result>of(resultOf(newRec(9)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), inject), db, tx(), null, ctx());

    var values = drainValues(view);
    assertEquals("All cached and injected rows must be emitted exactly once", 3, values.size());
    assertTrue(values.containsAll(List.of(1, 2, 9)));
  }

  /**
   * When a cached row and an inject row carry the EQUAL ORDER BY key, the both-heads merge arm hits
   * the {@code cmp == 0} tie branch ({@code orderBy.compare(...) == 0}), which favours the inject side
   * ({@code cmp <= 0} drains the inject first) and must then still emit the cached row — both exactly
   * once, neither dropped nor duplicated. Cached {10, 20}, inject {10} under ORDER BY ASC must come
   * back {10, 10, 20}. Every other RECORD-merge test uses distinct keys, so this is the only case that
   * drives the tie branch: a regression flipping the comparison to {@code cmp < 0} would drop one of
   * the two equal-key rows and pass every other test.
   */
  @Test
  public void injectWithEqualOrderByKeyEmitsBothExactlyOnce() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = recordEntry(orderBy, List.of(newRec(10), newRec(20)));
    // A distinct record carrying the same ORDER BY key as the cached 10, so the comparator ties.
    var inject = List.<Result>of(resultOf(newRec(10)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), inject), db, tx(), null, ctx());

    assertEquals("a tie on the ORDER BY key must emit both rows exactly once",
        List.of(10, 10, 20), drainValues(view));
  }

  // ===========================================================================
  // Stream-pull-with-skip-set unification
  // ===========================================================================

  /**
   * When the entry has no pre-cached rows the view must lazy-pull the full result from the stream,
   * append each pulled row to the shared {@code entry.results} / {@code cachedRids}, and emit them in
   * order. After draining, the entry is exhausted and its rows are visible to a later view.
   */
  @Test
  public void streamPullMaterializesAndAppendsRows() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = streamEntry(orderBy, List.of(newRec(1), newRec(2), newRec(3)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());

    assertEquals(List.of(1, 2, 3), drainValues(view));
    assertTrue("Stream drain must flip the entry to exhausted", entry.isExhausted());
    assertEquals("Every pulled row must be appended to the shared cache", 3,
        entry.getResults().size());
    assertEquals(3, entry.getCachedRids().size());
  }

  /**
   * Partial-iteration resume across two views on one entry. A first view pulls one of three rows from
   * the stream and is then abandoned (never closed, entry left non-exhausted with a single row
   * materialized and the stream paused at index 1). A second view over the SAME entry, the shape the
   * cache hit path builds when the same query runs twice, must replay the one cached row from
   * {@code entry.results} and then resume the paused stream to pull the remaining two, yielding the full
   * {1, 2, 3} without dropping or duplicating the boundary row. Regression guard for the
   * resume-from-mid-stream index that no single-view test reaches.
   */
  @Test
  public void secondViewResumesPartiallyPulledStreamEntry() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = streamEntry(orderBy, List.of(newRec(1), newRec(2), newRec(3)));

    // First view pulls exactly one row, then is abandoned without close() or exhaustion.
    var first = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    assertTrue(first.hasNext());
    assertEquals(Integer.valueOf(1), first.next().getProperty(FIELD));
    assertFalse("Entry must still be mid-stream after a single pull", entry.isExhausted());
    assertEquals("Only the first row is materialized so far", 1, entry.getResults().size());

    // Second view over the same entry replays the cached prefix then resumes the paused stream.
    var second = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    assertEquals("Second view replays row 0 then resumes the stream tail",
        List.of(1, 2, 3), drainValues(second));
    assertTrue("Resuming to the end flips the shared entry to exhausted", entry.isExhausted());
    assertEquals("All three rows are materialized in the shared cache", 3,
        entry.getResults().size());
  }

  /**
   * A RID in the skip-set must be suppressed even when it surfaces from the stream pull (not just from
   * the pre-cached prefix), closing the lazy-pull gap. The stream yields {1, 2, 3}; row 2 is skipped
   * (a post-populate delete of a record beyond the cached prefix), so the view emits {1, 3} while still
   * appending all three to the shared cache for later views.
   */
  @Test
  public void streamPulledRowInSkipSetIsSuppressed() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var one = newRec(1);
    var two = newRec(2);
    var three = newRec(3);
    var entry = streamEntry(orderBy, List.of(one, two, three));
    var view = new CachedResultSetView(
        entry, cursor(Set.of(ridOf(two)), List.of()), db, tx(), null, ctx());

    assertEquals("Skipped stream row must not be emitted", List.of(1, 3), drainValues(view));
    assertEquals("All pulled rows are still appended to the shared cache", 3,
        entry.getResults().size());
  }

  /**
   * The sorted-merge must materialize the next storage row before consulting the delta head, so a
   * delta inject is never emitted ahead of a not-yet-pulled storage row that sorts earlier. Stream
   * yields {10, 30}, inject {20}: even though the inject is available immediately, the view must pull
   * 10 first and emit {10, 20, 30}, not {20, 10, 30}.
   */
  @Test
  public void injectNeverPrecedesEarlierUnpulledStreamRow() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = streamEntry(orderBy, List.of(newRec(10), newRec(30)));
    var inject = List.<Result>of(resultOf(newRec(20)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), inject), db, tx(), null, ctx());

    assertEquals(List.of(10, 20, 30), drainValues(view));
  }

  // ===========================================================================
  // Single-view overflow relay (buffer release)
  // ===========================================================================

  /** Installs the per-entry record cap and a counting overflow callback, mirroring what the cache's
   * {@code QueryResultCache.put} wires up. Returns the fire counter so a test can assert the cap was
   * actually crossed. */
  private static AtomicInteger installCap(CachedEntry entry, int cap) {
    var fired = new AtomicInteger();
    entry.setOverflowGuard(cap, fired::incrementAndGet);
    return fired;
  }

  /**
   * Single-view RECORD overflow switches the entry to relay: it releases the buffered cap-worth and
   * stops appending, while the sole driving view still emits every row in correct sorted-merge order
   * with the delta applied. Stream {10,20,30,40,50} under cap 2 overflows on the third pull (value 30);
   * the delta injects 35 (a post-populate create) and skips 40 (a post-populate delete). The view must
   * emit {10,20,30,35,50} — proving the relay cache head merges correctly against the inject and that a
   * row skipped while pulled in relay is still suppressed — and afterwards {@code entry.results} must be
   * empty (the cap-worth released and never re-grown), with the entry flagged relay.
   *
   * <p>This also guards the comparison-memo fix. In relay the cache cursor {@code position} is frozen,
   * so two successive relay heads (30, then 50) are each compared against the same surviving inject head
   * (35) at the same position. A position-keyed memo would hand back the stale projection of head 30
   * when comparing head 50, mis-ordering the output to {10,20,30,50,35}; the by-reference memo keeps it
   * correct.
   */
  @Test
  public void singleViewOverflowReleasesBufferAndMergesTail() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var forty = newRec(40);
    var entry = streamEntry(
        orderBy, List.of(newRec(10), newRec(20), newRec(30), forty, newRec(50)));
    var fired = installCap(entry, 2);
    var inject = List.<Result>of(resultOf(newRec(35)));
    var view = new CachedResultSetView(
        entry, cursor(Set.of(ridOf(forty)), inject), db, tx(), null, ctx());

    assertEquals(List.of(10, 20, 30, 35, 50), drainValues(view));
    assertEquals("the cap must have been crossed exactly once", 1, fired.get());
    assertTrue("single-view overflow must switch the entry to relay", entry.isRelayMode());
    assertTrue("relay must release the buffer and stop appending", entry.getResults().isEmpty());
    assertTrue("relay must release the cached-RID set too", entry.getCachedRids().isEmpty());
  }

  /**
   * Single-view K0_NONE overflow releases the buffer the same way, on the delta-free direct-replay path.
   * A K0_NONE view returns each pulled row directly (no merge), so once relay clears {@code
   * entry.results} the positional prefix branch is simply dead and every row still streams through.
   * Stream {1,2,3,4,5} under cap 2 overflows on the third pull; the view must emit all five in order and
   * leave {@code entry.results} empty.
   */
  @Test
  public void singleViewOverflowReleasesBufferOnK0NonePath() {
    var rows = new ArrayList<Result>();
    for (var v : List.of(1, 2, 3, 4, 5)) {
      rows.add(resultOf(newRec(v)));
    }
    var entry = new CachedEntry(
        CacheableShape.K0_NONE, Set.of(CLASS_NAME), null, null,
        new ListExecutionStream(rows), null, ctx(), 0L);
    var fired = installCap(entry, 2);
    // K0_NONE carries no delta cursor (null delta), so the view takes the direct-replay path.
    var view = new CachedResultSetView(entry, null, db, tx(), null, ctx());

    assertEquals(List.of(1, 2, 3, 4, 5), drainValues(view));
    assertEquals("the cap must have been crossed exactly once", 1, fired.get());
    assertTrue("single-view overflow must switch the entry to relay", entry.isRelayMode());
    assertTrue("relay must release the buffer on the K0_NONE path too",
        entry.getResults().isEmpty());
  }

  /**
   * A per-entry overflow with more than one live view must NOT switch to relay: a lagging sibling still
   * replays {@code entry.results} by index, so buffering stays on and the buffer is kept complete. Two
   * views pin the entry (liveViewCount == 2); driving the first to exhaustion overflows it (cap 2, five
   * rows), but the entry stays non-relay with all five rows buffered, and the second view then replays
   * the full result from that buffer.
   */
  @Test
  public void multiViewOverflowKeepsBufferingForSibling() {
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = streamEntry(
        orderBy, List.of(newRec(10), newRec(20), newRec(30), newRec(40), newRec(50)));
    var fired = installCap(entry, 2);

    // Two views pin the entry before either pulls, so the overflow sees liveViewCount == 2.
    var driver = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    var sibling =
        new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());

    assertEquals("the driving view emits the full result", List.of(10, 20, 30, 40, 50),
        drainValues(driver));
    assertEquals("the cap must have been crossed exactly once", 1, fired.get());
    assertFalse("a multi-view overflow must not switch to relay", entry.isRelayMode());
    assertEquals("the buffer must stay complete for the lagging sibling", 5,
        entry.getResults().size());
    assertEquals("the lagging sibling replays the full buffered result",
        List.of(10, 20, 30, 40, 50), drainValues(sibling));
  }

  // ===========================================================================
  // K0_NONE direct replay
  // ===========================================================================

  /**
   * A K0_NONE view carries a null delta cursor and must replay the cached rows verbatim with no merge,
   * lazy-pulling the stream tail. The version gate already guaranteed no post-populate mutation, so the
   * cached output equals a fresh deterministic re-run.
   */
  @Test
  public void k0NoneReplaysStreamDirectlyWithoutDelta() {
    var entry = new CachedEntry(
        CacheableShape.K0_NONE, Set.of(CLASS_NAME), null, null,
        new ListExecutionStream(List.of(resultOf(newRec(7)), resultOf(newRec(8)))),
        null, ctx(), 0L);
    var view = new CachedResultSetView(entry, null, db, tx(), null, ctx());

    assertEquals(List.of(7, 8), drainValues(view));
    assertTrue(entry.isExhausted());
  }

  /** A K0_NONE view over pre-cached rows (no live stream) replays them in order. */
  @Test
  public void k0NoneReplaysPreCachedRows() {
    var entry = new CachedEntry(
        CacheableShape.K0_NONE, Set.of(CLASS_NAME), null, null, null, null, null, 0L);
    entry.getResults().add(resultOf(newRec(1)));
    entry.getResults().add(resultOf(newRec(2)));
    entry.setExhausted(true);
    var view = new CachedResultSetView(entry, null, db, tx(), null, ctx());

    assertEquals(List.of(1, 2), drainValues(view));
  }

  // ===========================================================================
  // View pinning and idempotent close
  // ===========================================================================

  /** Constructing a view pins its entry (liveViewCount becomes 1) so LRU eviction skips it. */
  @Test
  public void constructionPinsEntry() {
    var entry = recordEntry(null, List.of(newRec(1)));
    assertEquals(0, entry.getLiveViewCount());
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    assertEquals("View construction must pin the entry", 1, entry.getLiveViewCount());
    view.close();
  }

  /** Explicit close releases the pin exactly once. */
  @Test
  public void closeReleasesPin() {
    var entry = recordEntry(null, List.of(newRec(1)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    view.close();
    assertEquals("Close must release the pin", 0, entry.getLiveViewCount());
    assertTrue(view.isClosed());
  }

  /** Natural exhaustion releases the pin without an explicit close. */
  @Test
  public void exhaustionReleasesPin() {
    var entry = recordEntry(null, List.of(newRec(1)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    drainValues(view);
    assertFalse(view.hasNext());
    assertEquals("Draining to exhaustion must release the pin", 0, entry.getLiveViewCount());
  }

  /**
   * Close after natural exhaustion must not double-release the pin: exhaustion already decremented the
   * refcount, and a following close must be a no-op for the pin (releases exactly once).
   */
  @Test
  public void exhaustionThenCloseReleasesPinOnce() {
    var entry = recordEntry(null, List.of(newRec(1)));
    // Pin twice so a buggy double-release would visibly drop the count below the second pin.
    entry.incrementLiveViewCount();
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    assertEquals(2, entry.getLiveViewCount());
    drainValues(view);
    assertEquals("Exhaustion releases the view's own pin once", 1, entry.getLiveViewCount());
    view.close();
    assertEquals("Close after exhaustion must not release a second time", 1,
        entry.getLiveViewCount());
  }

  /** A second close is a no-op: the view stays closed and the pin is not released twice. */
  @Test
  public void doubleCloseIsIdempotent() {
    var entry = recordEntry(null, List.of(newRec(1)));
    entry.incrementLiveViewCount(); // baseline pin so a double-release would underflow below 1
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    assertEquals(2, entry.getLiveViewCount());
    view.close();
    view.close();
    assertEquals("Double close must release the pin exactly once", 1, entry.getLiveViewCount());
    assertTrue(view.isClosed());
  }

  /**
   * The row-loss regression at the view boundary: when the cache removes an entry while a view still
   * iterates it, it defers the stream close ({@link CachedEntry#markCloseWhenUnpinned}, the exact
   * action {@code QueryResultCache.closeOrDefer} takes on a pinned entry for invalidate / TRUNCATE /
   * overflow) instead of closing the stream out from under the view. Before the deferral, the removal
   * closed the stream and the next {@code pullOneFromStream} saw a null stream and silently dropped the
   * tail. Here the view pulls the first of three streamed rows, the entry is detached-while-pinned, and
   * the view must still drain the full tail; the stream closes exactly once, on natural exhaustion.
   */
  @Test
  public void deferredCloseWhilePinnedDoesNotTruncateView() {
    var stream = new ListExecutionStream(
        List.of(resultOf(newRec(10)), resultOf(newRec(20)), resultOf(newRec(30))));
    var entry = new CachedEntry(
        CacheableShape.K0_NONE, Set.of(CLASS_NAME), null, null, stream, null, ctx(), 0L);
    var view = new CachedResultSetView(entry, null, db, tx(), null, ctx());

    // Pull the first row, leaving 20 and 30 in the still-open stream.
    assertTrue(view.hasNext());
    Integer first = view.next().getProperty(FIELD);
    assertEquals(Integer.valueOf(10), first);

    // The cache removes this entry while the view pins it; a pinned removal defers the close.
    entry.markCloseWhenUnpinned();
    assertEquals(
        "A deferred removal must not close the stream under a live view", 0, stream.closeCount);

    // The view must still yield the full tail — no lost rows.
    var rest = new ArrayList<Integer>();
    while (view.hasNext()) {
      rest.add(view.next().getProperty(FIELD));
    }
    assertEquals(List.of(20, 30), rest);
    assertTrue(entry.isExhausted());
    assertEquals("The stream closes exactly once, on natural exhaustion", 1, stream.closeCount);
  }

  /** A closed view reports no more rows and throws on next(), per the ResultSet contract. */
  @Test
  public void closedViewHasNoNext() {
    var entry = recordEntry(null, List.of(newRec(1)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), List.of()), db, tx(), null, ctx());
    view.close();
    assertFalse(view.hasNext());
    assertThrows(NoSuchElementException.class, view::next);
  }

  /**
   * Two views built on the same entry pin it independently: both increment, and the entry stays pinned
   * until the last view releases. This is the multi-view pinning the LRU guard relies on.
   */
  @Test
  public void twoViewsPinIndependently() {
    var entry = recordEntry(null, List.of(newRec(1)));
    var skip = Collections.<RID>unmodifiableSet(new HashSet<>());
    var a = new CachedResultSetView(entry, cursor(skip, List.of()), db, tx(), null, ctx());
    var b = new CachedResultSetView(entry, cursor(skip, List.of()), db, tx(), null, ctx());
    assertEquals(2, entry.getLiveViewCount());
    a.close();
    assertEquals("Entry stays pinned while the second view iterates", 1, entry.getLiveViewCount());
    b.close();
    assertEquals(0, entry.getLiveViewCount());
  }

  // ===========================================================================
  // Null placement
  // ===========================================================================

  /**
   * A view over an entry with no ORDER BY never compares rows, so it must never resolve a null
   * placement. Resolving would take the storage configuration lock on a path that was free before.
   */
  @Test
  public void viewWithoutOrderByNeverResolvesNullPlacement() {
    var entry = recordEntry(null, List.of(newRec(1), newRec(2)));
    var inject = List.<Result>of(resultOf(newRec(9)));
    var view = new CachedResultSetView(entry, cursor(Set.of(), inject), db, tx(), null, ctx());

    assertEquals(3, drainValues(view).size());
    assertNull("a view with no sort key must not read the configuration",
        entry.fixedNullsDefault());
  }

  /**
   * The merge ranks rows by the entry's placement, which the delta build already used to sort the
   * inject list. The storage setting puts nulls last. An injected row with no sort key must be
   * emitted after both cached rows. The entry must then hold exactly one resolved placement.
   */
  @Test
  public void mergeUsesThePlacementOwnedByTheEntry() {
    var storageConfig = db.getStorage().getContextConfiguration();
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
    try {
      var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
      var entry = recordEntry(orderBy, List.of(newRec(1), newRec(2)));
      var inject = List.<Result>of(resultOf(db.newEntity(CLASS_NAME)));
      var view = new CachedResultSetView(entry, cursor(Set.of(), inject), db, tx(), null, ctx());

      assertEquals(Arrays.asList(1, 2, null), drainValues(view));
      assertEquals(new ResolvedOrderByNullsPlacement(
          OrderByNullsPlacement.LAST, OrderByNullsPlacement.LAST), entry.fixedNullsDefault());
    } finally {
      storageConfig.setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, null);
    }
  }
}
