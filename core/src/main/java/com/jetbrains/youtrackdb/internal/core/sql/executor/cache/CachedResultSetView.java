package com.jetbrains.youtrackdb.internal.core.sql.executor.cache;

import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.query.ResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The consumer-facing {@link ResultSet} a cached {@code query()} returns. It reconstructs the rows a
 * fresh uncached execution would emit by merging a {@link CachedEntry}'s frozen output
 * with the in-transaction mutations the {@link DeltaBuilder} captured into a {@link TxDeltaCursor},
 * without ever re-running storage.
 *
 * <p><b>RECORD path (sorted-merge).</b> {@link #next()} merges two cursors: the positional
 * <i>cache cursor</i> (a per-view index into the entry's lazily-grown {@link CachedEntry#getResults()})
 * and the per-view <i>delta cursor</i> ({@link TxDeltaCursor}). Each step drops any cache row whose RID
 * the delta marks for skip (DELETED or re-positioned), materializes the next storage row from the
 * shared paused stream when the cached prefix is exhausted but the stream is not (so a delta inject can
 * never be emitted ahead of a not-yet-pulled storage row that sorts earlier), then emits whichever of
 * the two heads sorts smaller per the entry's ORDER BY. Ties favour the inject side, which keeps a
 * mutated row at-or-before an equally-ranked cached row; either choice is correct because a tie can
 * only occur between distinct RIDs (a skip on the cache head was already consumed).
 *
 * <p><b>K0_NONE path (direct replay).</b> A K0_NONE entry carries no delta cursor: the lookup-time
 * mutation-version gate already guaranteed no post-populate mutation, so there is nothing to merge. The
 * view replays {@link CachedEntry#getResults()} directly, lazy-pulling the stream tail with no skip-set
 * filtering, exactly as a non-cached re-run of that deterministic plan would.
 *
 * <p><b>Aggregate path (single scalar row).</b> An {@code AGGREGATE_*} entry carries no cache cursor
 * and no stream to pull: the side-tap already drove the populating plan to exhaustion at cache-put, so
 * the per-RID material lives entirely in the entry's {@link AggregateState}. The view carries the
 * replayed delta copy ({@link DeltaBuilder#buildForAggregate}) and emits at most one row, {@link
 * AggregateState#toResult}, then drains. {@code hasNext()} is true at most once: a value aggregate
 * (SUM/AVG/MIN/MAX) over an empty contributor set emits no row, matching fresh execution.
 *
 * <p><b>Stream-pull unification.</b> Both paths share one {@link #pullOneFromStream} helper. On the
 * RECORD path it drops any pulled row whose RID is in the delta skip-set (closing the lazy-pull gap:
 * a mutated RID living beyond the cached prefix must not surface from storage with stale state); on the
 * K0_NONE path the skip-set is empty so nothing is dropped. Pulled rows are appended to the shared
 * {@code entry.results} / {@code cachedRids} so later views replay the full ordered result — except
 * after a single-view per-entry overflow, when the entry switches to relay ({@link
 * CachedEntry#isRelayMode()}): buffering stops and the sole view takes its RECORD cache head from
 * {@link #relayCacheHead} (the K0_NONE path returns the pulled row directly).
 *
 * <p><b>View pinning.</b> The constructor increments {@link CachedEntry#getLiveViewCount()} and
 * {@link #close()} (or natural exhaustion) decrements it exactly once, pinning the entry against LRU
 * eviction while this view iterates so a mid-iteration view never loses rows.
 *
 * <p><b>Re-entrancy guard coverage.</b> The session opens the transaction's cache-code guard
 * ({@code FrontendTransactionImpl.enterCacheCode()}) around the synchronous lookup. The view then
 * re-enters that guard for each row-production window (inside {@link #hasNext()} around {@link
 * #computeNext}): lazy stream pulls, delta merge, and MATCH RETURN projection can evaluate a UDF that
 * issues a nested {@code query()}, which must bypass the cache (depth &gt; 0) while that row is being
 * produced. The guard is released immediately after the row is computed, so a user-issued {@code
 * query()} between two {@link #next()} calls still uses the cache, and an abandoned view does not hold
 * the guard indefinitely.
 *
 * <p><b>Idempotent close.</b> {@link #close()} is a no-op after the first call and releases the pin
 * (the entry's live-view count) exactly once. It does not touch the cache-code guard, which is held
 * only per row inside {@link #hasNext()} (see above), never across the view's lifetime. The view never closes the entry's shared stream itself: the
 * stream is owned by the {@link CachedEntry} (closed at tx-end cache clear) and possibly other views,
 * so closing it here would truncate them.
 *
 * <p>Single-transaction state observed only by the owning thread; no field is synchronised.
 */
public final class CachedResultSetView implements ResultSet {

  private final CachedEntry entry;

  /** Null for K0_NONE (direct replay) and aggregate; non-null for RECORD (sorted-merge). */
  @Nullable private final TxDeltaCursor delta;

  /**
   * Non-null for an {@code AGGREGATE_*} view: the replayed {@link AggregateState} copy whose {@link
   * AggregateState#toResult} is the view's single output row. Null for every other shape.
   */
  @Nullable private final AggregateState aggregateDelta;

  /** True once the aggregate view has emitted its single scalar row; then the view is drained. */
  private boolean aggregateEmitted;

  /**
   * For a {@code DISTINCT_VALUES} view: the {@code {alias: value}} rows to emit, one per distinct bucket
   * key, sorted by the entry's ORDER BY. Built once from the replayed {@link #aggregateDelta} on first
   * {@link #computeNextDistinctValue} and fixed thereafter. Null until then.
   */
  @Nullable private List<Result> distinctRows;

  /** Cursor into {@link #distinctRows} for the {@code DISTINCT_VALUES} view. */
  private int distinctIndex;

  @Nullable private DatabaseSessionEmbedded session;

  /**
   * The transaction whose cache-code guard this view re-enters for each row-production window inside
   * {@link #hasNext()} (entered and exited around {@link #computeNext}, never held while the view is
   * idle). The session, not the view, brackets the guard around the synchronous lookup and build.
   */
  private final FrontendTransactionImpl tx;

  @Nullable private final InternalExecutionPlan executionPlan;

  /** Context used to drive the entry's shared stream when lazy-pulling the tail. */
  private final CommandContext ctx;

  /** Per-view positional cursor into the entry's shared {@link CachedEntry#getResults()}. */
  private int position;

  /**
   * RECORD relay lookahead. When the entry switches to single-view relay after a per-entry overflow
   * ({@link CachedEntry#isRelayMode()}), {@link CachedEntry#getResults()} is released and the cache
   * side of the sorted-merge is this one held storage row instead of {@code results.get(position)}. It
   * holds the most-recently-pulled, not-yet-emitted row; it is nulled when that row is emitted or
   * skipped so the next merge step pulls the next one. Unused before any overflow and on the K0_NONE
   * path (which returns the pulled row directly).
   */
  @Nullable private Result relayCacheHead;

  /** One-element lookahead the merge fills in {@link #hasNext()} and drains in {@link #next()}. */
  @Nullable private Result lookahead;

  // --- Comparison-time projection memo for the cache head (RECORD merge only) ---
  // While the inject side keeps winning, the cache head does not advance, so the same cache row is
  // compared (and re-projected) against successive inject heads. Projecting it once per emission
  // decision instead of once per losing compare turns the merge's worst-case O(n*k) projector calls
  // back into O(n+k). The memo keys on the raw cache-head reference, so advancing the cache head (the
  // row was consumed or skipped) self-invalidates it, and it stays correct for both the positional
  // cursor and the single-view-relay relayCacheHead (where `position` is frozen but the held row
  // changes). The inject head is naturally single-use — it advances when consumed — so only the cache
  // head needs memoizing.
  @Nullable private Result compareMemoRaw;
  @Nullable private Result compareMemoProjection;

  private boolean closed;

  /**
   * Guards the single release of the pin (the entry's live-view count) so {@link #close()} and
   * exhaustion together decrement it at most once.
   */
  private boolean pinReleased;

  public CachedResultSetView(
      @Nonnull CachedEntry entry,
      @Nullable TxDeltaCursor delta,
      @Nullable DatabaseSessionEmbedded session,
      @Nonnull FrontendTransactionImpl tx,
      @Nullable InternalExecutionPlan executionPlan,
      @Nonnull CommandContext ctx) {
    this(entry, delta, null, session, tx, executionPlan, ctx);
  }

  /**
   * Aggregate-shape factory: the view carries a replayed {@link AggregateState} copy instead of a
   * {@link TxDeltaCursor} and emits its single {@link AggregateState#toResult} row. A static factory
   * (rather than an overloaded constructor) keeps the {@code TxDeltaCursor}-arg constructor the only
   * one of its arity, so a {@code null} delta on the record/K0 path stays unambiguous. The pin and the
   * cache-code-guard lifetime are identical to the record/K0 path.
   */
  @Nonnull
  public static CachedResultSetView forAggregateState(
      @Nonnull CachedEntry entry,
      @Nonnull AggregateState aggregateDelta,
      @Nullable DatabaseSessionEmbedded session,
      @Nonnull FrontendTransactionImpl tx,
      @Nullable InternalExecutionPlan executionPlan,
      @Nonnull CommandContext ctx) {
    return new CachedResultSetView(entry, null, aggregateDelta, session, tx, executionPlan, ctx);
  }

  private CachedResultSetView(
      @Nonnull CachedEntry entry,
      @Nullable TxDeltaCursor delta,
      @Nullable AggregateState aggregateDelta,
      @Nullable DatabaseSessionEmbedded session,
      @Nonnull FrontendTransactionImpl tx,
      @Nullable InternalExecutionPlan executionPlan,
      @Nonnull CommandContext ctx) {
    this.entry = entry;
    this.delta = delta;
    this.aggregateDelta = aggregateDelta;
    this.session = session;
    this.tx = tx;
    this.executionPlan = executionPlan;
    this.ctx = ctx;
    // Pin the entry for this view's whole lifetime so LRU eviction cannot close the shared stream out
    // from under an in-flight iteration; the pin is released on close / natural exhaustion. The
    // cache-code re-entrancy guard is NOT held for the view's lifetime: hasNext() re-enters it around
    // each computeNext() so it covers only the row-production windows (stream pull, delta merge,
    // RETURN projection), not the view's idle time between next() calls.
    entry.incrementLiveViewCount();
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    if (lookahead != null) {
      return true;
    }
    // Re-enter the tx cache-code guard only for the row-production window: a lazy stream pull (and the
    // delta merge / RETURN projection) can evaluate a UDF that issues a nested query(), which must
    // bypass the cache. Released in the finally so a query() between two next() calls — outside any
    // row production — still uses the cache, and an abandoned view holds no guard.
    tx.enterCacheCode();
    try {
      lookahead = computeNext();
    } finally {
      tx.exitCacheCode();
    }
    if (lookahead == null) {
      // Both cursors are drained: release the pin now so an iterated-to-exhaustion view stops
      // blocking eviction even before the consumer calls close(). The release is idempotent.
      releasePin();
      return false;
    }
    return true;
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    var r = lookahead;
    lookahead = null;
    return r;
  }

  /**
   * Produces the next merged row, or {@code null} when the view is drained. Aggregate emits its single
   * scalar row once; K0_NONE routes to a delta-free replay; RECORD runs the sorted-merge.
   */
  @Nullable private Result computeNext() {
    if (aggregateDelta != null) {
      // Both the scalar AGGREGATE_* views and the DISTINCT_VALUES view carry a replayed AggregateState;
      // the entry shape decides whether to emit the single scalar or one row per distinct bucket key.
      if (entry.getShape() == CacheableShape.DISTINCT_VALUES) {
        return computeNextDistinctValue();
      }
      return computeNextAggregate();
    }
    if (delta == null) {
      return computeNextK0None();
    }
    return computeNextRecord();
  }

  /**
   * {@code DISTINCT_VALUES} emit: one {@code {alias: value}} row per distinct bucket key, sorted by
   * the entry's ORDER BY, then {@code null}. The key list is snapshotted once from the replayed
   * {@link #aggregateDelta} (kind {@code AGGREGATE_COUNT_DISTINCT}), so it reflects the post-delta value
   * set fixed at view construction, matching a fresh {@code SELECT distinct(prop)} at this moment.
   */
  @Nullable private Result computeNextDistinctValue() {
    if (distinctRows == null) {
      var rows = new ArrayList<Result>();
      for (var value : aggregateDelta.distinctValuesInOrder()) {
        var row = new ResultInternal(session, 1);
        row.setProperty(aggregateDelta.getAlias(), value);
        rows.add(row);
      }
      // The shape is admitted only with a deterministic ORDER BY on the projected column, so sorting the
      // bucket-key rows by that comparator reproduces a fresh execution's order exactly (the bucket
      // insertion order is not enough: a fresh re-scan interleaves a tx-mutated record at a scan position
      // the replay cannot mirror).
      var orderBy = entry.getOrderBy();
      if (orderBy != null && rows.size() > 1) {
        var nullsDefault = entry.nullsDefault(ctx);
        rows.sort((a, b) -> orderBy.compare(a, b, ctx, nullsDefault));
      }
      distinctRows = rows;
    }
    if (distinctIndex >= distinctRows.size()) {
      return null;
    }
    return distinctRows.get(distinctIndex++);
  }

  /**
   * Aggregate single-row emit: returns the replayed {@link AggregateState#toResult} the first time and
   * {@code null} thereafter, so {@code hasNext()} is true at most once. There is no stream to pull and
   * no cache cursor; the scalar was fully computed at delta build. A SUM/AVG/MIN/MAX whose contributor
   * set drained to empty emits zero rows (see {@link AggregateState#emitsNoRow}), matching a fresh
   * execution, which returns no row for those kinds over an empty input; only COUNT emits a 0 row.
   */
  @Nullable private Result computeNextAggregate() {
    if (aggregateEmitted) {
      return null;
    }
    aggregateEmitted = true;
    if (aggregateDelta.emitsNoRow()) {
      return null;
    }
    return aggregateDelta.toResult(session);
  }

  /**
   * K0_NONE direct replay: emit cached rows in order, lazy-pulling the stream tail (no skip-set, no
   * delta) until both are drained. The lookup-time version gate guarantees no post-populate mutation,
   * so the cached output already equals a fresh deterministic re-run.
   */
  @Nullable private Result computeNextK0None() {
    var results = entry.getResults();
    if (position < results.size()) {
      return results.get(position++);
    }
    if (!entry.isExhausted()) {
      var pulled = pullOneFromStream();
      if (pulled != null) {
        // Appended at the tail; advance past it and return it.
        position++;
        return pulled;
      }
    }
    return null;
  }

  /**
   * RECORD sorted-merge, transcribing the design's {@code view.next()} algorithm: skip cache heads the
   * delta replaces, materialize the next storage row before consulting the delta head (load-bearing for
   * sort correctness), then emit the smaller of the two heads with ties favouring the inject side.
   *
   * <p>For a single-alias MATCH entry the rows are raw, RID-identifiable records and the entry
   * carries a {@code returnProjector}: skip decisions still key on the raw record's RID (so the merge
   * stays RID-addressable), but the ORDER BY comparison projects both heads first (the ORDER BY ranks
   * the projected RETURN tuple, e.g. {@code u.name}) and the emitted row is the projected tuple — the
   * projector is the last transform before the consumer. A null projector (plain RECORD SELECT) leaves
   * every row unprojected, the original behaviour.
   */
  @Nullable private Result computeNextRecord() {
    var results = entry.getResults();
    while (true) {
      // In single-view relay (post-overflow) the entry released results and the cache side is the one
      // held relay row; otherwise it is the positional cursor into results. Read the flag freshly each
      // iteration: it can flip on inside the pullOneFromStream below — that pull is the overflow.
      var relay = entry.isRelayMode();
      var cacheHead =
          relay ? relayCacheHead : (position < results.size() ? results.get(position) : null);

      // Suppress a cached row the delta marks for replacement or deletion; consume the head without
      // emitting and re-loop. The skip probe reads the raw row's RID, so it is correct even when the
      // row will later be projected to a (non-identifiable) RETURN tuple. (A relay head is already
      // skip-filtered by pullOneFromStream, so this only fires on the positional path.)
      if (cacheHead != null && isSkipped(cacheHead)) {
        advanceCacheHead(relay);
        continue;
      }

      // Materialize the next storage row BEFORE looking at the delta head. The stream is the only
      // source of rows sorting between the pulled prefix and the storage tail; pulling here keeps a
      // delta inject from being emitted ahead of a not-yet-pulled storage row that sorts earlier.
      if (cacheHead == null && !entry.isExhausted()) {
        var pulled = pullOneFromStream();
        if (pulled != null) {
          // The pull may have flipped the entry into relay (this pull is the overflow), so re-read the
          // flag. In relay the pulled row is the new relay head — results was released, so the re-loop
          // could not find it by index; otherwise it was appended and the re-loop reads it at position.
          if (entry.isRelayMode()) {
            relayCacheHead = pulled;
          }
          continue;
        }
        // null means the stream just drained; fall through with cacheHead still null.
      }

      var deltaHead = delta.peekInject();

      if (cacheHead == null && deltaHead == null) {
        return null;
      }
      // Cache drained, delta has rows: drain the (already sorted) inject list.
      if (cacheHead == null) {
        return project(delta.advanceInject());
      }
      // Delta drained, cache has rows: drain the cache.
      if (deltaHead == null) {
        advanceCacheHead(relay);
        return project(cacheHead);
      }
      // Both heads present: emit the smaller per ORDER BY; ties favour the inject side.
      var orderBy = entry.getOrderBy();
      // With no ORDER BY there is no sort key, so the inject list (the in-tx CREATE / update-into-match
      // rows, in mutation-iteration order) drains first, then the cached prefix. This tracks the
      // engine's unordered forward-scan order, which emits the transaction's own records first and the
      // storage records after (see RecordIteratorCollection.hasNext: "FORWARD: transaction records
      // first, then storage records"). The match is not row-exact: tx records created before populate
      // already sit at the front of the cached prefix, so the relative order among tx records split
      // across the populate boundary can differ from a single fresh scan. That is acceptable because an
      // unordered SELECT does not contract a row order (storage-scan-dependent), and the row SET is
      // identical. With an ORDER BY and a returnProjector, both heads are projected before the
      // comparison so a projected ORDER BY column (e.g. u.name) resolves and the comparator ranks on the
      // projected value, not the raw record.
      // The entry owns the null placement, so the merge ranks rows exactly as the delta builder
      // sorted the inject list. The cache fixed that placement when it populated the entry. A view
      // without an ORDER BY never asks for it, which keeps a storage read off that path.
      var cmp = orderBy == null ? -1 : orderBy.compare(projectForCompare(deltaHead),
          projectCacheHeadForCompare(cacheHead), ctx, entry.nullsDefault(ctx));
      if (cmp <= 0) {
        return project(delta.advanceInject());
      }
      advanceCacheHead(relay);
      return project(cacheHead);
    }
  }

  /**
   * Advances past the just-consumed cache head. In single-view relay it nulls {@link #relayCacheHead}
   * so the next merge step pulls the next storage row; on the normal buffered path it bumps the
   * positional {@link #position} cursor into {@link CachedEntry#getResults()}.
   */
  private void advanceCacheHead(boolean relay) {
    if (relay) {
      relayCacheHead = null;
    } else {
      position++;
    }
  }

  /**
   * Applies the entry's single-alias match RETURN projector to a row about to be emitted, or returns the row
   * unchanged when there is no projector (plain RECORD SELECT). This is the last transform before the
   * row reaches the consumer.
   */
  @Nonnull
  private Result project(@Nonnull Result raw) {
    var projector = entry.getReturnProjector();
    return projector == null ? raw : projector.apply(raw);
  }

  /**
   * Projects a merge head for the ORDER BY comparison only. Identical to {@link #project} today; kept
   * as a separate seam so a future change to comparison-time projection (e.g. caching the projected
   * key) does not disturb the emit-time projection.
   */
  @Nonnull
  private Result projectForCompare(@Nonnull Result raw) {
    return project(raw);
  }

  /**
   * Comparison-time projection of the current cache head, memoized so the same cache row is projected
   * at most once per emission decision. While the inject side wins, the cache head stays fixed and
   * {@code raw} is the same row across iterations; without the memo each losing compare would re-run
   * the projector, giving the merge a worst-case O(n*k) projector calls where O(n+k) distinct
   * projections suffice. The memo keys on the raw row reference, so advancing the cache head (the row
   * was consumed or skipped) self-invalidates it, and it is correct for both the positional cursor and
   * the single-view-relay {@link #relayCacheHead} (where {@code position} is frozen but the held row
   * changes). Projection is a pure function of the row, so a by-reference key is always sound. This is
   * the cache the {@link #projectForCompare} seam was kept separate for.
   */
  @Nonnull
  private Result projectCacheHeadForCompare(@Nonnull Result raw) {
    if (compareMemoRaw == raw && compareMemoProjection != null) {
      return compareMemoProjection;
    }
    var projected = projectForCompare(raw);
    compareMemoRaw = raw;
    compareMemoProjection = projected;
    return projected;
  }

  /** Whether the delta skip-set suppresses this row. Non-identifiable rows are never skipped. */
  private boolean isSkipped(@Nonnull Result r) {
    if (delta == null) {
      return false;
    }
    if (!r.isIdentifiable()) {
      return false;
    }
    var rid = r.getIdentity();
    // getIdentity() is @Nullable, but an identifiable result always carries a non-null RID. The skip
    // set is a Set<RID>, so a null here would silently miss (a stale row would never be suppressed)
    // rather than fail loudly, degrading into a wrong-result merge with no test signal. Assert the
    // invariant so a future change to isIdentifiable() semantics surfaces in the -ea test runs the
    // suite mandates.
    assert rid != null : "isIdentifiable() result has a null getIdentity()";
    return delta.shouldSkip(rid);
  }

  /**
   * Pulls one row from the entry's shared paused stream and returns it. The row is handed to {@link
   * CachedEntry#recordPulledRow}, which appends it to {@code entry.results} / {@code cachedRids} so
   * later views replay it and enforces the per-entry cap — but after a single-view overflow the entry
   * is in relay and that append is a no-op (the RECORD caller holds the row as its {@link
   * #relayCacheHead} instead). A pulled row whose RID is in the delta skip-set is dropped (and the pull
   * recurses) so a mutated RID beyond the cached prefix never surfaces with stale storage state.
   * Returns {@code null} when the stream is drained, flipping the entry to exhausted and releasing its
   * stream.
   */
  @Nullable private Result pullOneFromStream() {
    var stream = entry.getStream();
    if (stream == null) {
      // No live stream (already exhausted/closed): nothing more to pull.
      entry.setExhausted(true);
      return null;
    }
    var streamCtx = entry.getCtx();
    while (stream.hasNext(streamCtx)) {
      var r = stream.next(streamCtx);
      // Append to the shared cache before any skip decision so later views see the full ordered result
      // regardless of this view's skip-set. recordPulledRow also enforces the per-entry record cap: a
      // pull that crosses maxRecordsPerEntry overflows the entry (evicted from the cache, key routed
      // non-cacheable) and stops caching, but this view keeps emitting r so the consumer loses nothing.
      // Once a single-view overflow has switched the entry to relay, the append is a no-op and the
      // RECORD caller holds r as its relayCacheHead; here we still return r so the consumer gets it.
      entry.recordPulledRow(r);
      if (isSkipped(r)) {
        // Suppressed for this view; keep pulling for a row this view can emit.
        continue;
      }
      return r;
    }
    // Stream drained: flip exhausted and release the stream so the entry stops holding it open.
    entry.setExhausted(true);
    entry.close();
    return null;
  }

  /**
   * Releases the entry's live-view pin exactly once across {@link #close()} and natural exhaustion, so
   * an iterated-to-exhaustion or explicitly-closed view stops blocking LRU eviction. Idempotent. The
   * cache-code re-entrancy guard is not touched here: it is entered and exited per row in
   * {@link #hasNext()}, never held across the view's lifetime, so there is nothing to release at pin
   * time — and the pool-shutdown close path (which may run cross-thread) decrements only the pin, a
   * floored no-op that does not assert the owning thread.
   *
   * <p><b>Abandoned views.</b> A view dropped without {@code close()} and without being iterated to
   * exhaustion never reaches either release site, so its pin stays held and its entry stays exempt from
   * LRU eviction. This is bounded by the transaction's lifetime, not a leak across transactions:
   * {@code FrontendTransactionImpl.clear()} (every tx-end path) calls {@code QueryResultCache.clear()},
   * which closes every entry regardless of pin count — including an entry the cache already detached
   * while pinned (invalidate / TRUNCATE / overflow), caught through the cache's {@code closePending}
   * set, so an abandoned view never leaks its stream even after the entry left the map. The only effect
   * is that one long transaction with
   * many abandoned, partially-consumed views can hold its cache transiently over {@code maxEntries}.
   * Acceptable for v1: ResultSets are expected to be closed or drained, so abandonment is the
   * exceptional path; tying the pin to a phantom-reference cleaner would be the lever if a long-tx
   * over-bound regression ever shows up.
   */
  private void releasePin() {
    if (!pinReleased) {
      pinReleased = true;
      entry.decrementLiveViewCount();
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    lookahead = null;
    session = null;
    // Release the pin. This view never closes a still-cached entry's shared stream directly; the cache
    // owns it. But if the cache already removed this entry while it was pinned (the K0_NONE / MATCH
    // invalidate gate, a TRUNCATE invalidateAll, or an overflow), releasing the last pin here closes
    // the deferred stream (see CachedEntry.decrementLiveViewCount), so the stream a removal left open
    // for this view is released as soon as the view is done.
    releasePin();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Nullable @Override
  public DatabaseSessionEmbedded getBoundToSession() {
    return session;
  }

  @Nullable @Override
  public ExecutionPlan getExecutionPlan() {
    return executionPlan;
  }

  @Override
  public void forEachRemaining(@Nonnull Consumer<? super Result> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super Result> action) {
    if (hasNext()) {
      action.accept(next());
      return true;
    }
    return false;
  }

  @Nullable @Override
  public ResultSet trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return ORDERED;
  }
}
