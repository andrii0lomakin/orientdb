package com.jetbrains.youtrackdb.internal.core.sql.executor.cache;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.IdempotentExecutionStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The per-transaction store of cached query results. One instance lives on a single {@code
 * FrontendTransactionImpl}, so every method runs on the transaction's owning thread; the only
 * cross-thread caller is {@link #clear()} during pool shutdown, which inherits the same best-effort
 * contract the transaction's own {@code clear()} sink already carries. No field is synchronised.
 *
 * <p><b>LRU bound.</b> Entries are held in a {@link LinkedHashMap} in access order. When a put would
 * push the live-entry count past {@link #maxEntries}, the cache evicts the least-recently-used entry
 * whose view refcount is zero. A pinned entry ({@code liveViewCount > 0}, a mid-iteration view) is
 * exempt so a view never loses rows under cache pressure; the map therefore grows transiently
 * above the bound while every eldest candidate is pinned. Eviction closes the entry's paused stream,
 * routes the evicted key to {@link #nonCacheableKeys} so the same query does not immediately
 * re-populate and churn the LRU, and counts an overflow.
 *
 * <p><b>Pinned-entry removal.</b> Every path that takes an entry out of {@link #entries} — LRU
 * eviction, the K0_NONE invalidation gate, the multi-alias MATCH invalidation gate ({@link #invalidateMatchMulti}),
 * bulk-DML {@link #invalidateAll}, and per-entry overflow ({@link #overflowEntry}) — goes through {@link
 * #closeOrDefer}, which closes the stream immediately only when the entry is unpinned. A still-pinned
 * entry has its close deferred to the last pin release (the entry is marked via {@link
 * CachedEntry#markCloseWhenUnpinned} and parked in {@link #closePending}), so a removal never closes a
 * stream out from under a mid-iteration view. The view drains its tail from the still-open stream and
 * the entry closes when its last view ends; future lookups miss because the entry already left the
 * map. This is what makes "a mid-iteration view never loses rows" hold for every remover, not just LRU.
 *
 * <p><b>Re-entrancy.</b> A query issued from inside the cache's own lookup-and-view scope must not
 * recursively consult the cache. The transaction-level {@code cacheCodeDepth} counter on {@code
 * FrontendTransactionImpl} brackets the synchronous lookup-and-build scope at the session (a
 * returned view re-enters the bracket per row during iteration, holding no guard while idle): a re-entrant
 * {@code query()} (for example a user-defined function in a WHERE clause issuing its own {@code
 * query()}) sees {@code cacheCodeDepth > 0} before {@link #lookup} is reached and falls back to
 * uncached execution, so {@code lookup} never runs a second time on the same thread.
 *
 * <p><b>K0_NONE version gate.</b> A {@link CacheableShape#K0_NONE} entry is reproducible from storage
 * plus the AST but cannot be reconciled record by record, so it serves a cached read only while no
 * mutation has happened since it was populated. {@link #lookup} compares the supplied current
 * mutation version against the entry's populate version: equal is a hit, diverged invalidates the
 * entry, counts a K0 invalidation, and — once a key has been invalidated {@code
 * k0NoneInvalidationThreshold} times — routes the key to {@link #nonCacheableKeys} to bound repopulate
 * churn in a write-heavy fragment.
 *
 * <p><b>Lifecycle.</b> {@link #clear()} is the transaction-end sink and is idempotent: it closes every
 * entry's paused stream — both the live map entries and any deferred-close entries parked in {@link
 * #closePending} — and empties all state; a second call finds everything empty and is a no-op. It
 * snapshots the entry set before iterating because {@code CachedEntry#close} touches the entry's own
 * state and a future change could reach back into the map. {@link #invalidateAll()} is the
 * bulk-DML hook with the same snapshot discipline; it keeps the cache instance live (the transaction
 * continues) but drops every cached result.
 */
public final class QueryResultCache {

  private final QueryCacheMetrics metrics;
  private final int maxEntries;
  private final int maxRecordsPerEntry;
  private final int k0NoneInvalidationThreshold;
  private final int multiInvalidationThreshold;

  /**
   * The cached entries in access order. {@code accessOrder=true} so a successful lookup moves an entry
   * to the most-recently-used end and eviction always targets the cold end. Auto-eviction via {@code
   * removeEldestEntry} is disabled (it cannot skip a pinned eldest); eviction is driven manually in
   * {@link #put}, which evicts the eldest only when it is unpinned and otherwise stays over the bound
   * rather than scanning for another victim.
   */
  private final LinkedHashMap<CacheKey, CachedEntry> entries =
      new LinkedHashMap<>(16, 0.75f, true);

  /**
   * Keys that must bypass the cache for the rest of the transaction: overflowed entries and
   * K0_NONE keys that crossed the invalidation-strike threshold. Uncapped per transaction.
   */
  private final Set<CacheKey> nonCacheableKeys = new HashSet<>();

  /**
   * Cumulative K0_NONE invalidation strikes per key. The strike count must survive the entry it
   * counts because an invalidation removes the entry, so it is tracked here on the cache rather than
   * on {@link CachedEntry}.
   */
  private final Map<CacheKey, Integer> k0Strikes = new HashMap<>();

  /**
   * Entries removed from {@link #entries} while still pinned by a live view: their stream close was
   * deferred to the last pin release (see {@link #closeOrDefer}). A well-behaved view closes such an
   * entry promptly on its own close or drain; this set is the backstop for an <i>abandoned</i> view
   * that never releases its pin, so {@link #clear()} at transaction end still closes the entry. Without
   * it a detached, abandoned entry would be unreachable from the map and leak its stream/plan for the
   * rest of the transaction. Uncapped per transaction, bounded by the number of mid-iteration removals;
   * cleared at tx end.
   */
  private final Set<CachedEntry> closePending = new HashSet<>();

  public QueryResultCache(@Nonnull QueryCacheMetrics metrics) {
    this.metrics = metrics;
    this.maxEntries = GlobalConfiguration.QUERY_TX_RESULT_CACHE_MAX_ENTRIES.getValueAsInteger();
    this.maxRecordsPerEntry =
        GlobalConfiguration.QUERY_TX_RESULT_CACHE_MAX_RECORDS_PER_ENTRY.getValueAsInteger();
    this.k0NoneInvalidationThreshold =
        GlobalConfiguration.QUERY_TX_RESULT_CACHE_K0_NONE_INVALIDATION_THRESHOLD
            .getValueAsInteger();
    this.multiInvalidationThreshold =
        GlobalConfiguration.QUERY_TX_RESULT_CACHE_MULTI_INVALIDATION_THRESHOLD
            .getValueAsInteger();
  }

  /**
   * Looks up the entry for {@code key} with no null placement gate, for a caller that cannot resolve
   * the current placement. Every production caller uses {@link #lookup(CacheKey, long, Supplier)}
   * instead, because an entry frozen under a placement that has since changed stays servable here.
   *
   * @param currentMutationVersion the owning transaction's mutation version at lookup time, used only
   *     to gate {@link CacheableShape#K0_NONE} entries.
   */
  @Nullable public CachedEntry lookup(@Nonnull CacheKey key, long currentMutationVersion) {
    return lookup(key, currentMutationVersion, null);
  }

  /**
   * Looks up the entry for {@code key} at the transaction's current mutation version, rejecting an
   * entry whose populate-time null placement no longer matches the database setting.
   *
   * <p>Returns {@code null} — caller falls back to uncached execution — when: the key is in {@link
   * #nonCacheableKeys}; no entry is cached; a {@link CacheableShape#K0_NONE} entry's populate version
   * no longer equals {@code currentMutationVersion}; or a placement-sensitive entry froze another
   * placement than {@code currentNullPlacements} reports. The version-mismatch case also evicts the
   * stale entry, counts a K0 invalidation, and routes the key to the non-cacheable set after the
   * configured number of strikes. The placement-mismatch case evicts the entry too.
   *
   * <p>Accounting: a returned entry has been moved to the most-recently-used end and its {@code hits}
   * counter incremented. An absent key counts a miss, and so does a placement rejection, because
   * either way the caller runs the query for real. A bypassed key and a version-gate rejection count
   * no miss, since the first was accounted when the key was routed out and the second counts its own
   * invalidation.
   *
   * <p>The supplier runs only after a placement-sensitive entry is found, so a miss, a bypass and a
   * hit on an entry that ranks nothing all stay free of configuration resolution.
   *
   * @param currentMutationVersion the owning transaction's mutation version at lookup time, used only
   *     to gate {@link CacheableShape#K0_NONE} entries.
   */
  @Nullable public CachedEntry lookup(
      @Nonnull CacheKey key,
      long currentMutationVersion,
      @Nullable Supplier<ResolvedOrderByNullsPlacement> currentNullPlacements) {
    if (nonCacheableKeys.contains(key)) {
      // Permanently bypassed key (overflowed or K0-strike-exceeded). Not counted as a cache miss: the
      // decision to bypass was already accounted for when the key was routed to the set.
      return null;
    }
    var entry = entries.get(key);
    if (entry == null) {
      metrics.incrementMisses();
      return null;
    }
    if (entry.getShape() == CacheableShape.K0_NONE
        && entry.getPopulateMutationVersion() != currentMutationVersion) {
      // The transaction mutated since this K0_NONE entry was populated, and a K0_NONE result cannot
      // be reconciled record by record, so the cached answer is stale. Drop it, count the strike,
      // and route the key out of the cache once it has been invalidated often enough.
      invalidate(key, entry);
      metrics.incrementK0Invalidations();
      var strikes = k0Strikes.merge(key, 1, Integer::sum);
      if (strikes >= k0NoneInvalidationThreshold) {
        nonCacheableKeys.add(key);
      }
      return null;
    }
    if (currentNullPlacements != null
        && entry.isPlacementSensitive()
        && !entry.hasCurrentNullPlacement(currentNullPlacements.get())) {
      // The frozen rows use the populate-time placement. Remove the entry before recording a hit,
      // then count the fresh execution driven by this rejection as a cache miss.
      invalidate(key, entry);
      metrics.incrementMisses();
      return null;
    }
    if (entry.getShape() == CacheableShape.MATCH_TUPLE_MULTI) {
      // A multi-alias MATCH hit is only confirmed after the caller's class-scoped staleness gate runs
      // (it needs the transaction's record operations, which lookup does not carry). The caller counts
      // the hit via recordHit() on the served branch, or an invalidation via invalidateMatchMulti() on
      // the stale branch. Counting a hit here would double-count a stale hit as hit + invalidation,
      // diverging from the K0_NONE gate, which decides inside lookup and counts only the invalidation.
      return entry;
    }
    metrics.incrementHits();
    return entry;
  }

  /**
   * Records a cache hit whose decision is made outside {@link #lookup}: the multi-alias MATCH
   * class-scoped gate, which returns a tentative entry from {@code lookup} and confirms it is served
   * only after the staleness scan. Keeps multi-alias MATCH hit accounting consistent with every other
   * shape (one hit per served query) without double-counting a stale hit.
   */
  public void recordHit() {
    metrics.incrementHits();
  }

  /**
   * Records a cache miss whose decision is made outside {@link #lookup}: the empty-cache
   * short-circuit in the session, which skips the key build and {@code lookup} call when {@link
   * #isEmpty} holds. {@code lookup} would have found the key absent and counted the miss itself; this
   * keeps the hit/miss metric identical whether or not the short-circuit fired, so a transaction's
   * first (cache-empty) query still counts as the miss it is.
   */
  public void recordMiss() {
    metrics.incrementMisses();
  }

  /**
   * Stores {@code entry} under {@code key}, then enforces the LRU bound. A key in {@link
   * #nonCacheableKeys} is never stored: the entry is closed immediately and the put is a no-op, so an
   * overflowed or strike-exceeded query stays uncached for the rest of the transaction. After the
   * insert, if the entry count exceeds {@link #maxEntries}, the eldest (least-recently-used) entry is
   * evicted when it is unpinned (see {@link #evictEldestIfUnpinned}).
   */
  public void put(@Nonnull CacheKey key, @Nonnull CachedEntry entry) {
    if (nonCacheableKeys.contains(key)) {
      // The key was already routed out of the cache; do not resurrect it. Release the just-built
      // entry's stream so the populating execution's resources are not leaked.
      entry.close();
      return;
    }
    entries.put(key, entry);
    // Install the per-entry record cap. The entry's lazy row append fires this callback the moment a
    // populate would push it past maxRecordsPerEntry: the entry is dropped from the cache and its key
    // routed out, so no further query re-populates it, while the live view keeps streaming every row.
    entry.setOverflowGuard(maxRecordsPerEntry, () -> overflowEntry(key));
    if (entries.size() > maxEntries) {
      evictEldestIfUnpinned();
    }
  }

  /**
   * Handles a per-entry record-cap overflow reported by a populating view: removes the over-cap
   * entry from the map, routes its key to the non-cacheable set so the same query stays uncached for
   * the rest of the transaction, and counts an overflow. The overflow always fires from inside a live
   * view's stream pull, so the entry is pinned: {@link #closeOrDefer} therefore defers the close, the
   * view keeps streaming every row, and the entry's stream/plan close when that view drains or closes
   * (or, if the view is abandoned, at the tx-end {@link #clear()} sweep of {@link #closePending}).
   */
  private void overflowEntry(@Nonnull CacheKey key) {
    var entry = entries.remove(key);
    if (entry != null) {
      nonCacheableKeys.add(key);
      metrics.incrementOverflows();
      closeOrDefer(entry);
    }
  }

  /**
   * Mirrors the {@code LinkedHashMap.removeEldestEntry} decision: inspects only the eldest
   * (least-recently-used) entry. When it is unpinned ({@code liveViewCount == 0}) it is evicted — its
   * paused stream is closed, its key is routed to {@link #nonCacheableKeys} so the same query
   * does not immediately re-populate and churn the LRU, and an overflow is counted. When the
   * eldest is pinned by a live view the map is left transiently over the bound rather than truncating
   * that view; a deeper hot entry is never evicted in its place, because doing so would discard a
   * fresher, more useful result to spare the cold pinned one.
   */
  private void evictEldestIfUnpinned() {
    // The first entry of an access-ordered LinkedHashMap is the eldest (least-recently-used); a put
    // does not promote, so the just-stored entry is at the hot end and is never the eldest here.
    var eldest = entries.entrySet().iterator().next();
    if (eldest.getValue().getLiveViewCount() != 0) {
      // Eldest is pinned by a live view; stay over the bound rather than truncate it.
      return;
    }
    var victimKey = eldest.getKey();
    var victimEntry = eldest.getValue();
    entries.remove(victimKey);
    nonCacheableKeys.add(victimKey);
    // The eldest is unpinned (checked above), so closeOrDefer closes it immediately; routed through
    // the shared helper so every map-removal path closes through one place.
    closeOrDefer(victimEntry);
    metrics.incrementOverflows();
  }

  /**
   * Closes an entry just removed from {@link #entries}, or defers the close when a live view still
   * pins it. An unpinned entry is closed immediately. A pinned entry is marked {@link
   * CachedEntry#markCloseWhenUnpinned} and parked in {@link #closePending}: its stream stays open so
   * the mid-iteration view drains its tail without truncation, and the entry closes at the last pin
   * release (see {@link CachedEntry#decrementLiveViewCount}) or, for an abandoned view, at the tx-end
   * {@link #clear()} sweep. Every map-removal path (eviction, invalidate, invalidateAll, overflow)
   * routes through here so the pinned-entry contract holds uniformly.
   */
  private void closeOrDefer(@Nonnull CachedEntry entry) {
    if (entry.getLiveViewCount() > 0) {
      entry.markCloseWhenUnpinned();
      closePending.add(entry);
    } else {
      entry.close();
    }
  }

  /**
   * Removes {@code key}'s entry from the map and closes it (or defers the close while a view pins it,
   * via {@link #closeOrDefer}). Used by the K0 version gate.
   */
  private void invalidate(@Nonnull CacheKey key, @Nonnull CachedEntry entry) {
    entries.remove(key);
    closeOrDefer(entry);
  }

  /**
   * Invalidates a multi-alias MATCH ({@link CacheableShape#MATCH_TUPLE_MULTI}) entry whose class-scoped
   * version gate found a post-populate mutation in one of the pattern's read classes. Mirrors the
   * K0_NONE gate in {@link #lookup}: drops the entry, counts an invalidation, and routes the key
   * non-cacheable once it has been invalidated the configured number of times, bounding repopulate churn
   * in a write-heavy fragment. The strike count shares {@link #k0Strikes} with the K0_NONE gate, which
   * is sound because a given key has one fixed shape, so the two gates never count the same key. Called
   * from the session's hit-path gate rather than from {@link #lookup}, because the class-scoped scan
   * needs the transaction's record operations the lookup contract does not carry. A no-op when the key
   * has no live entry (already evicted).
   */
  public void invalidateMatchMulti(@Nonnull CacheKey key) {
    var entry = entries.get(key);
    if (entry == null) {
      return;
    }
    invalidate(key, entry);
    metrics.incrementMultiInvalidations();
    var strikes = k0Strikes.merge(key, 1, Integer::sum);
    if (strikes >= multiInvalidationThreshold) {
      nonCacheableKeys.add(key);
    }
  }

  /**
   * Drops every cached entry while keeping the cache instance live, closing each unpinned entry's
   * paused stream (a still-pinned entry defers its close via {@link #closeOrDefer} so a TRUNCATE CLASS
   * issued while a view iterates does not truncate it; the view drains its frozen result, matching a
   * fresh execution taken at the view's start). The bulk-DML hook for the one mid-transaction operation
   * (TRUNCATE CLASS) that can change stored data without flowing through the mutation log. Snapshots
   * the values before iterating so an entry close cannot disturb the map mid-walk. {@link
   * #nonCacheableKeys} and {@link #k0Strikes} are left intact: a key bypassed before the bulk operation
   * stays bypassed.
   */
  public void invalidateAll() {
    var snapshot = new ArrayList<>(entries.values());
    entries.clear();
    for (var entry : snapshot) {
      closeOrDefer(entry);
    }
  }

  /**
   * Transaction-end sink: closes every entry's paused stream — both the live map entries and any
   * deferred-close entries parked in {@link #closePending} by a removal that found them pinned — and
   * empties all cache state. Idempotent — a second call finds everything empty and returns without
   * effect — and safe to reach from the pool-shutdown thread because the underlying stream close is
   * itself idempotent (via {@link IdempotentExecutionStream}). Snapshots the values before iterating
   * for the same reason as {@link #invalidateAll()}.
   */
  public void clear() {
    // Close detached-but-pinned entries whose view was abandoned (never released its pin), so they do
    // not leak past the transaction. Done first so it runs even on the empty-map fast path below.
    // Idempotent: an entry already closed on its last pin release or on drain is a no-op here.
    if (!closePending.isEmpty()) {
      var pending = new ArrayList<>(closePending);
      closePending.clear();
      for (var entry : pending) {
        entry.close();
      }
    }
    if (entries.isEmpty()) {
      // Idempotent fast path: a second tx-end clear finds nothing to release. Still drop the bypass
      // sets so a reused cache instance starts a fresh transaction clean.
      nonCacheableKeys.clear();
      k0Strikes.clear();
      return;
    }
    var snapshot = new ArrayList<>(entries.values());
    entries.clear();
    nonCacheableKeys.clear();
    k0Strikes.clear();
    for (var entry : snapshot) {
      entry.close();
    }
  }

  /**
   * Installs the aggregate contributor cap and a one-shot overflow callback on {@code state}, mirroring
   * the per-entry record cap {@link #put} installs on a {@link CachedEntry}. The cap bounds the
   * aggregate's per-contributor collections (not its single-scalar {@code results}); the callback fires
   * the first time {@link AggregateState#observe} crosses {@code maxRecordsPerEntry} during the eager
   * populate drive, routing {@code key} non-cacheable and counting an overflow. Unlike the record cap,
   * the entry is not yet in the map at populate-time overflow (the cache-put happens after the drive), so
   * the callback adds the key directly rather than removing a mapped entry; a later {@link #put} of that
   * now-overflowed key is a no-op (the {@link #nonCacheableKeys} guard in {@code put} closes it).
   */
  public void installAggregateOverflowGuard(
      @Nonnull CacheKey key, @Nonnull AggregateState state) {
    state.setOverflowGuard(maxRecordsPerEntry, () -> {
      // Idempotent on the key: nonCacheableKeys is a Set and the metric counts once because the state's
      // own one-shot latch fires this callback at most once per populate.
      nonCacheableKeys.add(key);
      metrics.incrementOverflows();
    });
  }

  /**
   * Counts an aggregate-splice fallback: a miss whose populating plan had no {@code
   * AggregateProjectionCalculationStep} to splice the side-tap above (e.g. a hardwired {@code
   * CountFromClassStep}, or any unexpected plan shape), so the query ran uncached. Routes through the
   * metric bridge to the global splice-failure rate.
   */
  public void incrementSpliceFailures() {
    metrics.incrementSpliceFailures();
  }

  /** Number of live cache entries. */
  public int size() {
    return entries.size();
  }

  /**
   * Whether the cache holds nothing a lookup or non-cacheable check could act on: no cached entry and
   * no key routed out of the cache. When both hold, {@link #lookup} can only miss and {@link
   * #isNonCacheable} can only return {@code false} for any key, so a caller may skip building a {@link
   * CacheKey} for those two checks and go straight to the populate decision, allocating the key only
   * if it ends up storing an entry. {@link #k0Strikes} is deliberately not consulted: a residual
   * strike count never changes a lookup or non-cacheable result on an otherwise empty cache (it only
   * tracks progress toward a future bypass), so an empty {@code entries}/{@code nonCacheableKeys} pair
   * is a sufficient short-circuit even when strikes remain.
   */
  public boolean isEmpty() {
    return entries.isEmpty() && nonCacheableKeys.isEmpty();
  }

  /** Whether a key has been routed out of the cache for the rest of this transaction. */
  public boolean isNonCacheable(@Nonnull CacheKey key) {
    return nonCacheableKeys.contains(key);
  }

  public QueryCacheMetrics getMetrics() {
    return metrics;
  }

  /**
   * A snapshot of the live cache entries, for tests that assert an entry's populate-time metadata
   * (shape, class closure, projector) without going through {@link #lookup} and perturbing the
   * hit/miss metrics. Package-visible: the cache exposes no entry enumeration on its production API.
   */
  @Nonnull
  Collection<CachedEntry> entriesForTest() {
    return new ArrayList<>(entries.values());
  }
}
