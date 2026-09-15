package com.jetbrains.youtrackdb.internal.core.sql.executor.cache;

import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Entity;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import javax.annotation.Nonnull;

/**
 * Builds the {@link TxDeltaCursor} that reconciles a {@link CachedEntry}'s frozen RECORD output
 * against the in-transaction mutations that landed after the entry was populated. Stateless: every
 * method takes the entry, the transaction, and the original query's command context, so a single
 * shared instance (or static use) serves all entries.
 *
 * <p><b>Populate-version filter.</b> Only mutations whose {@link RecordOperation#version}
 * exceeds {@link CachedEntry#getPopulateMutationVersion()} enter the build. Every earlier mutation is
 * already reflected in {@link CachedEntry#getResults()} by the transaction-aware executor that
 * populated the entry, so replaying it here would double-apply it. Because {@code
 * addRecordOperation} re-stamps {@code version} to the latest mutation on its collapse path, a single
 * collapsed operation carries the version of its most recent change; the filter therefore admits a
 * collapsed pre-populate record exactly when a post-populate mutation touched it.
 *
 * <p><b>Dispatch table.</b> For each surviving operation the build computes two runtime facts —
 * {@code cached_at_build} (was the RID already pulled into {@link CachedEntry#getCachedRids()}?) and
 * {@code match_after} (does the post-mutation record still satisfy the WHERE clause, re-evaluated
 * with the original query's context so {@code :param} bindings resolve identically?) — then dispatches
 * on {@code (op.type, cached_at_build, match_after)}:
 *
 * <pre>
 *   op.type   cached  match  action
 *   CREATED   true    true   skip + inject  (collapsed pre-populate row whose ORDER BY key may have moved)
 *   CREATED   true    false  skip           (collapsed update drove WHERE false; drop the cached row)
 *   CREATED   false   true   inject         (true post-populate create; never in cache)
 *   CREATED   false   false  no-op          (true post-populate create that does not match)
 *   UPDATED   true    true   skip + inject  (re-position in case the ORDER BY key changed)
 *   UPDATED   true    false  skip           (no longer matches WHERE; remove from result)
 *   UPDATED   false   true   skip + inject  (not yet stream-pulled; inject post-mutation, suppress stale pull)
 *   UPDATED   false   false  skip           (suppress later stream-pull of the pre-mutation state)
 *   DELETED   *       *      skip           (suppress both the cache cursor and the stream pull)
 * </pre>
 *
 * <p>The collapse semantics make {@code cached_at_build} load-bearing even under the version filter:
 * {@code addRecordOperation} keeps a CREATE→UPDATE collapsed operation typed {@code CREATED} while
 * bumping its version, so a {@code CREATED} operation can be either a true post-populate create
 * (never in cache) or a pre-populate create whose post-populate update re-stamped it (in cache from
 * populate). {@code cached_at_build} is the runtime distinguisher: same type, different cache state,
 * different action. CREATE→DELETE and UPDATE→DELETE both collapse to {@code DELETED}, which always
 * skips regardless of the other two facts.
 *
 * <p><b>Cross-view sharing.</b> The {@code (skipSet, injectList)} pair is a pure function of the
 * entry's frozen metadata and the transaction's current mutation state, so two views built on one
 * entry at the same {@link FrontendTransactionImpl#getMutationVersion()} compute the identical pair.
 * The build caches the latest pair on the entry tagged by that version and reuses it when a later view
 * observes the same version, avoiding a re-walk of the mutation log. A view at a fresher version
 * rebuilds and overwrites the cached pair; older views keep their own reference to the prior pair via
 * their cursor, so the prior pair stays correct until those views finish.
 *
 * <p>Single-transaction state observed only by the owning thread; no synchronisation.
 */
public final class DeltaBuilder {

  private DeltaBuilder() {
  }

  /**
   * Builds (or reuses) the RECORD-shape delta cursor for {@code entry} against the transaction's
   * current mutation state. Honours the cross-view cache on the entry: when the entry already holds a
   * pair tagged at the transaction's current mutation version, the returned cursor wraps that shared
   * immutable pair; otherwise the build walks the version-filtered mutation snapshot, dispatches per the
   * table above, sorts the inject list by the entry's ORDER BY, promotes the new pair onto the entry,
   * and returns a cursor over it.
   *
   * @param entry the cached RECORD entry to reconcile
   * @param tx    the active transaction whose {@code recordOperations} carry the post-populate deltas
   * @param ctx   the original query's command context, reused so WHERE {@code :param} bindings resolve
   *              identically to the populating execution
   */
  @Nonnull
  public static TxDeltaCursor buildForRecord(
      @Nonnull CachedEntry entry,
      @Nonnull FrontendTransactionImpl tx,
      @Nonnull CommandContext ctx) {
    final var version = tx.getMutationVersion();

    // Fast path: another view on this entry already built the delta at this exact tx state. The
    // cached pair is immutable, so the new cursor shares it and only tracks its own inject position.
    // Reuse ignores ctx safely: a CachedEntry is keyed by (AST, normalized params), so every view
    // reaching this entry carries identical :param bindings and would re-evaluate WHERE to the same
    // result. A future broadening of the cache key would invalidate this reuse and must revisit it.
    if (entry.getCachedDeltaVersion() == version && entry.getCachedSkipSet() != null) {
      // The skip set and inject list are promoted as a pair (setCachedSkipSet + setCachedInjectList +
      // setCachedDeltaVersion are written together below), so a non-null skip set implies a non-null
      // inject list. State that pairing here so the @Nullable getCachedInjectList() flowing into the
      // @Nonnull TxDeltaCursor parameter is justified at the use site and fails loudly under -ea if the
      // invariant is ever broken.
      assert entry.getCachedInjectList() != null
          : "cachedSkipSet and cachedInjectList are promoted as a pair; a non-null skip set implies a"
              + " non-null inject list";
      return new TxDeltaCursor(entry.getCachedSkipSet(), entry.getCachedInjectList());
    }

    // Snapshot the relevant operations before iterating: WHERE re-evaluation may invoke a UDF that
    // calls save(), which would structurally modify recordOperations mid-iteration. A snapshot
    // (a) keeps the iteration ConcurrentModificationException-free and (b) excludes any record a
    // UDF-triggered mutation adds during the build — that record becomes visible to the NEXT view,
    // when the mutation version has advanced and a fresh delta is built.
    //
    // The snapshot applies both filters that depend only on the op and the entry's frozen metadata —
    // the populate-version filter and the class-closure filter — so the transient list and the
    // dispatch loop below only ever see ops this RECORD query can possibly select. The class checks
    // are pure reads (no save()), so hoisting them ahead of the WHERE re-eval is safe and keeps the
    // O(d) dispatch loop free of ops it would only skip. The remaining full-log walk is the accepted
    // v1 cost; a per-class mutation index is the deferred v2 lever if a regression ever justifies it.
    final var effectiveFromClasses = entry.getEffectiveFromClasses();
    final var snapshot = new ArrayList<RecordOperation>();
    for (final var op : tx.getRecordOperationsInternal()) {
      if (op.version <= entry.getPopulateMutationVersion()) {
        // Already reflected in the populated result; replaying it would double-apply.
        continue;
      }
      // Class filter (subclass closure, O(1) hash-set probe). Non-Entity records (e.g. Blobs) and
      // entities whose schema class is null or outside the query's class closure are not part of
      // this RECORD query's result and contribute no delta.
      if (!(op.record instanceof Entity entity)) {
        continue;
      }
      final var className = entity.getSchemaClassName();
      if (className == null || !effectiveFromClasses.contains(className)) {
        continue;
      }
      snapshot.add(op);
    }

    final var skipSet = new HashSet<RID>();
    final var injectList = new ArrayList<Result>();
    final var session = ctx.getDatabaseSession();

    for (final var op : snapshot) {
      final var record = op.record;
      final var rid = record.getIdentity();
      final var cachedAtBuild = entry.getCachedRids().contains(rid);
      // Re-evaluate WHERE against the post-mutation record using the original query's context so
      // parameterized predicates bind identically. A null WHERE clause matches every record.
      final var whereClause = entry.getWhereClause();
      final var matchAfter = whereClause == null || whereClause.matchesFilters(record, ctx);

      switch (op.type) {
        case RecordOperation.DELETED ->
            // Suppress from both the cache cursor and the stream pull; no inject.
            skipSet.add(rid);
        case RecordOperation.CREATED -> {
          if (cachedAtBuild) {
            // Collapsed pre-populate create that absorbed a post-populate update in place: the row
            // is already in the cache, so always skip the cached copy. Re-inject the post-mutation
            // state only when it still matches WHERE (its ORDER BY position may have shifted).
            skipSet.add(rid);
            if (matchAfter) {
              injectList.add(new ResultInternal(session, record));
            }
          } else if (matchAfter) {
            // True post-populate create that matches: inject only (never emitted from cache, and its
            // temp RID was never reachable by the stream, so no skip is needed).
            injectList.add(new ResultInternal(session, record));
          }
          // cached=false, match=false: a true post-populate create that does not match — no-op.
        }
        case RecordOperation.UPDATED -> {
          // Always suppress the pre-mutation copy from the cache cursor and the stream pull; inject
          // the post-mutation state when it still matches WHERE.
          skipSet.add(rid);
          if (matchAfter) {
            injectList.add(new ResultInternal(session, record));
          }
        }
        default ->
            // RecordOperation only ever carries CREATED / UPDATED / DELETED; any other byte is a
            // contract violation upstream.
            throw new IllegalStateException(
                "Unexpected RecordOperation type " + op.type + " for RID " + rid);
      }
    }

    // Sort the inject list by the query's ORDER BY so the view's sorted-merge stays correct. With no
    // ORDER BY the rows keep their mutation-iteration order, matching the unsorted fresh-execution
    // contract for that query shape. For a single-alias MATCH the ORDER BY ranks the projected
    // RETURN tuples (e.g. ORDER BY u.name), but the inject rows are raw records, so each operand is
    // projected through the entry's returnProjector before the comparison — the same projected-head
    // comparison the view's merge uses, so the inject list and the cache cursor stay co-ordered.
    final var orderBy = entry.getOrderBy();
    if (orderBy != null && injectList.size() > 1) {
      final var projector = entry.getReturnProjector();
      // Null placement comes from the entry, which resolves it once and keeps it. The view merges
      // this sorted list against the cached rows. A second resolution there could disagree after a
      // configuration change, and the merged result would come out unsorted.
      final var nullsDefault = entry.nullsDefault(ctx);
      if (projector == null) {
        injectList.sort((a, b) -> orderBy.compare(a, b, ctx, nullsDefault));
      } else {
        injectList.sort(
            (a, b) -> orderBy.compare(projector.apply(a), projector.apply(b), ctx, nullsDefault));
      }
    }

    // Promote the new pair onto the entry (overwriting any older version's pair) using unmodifiable
    // wrappers so every cursor — first-build and reuse alike — shares the identical immutable surface.
    final var sharedSkipSet = Collections.unmodifiableSet(skipSet);
    final var sharedInjectList = Collections.unmodifiableList(injectList);
    entry.setCachedSkipSet(sharedSkipSet);
    entry.setCachedInjectList(sharedInjectList);
    entry.setCachedDeltaVersion(version);

    return new TxDeltaCursor(sharedSkipSet, sharedInjectList);
  }

  /**
   * Builds the aggregate-shape replay state for {@code entry} against the transaction's current
   * mutation state. Copies the entry's seeded {@link AggregateState} (so the entry's populate-time
   * state is never disturbed) and replays every post-populate mutation onto the copy, then returns the
   * copy for the view to finalise through {@link AggregateState#toResult}.
   *
   * <p><b>Populate-version filter.</b> Like {@link #buildForRecord}, only operations whose
   * {@link RecordOperation#version} exceeds {@link CachedEntry#getPopulateMutationVersion()} enter the
   * replay: pre-populate mutations were already observed by the side-tap at populate and are baked into
   * the seeded state, so replaying them would double-apply.
   *
   * <p><b>Class filter.</b> Non-Entity records and entities outside the query's class closure cannot
   * contribute to this aggregate and are skipped, mirroring the record path.
   *
   * <p><b>Membership dispatch (collapse safety).</b> {@link AggregateState#applyMutation} derives
   * its transition from cache membership and {@code matchAfter} (with a {@code DELETED} op forcing a
   * drop), not from the operation type as a stand-in for the before-state, so a
   * collapsed pre-populate CREATE that the version filter admits is reconciled correctly. {@code
   * matchAfter} re-evaluates the entry's WHERE against the post-mutation record using the original
   * query's context, so {@code :param} bindings resolve identically to the populating execution.
   *
   * <p>Unlike the record path, the aggregate state is not cross-view cached on the entry: a single
   * call produces a fresh copy per view. Aggregate views are not consumer-paced (the scalar is fully
   * computed at build), so there is no pause/resume to share.
   *
   * @param entry the cached {@code AGGREGATE_*} entry whose seeded state to reconcile
   * @param tx    the active transaction whose {@code recordOperations} carry the post-populate deltas
   * @param ctx   the original query's command context, reused so WHERE {@code :param} bindings resolve
   *              identically to the populating execution
   */
  @Nonnull
  public static AggregateState buildForAggregate(
      @Nonnull CachedEntry entry,
      @Nonnull FrontendTransactionImpl tx,
      @Nonnull CommandContext ctx) {
    final var seeded = entry.getAggregateState();
    // An AGGREGATE_* entry always carries a seeded state from the eager-drive populate; a null here is
    // a wiring bug (a non-aggregate entry routed to this builder). Assert it (no-op without -ea) so the
    // contract violation fails loudly in tests rather than NPEing mid-replay.
    assert seeded != null : "buildForAggregate on an entry with no aggregateState";
    final var deltaState = seeded.copy();

    final var effectiveFromClasses = entry.getEffectiveFromClasses();
    final var whereClause = entry.getWhereClause();
    for (final var op : tx.getRecordOperationsInternal()) {
      if (op.version <= entry.getPopulateMutationVersion()) {
        // Already observed by the populate-time side-tap and baked into the seeded state.
        continue;
      }
      final var record = op.record;
      if (!(record instanceof Entity entity)) {
        continue;
      }
      final var className = entity.getSchemaClassName();
      if (className == null || !effectiveFromClasses.contains(className)) {
        continue;
      }
      // matchAfter carries WHERE-membership only; the new value for a still-contributing record is read
      // from the mutated record inside applyMutation. A null WHERE matches every record.
      final var matchAfter = whereClause == null || whereClause.matchesFilters(record, ctx);
      deltaState.applyMutation(record, op.type, matchAfter);
    }

    return deltaState;
  }

  /**
   * Class-scoped version gate for a multi-alias MATCH ({@code MATCH_TUPLE_MULTI}) entry: reports whether
   * a post-populate mutation has touched any class in the entry's read-class closure, so the frozen
   * tuple set can no longer be trusted. A multi-alias MATCH is replayed verbatim while no pattern-class
   * mutation has happened and re-executed once one has; unlike RECORD it builds no per-tuple delta,
   * because reconstructing tuples from a projected RETURN row (which carries scalars such as {@code
   * u.name}, not the bound records) is not possible, and a coarse class-scoped invalidation suits the
   * read-mostly workload the cache targets.
   *
   * <p><b>Fast path.</b> When the transaction's mutation version equals the entry's populate version no
   * mutation has happened at all, so the entry is a definite hit and the record-operation scan is
   * skipped. This is the common case in a read-mostly transaction, where most queries between two
   * mutations are pure hits.
   *
   * <p><b>Scan.</b> Otherwise every post-populate operation (version filter, as in {@link
   * #buildForRecord}) is checked against the entry's class closure: a single Entity operation whose
   * schema class is in the closure makes the entry stale. Operations on unrelated classes leave the
   * entry serviceable, which is the advantage of the class-scoped gate over a global one.
   *
   * @param entry the cached multi-alias MATCH entry to test
   * @param tx    the active transaction whose {@code recordOperations} carry the post-populate deltas
   * @return {@code true} when the entry must be invalidated and re-executed; {@code false} when it may
   *     still be replayed verbatim
   */
  public static boolean matchMultiStale(
      @Nonnull CachedEntry entry, @Nonnull FrontendTransactionImpl tx) {
    if (entry.getPopulateMutationVersion() == tx.getMutationVersion()) {
      return false;
    }
    final var effectiveFromClasses = entry.getEffectiveFromClasses();
    for (final var op : tx.getRecordOperationsInternal()) {
      if (op.version <= entry.getPopulateMutationVersion()) {
        continue;
      }
      if (!(op.record instanceof Entity entity)) {
        continue;
      }
      final var className = entity.getSchemaClassName();
      if (className != null && effectiveFromClasses.contains(className)) {
        return true;
      }
    }
    return false;
  }
}
