package com.jetbrains.youtrackdb.internal.core.sql.executor.match;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.concur.TimeoutException;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.db.record.ridbag.LinkBag;
import com.jetbrains.youtrackdb.internal.core.index.Index;
import com.jetbrains.youtrackdb.internal.core.index.engine.EquiDepthHistogram;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.AbstractExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ExecutionStepInternal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.IndexSearchDescriptor;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.RidFilteredIndexValuesStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.RidSet;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.RidPair;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MATCH execution step that traverses an edge using an index-ordered scan.
 * Replaces {@link MatchStep} for edges where:
 * <ol>
 *   <li>The ORDER BY references a property on the edge's target alias</li>
 *   <li>An index exists on that property</li>
 *   <li>The edge is simple (not WHILE/recursive)</li>
 * </ol>
 *
 * <h3>Two execution modes</h3>
 *
 * <p><b>Single-source</b> (one upstream row): builds a RidSet from the source
 * vertex's LinkBag, scans the index filtered by RidSet membership. Only
 * matching records are loaded.
 *
 * <p><b>Multi-source</b> (many upstream rows): collects all upstream rows,
 * builds a small {@code sourceMap} (source RID → upstream row), then scans
 * the index globally. For each entry, loads the record, follows the reverse
 * edge to find the source vertex, and checks {@code sourceMap} membership.
 * This produces globally sorted results across all sources.
 *
 * <h3>Cost-based heuristic</h3>
 *
 * <p>Both modes compare index scan cost vs load-all-and-sort cost using
 * {@link CostModel} constants. When the cost model rejects the index scan,
 * both modes fall back to loading all targets and sorting in-memory — still
 * producing correctly ordered results.
 */
public class IndexOrderedEdgeStep extends AbstractExecutionStep {

  private static final Logger logger =
      LoggerFactory.getLogger(IndexOrderedEdgeStep.class);

  private final String sourceAlias;
  private final String targetAlias;
  private final String edgeClassName;
  private final String linkBagFieldName;
  private final Index index;
  private final boolean orderAsc;
  private final SQLOrderByItem comparisonItem;
  private final ResolvedOrderByNullsPlacement nullsPlacement;
  private final EdgeTraversal edge;
  private final long limit;

  /** Multi-source execution mode, or null for single-source. */
  @Nullable private final IndexOrderedPlanner.MultiSourceMode multiSourceMode;

  /** Reverse LinkBag field on target for reverse edge lookup (multi-source). */
  @Nullable private final String reverseFieldName;

  /** Source vertex class name for class-check modes (UNFILTERED_BOUND/UNBOUND). */
  @Nullable private final String sourceClassName;

  /** WHERE filter on the target alias (e.g., creationDate < :maxDate). */
  @Nullable private final SQLWhereClause targetFilter;

  /**
   * Class constraint on the target alias (for class-based filtering). Never
   * null: {@link IndexOrderedPlanner} rejects the candidate when the target
   * alias has no resolvable class, so this step is only built with one.
   */
  @Nonnull
  private final String targetClassName;

  /**
   * When true, the traversal is .inE()/.outE() and we want edge record RIDs
   * (primaryRid) from LinkBag pairs instead of opposite vertex RIDs (secondaryRid).
   */
  private final boolean edgeTraversal;

  /**
   * Number of MATCH edges scheduled after this step's target alias.
   * Used by the cost model to estimate downstream traversal work that
   * index scan + LIMIT avoids (only K rows go downstream vs all N).
   */
  private final int downstreamEdgeCount;

  /**
   * When true, the plan's ORDER BY carries a trailing record identifier item on the target alias and
   * the planner accepted it as already produced by this scan (see
   * {@code IndexOrderedPlanner.acceptsRidTieBreak}). The pre-sorted signal then holds only where
   * equal keys really come back in scan-direction identifier order, which is the index scan itself
   * and only while the transaction holds no pending change for the index.
   */
  private final boolean ridTieBreakAccepted;

  /**
   * Runtime strategy chosen on the last {@link #internalStart} of this step.
   * Null until the step runs — EXPLAIN before execution will not show it;
   * PROFILE / post-query {@link #prettyPrint} will.
   */
  @Nullable private volatile RuntimePath chosenRuntimePath;

  /**
   * Which physical strategy {@link IndexOrderedEdgeStep} actually ran.
   * Distinct from plan-time {@code multiSourceMode}: that selects the MATCH
   * binding shape; this records index scan vs local load/sort after the cost
   * model runs.
   */
  public enum RuntimePath {
    /** Single-source RidSet-filtered B-tree scan ({@code indexScanFiltered}). */
    INDEX_SCAN("index-scan"),
    /** Single-source load LinkBag + local sort ({@code loadSortFromLinkBag}). */
    LOAD_SORT("load-sort"),
    /** Single-source load LinkBag unsorted ({@code loadFromLinkBag}). */
    LOAD_UNSORTED("load-unsorted"),
    /** Multi-source union RidSet + filtered index scan. */
    UNION_SCAN("union-scan"),
    /** Multi-source global index scan (no RidSet filter). */
    GLOBAL_SCAN("global-scan"),
    /** Multi-source / unbound fallback: stream LinkBags, OrderByStep sorts. */
    LOAD_UNSORTED_MULTI("load-unsorted-multi");

    private final String label;

    RuntimePath(String label) {
      this.label = label;
    }

    String label() {
      return label;
    }
  }

  /** Last runtime path chosen by this step, or null if not started yet. */
  @Nullable public RuntimePath getChosenRuntimePath() {
    return chosenRuntimePath;
  }

  public IndexOrderedEdgeStep(
      CommandContext ctx,
      String sourceAlias,
      String targetAlias,
      String edgeClassName,
      String linkBagFieldName,
      Index index,
      boolean orderAsc,
      SQLOrderByItem comparisonItem,
      ResolvedOrderByNullsPlacement nullsPlacement,
      EdgeTraversal edge,
      long limit,
      @Nullable IndexOrderedPlanner.MultiSourceMode multiSourceMode,
      @Nullable String reverseFieldName,
      @Nullable String sourceClassName,
      @Nullable SQLWhereClause targetFilter,
      @Nonnull String targetClassName,
      boolean edgeTraversal,
      int downstreamEdgeCount,
      boolean ridTieBreakAccepted,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.sourceAlias = sourceAlias;
    this.targetAlias = targetAlias;
    this.edgeClassName = edgeClassName;
    this.linkBagFieldName = linkBagFieldName;
    this.index = index;
    this.orderAsc = orderAsc;
    this.comparisonItem = comparisonItem;
    this.nullsPlacement = nullsPlacement;
    this.edge = edge;
    this.limit = limit;
    this.multiSourceMode = multiSourceMode;
    this.reverseFieldName = reverseFieldName;
    this.sourceClassName = sourceClassName;
    this.targetFilter = targetFilter;
    this.targetClassName = targetClassName;
    this.edgeTraversal = edgeTraversal;
    this.downstreamEdgeCount = downstreamEdgeCount;
    this.ridTieBreakAccepted = ridTieBreakAccepted;
  }

  /**
   * Signals that this step's output is already in the plan's ORDER BY sequence, so the downstream
   * {@code OrderByStep} can stream it. Called from the paths that hand rows straight out of an
   * ordered index scan: the scan walks the composite {@code (property, rid)} key, so equal property
   * values come back in scan-direction identifier order, which is what an accepted trailing record
   * identifier item asks for.
   *
   * <p>A pending index change inside the running transaction withdraws the signal when such an item
   * was accepted. The transaction-local entries are merged into the scan by key alone, so two
   * entries of one key can arrive in either identifier order. This is checked per execution rather
   * than per plan, because the plan outlives the transaction in the plan cache.
   */
  private void signalIndexOrderedOutput(CommandContext ctx) {
    var ridOrderHolds = !ridTieBreakAccepted || !hasPendingIndexChanges(ctx);
    ctx.setSystemVariable(
        CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.valueOf(ridOrderHolds));
  }

  /** Signals pre-sorted output after the local sort has produced the complete accepted order. */
  private void signalLoadSortedOutput(CommandContext ctx) {
    ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.TRUE);
  }

  /**
   * Whether the running transaction holds an uncommitted record change, which is what can put the
   * scan out of identifier order.
   *
   * <p>The test is any record change rather than a change to this index, because the index entry of
   * an uncommitted record is written under a provisional identifier. The scan then hands that entry
   * back at the position of the provisional identifier while the row carries the identifier the
   * record has by then, so the entry can arrive anywhere inside its key group. The per-index change
   * map of the transaction does not see this at all: it stays empty, since the embedded engine
   * writes index entries into the transaction's atomic operation instead.
   */
  private boolean hasPendingIndexChanges(CommandContext ctx) {
    return ctx.getDatabaseSession().getTransactionInternal().getEntryCount() > 0;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert MatchAssertions.checkNotNull(prev, "previous step");
    if (multiSourceMode != null) {
      return multiSourceDispatch(ctx);
    }
    // Guaranteed single-source: exactly 1 upstream row (source has RID constraint).
    // Consume the upstream eagerly and compute the cost model decision NOW, so
    // that VAR_INDEX_ORDERED_PRE_SORTED is correctly set BEFORE OrderByStep
    // checks it. The pipeline starts bottom-up (LimitStep → OrderByStep →
    // ... → IndexOrderedEdgeStep), so OrderByStep.internalStart() runs first
    // and checks the flag after calling prev.start() which reaches here.
    // If we deferred the cost model to a lazy flatMap, OrderByStep would see
    // the flag as null and always fall through to collect-all mode — defeating
    // the sort push-down that makes index scan + LIMIT worthwhile.
    var resultSet = prev.start(ctx);
    if (!resultSet.hasNext(ctx)) {
      resultSet.close(ctx);
      ctx.setSystemVariable(
          CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return ExecutionStream.empty();
    }
    var upstreamRow = resultSet.next(ctx);
    resultSet.close(ctx); // single-source: at most 1 row

    return processUpstreamRow(upstreamRow, ctx);
  }

  // =====================================================================
  // Single-source mode
  // =====================================================================

  private ExecutionStream processUpstreamRow(Result upstreamRow, CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    var sourceRid = extractSourceRid(upstreamRow);
    if (sourceRid == null) {
      ctx.setSystemVariable(
          CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return ExecutionStream.empty();
    }
    var linkBag = loadLinkBag(sourceRid, session);
    if (linkBag == null || linkBag.size() == 0) {
      ctx.setSystemVariable(
          CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return ExecutionStream.empty();
    }

    long indexSize = index.size(session);
    var histogram = index.getHistogram(session);
    if (shouldUseIndexScan(linkBag.size(), indexSize, histogram)) {
      // Index scan: results are pre-sorted. Signal OrderByStep to pass through.
      // Build RidSet only now — it is needed as the bitmap filter for the
      // index-value scan. The fallback paths below iterate the LinkBag
      // directly, so they skip the RidSet allocation.
      chosenRuntimePath = RuntimePath.INDEX_SCAN;
      signalIndexOrderedOutput(ctx);
      return indexScanFiltered(ridSetFromLinkBag(linkBag), ctx, upstreamRow);
    } else if (downstreamEdgeCount > 0 && limit > 0) {
      // Low density but downstream edges + LIMIT: load all, sort locally,
      // and mark PRE_SORTED=true. This enables LIMIT to short-circuit the
      // pipeline — only K records traverse expensive downstream edges
      // (REPLY_OF chain, HAS_CREATOR, etc.) instead of all N.
      // Sort cost O(N log N) is trivial for typical LinkBag sizes (~50-500).
      chosenRuntimePath = RuntimePath.LOAD_SORT;
      signalLoadSortedOutput(ctx);
      return loadSortFromLinkBag(linkBag, ctx, upstreamRow);
    } else {
      // No downstream edges or no LIMIT: stream unsorted to OrderByStep.
      // OrderByStep's bounded heap handles sorting with O(N log K).
      chosenRuntimePath = RuntimePath.LOAD_UNSORTED;
      ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return loadFromLinkBag(linkBag, ctx, upstreamRow);
    }
  }

  /**
   * Builds a RidSet bitmap from the source LinkBag. Used only on the
   * index-scan branch, where the bitmap is required as a membership filter
   * for {@link RidFilteredIndexValuesStep}. The fallback branches iterate
   * the LinkBag directly to avoid this allocation.
   */
  private RidSet ridSetFromLinkBag(LinkBag linkBag) {
    var ridSet = new RidSet();
    for (RidPair pair : linkBag) {
      ridSet.add(ridFromPair(pair));
    }
    return ridSet;
  }

  /**
   * Single-source index scan: filtered by RidSet, only matching records loaded.
   * Output is sorted by the ORDER BY property (pre-sorted).
   */
  private ExecutionStream indexScanFiltered(
      RidSet ridSet, CommandContext ctx, Result upstreamRow) {
    var session = ctx.getDatabaseSession();
    var indexDesc = new IndexSearchDescriptor(index);
    var filteredStep = new RidFilteredIndexValuesStep(
        indexDesc, orderAsc, nullsFirst(), ctx, profilingEnabled, ridSet);
    var indexStream = filteredStep.internalStart(ctx);

    return indexStream.map((indexResult, mapCtx) -> {
      var rid = (RID) indexResult.getProperty("rid");
      var targetRecord = loadRecord(rid, session);
      if (targetRecord == null) {
        return null;
      }
      if (!matchesTargetFilter(targetRecord, rid, mapCtx)) {
        return null;
      }
      if (isAlreadyBoundAndDifferent(upstreamRow, targetRecord, session)) {
        return null;
      }
      return new MatchResultRow(session, upstreamRow, targetAlias, targetRecord);
    }).filter(ExecutionStream.IDENTITY_FILTER);
  }

  /**
   * Single-source low-density fallback with downstream edges + LIMIT:
   * load all targets from the source LinkBag, sort by ORDER BY property,
   * and emit as a pre-sorted stream. With PRE_SORTED=true, OrderByStep
   * passes through and LimitStep stops after K rows — only K records
   * traverse expensive downstream MATCH edges instead of all N.
   *
   * <p>Iterates the LinkBag directly instead of going through an
   * intermediate RidSet: the cost-model already decided we are not running
   * an index scan, so the RidSet bitmap would be pure allocation overhead.
   */
  @SuppressWarnings("unchecked")
  private ExecutionStream loadSortFromLinkBag(
      LinkBag linkBag, CommandContext ctx, Result upstreamRow) {
    var session = ctx.getDatabaseSession();
    // Class filter is per LinkBag entry; resolve the target class once for the scan.
    var targetClass = resolveTargetClass(ctx);

    var records = new ArrayList<Result>();
    for (RidPair pair : linkBag) {
      var rid = ridFromPair(pair);
      var record = loadRecord(rid, session);
      if (record != null
          && matchesTargetFilter(record, rid, ctx, targetClass)
          && !isAlreadyBoundAndDifferent(upstreamRow, record, session)) {
        records.add(record);
      }
    }

    sortByOrderProperty(records);

    return ExecutionStream.resultIterator(
        records.stream()
            .map(record -> (Result) new MatchResultRow(
                session, upstreamRow, targetAlias, record))
            .iterator());
  }

  /**
   * Single-source low-density fallback: load targets directly from the
   * source LinkBag without sorting. Downstream OrderByStep handles sorting
   * with its bounded heap (or unbounded collect when there is no LIMIT).
   *
   * <p>Streams lazily from the LinkBag iterator — no intermediate RidSet
   * and no up-front materialisation.
   */
  private ExecutionStream loadFromLinkBag(
      LinkBag linkBag, CommandContext ctx, Result upstreamRow) {
    var session = ctx.getDatabaseSession();
    var targetClass = resolveTargetClass(ctx);
    var iter = linkBag.iterator();
    return ExecutionStream.resultIterator(new Iterator<Result>() {
      private Result pending;

      @Override
      public boolean hasNext() {
        while (pending == null && iter.hasNext()) {
          var rid = ridFromPair(iter.next());
          var record = loadRecord(rid, session);
          if (record != null
              && matchesTargetFilter(record, rid, ctx, targetClass)
              && !isAlreadyBoundAndDifferent(upstreamRow, record, session)) {
            pending = new MatchResultRow(session, upstreamRow, targetAlias, record);
          }
        }
        return pending != null;
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException();
        }
        var result = pending;
        pending = null;
        return result;
      }
    });
  }

  // =====================================================================
  // Sort helper
  // =====================================================================

  /**
   * Sorts records by the ORDER BY property from the index definition. When the planner accepted a
   * trailing record identifier item, equal property values are also ordered by identifier. This
   * reproduces a forward index scan's {@code (property ASC, rid ASC)} order and reverses both keys
   * for a backward scan. A one-item ORDER BY keeps its existing unspecified tie order.
   *
   * <p>Used by loadSortFromLinkBag to produce pre-sorted output that enables LIMIT-based early
   * termination through downstream MATCH edges.
   *
   * <p>The planner resolves null placement once and stores it in this step. The shared ORDER BY
   * comparison then applies explicit clauses and direction-specific defaults consistently.
   */
  private void sortByOrderProperty(List<Result> records) {
    records.sort((a, b) -> {
      var cmp = comparisonItem.compare(a, b, ctx, nullsPlacement);
      if (cmp == 0 && ridTieBreakAccepted) {
        cmp = a.getIdentity().compareTo(b.getIdentity());
        return orderAsc ? cmp : -cmp;
      }
      return cmp;
    });
  }

  private boolean nullsFirst() {
    return comparisonItem.nullsFirstFor(nullsPlacement);
  }

  // =====================================================================
  // Multi-source mode — dispatch
  // =====================================================================

  /** Routes to the appropriate multi-source strategy based on the mode. */
  private ExecutionStream multiSourceDispatch(CommandContext ctx) {
    return switch (multiSourceMode) {
      case FILTERED_BOUND -> filteredBound(ctx);
      case FILTERED_UNBOUND -> filteredUnbound(ctx);
      case UNFILTERED_BOUND -> {
        // Full index scan + reverse-edge class check; no RidSet filter.
        chosenRuntimePath = RuntimePath.GLOBAL_SCAN;
        signalIndexOrderedOutput(ctx);
        yield unfilteredBound(ctx);
      }
      case UNFILTERED_UNBOUND -> {
        chosenRuntimePath = RuntimePath.GLOBAL_SCAN;
        signalIndexOrderedOutput(ctx);
        yield unfilteredUnbound(ctx);
      }
      case null -> throw new IllegalStateException("multiSourceMode is null");
    };
  }

  // ---- Mode A: FILTERED_BOUND (sourceMap + reverse lookup) ----

  /**
   * Materializes filtered upstream rows into sourceMap, then picks the
   * cheapest scan strategy (union/global/sort) with reverse edge lookup.
   */
  private ExecutionStream filteredBound(CommandContext ctx) {
    var session = ctx.getDatabaseSession();

    // LinkedHashMap preserves insertion order for deterministic sampling
    // in estimateTotalEdges().
    var sourceMap = new LinkedHashMap<RID, List<Result>>();
    int sourceCount = 0;
    var upstream = prev.start(ctx);
    while (upstream.hasNext(ctx)) {
      var row = upstream.next(ctx);
      var sourceRid = extractSourceRid(row);
      if (sourceRid != null) {
        sourceMap.computeIfAbsent(sourceRid, k -> new ArrayList<>(1)).add(row);
        sourceCount++;
      }
    }
    upstream.close(ctx);

    if (sourceMap.isEmpty()) {
      return ExecutionStream.empty();
    }

    int maxSources =
        GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SOURCES.getValueAsInteger();
    if (sourceCount > maxSources) {
      chosenRuntimePath = RuntimePath.LOAD_UNSORTED_MULTI;
      ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return loadFromSourcesUnsorted(sourceMap, ctx);
    }

    long indexSize = index.size(session);
    var histogram = index.getHistogram(session);
    int estimatedTotalEdges = estimateTotalEdges(sourceMap, session);
    var strategy = pickMultiSourceStrategy(
        estimatedTotalEdges, indexSize, histogram);

    return switch (strategy) {
      case UNION_RIDSET_SCAN -> {
        chosenRuntimePath = RuntimePath.UNION_SCAN;
        signalIndexOrderedOutput(ctx);
        yield indexScanWithUnion(sourceMap, ctx);
      }
      case GLOBAL_SCAN -> {
        chosenRuntimePath = RuntimePath.GLOBAL_SCAN;
        signalIndexOrderedOutput(ctx);
        yield indexScanGlobal(sourceMap, ctx);
      }
      case LOAD_ALL_SORT -> {
        chosenRuntimePath = RuntimePath.LOAD_UNSORTED_MULTI;
        ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
        yield loadFromSourcesUnsorted(sourceMap, ctx);
      }
    };
  }

  // ---- Mode B: FILTERED_UNBOUND (union RidSet, no binding) ----

  /**
   * Builds union RidSet from filtered upstream LinkBags. No sourceMap needed
   * — just bitmap check per entry. Source alias is NOT bound in results.
   */
  private ExecutionStream filteredUnbound(CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    int maxRidSetSize =
        GlobalConfiguration.QUERY_PREFILTER_MAX_RIDSET_SIZE.getValueAsInteger();

    // Collect upstream source RIDs + build union RidSet simultaneously.
    // If union exceeds maxRidSetSize, stop adding to union but keep collecting
    // source RIDs for the fallback path. When the source count exceeds
    // maxSources the index scan is no longer profitable, so release the
    // (expensive, per-edge) union RidSet and fall back to streaming from all
    // source LinkBags. We keep collecting the (cheap) source RID list to the
    // end: dropping sources here would silently truncate results. This mirrors
    // filteredBound, which retains the full sourceMap on overflow.
    int maxSources =
        GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SOURCES.getValueAsInteger();
    var sourceRids = new ArrayList<RID>();
    var unionRidSet = new RidSet();
    boolean ridSetOverflow = false;
    boolean sourceOverflow = false;
    var upstream = prev.start(ctx);
    while (upstream.hasNext(ctx)) {
      var row = upstream.next(ctx);
      var sourceRid = extractSourceRid(row);
      if (sourceRid != null) {
        sourceRids.add(sourceRid);
        if (!sourceOverflow && sourceRids.size() > maxSources) {
          sourceOverflow = true;
          ridSetOverflow = true;
          unionRidSet = new RidSet(); // release partial set; we will fall back
        }
      }
      if (!ridSetOverflow) {
        var ridSet = resolveEdgeRidSet(row, session);
        if (ridSet != null) {
          for (var rid : ridSet) {
            unionRidSet.add(rid);
          }
          if (unionRidSet.size() > maxRidSetSize) {
            ridSetOverflow = true;
            unionRidSet = new RidSet(); // release partial set
          }
        }
      }
    }
    upstream.close(ctx);

    if (sourceRids.isEmpty()) {
      return ExecutionStream.empty();
    }

    // If source or union overflowed, or cost model rejects, stream from
    // source LinkBags without sorting. OrderByStep handles sort downstream.
    long indexSize = index.size(session);
    var histogram = index.getHistogram(session);
    if (sourceOverflow || ridSetOverflow || unionRidSet.isEmpty()
        || !shouldUseIndexScan(unionRidSet.size(), indexSize, histogram)) {
      chosenRuntimePath = RuntimePath.LOAD_UNSORTED_MULTI;
      ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return loadFromSourcesUnbound(sourceRids, ctx);
    }

    chosenRuntimePath = RuntimePath.UNION_SCAN;
    signalIndexOrderedOutput(ctx);
    var indexDesc = new IndexSearchDescriptor(index);
    var filteredStep = new RidFilteredIndexValuesStep(
        indexDesc, orderAsc, nullsFirst(), ctx, profilingEnabled, unionRidSet);
    var indexStream = filteredStep.internalStart(ctx);

    // Shared empty upstream — safe because MatchResultRow never writes to parent
    var emptyUpstream = new ResultInternal(session);
    return indexStream.map((indexResult, mapCtx) -> {
      var rid = (RID) indexResult.getProperty("rid");
      var targetRecord = loadRecord(rid, session);
      if (targetRecord == null) {
        return null;
      }
      if (!matchesTargetFilter(targetRecord, rid, mapCtx)) {
        return null;
      }
      return new MatchResultRow(session, emptyUpstream, targetAlias, targetRecord);
    }).filter(ExecutionStream.IDENTITY_FILTER);
  }

  /**
   * Mode B fallback: iterate source LinkBags, load targets, emit
   * without sorting or source binding. OrderByStep sorts downstream.
   */
  private ExecutionStream loadFromSourcesUnbound(
      List<RID> sourceRids, CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    var emptyUpstream = new ResultInternal(session);
    return batchedStream(
        sourceRids.iterator(),
        sourceRid -> loadSourceEdgesUnbound(sourceRid, emptyUpstream, session, ctx));
  }

  private ExecutionStream loadSourceEdgesUnbound(
      RID sourceRid, Result emptyUpstream,
      DatabaseSessionEmbedded session, CommandContext ctx) {
    var linkBag = loadLinkBag(sourceRid, session);
    if (linkBag == null) {
      return ExecutionStream.empty();
    }
    var results = new ArrayList<Result>();
    forEachMatchingTarget(linkBag, session, ctx, record -> results.add(
        new MatchResultRow(session, emptyUpstream, targetAlias, record)));
    return ExecutionStream.resultIterator(results.iterator());
  }

  /**
   * Iterates a source LinkBag, loading each target record (via
   * {@link #ridFromPair}) and applying {@link #matchesTargetFilter}. Every
   * target that loads and passes the filter is handed to {@code consumer}.
   * Shared by the eager list-building fallback paths (bound and unbound) so the
   * load-and-filter loop is written once.
   */
  private void forEachMatchingTarget(
      LinkBag linkBag, DatabaseSessionEmbedded session, CommandContext ctx,
      java.util.function.Consumer<Result> consumer) {
    var targetClass = resolveTargetClass(ctx);
    for (RidPair pair : linkBag) {
      var rid = ridFromPair(pair);
      var record = loadRecord(rid, session);
      if (record != null && matchesTargetFilter(record, rid, ctx, targetClass)) {
        consumer.accept(record);
      }
    }
  }

  // ---- Mode C: UNFILTERED_BOUND (class check + lazy load) ----

  /**
   * No sourceMap, no union. Scans index globally, per hit: loads record,
   * follows reverse edge, verifies source class, loads source on-demand
   * for binding.
   */
  private ExecutionStream unfilteredBound(CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    var srcClass = schema.getClassInternal(sourceClassName);

    // Start and immediately close upstream: this mode does not read source
    // rows (it scans the index and class-checks reverse edges), but the
    // upstream chain must still be started/closed so its own side effects and
    // resources run and release.
    var upstream = prev.start(ctx);
    upstream.close(ctx);

    if (srcClass == null) {
      return ExecutionStream.empty();
    }

    var indexDesc = new IndexSearchDescriptor(index);
    var fullScan = new RidFilteredIndexValuesStep(
        indexDesc, orderAsc, nullsFirst(), ctx, profilingEnabled, null);
    var indexStream = fullScan.internalStart(ctx);

    return indexStream.flatMap((indexResult, mapCtx) -> {
      var rid = (RID) indexResult.getProperty("rid");
      var targetRecord = loadRecord(rid, session);
      if (targetRecord == null) {
        return ExecutionStream.empty();
      }
      if (!matchesTargetFilter(targetRecord, rid, mapCtx)) {
        return ExecutionStream.empty();
      }

      // Check ALL reverse edges — a target may link to multiple valid
      // sources of the correct class. Emit one row per valid source.
      var reverseRids = resolveReverseEdges(targetRecord);
      var results = new ArrayList<Result>(1);
      for (var sourceRid : reverseRids) {
        if (!srcClass.hasPolymorphicCollectionId(sourceRid.getCollectionId())) {
          continue;
        }
        var sourceRecord = loadRecord(sourceRid, session);
        if (sourceRecord == null) {
          continue;
        }
        var upstreamRow = new ResultInternal(session);
        ((ResultInternal) upstreamRow).setProperty(sourceAlias, sourceRecord);
        results.add(
            new MatchResultRow(session, upstreamRow, targetAlias, targetRecord));
      }
      return ExecutionStream.resultIterator(results.iterator());
    });
  }

  // ---- Mode D: UNFILTERED_UNBOUND (class check, no source load) ----

  /**
   * Lightest mode: scan index, per hit verify reverse edge points to correct
   * source class (no load of source, no binding). Only target alias bound.
   */
  private ExecutionStream unfilteredUnbound(CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    var srcClass = schema.getClassInternal(sourceClassName);

    // Start and immediately close upstream: this mode does not read source
    // rows, but the upstream chain must still be started/closed so its own
    // side effects and resources run and release.
    var upstream = prev.start(ctx);
    upstream.close(ctx);

    if (srcClass == null) {
      return ExecutionStream.empty();
    }

    var indexDesc = new IndexSearchDescriptor(index);
    var fullScan = new RidFilteredIndexValuesStep(
        indexDesc, orderAsc, nullsFirst(), ctx, profilingEnabled, null);
    var indexStream = fullScan.internalStart(ctx);

    var emptyUpstream = new ResultInternal(session);
    return indexStream.map((indexResult, mapCtx) -> {
      var rid = (RID) indexResult.getProperty("rid");
      var targetRecord = loadRecord(rid, session);
      if (targetRecord == null) {
        return null;
      }
      if (!matchesTargetFilter(targetRecord, rid, mapCtx)) {
        return null;
      }

      // Source is not bound — just verify ANY reverse edge points to a
      // source of the correct class. Check all because the first might
      // point to a wrong-class vertex while a later one is valid.
      var reverseRids = resolveReverseEdges(targetRecord);
      boolean anyValid = false;
      for (var sourceRid : reverseRids) {
        if (srcClass.hasPolymorphicCollectionId(sourceRid.getCollectionId())) {
          anyValid = true;
          break;
        }
      }
      if (!anyValid) {
        return null;
      }

      return new MatchResultRow(session, emptyUpstream, targetAlias, targetRecord);
    }).filter(ExecutionStream.IDENTITY_FILTER);
  }

  /**
   * Strategy 1: Build union RidSet from all sources' LinkBags, scan index
   * with bitmap filter. Only matching records are loaded. Per match: reverse
   * edge lookup to find and bind the upstream row.
   */
  private ExecutionStream indexScanWithUnion(
      Map<RID, List<Result>> sourceMap, CommandContext ctx) {
    var session = ctx.getDatabaseSession();

    // Build union RidSet from all sources' LinkBags.
    // If union exceeds maxRidSetSize, fall back to unsorted streaming.
    int maxRidSetSize =
        GlobalConfiguration.QUERY_PREFILTER_MAX_RIDSET_SIZE.getValueAsInteger();
    var unionRidSet = new RidSet();
    boolean overflow = false;
    for (var sourceRid : sourceMap.keySet()) {
      if (overflow) {
        break;
      }
      var linkBag = loadLinkBag(sourceRid, session);
      if (linkBag != null) {
        for (RidPair pair : linkBag) {
          unionRidSet.add(ridFromPair(pair));
        }
        if (unionRidSet.size() > maxRidSetSize) {
          overflow = true;
        }
      }
    }

    if (overflow) {
      chosenRuntimePath = RuntimePath.LOAD_UNSORTED_MULTI;
      ctx.setSystemVariable(
          CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.FALSE);
      return loadFromSourcesUnsorted(sourceMap, ctx);
    }

    if (unionRidSet.isEmpty()) {
      return ExecutionStream.empty();
    }

    // Scan index filtered by union RidSet (seqRead per entry, bitmap check)
    var indexDesc = new IndexSearchDescriptor(index);
    var filteredStep = new RidFilteredIndexValuesStep(
        indexDesc, orderAsc, nullsFirst(), ctx, profilingEnabled, unionRidSet);
    var indexStream = filteredStep.internalStart(ctx);

    // Per match: load record, reverse edge → find upstream row(s).
    // flatMap handles shared targets (one target linked to multiple sources).
    return indexStream.flatMap(
        (indexResult, mapCtx) -> matchTargetToSources(indexResult, sourceMap, mapCtx));
  }

  /**
   * Strategy 2: Scan index without filter, load every entry, check reverse
   * edge against sourceMap. Cheaper than union when density is high and LIMIT
   * is small (avoids union build cost).
   */
  private ExecutionStream indexScanGlobal(
      Map<RID, List<Result>> sourceMap, CommandContext ctx) {
    var indexDesc = new IndexSearchDescriptor(index);
    var fullScan = new RidFilteredIndexValuesStep(
        indexDesc, orderAsc, nullsFirst(), ctx, profilingEnabled, null);
    var indexStream = fullScan.internalStart(ctx);

    return indexStream.flatMap(
        (indexResult, mapCtx) -> matchTargetToSources(indexResult, sourceMap, mapCtx));
  }

  /**
   * Low-density fallback: iterate all sources' LinkBags, load targets,
   * emit WITHOUT sorting. Downstream OrderByStep handles sort with
   * bounded heap (O(LIMIT) memory).
   */
  private ExecutionStream loadFromSourcesUnsorted(
      Map<RID, List<Result>> sourceMap, CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    return batchedStream(
        sourceMap.entrySet().iterator(),
        entry -> loadSourceEdgesBound(entry, session, ctx));
  }

  /** Load edges from one source, emit as MatchResultRows with source binding. */
  private ExecutionStream loadSourceEdgesBound(
      Map.Entry<RID, List<Result>> entry,
      DatabaseSessionEmbedded session, CommandContext ctx) {
    var linkBag = loadLinkBag(entry.getKey(), session);
    if (linkBag == null) {
      return ExecutionStream.empty();
    }
    var upstreamRows = entry.getValue();
    var results = new ArrayList<Result>();
    forEachMatchingTarget(linkBag, session, ctx, record -> {
      for (var row : upstreamRows) {
        if (!isAlreadyBoundAndDifferent(row, record, session)) {
          results.add(
              new MatchResultRow(session, row, targetAlias, record));
        }
      }
    });
    return ExecutionStream.resultIterator(results.iterator());
  }

  // =====================================================================
  // Cost-based heuristic
  // =====================================================================

  /**
   * Single-source: compare RidSet-filtered index scan vs load-all-and-sort.
   * Callers should cache indexSize/histogram to avoid repeated index lookups.
   */
  private boolean shouldUseIndexScan(
      int linkBagSize, long indexSize,
      @Nullable EquiDepthHistogram histogram) {
    var costs = IndexOrderedCostModel.computeCosts(
        linkBagSize, indexSize, limit, histogram, orderAsc,
        downstreamEdgeCount);
    if (costs == null) {
      return false;
    }
    return costs.costUnionScan() < costs.costLoadSort();
  }

  /**
   * Multi-source: pick the cheapest of three strategies.
   *
   * <ul>
   *   <li><b>Union RidSet scan</b> — build union from all LinkBags, scan
   *       index with bitmap filter (seqRead per entry), load only matches,
   *       reverse edge lookup per match. Best at low density.</li>
   *   <li><b>Global scan</b> — scan index without filter, load every entry
   *       (randRead per entry), check reverse edge. Best at high density +
   *       small LIMIT (avoids union build cost).</li>
   *   <li><b>Load all + sort</b> — iterate all LinkBags, load everything,
   *       sort in-memory. Best when index scan is too expensive.</li>
   * </ul>
   */
  private IndexOrderedCostModel.MultiSourceStrategy pickMultiSourceStrategy(
      int totalEdges, long indexSize,
      @Nullable EquiDepthHistogram histogram) {
    return IndexOrderedCostModel.pickMultiSourceStrategy(
        totalEdges, indexSize, limit, histogram, orderAsc);
  }

  // Cost model logic lives in IndexOrderedCostModel.

  /**
   * For a single index hit, loads the target record, follows reverse edges to
   * find ALL matching source vertices, and emits one MatchResultRow per
   * (source, target) pair. Handles shared targets correctly.
   */
  private ExecutionStream matchTargetToSources(
      Result indexResult,
      Map<RID, List<Result>> sourceMap,
      CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    var rid = (RID) indexResult.getProperty("rid");
    var targetRecord = loadRecord(rid, session);
    if (targetRecord == null) {
      return ExecutionStream.empty();
    }
    if (!matchesTargetFilter(targetRecord, rid, ctx)) {
      return ExecutionStream.empty();
    }

    var sourceRids = resolveReverseEdges(targetRecord);
    var results = new ArrayList<Result>(1);
    for (var sourceRid : sourceRids) {
      var upstreamRows = sourceMap.get(sourceRid);
      if (upstreamRows == null) {
        continue;
      }
      for (var upstreamRow : upstreamRows) {
        if (!isAlreadyBoundAndDifferent(upstreamRow, targetRecord, session)) {
          results.add(
              new MatchResultRow(session, upstreamRow, targetAlias, targetRecord));
        }
      }
    }
    return ExecutionStream.resultIterator(results.iterator());
  }

  // =====================================================================
  // Helpers
  // =====================================================================

  /**
   * Creates a lazy ExecutionStream that iterates over sources, producing a
   * batch ExecutionStream per source via {@code batchProducer}. Advances to
   * the next source when the current batch is exhausted.
   */
  private static <T> ExecutionStream batchedStream(
      Iterator<T> sourceIter,
      Function<T, ExecutionStream> batchProducer) {
    return new ExecutionStream() {
      private ExecutionStream currentBatch = ExecutionStream.empty();

      @Override
      public boolean hasNext(CommandContext c) {
        while (!currentBatch.hasNext(c)) {
          currentBatch.close(c);
          if (!sourceIter.hasNext()) {
            return false;
          }
          currentBatch = batchProducer.apply(sourceIter.next());
        }
        return true;
      }

      @Override
      public Result next(CommandContext c) {
        return currentBatch.next(c);
      }

      @Override
      public void close(CommandContext c) {
        currentBatch.close(c);
      }
    };
  }

  /** Extract the source vertex RID from an upstream row. */
  @Nullable private RID extractSourceRid(Result upstreamRow) {
    var sourceRecord = upstreamRow.getProperty(sourceAlias);
    if (sourceRecord instanceof Result result) {
      return result.getIdentity();
    } else if (sourceRecord instanceof RID rid) {
      return rid;
    }
    return null;
  }

  /**
   * Follow the reverse edge on a target record to find ALL source vertex RIDs.
   * Returns all RIDs from the reverse LinkBag (handles shared targets where
   * a record is linked to multiple sources).
   */
  private List<RID> resolveReverseEdges(Result targetRecord) {
    if (reverseFieldName == null) {
      return List.of();
    }
    var entity = targetRecord.asEntityOrNull();
    if (!(entity instanceof EntityImpl impl)) {
      return List.of();
    }

    if (edgeTraversal) {
      // Edge record: reverse field ("out"/"in") is a direct LINK to a single vertex,
      // not a LinkBag.
      var link = impl.getLinkPropertyInternal(reverseFieldName);
      return link != null ? List.of(link) : List.of();
    }

    // Vertex: reverse field is a LinkBag (e.g., out_LIKES).
    var fieldValue = impl.getPropertyInternal(reverseFieldName);
    if (fieldValue instanceof LinkBag linkBag) {
      var rids = new ArrayList<RID>(linkBag.size());
      for (RidPair pair : linkBag) {
        rids.add(pair.secondaryRid());
      }
      return rids;
    } else if (fieldValue instanceof RID rid) {
      return List.of(rid);
    }
    return List.of();
  }

  /**
   * Estimate total edges for multi-source cost model by sampling up to 5
   * source vertices' LinkBag sizes, then extrapolating to all sources.
   * Falls back to {@code sourceCount × defaultFanOut} if sampling fails.
   */
  private int estimateTotalEdges(
      Map<RID, ?> sourceMap, DatabaseSessionEmbedded session) {
    int sampleSize = Math.min(sourceMap.size(), 5);
    int totalSampled = 0;
    int sampled = 0;
    for (var sourceRid : sourceMap.keySet()) {
      if (sampled >= sampleSize) {
        break;
      }
      var linkBag = loadLinkBag(sourceRid, session);
      if (linkBag != null) {
        totalSampled += linkBag.size();
        sampled++;
      }
    }
    // Round rather than truncate: integer division would systematically
    // undercount the fan-out (e.g. an average of 9.8 edges/source read as 9),
    // biasing the cost model toward load-all-and-sort.
    long avgPerSource = sampled > 0
        ? Math.round((double) totalSampled / sampled)
        : GlobalConfiguration.QUERY_STATS_DEFAULT_FAN_OUT.getValueAsInteger();
    return (int) Math.min(
        (long) sourceMap.size() * avgPerSource, Integer.MAX_VALUE);
  }

  /**
   * Extracts the appropriate RID from a LinkBag pair based on traversal mode.
   * For vertex traversal (.in/.out): returns secondaryRid (opposite vertex).
   * For edge traversal (.inE/.outE): returns primaryRid (edge record).
   */
  private RID ridFromPair(RidPair pair) {
    return edgeTraversal ? pair.primaryRid() : pair.secondaryRid();
  }

  /**
   * Loads the entity for the given RID and extracts its LinkBag field.
   * Returns null if the record cannot be loaded or the field is not a LinkBag.
   */
  @Nullable private LinkBag loadLinkBag(
      RID sourceRid, DatabaseSessionEmbedded session) {
    try {
      var rec = session.getActiveTransaction().load(sourceRid);
      if (!(rec instanceof EntityImpl entity)) {
        return null;
      }
      var fieldValue = entity.getPropertyInternal(linkBagFieldName);
      return fieldValue instanceof LinkBag lb ? lb : null;
    } catch (Exception e) {
      logger.debug("Failed to load LinkBag for RID {}: {}", sourceRid, e.getMessage(), e);
      return null;
    }
  }

  @Nullable private RidSet resolveEdgeRidSet(
      Result upstreamRow, DatabaseSessionEmbedded session) {
    var sourceRid = extractSourceRid(upstreamRow);
    if (sourceRid == null) {
      return null;
    }

    var linkBag = loadLinkBag(sourceRid, session);
    if (linkBag == null) {
      return null;
    }

    var ridSet = new RidSet();
    for (RidPair pair : linkBag) {
      ridSet.add(ridFromPair(pair));
    }
    return ridSet;
  }

  @Nullable private Result loadRecord(
      RID rid, DatabaseSessionEmbedded session) {
    try {
      var rec = session.getActiveTransaction().load(rid);
      if (rec == null) {
        return null;
      }
      return new ResultInternal(session, rec);
    } catch (Exception e) {
      logger.debug("Failed to load record {}: {}", rid, e.getMessage(), e);
      return null;
    }
  }

  /** Consistency check: if target alias was already bound, verify match. */
  private boolean isAlreadyBoundAndDifferent(
      Result upstreamRow, Result targetRecord,
      DatabaseSessionEmbedded session) {
    var prevValue = ResultInternal.toResult(
        upstreamRow.getProperty(targetAlias), session);
    return prevValue != null && !Objects.equals(targetRecord, prevValue);
  }

  /**
   * Resolves the MATCH target class from the session's immutable schema
   * snapshot. The planner refuses to build this step unless the target class
   * name resolves at plan time, so {@code null} here means the class was
   * dropped between planning and execution.
   *
   * <p>The snapshot is the required source, not merely a convenient one. Its
   * classes carry a precomputed polymorphic collection-id set, which is what
   * makes the per-record membership test in
   * {@link #matchesTargetFilter(Result, RID, CommandContext, SchemaClass)}
   * lock-free. Reading the class off the live shared schema instead would
   * reintroduce the schema lock this step exists to avoid.
   */
  @Nullable private SchemaClass resolveTargetClass(CommandContext ctx) {
    var schema = ctx.getDatabaseSession().getMetadata().getImmutableSchemaSnapshot();
    assert schema != null;
    return schema.getClassInternal(targetClassName);
  }

  /**
   * Convenience overload for call sites that filter a single record and have no
   * loop to hoist the class resolution out of. Loops should resolve the class
   * once with {@link #resolveTargetClass} and call the four-argument form.
   */
  private boolean matchesTargetFilter(
      Result targetRecord, RID targetRid, CommandContext ctx) {
    return matchesTargetFilter(
        targetRecord, targetRid, ctx, resolveTargetClass(ctx));
  }

  /**
   * Checks if a target record passes the WHERE filter and class constraint.
   * Returns true if the record should be included, false if filtered out.
   *
   * <p>Class membership is decided from the record's collection id against
   * {@code targetClass}, which covers the target class and its subclasses. That
   * is equivalent to an {@code isSubClassOf} check, but it does not take the
   * shared schema lock on every record — important when many targets are
   * filtered under concurrent queries. Same approach as
   * {@link #unfilteredBound}.
   *
   * <p>The caller supplies {@code targetClass} so a filtering loop resolves it
   * once instead of once per record.
   */
  private boolean matchesTargetFilter(
      Result targetRecord,
      RID targetRid,
      CommandContext ctx,
      @Nullable SchemaClass targetClass) {
    if (targetFilter != null
        && !targetFilter.matchesFilters(targetRecord, ctx)) {
      return false;
    }
    // A class dropped between planning and execution leaves nothing that can
    // satisfy the constraint.
    if (targetClass == null) {
      return false;
    }
    return targetClass.hasPolymorphicCollectionId(targetRid.getCollectionId());
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  /** Plan-text marker for an accepted trailing record identifier sort item. */
  public static final String RID_TIE_BREAK_MARKER = "+@rid";

  @Override
  public String prettyPrint(int depth, int indent) {
    var spaces = ExecutionStepInternal.getIndent(depth, indent);
    var direction = orderAsc ? "ASC" : "DESC";
    var mode = multiSourceMode != null ? " (" + multiSourceMode + ")" : "";
    var edgeSuffix = edgeTraversal ? "E" : "";
    var path = chosenRuntimePath != null ? " [" + chosenRuntimePath.label() + "]" : "";
    // The marker is the only plan-visible sign that the trailing record identifier item was
    // accepted, so a test can tell an accepted shape from a refused one without inferring it from
    // whether the query buffered.
    var tieBreak = ridTieBreakAccepted ? " " + RID_TIE_BREAK_MARKER : "";
    return spaces + "+ INDEX ORDERED MATCH" + edgeSuffix + " " + direction + mode + path
        + tieBreak + "\n"
        + spaces + "  {" + sourceAlias + "}." + edgeClassName
        + "{" + targetAlias + "} via " + index.getName();
  }

  @Override
  public IndexOrderedEdgeStep copy(CommandContext ctx) {
    return new IndexOrderedEdgeStep(
        ctx, sourceAlias, targetAlias, edgeClassName, linkBagFieldName,
        index, orderAsc, comparisonItem.copy(), nullsPlacement, edge.copy(), limit, multiSourceMode,
        reverseFieldName, sourceClassName, targetFilter, targetClassName,
        edgeTraversal, downstreamEdgeCount, ridTieBreakAccepted, profilingEnabled);
  }
}
