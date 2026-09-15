package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.concur.TimeoutException;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionStep;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.sql.OrderByNullsUtil;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;

/**
 * Intermediate <b>blocking</b> step that sorts all upstream records according to an
 * ORDER BY clause.
 *
 * <p>This step must consume the entire upstream before producing any output (it cannot
 * stream sorted results lazily). The sorted records are cached in-memory.
 *
 * <h2>Bounded path (ORDER BY + LIMIT)</h2>
 * When {@code maxResults} is set (derived from SKIP + LIMIT), a bounded min-heap
 * (priority queue) of size {@code maxResults} is used. Each incoming row is compared
 * against the heap's worst element (O(1) peek). Rows that rank higher replace the
 * worst element (O(log N) re-heapify); rows that rank lower are discarded immediately.
 * After all upstream rows are consumed, the heap is drained and sorted to produce the
 * final ordered output.
 *
 * <p>This reduces memory from O(|all results|) to O(N) and time from
 * O(|results| &times; log(|results|)) to O(|results| &times; log(N)).
 *
 * <h2>Unbounded path (ORDER BY without LIMIT)</h2>
 * All upstream rows are collected into a list and sorted once. The step respects
 * {@code QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} and throws a
 * {@link CommandExecutionException} if the result set exceeds the configured limit.
 *
 * @see SelectExecutionPlanner#handleOrderBy
 * @see SelectExecutionPlanner#handleProjectionsBlock
 */
public class OrderByStep extends AbstractExecutionStep {

  /** The ORDER BY clause defining sort keys and directions. */
  private final SQLOrderBy orderBy;

  /** Query timeout in milliseconds (-1 = no timeout). */
  private final long timeoutMillis;

  /**
   * When non-null, the maximum number of results needed (SKIP + LIMIT).
   * Enables the bounded min-heap path that holds at most this many elements.
   */
  private Integer maxResults;

  /**
   * When non-null, the input stream is known to be sorted by this ORDER BY
   * item (typically the first field). In the bounded heap path, the heap
   * can stop reading when this item's value is strictly "worse" (sorts
   * later) than the worst element in the heap — because all subsequent
   * items will also be worse.
   *
   * <p>Set by the MATCH planner when IndexOrderedEdgeStep produces results
   * sorted by the primary ORDER BY field, enabling early termination for
   * multi-field ORDER BY queries.
   */
  @Nullable private final SQLOrderByItem primaryKeySortedInput;

  /**
   * When true, the upstream may be an {@code IndexOrderedEdgeStep} that
   * signals at runtime whether its output is already sorted. If the
   * runtime context variable {@code indexOrderedPreSorted} is {@code true},
   * this step becomes a pass-through (streaming, no sort, no buffering).
   * Otherwise it falls back to normal bounded-heap or unbounded sort.
   */
  private final boolean indexOrderedUpstream;

  /**
   * @param orderBy          the ORDER BY clause defining sort keys and directions
   * @param maxResults       max rows needed (SKIP+LIMIT); null for unlimited.
   *                         When set, enables the bounded min-heap path.
   * @param ctx              the query context
   * @param timeoutMillis    query timeout in milliseconds (-1 = no timeout)
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public OrderByStep(
      SQLOrderBy orderBy,
      Integer maxResults,
      CommandContext ctx,
      long timeoutMillis,
      boolean profilingEnabled) {
    this(orderBy, maxResults, null, false, ctx, timeoutMillis, profilingEnabled);
  }

  /**
   * @param primaryKeySortedInput when non-null, enables early termination in
   *        the bounded heap: stop reading when this item's value is strictly
   *        worse than the worst in the heap.
   * @param indexOrderedUpstream when true, check runtime context for pre-sorted
   *        signal from IndexOrderedEdgeStep; if sorted, pass through without sorting
   */
  public OrderByStep(
      SQLOrderBy orderBy,
      Integer maxResults,
      @Nullable SQLOrderByItem primaryKeySortedInput,
      boolean indexOrderedUpstream,
      CommandContext ctx,
      long timeoutMillis,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.orderBy = orderBy;
    this.maxResults = maxResults;
    if (this.maxResults != null && this.maxResults < 0) {
      this.maxResults = null;
    }
    this.primaryKeySortedInput = primaryKeySortedInput;
    this.indexOrderedUpstream = indexOrderedUpstream;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    if (prev == null) {
      return ExecutionStream.empty();
    }

    // Start the upstream pipeline FIRST. IndexOrderedEdgeStep sets the
    // VAR_INDEX_ORDERED_PRE_SORTED context variable inside its
    // internalStart(), which runs when we call prev.start(). Checking the
    // flag before starting upstream would always see null (unset) because
    // the pipeline is started top-down: OrderByStep.internalStart() runs
    // before IndexOrderedEdgeStep.internalStart().
    var upstream = prev.start(ctx);

    // Pass-through: IndexOrderedEdgeStep signaled that output is fully
    // pre-sorted (single-field ORDER BY). No buffering, no sorting — just
    // forward the stream for downstream LimitStep to handle early termination.
    // Multi-field ORDER BY (primaryKeySortedInput != null) still needs the
    // bounded heap for the composite sort, so pass-through is not used.
    if (indexOrderedUpstream
        && primaryKeySortedInput == null
        && Boolean.TRUE.equals(
            ctx.getSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED))) {
      return upstream;
    }

    var results = init(upstream, ctx);
    return ExecutionStream.resultIterator(results.iterator());
  }

  /**
   * Pulls all records from the provided upstream stream, sorts them, and returns the
   * sorted list.
   *
   * <p>When {@code maxResults} is set (derived from SKIP + LIMIT), a bounded min-heap
   * (priority queue) of size {@code maxResults} is used. Each incoming row is compared
   * against the heap's worst element: rows that rank higher replace the worst and
   * re-heapify; rows that rank lower are discarded immediately. This reduces memory
   * from O(|all results|) to O(maxResults) and avoids repeated full sorts.
   *
   * <p>When {@code maxResults} is not set, all rows are collected and sorted once.
   *
   * @param upstream the already-started upstream stream to pull from
   * @param ctx      the command context
   * @return the sorted (and possibly truncated) list of results
   * @throws CommandExecutionException if the number of elements exceeds
   *         {@code QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP}
   */
  private List<Result> init(ExecutionStream upstream, CommandContext ctx) {
    var timeoutBegin = System.currentTimeMillis();

    if (maxResults != null) {
      if (maxResults == 0) {
        upstream.close(ctx);
        return Collections.emptyList();
      }
      return initBoundedHeap(upstream, ctx, timeoutBegin);
    }
    return initUnbounded(upstream, ctx, timeoutBegin);
  }

  /**
   * Bounded top-N selection using a min-heap. The heap holds at most {@code maxResults}
   * elements, with the <em>worst</em> element (last in ORDER BY order) at the root.
   * A reversed comparator is used so that {@link PriorityQueue#peek()} returns the
   * worst kept element, enabling O(1) comparison for each incoming row.
   */
  private List<Result> initBoundedHeap(
      ExecutionStream upstream, CommandContext ctx, long timeoutBegin) {
    var maxElementsAllowed =
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      if (maxElementsAllowed >= 0 && maxResults > maxElementsAllowed) {
        throw new CommandExecutionException(ctx.getDatabaseSession(),
            "Limit of allowed entities for in-heap ORDER BY in a single query exceeded ("
                + maxElementsAllowed
                + ") . You can set "
                + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
                + " to increase this limit");
      }
      // Null placement is fixed for the whole scan, exactly like the early-termination flag below.
      // Resolving per comparison would take a storage read lock on every null-involved compare. A
      // configuration change mid-sort would also break the comparator contract.
      var nullsDefault = OrderByNullsUtil.resolvePlacementsForSort(ctx);

      // Reversed comparator: peek() returns the element that sorts LAST (worst in top-N).
      var heap = new PriorityQueue<Result>(
          maxResults, (a, b) -> orderBy.compare(b, a, ctx, nullsDefault));

      // Early-termination eligibility is fixed for the whole scan:
      // IndexOrderedEdgeStep sets VAR_INDEX_ORDERED_PRE_SORTED before this step
      // starts and never changes it mid-iteration, so read it once here rather
      // than per row.
      boolean earlyTerminationEnabled =
          primaryKeySortedInput != null
              && indexOrderedUpstream
              && Boolean.TRUE.equals(
                  ctx.getSystemVariable(
                      CommandContext.VAR_INDEX_ORDERED_PRE_SORTED));

      while (upstream.hasNext(ctx)) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          sendTimeout();
        }
        var item = upstream.next(ctx);

        // Early termination: when the input is sorted by the primary ORDER BY
        // field and the heap is full, stop reading as soon as the primary key
        // of the new item is strictly worse than the heap's worst element.
        // All subsequent items will also be worse (input is sorted).
        // Only active when IndexOrderedEdgeStep confirmed pre-sorted output;
        // when the fallback (unsorted) path was taken, cutoff is unsafe.
        if (earlyTerminationEnabled
            && heap.size() >= maxResults
            && primaryKeySortedInput.compare(item, heap.peek(), ctx, nullsDefault) > 0) {
          break;
        }

        if (heap.size() < maxResults) {
          heap.offer(item);
        } else if (orderBy.compare(item, heap.peek(), ctx, nullsDefault) < 0) {
          heap.poll();
          heap.offer(item);
        }
      }

      var result = new LinkedList<Result>();
      while (!heap.isEmpty()) {
        result.addFirst(heap.poll());
      }
      return result;
    } finally {
      upstream.close(ctx);
    }
  }

  /**
   * Unbounded path: collects all upstream rows, then sorts once.
   * Enforces {@code QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP}.
   */
  private List<Result> initUnbounded(
      ExecutionStream upstream, CommandContext ctx, long timeoutBegin) {
    var maxElementsAllowed =
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    List<Result> cachedResult = new ArrayList<>();
    // Fix placement when the sort starts, before a potentially long upstream drain.
    var nullsDefault = OrderByNullsUtil.resolvePlacementsForSort(ctx);
    try {
      while (upstream.hasNext(ctx)) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          sendTimeout();
        }
        var item = upstream.next(ctx);
        cachedResult.add(item);
        if (maxElementsAllowed >= 0 && maxElementsAllowed < cachedResult.size()) {
          throw new CommandExecutionException(ctx.getDatabaseSession(),
              "Limit of allowed entities for in-heap ORDER BY in a single query exceeded ("
                  + maxElementsAllowed
                  + ") . You can set "
                  + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
                  + " to increase this limit");
        }
      }
      cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx, nullsDefault));
      return cachedResult;
    } finally {
      upstream.close(ctx);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var result = ExecutionStepInternal.getIndent(depth, indent) + "+ " + orderBy;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (maxResults != null ? "\n  (buffer size: " + maxResults + ")" : "");
    return result;
  }

  /** Cacheable: the ORDER BY clause is a structural AST node deep-copied per execution. */
  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    var copiedOrderBy = orderBy.copy();
    // Derive hint from copied orderBy: find the same item by index position
    // to avoid stale reference to the original AST node.
    SQLOrderByItem copiedHint = null;
    if (primaryKeySortedInput != null) {
      int idx = orderBy.getItems().indexOf(primaryKeySortedInput);
      // Enforced unconditionally (not via assert, which is off in production):
      // primaryKeySortedInput is always one of orderBy's items, so a negative
      // index signals a construction bug and must fail loudly rather than
      // producing an obscure IndexOutOfBounds from get(-1) below.
      if (idx < 0) {
        throw new IllegalStateException(
            "primaryKeySortedInput not found in orderBy items");
      }
      copiedHint = copiedOrderBy.getItems().get(idx);
    }
    return new OrderByStep(
        copiedOrderBy, maxResults, copiedHint,
        indexOrderedUpstream, ctx, timeoutMillis, profilingEnabled);
  }
}
