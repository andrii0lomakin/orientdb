package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.common.concur.TimeoutException;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionStep;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SequentialTest.class)
@SuppressWarnings("DataFlowIssue")
public class OrderByStepTest extends DbTestBase {

  private Object previousAscending;

  @Before
  public void saveNullPlacementGlobal() {
    previousAscending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
  }

  @After
  public void restoreNullPlacementGlobal() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
  }

  private CommandContext ctx() {
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(session);
    return ctx;
  }

  private static final String SORT_FIELD = "val";

  private SQLOrderBy orderBy(String direction) {
    var item = new SQLOrderByItem();
    item.setAlias(SORT_FIELD);
    item.setType(direction);
    var orderBy = new SQLOrderBy(-1);
    orderBy.setItems(new ArrayList<>(List.of(item)));
    return orderBy;
  }

  private AbstractExecutionStep upstream(CommandContext ctx, List<Result> rows) {
    return new AbstractExecutionStep(ctx, false) {
      boolean done = false;

      @Override
      public ExecutionStep copy(CommandContext ctx) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
        if (done) {
          return ExecutionStream.empty();
        }
        done = true;
        return ExecutionStream.resultIterator(new ArrayList<>(rows).iterator());
      }
    };
  }

  private List<Result> makeRows(CommandContext ctx, int... values) {
    var rows = new ArrayList<Result>();
    for (var v : values) {
      var r = new ResultInternal(ctx.getDatabaseSession());
      r.setProperty("val", v);
      rows.add(r);
    }
    return rows;
  }

  private List<Result> collect(ExecutionStream stream, CommandContext ctx) {
    var out = new ArrayList<Result>();
    while (stream.hasNext(ctx)) {
      out.add(stream.next(ctx));
    }
    return out;
  }

  // ── Unbounded path ──

  @Test
  public void unboundedSortsAllRowsAsc() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(5, results.size());
    for (var i = 0; i < 5; i++) {
      Assert.assertEquals(i + 1, (int) results.get(i).getProperty("val"));
    }
  }

  @Test
  public void unboundedSortsAllRowsDesc() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.DESC), null, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(5, results.size());
    for (var i = 0; i < 5; i++) {
      Assert.assertEquals(5 - i, (int) results.get(i).getProperty("val"));
    }
  }

  @Test
  public void unboundedEmptyUpstream() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    step.setPrevious(upstream(ctx, List.of()));

    var results = collect(step.start(ctx), ctx);
    Assert.assertTrue(results.isEmpty());
  }

  @Test
  public void unboundedSingleRow() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 42)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(42, (int) results.getFirst().getProperty("val"));
  }

  // ── Bounded heap path ──

  @Test
  public void boundedHeapKeepsTopN() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 3, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals(1, (int) results.get(0).getProperty("val"));
    Assert.assertEquals(2, (int) results.get(1).getProperty("val"));
    Assert.assertEquals(3, (int) results.get(2).getProperty("val"));
  }

  @Test
  public void boundedHeapKeepsTopNDesc() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.DESC), 3, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals(5, (int) results.get(0).getProperty("val"));
    Assert.assertEquals(4, (int) results.get(1).getProperty("val"));
    Assert.assertEquals(3, (int) results.get(2).getProperty("val"));
  }

  @Test
  public void boundedHeapWithMaxResultsEqualToInputSize() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 5, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(5, results.size());
    for (var i = 0; i < 5; i++) {
      Assert.assertEquals(i + 1, (int) results.get(i).getProperty("val"));
    }
  }

  @Test
  public void boundedHeapWithMaxResultsLargerThanInput() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 10, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 3, 1, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals(1, (int) results.get(0).getProperty("val"));
    Assert.assertEquals(2, (int) results.get(1).getProperty("val"));
    Assert.assertEquals(3, (int) results.get(2).getProperty("val"));
  }

  @Test
  public void boundedHeapWithMaxResultsOne() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 1, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(1, (int) results.getFirst().getProperty("val"));
  }

  @Test
  public void boundedHeapWithMaxResultsZeroReturnsEmpty() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 0, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertTrue(results.isEmpty());
  }

  @Test
  public void boundedHeapDiscardsWorseElements() {
    var ctx = ctx();
    // ASC order, keep top 2 → should keep 1, 2 and discard 3, 4, 5
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 2, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 4, 2, 5, 1, 3)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(1, (int) results.get(0).getProperty("val"));
    Assert.assertEquals(2, (int) results.get(1).getProperty("val"));
  }

  @Test
  public void boundedHeapPreservesOrderWithDuplicates() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 4, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 3, 1, 3, 1, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(4, results.size());
    Assert.assertEquals(1, (int) results.get(0).getProperty("val"));
    Assert.assertEquals(1, (int) results.get(1).getProperty("val"));
    Assert.assertEquals(2, (int) results.get(2).getProperty("val"));
    Assert.assertEquals(3, (int) results.get(3).getProperty("val"));
  }

  // ── Constructor: negative maxResults treated as null (unbounded) ──

  @Test
  public void negativeMaxResultsTreatedAsUnbounded() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), -5, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, 3, 1, 2)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals(1, (int) results.get(0).getProperty("val"));
    Assert.assertEquals(2, (int) results.get(1).getProperty("val"));
    Assert.assertEquals(3, (int) results.get(2).getProperty("val"));
  }

  // ── No upstream (prev == null) ──

  @Test
  public void noPreviousStepReturnsEmpty() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);

    var results = collect(step.start(ctx), ctx);
    Assert.assertTrue(results.isEmpty());
  }

  // ── prettyPrint ──

  @Test
  public void prettyPrintShowsBufferSizeWhenBounded() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 20, ctx, -1, false);
    var text = step.prettyPrint(0, 2);
    Assert.assertTrue(
        "prettyPrint should contain buffer size", text.contains("buffer size: 20"));
  }

  @Test
  public void prettyPrintOmitsBufferSizeWhenUnbounded() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    var text = step.prettyPrint(0, 2);
    Assert.assertFalse(
        "prettyPrint should not contain buffer size", text.contains("buffer size"));
  }

  // ── copy ──

  @Test
  public void copyPreservesMaxResults() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 7, ctx, -1, false);
    var copied = (OrderByStep) step.copy(ctx);
    var text = copied.prettyPrint(0, 2);
    Assert.assertTrue(
        "copied step should preserve buffer size", text.contains("buffer size: 7"));
  }

  @Test
  public void copyPreservesUnbounded() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    var copied = (OrderByStep) step.copy(ctx);
    var text = copied.prettyPrint(0, 2);
    Assert.assertFalse(
        "copied step should preserve unbounded", text.contains("buffer size"));
  }

  // ── Bounded heap: equal elements keep the first inserted (stability) ──

  @Test
  public void boundedHeap_equalSortKey_keepsFirstInserted() {
    var ctx = ctx();
    var rows = new ArrayList<Result>();
    var r1 = new ResultInternal(ctx.getDatabaseSession());
    r1.setProperty("val", 1);
    r1.setProperty("tag", "first");
    rows.add(r1);
    var r2 = new ResultInternal(ctx.getDatabaseSession());
    r2.setProperty("val", 1);
    r2.setProperty("tag", "second");
    rows.add(r2);

    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 1, ctx, -1, false);
    step.setPrevious(upstream(ctx, rows));
    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("first", results.getFirst().getProperty("tag"));
  }

  // ── Bounded heap: maxElements boundary tests ──

  @Test
  public void boundedHeap_maxResultsEqualsElementLimit_doesNotThrow() {
    var original = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(3);
    try {
      var ctx = ctx();
      var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 3, ctx, -1, false);
      step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));
      var results = collect(step.start(ctx), ctx);
      Assert.assertEquals(3, results.size());
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(original);
    }
  }

  @Test(expected = CommandExecutionException.class)
  public void boundedHeap_maxElementsZero_throws() {
    var original = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(0);
    try {
      var ctx = ctx();
      var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 3, ctx, -1, false);
      step.setPrevious(upstream(ctx, makeRows(ctx, 1)));
      collect(step.start(ctx), ctx);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(original);
    }
  }

  // ── Unbounded: maxElements boundary tests ──

  @Test
  public void unbounded_sizeExactlyAtLimit_doesNotThrow() {
    var original = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(5);
    try {
      var ctx = ctx();
      var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
      step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));
      var results = collect(step.start(ctx), ctx);
      Assert.assertEquals(5, results.size());
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(original);
    }
  }

  @Test(expected = CommandExecutionException.class)
  public void unbounded_sizeExceedsLimit_throws() {
    var original = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(4);
    try {
      var ctx = ctx();
      var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
      step.setPrevious(upstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2)));
      collect(step.start(ctx), ctx);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(original);
    }
  }

  @Test(expected = CommandExecutionException.class)
  public void unbounded_maxElementsZero_throws() {
    var original = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(0);
    try {
      var ctx = ctx();
      var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
      step.setPrevious(upstream(ctx, makeRows(ctx, 1)));
      collect(step.start(ctx), ctx);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(original);
    }
  }

  // ── canBeCached ──

  @Test
  public void canBeCachedReturnsTrue() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    Assert.assertTrue(step.canBeCached());
  }

  // ── Boundary: bounded heap correctness with large input ──

  @Test
  public void boundedHeapLargeInput() {
    var ctx = ctx();
    var values = new int[100];
    for (var i = 0; i < 100; i++) {
      values[i] = 100 - i;
    }
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 5, ctx, -1, false);
    step.setPrevious(upstream(ctx, makeRows(ctx, values)));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(5, results.size());
    for (var i = 0; i < 5; i++) {
      Assert.assertEquals(i + 1, (int) results.get(i).getProperty("val"));
    }
  }

  // ── Stream close() verification ──

  private record TrackingUpstreamResult(
      AbstractExecutionStep step, AtomicBoolean streamClosed) {
  }

  private TrackingUpstreamResult trackingUpstream(CommandContext ctx, List<Result> rows) {
    var closed = new AtomicBoolean(false);
    var step = new AbstractExecutionStep(ctx, false) {
      boolean done = false;

      @Override
      public ExecutionStep copy(CommandContext ctx) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ExecutionStream internalStart(CommandContext ctx) {
        if (done) {
          return ExecutionStream.empty();
        }
        done = true;
        var inner = ExecutionStream.resultIterator(new ArrayList<>(rows).iterator());
        return new ExecutionStream() {
          @Override
          public boolean hasNext(CommandContext c) {
            return inner.hasNext(c);
          }

          @Override
          public Result next(CommandContext c) {
            return inner.next(c);
          }

          @Override
          public void close(CommandContext c) {
            closed.set(true);
            inner.close(c);
          }
        };
      }
    };
    return new TrackingUpstreamResult(step, closed);
  }

  @Test
  public void boundedHeap_maxResultsZero_closesUpstream() {
    var ctx = ctx();
    var tracking = trackingUpstream(ctx, makeRows(ctx, 5, 3, 1));
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 0, ctx, -1, false);
    step.setPrevious(tracking.step());

    var results = collect(step.start(ctx), ctx);
    Assert.assertTrue(results.isEmpty());
    Assert.assertTrue("Upstream stream should be closed when maxResults=0",
        tracking.streamClosed().get());
  }

  @Test
  public void boundedHeap_closesUpstreamOnCompletion() {
    var ctx = ctx();
    var tracking = trackingUpstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2));
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 3, ctx, -1, false);
    step.setPrevious(tracking.step());

    collect(step.start(ctx), ctx);
    Assert.assertTrue("Upstream stream should be closed after bounded heap completes",
        tracking.streamClosed().get());
  }

  @Test
  public void unbounded_closesUpstreamOnCompletion() {
    var ctx = ctx();
    var tracking = trackingUpstream(ctx, makeRows(ctx, 3, 1, 2));
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    step.setPrevious(tracking.step());

    collect(step.start(ctx), ctx);
    Assert.assertTrue("Upstream stream should be closed after unbounded sort completes",
        tracking.streamClosed().get());
  }

  // ── Timeout tests ──

  private AbstractExecutionStep timeoutAwareUpstream(
      CommandContext ctx, List<Result> rows, long delayPerNextMs) {
    return new AbstractExecutionStep(ctx, false) {
      boolean done = false;

      @Override
      public ExecutionStep copy(CommandContext ctx) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void sendTimeout() {
        throw new TimeoutException("test timeout");
      }

      @SuppressWarnings("BusyWait")
      @Override
      public ExecutionStream internalStart(CommandContext ctx) {
        try {
          Thread.sleep(2);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (done) {
          return ExecutionStream.empty();
        }
        done = true;
        var iter = new ArrayList<>(rows).iterator();
        return new ExecutionStream() {
          @Override
          public boolean hasNext(CommandContext c) {
            return iter.hasNext();
          }

          @SuppressWarnings("BusyWait")
          @Override
          public Result next(CommandContext c) {
            if (delayPerNextMs > 0) {
              try {
                Thread.sleep(delayPerNextMs);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            return iter.next();
          }

          @Override
          public void close(CommandContext c) {
          }
        };
      }
    };
  }

  @Test
  public void boundedHeap_timeoutZero_noTimeout() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 3, ctx, 0, false);
    step.setPrevious(timeoutAwareUpstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2), 0));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
  }

  @Test
  public void boundedHeap_largeTimeout_completesNormally() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 3, ctx, 60_000, false);
    step.setPrevious(timeoutAwareUpstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2), 0));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
  }

  @Test(expected = TimeoutException.class)
  public void boundedHeap_shortTimeout_triggersTimeout() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 5, ctx, 1, false);
    step.setPrevious(timeoutAwareUpstream(ctx, makeRows(ctx, 5, 3, 1, 4, 2), 100));

    collect(step.start(ctx), ctx);
  }

  @Test
  public void unbounded_timeoutZero_noTimeout() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, 0, false);
    step.setPrevious(timeoutAwareUpstream(ctx, makeRows(ctx, 3, 1, 2), 0));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
  }

  @Test
  public void unbounded_largeTimeout_completesNormally() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, 60_000, false);
    step.setPrevious(timeoutAwareUpstream(ctx, makeRows(ctx, 3, 1, 2), 0));

    var results = collect(step.start(ctx), ctx);
    Assert.assertEquals(3, results.size());
  }

  @Test(expected = TimeoutException.class)
  public void unbounded_shortTimeout_triggersTimeout() {
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, 1, false);
    step.setPrevious(timeoutAwareUpstream(ctx, makeRows(ctx, 3, 1, 2), 100));

    collect(step.start(ctx), ctx);
  }

  // ── Null placement: resolved once per sort ──

  /**
   * Rows carrying an explicit {@code null} sort key. A value of {@code null} in {@code values} is
   * emitted as a row whose sort property is absent, which the comparison treats as null.
   */
  private List<Result> makeNullableRows(CommandContext ctx, Integer... values) {
    var rows = new ArrayList<Result>();
    for (var v : values) {
      var r = new ResultInternal(ctx.getDatabaseSession());
      if (v != null) {
        r.setProperty(SORT_FIELD, v);
      }
      rows.add(r);
    }
    return rows;
  }

  /**
   * Upstream that sets the global null-ordering default to {@code flipTo} right after it has handed
   * out its first row. That stands for a configuration change landing while a sort runs.
   */
  private AbstractExecutionStep flippingUpstream(
      CommandContext ctx, List<Result> rows, OrderByNullsPlacement flipTo) {
    return new AbstractExecutionStep(ctx, false) {
      @Override
      public ExecutionStep copy(CommandContext ctx) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
        var delegate = new ArrayList<>(rows).iterator();
        return ExecutionStream.resultIterator(
            new java.util.Iterator<>() {
              private int served;

              @Override
              public boolean hasNext() {
                return delegate.hasNext();
              }

              @Override
              public Result next() {
                var row = delegate.next();
                served++;
                if (served == 1) {
                  GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(flipTo);
                }
                return row;
              }
            });
      }
    };
  }

  /**
   * The bounded heap must resolve null placement once, when the sort starts, not on every comparison
   * that touches a null. The upstream flips the global default to LAST after its first row,
   * which for ASC would move nulls to the end. Resolving per comparison would therefore reject the
   * trailing null row and return [1, 2]. One placement for the whole run keeps the FIRST
   * that was in force at sort start and returns [null, 1].
   */
  @Test
  public void boundedHeapKeepsOnePlacementWhenGlobalFlipsMidSort() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);
    try {
      var ctx = ctx();
      var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), 2, ctx, -1, false);
      step.setPrevious(
          flippingUpstream(
              ctx,
              makeNullableRows(ctx, 1, 2, null),
              OrderByNullsPlacement.LAST));

      var results = collect(step.start(ctx), ctx);

      Assert.assertEquals(2, results.size());
      Assert.assertNull(results.getFirst().getProperty(SORT_FIELD));
      Assert.assertEquals(1, (int) results.get(1).getProperty(SORT_FIELD));
      // The flip did land: the test would be vacuous if the upstream never changed the value.
      Assert.assertEquals(
          OrderByNullsPlacement.LAST,
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.<OrderByNullsPlacement>getValue());
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
    }
  }

  /**
   * The unbounded sort also fixes placement before draining its upstream. A mid-drain flip must not
   * change the placement used when the collected rows are sorted.
   */
  @Test
  public void unboundedSortKeepsOnePlacementWhenGlobalFlipsMidDrain() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);
    var ctx = ctx();
    var step = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    step.setPrevious(
        flippingUpstream(
            ctx, makeNullableRows(ctx, 1, 2, null), OrderByNullsPlacement.LAST));

    var results = collect(step.start(ctx), ctx);

    Assert.assertNull(results.getFirst().getProperty(SORT_FIELD));
    Assert.assertEquals(1, (int) results.get(1).getProperty(SORT_FIELD));
    Assert.assertEquals(2, (int) results.get(2).getProperty(SORT_FIELD));
  }

  /**
   * Under the shipped configuration (FIRST) null placement is unchanged by the hoist. Nulls
   * sort first for ASC and last for DESC, on both the unbounded and the bounded path.
   */
  @Test
  public void shippedConfigurationPlacesNullsByDirection() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);
    var ctx = ctx();
    var ascending = new OrderByStep(orderBy(SQLOrderByItem.ASC), null, ctx, -1, false);
    ascending.setPrevious(upstream(ctx, makeNullableRows(ctx, 2, null, 1)));

    var ascendingRows = collect(ascending.start(ctx), ctx);
    Assert.assertEquals(3, ascendingRows.size());
    Assert.assertNull(ascendingRows.get(0).getProperty(SORT_FIELD));
    Assert.assertEquals(1, (int) ascendingRows.get(1).getProperty(SORT_FIELD));
    Assert.assertEquals(2, (int) ascendingRows.get(2).getProperty(SORT_FIELD));

    var descending = new OrderByStep(orderBy(SQLOrderByItem.DESC), 2, ctx, -1, false);
    descending.setPrevious(upstream(ctx, makeNullableRows(ctx, 2, null, 1)));

    var descendingRows = collect(descending.start(ctx), ctx);
    Assert.assertEquals(2, descendingRows.size());
    Assert.assertEquals(2, (int) descendingRows.get(0).getProperty(SORT_FIELD));
    Assert.assertEquals(1, (int) descendingRows.get(1).getProperty(SORT_FIELD));
  }

  // ── Early termination with null sort keys ──

  /** A two-item ORDER BY, so the first item can act as the pre-sorted primary key hint. */
  private SQLOrderBy orderByPrimaryAndTag() {
    var primary = new SQLOrderByItem();
    primary.setAlias(SORT_FIELD);
    primary.setType(SQLOrderByItem.ASC);
    var secondary = new SQLOrderByItem();
    secondary.setAlias("tag");
    secondary.setType(SQLOrderByItem.ASC);
    var orderBy = new SQLOrderBy(-1);
    orderBy.setItems(new ArrayList<>(List.of(primary, secondary)));
    return orderBy;
  }

  /** Rows carrying the primary sort key and a tag, where a {@code null} key means no property. */
  private List<Result> makeTaggedRows(CommandContext ctx, Integer... values) {
    var rows = new ArrayList<Result>();
    for (var v : values) {
      var r = new ResultInternal(ctx.getDatabaseSession());
      if (v != null) {
        r.setProperty(SORT_FIELD, v);
      }
      r.setProperty("tag", String.valueOf(v));
      rows.add(r);
    }
    return rows;
  }

  /** Upstream that counts how many rows the step actually pulled. */
  private AbstractExecutionStep countingUpstream(
      CommandContext ctx, List<Result> rows, AtomicInteger pulled) {
    return new AbstractExecutionStep(ctx, false) {
      @Override
      public ExecutionStep copy(CommandContext ctx) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
        var delegate = new ArrayList<>(rows).iterator();
        return ExecutionStream.resultIterator(
            new java.util.Iterator<>() {
              @Override
              public boolean hasNext() {
                return delegate.hasNext();
              }

              @Override
              public Result next() {
                pulled.incrementAndGet();
                return delegate.next();
              }
            });
      }
    };
  }

  /**
   * Early termination on an index-ordered input, with null primary keys, under FIRST. The
   * input arrives in that order, so both null keys come first and the heap fills with them. The next
   * row sorts worse by the primary key, which ends the scan before the input is drained. This is the
   * only path where the resolved placement reaches the primary-key cutoff comparison.
   */
  @Test
  public void earlyTerminationStopsOnWorsePrimaryKeyWithFirstPlacement() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);
    try {
      var ctx = ctx();
      ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.TRUE);
      var orderBy = orderByPrimaryAndTag();
      var step =
          new OrderByStep(orderBy, 2, orderBy.getItems().getFirst(), true, ctx, -1, false);
      var pulled = new AtomicInteger();
      step.setPrevious(countingUpstream(ctx, makeTaggedRows(ctx, null, null, 1, 2), pulled));

      var results = collect(step.start(ctx), ctx);

      Assert.assertEquals(2, results.size());
      Assert.assertNull(results.get(0).getProperty(SORT_FIELD));
      Assert.assertNull(results.get(1).getProperty(SORT_FIELD));
      Assert.assertEquals("the scan must stop at the first worse key", 3, pulled.get());
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
    }
  }

  /**
   * The same cutoff under LAST, where the index-ordered input ends with the null keys. The
   * cutoff must treat a null primary key as worse than the kept rows and stop, keeping [1, 2]. A
   * placement that disagreed with the input order would rank the null key first. The heap would then
   * admit it and evict a row that belongs in the answer.
   */
  @Test
  public void earlyTerminationTreatsNullKeyAsWorseUnderLastPlacement() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    try {
      var ctx = ctx();
      ctx.setSystemVariable(CommandContext.VAR_INDEX_ORDERED_PRE_SORTED, Boolean.TRUE);
      var orderBy = orderByPrimaryAndTag();
      var step =
          new OrderByStep(orderBy, 2, orderBy.getItems().getFirst(), true, ctx, -1, false);
      var pulled = new AtomicInteger();
      step.setPrevious(countingUpstream(ctx, makeTaggedRows(ctx, 1, 2, null, null), pulled));

      var results = collect(step.start(ctx), ctx);

      Assert.assertEquals(2, results.size());
      Assert.assertEquals(1, (int) results.get(0).getProperty(SORT_FIELD));
      Assert.assertEquals(2, (int) results.get(1).getProperty(SORT_FIELD));
      Assert.assertEquals("the scan must stop at the first null key", 3, pulled.get());
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
    }
  }
}
