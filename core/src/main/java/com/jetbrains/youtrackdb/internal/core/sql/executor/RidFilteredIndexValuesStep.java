package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.internal.common.util.RawPair;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStreamProducer;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.MultipleExecutionStream;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Ordered index scan filtered by a pre-computed {@link RidSet}. Extends
 * {@link FetchFromIndexValuesStep} (full index scan in ASC/DESC order) and
 * adds a membership filter: only entries whose RID is present in the
 * {@code ridFilter} set are passed downstream.
 *
 * <p>This is the foundational building block for index-ordered MATCH
 * traversals: the MATCH planner resolves an edge's LinkBag into a RidSet,
 * then creates this step to scan a property index in ORDER BY direction,
 * emitting only edge-reachable records.
 *
 * <p>When {@code ridFilter} is null, behaves identically to
 * {@link FetchFromIndexValuesStep} (zero overhead — the filter check is
 * a simple null guard).
 *
 * <p>Inherits {@code canBeCached() = false} from the parent, which is
 * correct: the RidSet is per-execution (specific to one source vertex's
 * edge set) and cannot be serialized.
 */
public class RidFilteredIndexValuesStep extends FetchFromIndexValuesStep {

  @Nullable private final RidSet ridFilter;

  public RidFilteredIndexValuesStep(
      IndexSearchDescriptor desc,
      boolean orderAsc,
      boolean nullsFirst,
      CommandContext ctx,
      boolean profilingEnabled,
      @Nullable RidSet ridFilter) {
    super(desc, orderAsc, nullsFirst, ctx, profilingEnabled);
    this.ridFilter = ridFilter;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) {
    if (ridFilter == null) {
      return super.internalStart(ctx);
    }

    var prev = this.prev;
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    var session = ctx.getDatabaseSession();
    var tx = session.getTransactionInternal();
    tx.preProcessRecordsAndExecuteCallCallbacks();

    List<Stream<RawPair<Object, RID>>> streams =
        init(desc, isOrderAsc(), isNullsFirst(), ctx);
    var filter = this.ridFilter;
    var res =
        new ExecutionStreamProducer() {
          private final Iterator<Stream<RawPair<Object, RID>>> iter = streams.iterator();

          @Override
          public ExecutionStream next(CommandContext ctx) {
            Stream<RawPair<Object, RID>> s =
                iter.next().filter(pair -> filter.contains(pair.second()));
            return ExecutionStream.resultIterator(
                s.map((RawPair<Object, RID> nextEntry) -> {
                  tx.preProcessRecordsAndExecuteCallCallbacks();
                  return readResult(ctx, nextEntry);
                }).iterator());
          }

          @Override
          public boolean hasNext(CommandContext ctx) {
            tx.preProcessRecordsAndExecuteCallCallbacks();
            return iter.hasNext();
          }

          @Override
          public void close(CommandContext ctx) {
            while (iter.hasNext()) {
              iter.next().close();
            }
          }
        };
    return new MultipleExecutionStream(res);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var direction = isOrderAsc() ? "ASC" : "DESC";
    var filterInfo = ridFilter != null
        ? " (filtered by RidSet, size=" + ridFilter.size() + ")"
        : "";
    return ExecutionStepInternal.getIndent(depth, indent)
        + "+ FETCH FROM INDEX VALUES " + direction + " "
        + desc.getIndex().getName() + filterInfo;
  }
}
