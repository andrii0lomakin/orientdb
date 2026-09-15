package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.internal.core.command.CommandContext;

/**
 * Specialized variant of {@link FetchFromIndexStep} used when the planner decides
 * to scan an index purely for sorting (ORDER BY matches the index field order)
 * without using it for WHERE filtering.
 *
 * <p>This step iterates over all index entries in ASC or DESC order, providing a
 * pre-sorted stream. The downstream {@link GetValueFromIndexEntryStep} then loads
 * the actual records from the RIDs.
 *
 * <p>Cannot be cached because the index state may change between executions.
 *
 * @see SelectExecutionPlanner#handleClassWithIndexForSortOnly
 */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  public FetchFromIndexValuesStep(
      IndexSearchDescriptor desc, boolean orderAsc, CommandContext ctx, boolean profilingEnabled) {
    super(desc, orderAsc, ctx, profilingEnabled);
  }

  public FetchFromIndexValuesStep(
      IndexSearchDescriptor desc,
      boolean orderAsc,
      boolean nullsFirst,
      CommandContext ctx,
      boolean profilingEnabled) {
    super(desc, orderAsc, nullsFirst, ctx, profilingEnabled);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (isOrderAsc()) {
      return ExecutionStepInternal.getIndent(depth, indent)
          + "+ FETCH FROM INDEX VALUES ASC "
          + desc.getIndex().getName();
    } else {
      return ExecutionStepInternal.getIndent(depth, indent)
          + "+ FETCH FROM INDEX VALUES DESC "
          + desc.getIndex().getName();
    }
  }

  /**
   * Not cacheable: a full index scan has no parameterized key conditions to
   * structurally cache. Unlike {@link FetchFromIndexStep}, which caches the
   * shape of its key lookup, this step always scans all entries.
   */
  @Override
  public boolean canBeCached() {
    return false;
  }
}
