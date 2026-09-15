package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBatch;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLDeleteEdgeStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFromItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIndexIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLimit;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLRid;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YqlExecutionPlanCache;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Planner that builds an execution plan for DELETE EDGE statements. */
public class DeleteEdgeExecutionPlanner {

  private final SQLDeleteEdgeStatement statement;

  protected SQLIdentifier className;
  protected SQLIdentifier targetCollectionName;

  protected List<SQLRid> rids;

  private SQLExpression leftExpression;
  private SQLExpression rightExpression;

  protected SQLBatch batch = null;

  private SQLWhereClause whereClause;

  private SQLLimit limit;

  public DeleteEdgeExecutionPlanner(SQLDeleteEdgeStatement stm2) {
    this.statement = stm2;
  }

  private void init() {
    this.className =
        this.statement.getClassName() == null ? null : this.statement.getClassName().copy();
    this.targetCollectionName =
        this.statement.getTargetCollectionName() == null
            ? null
            : this.statement.getTargetCollectionName().copy();
    if (this.statement.getRid() != null) {
      this.rids = new ArrayList<>();
      rids.add(this.statement.getRid().copy());
    } else if (this.statement.getRids() == null) {
      this.rids = null;
    } else {
      this.rids = this.statement.getRids().stream().map(x -> x.copy()).collect(Collectors.toList());
    }

    this.leftExpression =
        this.statement.getLeftExpression() == null
            ? null
            : this.statement.getLeftExpression().copy();
    this.rightExpression =
        this.statement.getRightExpression() == null
            ? null
            : this.statement.getRightExpression().copy();

    this.whereClause =
        this.statement.getWhereClause() == null ? null : this.statement.getWhereClause().copy();
    this.batch = this.statement.getBatch() == null ? null : this.statement.getBatch().copy();
    this.limit = this.statement.getLimit() == null ? null : this.statement.getLimit().copy();
  }

  public InternalExecutionPlan createExecutionPlan(
      CommandContext ctx, boolean enableProfiling, boolean useCache) {
    var scopeSession = ctx.getDatabaseSession();
    if (scopeSession == null) {
      // A context with no session reads no configuration, so there is no placement to scope.
      return buildExecutionPlan(ctx, enableProfiling, useCache);
    }
    // Bracket the build in a null placement scope so a nested plan built for a source subquery keeps
    // its placement recorded until this plan is published with it as its stamp.
    var placements = scopeSession.getPlanNullPlacements();
    placements.open();
    try {
      return buildExecutionPlan(ctx, enableProfiling, useCache);
    } finally {
      placements.close();
    }
  }

  /** Runs the planning pipeline inside an open null placement scope. */
  private InternalExecutionPlan buildExecutionPlan(
      CommandContext ctx, boolean enableProfiling, boolean useCache) {
    var db = ctx.getDatabaseSession();
    if (useCache && !enableProfiling && statement.executinPlanCanBeCached(db)) {
      var plan = YqlExecutionPlanCache.get(statement.getOriginalStatement(), ctx, db);
      if (plan != null) {
        return (InternalExecutionPlan) plan;
      }
    }
    var planningStart = System.nanoTime();

    init();
    var result = new DeleteExecutionPlan(ctx);

    if (leftExpression != null || rightExpression != null) {
      handleGlobalLet(
          result,
          new SQLIdentifier("$__YOUTRACKDB_DELETE_EDGE_fromV"),
          leftExpression,
          ctx,
          enableProfiling);
      handleGlobalLet(
          result,
          new SQLIdentifier("$__YOUTRACKDB_DELETE_EDGE_toV"),
          rightExpression,
          ctx,
          enableProfiling);
      String fromLabel = null;
      if (leftExpression != null) {
        fromLabel = "$__YOUTRACKDB_DELETE_EDGE_fromV";
      }
      handleFetchFromTo(
          result,
          ctx,
          fromLabel,
          "$__YOUTRACKDB_DELETE_EDGE_toV",
          className,
          targetCollectionName,
          enableProfiling);
      handleWhere(result, ctx, whereClause, enableProfiling);
    } else if (whereClause != null) {
      var fromClause = new SQLFromClause(-1);
      var item = new SQLFromItem(-1);
      if (className == null) {
        item.setIdentifier(new SQLIdentifier("E"));
      } else {
        item.setIdentifier(className);
      }
      fromClause.setItem(item);
      handleTarget(result, ctx, fromClause, this.whereClause, enableProfiling);
    } else {
      handleTargetClass(result, ctx, className, enableProfiling);
      handleTargetCollection(result, ctx, targetCollectionName, enableProfiling);
      handleTargetRids(result, ctx, rids, enableProfiling);
    }

    handleLimit(result, ctx, this.limit, enableProfiling);
    handleCastToEdge(result, ctx, enableProfiling);
    handleDelete(result, ctx, enableProfiling);
    handleReturn(result, ctx, enableProfiling);

    if (useCache
        && !enableProfiling
        && this.statement.executinPlanCanBeCached(db)
        && result.canBeCached()
        && YqlExecutionPlanCache.getLastInvalidation(db) < planningStart) {
      YqlExecutionPlanCache.put(
          this.statement.getOriginalStatement(), result, db,
          db.getPlanNullPlacements().recorded());
    }

    return result;
  }

  private void handleWhere(
      DeleteExecutionPlan result,
      CommandContext ctx,
      SQLWhereClause whereClause,
      boolean profilingEnabled) {
    if (whereClause != null) {
      result.chain(new FilterStep(whereClause, ctx, -1, profilingEnabled));
    }
  }

  private void handleFetchFromTo(
      DeleteExecutionPlan result,
      CommandContext ctx,
      String fromAlias,
      String toAlias,
      SQLIdentifier targetClass,
      SQLIdentifier targetCollection,
      boolean profilingEnabled) {
    if (fromAlias != null && toAlias != null) {
      result.chain(
          new FetchEdgesFromToVerticesStep(
              fromAlias, toAlias, targetClass, targetCollection, ctx, profilingEnabled));
    } else if (toAlias != null) {
      result.chain(
          new FetchEdgesToVerticesStep(toAlias, targetClass, targetCollection, ctx,
              profilingEnabled));
    }
  }

  private void handleTargetRids(
      DeleteExecutionPlan result, CommandContext ctx, List<SQLRid> rids,
      boolean profilingEnabled) {
    if (rids != null) {
      result.chain(
          new FetchFromRidsStep(
              rids.stream()
                  .map(x -> x.toRecordId((Result) null, ctx))
                  .collect(Collectors.toList()),
              ctx,
              profilingEnabled));
    }
  }

  private void handleTargetCollection(
      DeleteExecutionPlan result,
      CommandContext ctx,
      SQLIdentifier targetCollectionName,
      boolean profilingEnabled) {
    if (targetCollectionName != null) {
      var name = targetCollectionName.getStringValue();
      var collectionId = ctx.getDatabaseSession().getCollectionIdByName(name);
      if (collectionId < 0) {
        throw new CommandExecutionException(ctx.getDatabaseSession(),
            "Collection not found: " + name);
      }
      result.chain(new FetchFromCollectionExecutionStep(collectionId, ctx, profilingEnabled));
    }
  }

  private void handleTargetClass(
      DeleteExecutionPlan result,
      CommandContext ctx,
      SQLIdentifier className,
      boolean profilingEnabled) {
    if (className != null) {
      result.chain(
          new FetchFromClassExecutionStep(
              className.getStringValue(), null, ctx, null, profilingEnabled));
    }
  }

  private boolean handleIndexAsTarget(
      DeleteExecutionPlan unusedResult,
      SQLIndexIdentifier indexIdentifier,
      SQLWhereClause unusedWhereClause,
      CommandContext ctx,
      boolean unusedProfilingEnabled) {
    if (indexIdentifier == null) {
      return false;
    }
    throw new CommandExecutionException(ctx.getDatabaseSession(),
        "DELETE VERTEX FROM INDEX is not supported");
  }

  private void handleDelete(
      DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

  private void handleReturn(
      DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new CountStep(ctx, profilingEnabled));
  }

  private void handleLimit(
      UpdateExecutionPlan plan, CommandContext ctx, SQLLimit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleCastToEdge(
      DeleteExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    plan.chain(new CastToEdgeStep(ctx, profilingEnabled));
  }

  private void handleTarget(
      UpdateExecutionPlan result,
      CommandContext ctx,
      SQLFromClause target,
      SQLWhereClause whereClause,
      boolean profilingEnabled) {
    var sourceStatement = new SQLSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    var planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(
        new SubQueryStep(
            planner.createExecutionPlan(ctx, profilingEnabled, false), ctx, ctx, profilingEnabled));
  }

  private void handleGlobalLet(
      DeleteExecutionPlan result,
      SQLIdentifier name,
      SQLExpression expression,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (expression != null) {
      result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
    }
  }
}
