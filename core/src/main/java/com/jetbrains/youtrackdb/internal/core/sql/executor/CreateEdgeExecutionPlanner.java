package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.index.Index;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLCreateEdgeStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLInsertBody;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLUpdateItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YqlExecutionPlanCache;
import java.util.ArrayList;
import java.util.List;

public class CreateEdgeExecutionPlanner {

  public static final String FROM_VERTICES_ALIAS = "$__YOUTRACKDB_CREATE_EDGE_fromV";
  public static final String TO_VERTICES_ALIAS = "$__YOUTRACKDB_CREATE_EDGE_toV";

  private final SQLCreateEdgeStatement statement;
  protected SQLIdentifier targetClass;
  protected SQLExpression leftExpression;
  protected SQLExpression rightExpression;

  protected boolean upsert = false;

  protected SQLInsertBody body;

  public CreateEdgeExecutionPlanner(SQLCreateEdgeStatement statement) {
    this.statement = statement;
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.leftExpression =
        statement.getLeftExpression() == null ? null : statement.getLeftExpression().copy();
    this.rightExpression =
        statement.getRightExpression() == null ? null : statement.getRightExpression().copy();
    this.upsert = statement.isUpsert();
    this.body = statement.getBody() == null ? null : statement.getBody().copy();
  }

  public InsertExecutionPlan createExecutionPlan(
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
  private InsertExecutionPlan buildExecutionPlan(
      CommandContext ctx, boolean enableProfiling, boolean useCache) {
    var session = ctx.getDatabaseSession();
    if (useCache && !enableProfiling && statement.executinPlanCanBeCached(session)) {
      var plan = YqlExecutionPlanCache.get(statement.getOriginalStatement(), ctx, session);
      if (plan != null) {
        return (InsertExecutionPlan) plan;
      }
    }

    var planningStart = System.nanoTime();

    if (targetClass == null) {
      targetClass = new SQLIdentifier("E");
    }

    var result = new InsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);

    if (!leftExpression.isStatement()) {
      handleGlobalLet(
          result,
          new SQLIdentifier(FROM_VERTICES_ALIAS),
          leftExpression,
          ctx,
          enableProfiling);
    }
    if (!rightExpression.isStatement()) {
      handleGlobalLet(
          result,
          new SQLIdentifier(TO_VERTICES_ALIAS),
          rightExpression,
          ctx,
          enableProfiling);
    }

    String uniqueIndexName = null;
    if (upsert) {
      var clazz =
          ctx.getDatabaseSession()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClassInternal(targetClass.getStringValue());
      if (clazz == null) {
        throw new CommandExecutionException(session,
            "Class " + targetClass + " not found in the db schema");
      }
      uniqueIndexName =
          clazz.getIndexesInternal().stream()
              .filter(Index::isUnique)
              .filter(
                  x -> x.getDefinition().getProperties().size() == 2
                      && x.getDefinition().getProperties().contains("out")
                      && x.getDefinition().getProperties().contains("in"))
              .map(Index::getName)
              .findFirst()
              .orElse(null);

      if (uniqueIndexName == null) {
        throw new CommandExecutionException(session,
            "Cannot perform an UPSERT on "
                + targetClass
                + " edge class: no unique index present on out/in");
      }
    }

    result.chain(
        new CreateEdgesStep(
            targetClass,
            uniqueIndexName,
            !leftExpression.isStatement() ? new SQLIdentifier(FROM_VERTICES_ALIAS) : null,
            !rightExpression.isStatement() ? new SQLIdentifier(TO_VERTICES_ALIAS) : null,
            leftExpression.isStatement() ? leftExpression.asStatement() : null,
            rightExpression.isStatement() ? rightExpression.asStatement() : null,
            ctx,
            enableProfiling));

    handleSetFields(result, body, ctx, enableProfiling);

    if (useCache
        && !enableProfiling
        && statement.executinPlanCanBeCached(session)
        && result.canBeCached()
        && YqlExecutionPlanCache.getLastInvalidation(session) < planningStart) {
      YqlExecutionPlanCache.put(
          statement.getOriginalStatement(), result, session,
          session.getPlanNullPlacements().recorded());
    }

    return result;
  }

  private static void handleGlobalLet(
      InsertExecutionPlan result,
      SQLIdentifier name,
      SQLExpression expression,
      CommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
  }

  private void handleCheckType(
      InsertExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(
          new CheckClassTypeStep(targetClass.getStringValue(), "E", ctx, profilingEnabled));
    }
  }

  private static void handleSetFields(
      InsertExecutionPlan result,
      SQLInsertBody insertBody,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(
          new InsertValuesStep(
              insertBody.getIdentifierList(),
              insertBody.getValueExpressions(),
              ctx,
              profilingEnabled));
    } else if (insertBody.getContent() != null) {
      for (var json : insertBody.getContent()) {
        result.chain(new UpdateContentStep(json, ctx, profilingEnabled));
      }
    } else if (insertBody.getContentInputParam() != null) {
      for (var inputParam : insertBody.getContentInputParam()) {
        result.chain(new UpdateContentStep(inputParam, ctx, profilingEnabled));
      }
    } else if (insertBody.getSetExpressions() != null) {
      List<SQLUpdateItem> items = new ArrayList<>();
      for (var exp : insertBody.getSetExpressions()) {
        var item = new SQLUpdateItem(-1);
        item.setOperator(SQLUpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx, profilingEnabled));
    }
  }
}
