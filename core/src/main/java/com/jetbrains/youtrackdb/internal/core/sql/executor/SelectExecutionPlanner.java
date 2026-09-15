package com.jetbrains.youtrackdb.internal.core.sql.executor;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.collection.MultiValue;
import com.jetbrains.youtrackdb.internal.common.util.PairIntegerObject;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Identifiable;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.id.RecordIdInternal;
import com.jetbrains.youtrackdb.internal.core.index.Index;
import com.jetbrains.youtrackdb.internal.core.index.IndexDefinition;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.PropertyTypeInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Collate;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.IndexOrderedPlanner;
import com.jetbrains.youtrackdb.internal.core.sql.operator.QueryOperatorEquals;
import com.jetbrains.youtrackdb.internal.core.sql.parser.AggregateProjectionSplit;
import com.jetbrains.youtrackdb.internal.core.sql.parser.OrderByCollationResolver;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBaseExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBinaryCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLEqualsOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFromItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFunctionCall;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGroupBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLInputParameter;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLInteger;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIsNullCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLetClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLetItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMetadataIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLProjectionItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLRecordAttribute;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLRid;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLTimeout;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SubQueryCollector;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YqlExecutionPlanCache;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Converts a parsed {@link SQLSelectStatement} AST into a physical
 * {@link SelectExecutionPlan} -- an ordered chain of {@link ExecutionStepInternal}
 * nodes that, when started, produce a pull-based stream of results.
 *
 * <h2>High-level pipeline</h2>
 * <pre>
 * SQLSelectStatement (AST from JavaCC parser)
 *        |
 *        v
 * +-------------------------------+
 * | SelectExecutionPlanner        |
 * |  1. init()          copy AST  |
 * |  2. optimizeQuery() rewrite   |
 * |  3. build step chain          |
 * +-------------------------------+
 *        |
 *        v
 * SelectExecutionPlan (linked list of steps)
 *
 *  step1 --next--&gt; step2 --next--&gt; ... --next--&gt; stepN
 *        &lt;--prev--       &lt;--prev--     &lt;--prev--
 *
 *  Execution is PULL-BASED: the caller invokes stepN.start(ctx)
 *  which recursively pulls from step(N-1), etc.
 * </pre>
 *
 * <h2>Planning phases (in order)</h2>
 * <pre>
 *  Phase                        | Method(s)
 *  -----------------------------|-------------------------------------------
 *  1. Copy &amp; normalize AST      | init()
 *  2. Optimize query            | optimizeQuery()
 *     a. Split LET              |   splitLet()
 *     b. Rewrite index chains   |   rewriteIndexChainsAsSubqueries()
 *     c. Extract subqueries     |   extractSubQueries()
 *     d. Flatten WHERE          |   whereClause.flatten()
 *     e. Move equalities left   |   moveFlattenedEqualitiesLeft()
 *     f. Split projections      |   splitProjectionsForGroupBy()
 *     g. Add ORDER BY projs     |   addOrderByProjections()
 *  3. Hard-wired optimizations  | handleHardwiredOptimizations()
 *     (COUNT(*) short-circuits) |
 *  4. Global LET                | handleGlobalLet()
 *  5. Fetch from target         | handleFetchFromTarget()
 *  6. LET pre-filter            | handleLetPreFilter()
 *      (push LET-independent    |   (skips LET subqueries for
 *       WHERE before LET)       |    filtered-out rows)
 *  7. Per-record LET            | handleLet()
 *  8. WHERE filtering           | handleWhere()
 *  9. Projections block         | handleProjectionsBlock()
 *     (projections, EXPAND,     |
 *      UNWIND, ORDER BY,        |
 *      SKIP, LIMIT, DISTINCT)   |
 *  10. Timeout                  | AccumulatingTimeoutStep
 *  11. Cache plan (optional)    | YqlExecutionPlanCache.put()
 * </pre>
 *
 * <h2>Projection splitting for aggregation</h2>
 * When the SELECT list contains aggregate functions (e.g. {@code count(*), max(price)}),
 * the planner splits projections into three phases to support GROUP BY correctly:
 * <pre>
 *   SELECT city, count(*), max(price) FROM Product GROUP BY city
 *
 *   preAggregateProjection :  city           (per-record fields needed before grouping)
 *   aggregateProjection    :  count(*), max  (aggregate accumulators, grouped by city)
 *   projection (post)      :  city, count, max (final output mapping)
 *
 *   Pipeline:
 *   FetchFromClass -&gt; ProjectionCalc(pre) -&gt; AggregateProjectionCalc -&gt; ProjectionCalc(post)
 * </pre>
 *
 * <h2>Index selection strategy</h2>
 * For class-targeted queries with a WHERE clause the planner attempts, in order:
 * <ol>
 *   <li>Indexed function execution (e.g. spatial / full-text custom functions)</li>
 *   <li>Best-fit B-tree / hash index lookup via {@link #findBestIndexFor}</li>
 *   <li>Index-only sort (ORDER BY matches index field order)</li>
 *   <li>Full class scan with optional RID-ordering optimization</li>
 * </ol>
 *
 * <h2>Thread safety</h2>
 * Instances are <b>not</b> thread-safe. A new planner is created per query execution.
 * The resulting {@link SelectExecutionPlan} may be cached and copied for reuse.
 *
 * @see SelectExecutionPlan
 * @see QueryPlanningInfo
 * @see ExecutionStepInternal
 */
public class SelectExecutionPlanner {

  /** Mutable planning state -- populated by {@link #init} and mutated by optimization passes. */
  private QueryPlanningInfo info;

  /** The parsed SQL SELECT statement (immutable AST from the JavaCC parser). */
  private final SQLSelectStatement statement;

  public SelectExecutionPlanner(SQLSelectStatement oSelectStatement) {
    this.statement = oSelectStatement;
  }

  /**
   * Copies all relevant clauses from the parsed {@link SQLSelectStatement} into a mutable
   * {@link QueryPlanningInfo} so that subsequent optimization passes can freely rewrite them
   * without mutating the original AST (which may be cached or reused).
   *
   * <p>Also applies a default command-level timeout from {@link GlobalConfiguration#COMMAND_TIMEOUT}
   * if no explicit TIMEOUT clause was specified in the SQL.
   */
  private void init(CommandContext ctx) {
    // copying the content, so that it can be manipulated and optimized
    info = new QueryPlanningInfo();
    info.projection =
        this.statement.getProjection() == null ? null : this.statement.getProjection().copy();
    info.projection = translateDistinct(info.projection);
    info.distinct = info.projection != null && info.projection.isDistinct();
    if (info.projection != null) {
      info.projection.setDistinct(false);
    }

    info.target = this.statement.getTarget();
    info.whereClause =
        this.statement.getWhereClause() == null ? null : this.statement.getWhereClause().copy();
    info.whereClause = translateLucene(info.whereClause);
    info.perRecordLetClause =
        this.statement.getLetClause() == null ? null : this.statement.getLetClause().copy();
    info.groupBy = this.statement.getGroupBy() == null ? null : this.statement.getGroupBy().copy();
    info.orderBy = this.statement.getOrderBy() == null ? null : this.statement.getOrderBy().copy();
    info.unwind = this.statement.getUnwind() == null ? null : this.statement.getUnwind().copy();
    info.skip = this.statement.getSkip();
    info.limit = this.statement.getLimit();
    info.timeout = this.statement.getTimeout() == null ? null : this.statement.getTimeout().copy();
    if (info.timeout == null
        &&
        ctx.getDatabaseSession().getConfiguration()
            .getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT)
            > 0) {
      info.timeout = new SQLTimeout(-1);
      info.timeout.setVal(
          ctx.getDatabaseSession()
              .getConfiguration()
              .getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT));
    }
  }

  /**
   * Main entry point: builds and returns a fully assembled execution plan for the
   * SELECT statement that was passed to the constructor.
   *
   * <p>The method first checks the plan cache (when {@code useCache} is true and profiling
   * is disabled). If no cached plan is found it runs the full planning pipeline:
   * <pre>
   *   init() --&gt; optimizeQuery() --&gt; hardwired opts? --&gt; globalLet --&gt; fetch
   *          --&gt; perRecordLet --&gt; where --&gt; projectionsBlock --&gt; timeout
   * </pre>
   *
   * @param ctx              command context carrying the database session and input parameters
   * @param enableProfiling  if true, each step wraps its output stream with profiling counters
   * @param useCache         if true, the planner will check / populate the
   *                         {@link YqlExecutionPlanCache}
   * @return a ready-to-execute {@link InternalExecutionPlan}
   */
  public InternalExecutionPlan createExecutionPlan(
      CommandContext ctx, boolean enableProfiling, boolean useCache) {
    var scopeSession = ctx.getDatabaseSession();
    if (scopeSession == null) {
      // A context with no session reads no configuration, so there is no placement to scope.
      return buildExecutionPlan(ctx, enableProfiling, useCache);
    }
    // Bracket the whole build in a null placement scope. Every placement read below then returns one
    // resolved pair, and the plan published to the shared cache carries exactly that pair as its
    // stamp. A nested build joins this scope, so an embedded plan cannot disagree with its host.
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
    var session = ctx.getDatabaseSession();

    // --- 1. Check the plan cache before doing any work ---
    // Include skipExpandPushDown in the cache key so that plans compiled with
    // push-down disabled (materialized LET per-entry) are stored separately
    // from plans compiled with push-down enabled (normal execution).
    var cacheKey = statement.getOriginalStatement();
    if (ctx.isSkipExpandPushDown()) {
      cacheKey += "\0skipExpandPushDown";
    }
    var letHostedForCache =
        ctx.isLetHostedCorrelatedRidFetch() || statementHasUserPerRecordLet(statement);
    if (letHostedForCache) {
      cacheKey += "\0letHostedCorrelatedRidFetch";
    }
    if (useCache && !enableProfiling && statement.executinPlanCanBeCached(session)) {
      var plan = YqlExecutionPlanCache.get(cacheKey, ctx, session);
      if (plan != null) {
        return (InternalExecutionPlan) plan;
      }
    }

    // Record the timestamp so we can avoid caching a stale plan if the schema
    // was modified concurrently during planning.
    var planningStart = System.nanoTime();

    // --- 2. Copy AST into mutable QueryPlanningInfo ---
    init(ctx);
    var result = new SelectExecutionPlan(ctx);

    // DISTINCT + expand() is an unsupported combination -- fail fast.
    if (info.expand && info.distinct) {
      throw new CommandExecutionException(session,
          "Cannot execute a statement with DISTINCT expand(), please use a subquery");
    }

    // --- 3. Optimize the query: rewrite LET, flatten WHERE, split projections, etc. ---
    optimizeQuery(info, ctx);

    // --- 4. Try hardwired short-circuit optimizations (e.g. COUNT(*) without scan) ---
    if (handleHardwiredOptimizations(result, ctx, enableProfiling)) {
      return result;
    }

    // --- 5. Build the step chain in pipeline order ---
    handleGlobalLet(result, info, ctx, enableProfiling); // global LET (executed once)

    handleFetchFromTarget(result, info, ctx, enableProfiling); // data source

    // Push LET-independent WHERE conjuncts before per-record LET evaluation
    // so that expensive LET subqueries are skipped for filtered-out rows.
    handleLetPreFilter(result, info, ctx, enableProfiling);

    handleLet(result, info, ctx, enableProfiling); // per-record LET

    handleWhere(result, info, ctx, enableProfiling); // WHERE filtering

    // --- 5b. Predicate push-down: move outer WHERE into expand() ---
    tryPushDownFilterIntoExpand(result, info);

    handleProjectionsBlock(result, info, ctx, enableProfiling);// projections, ORDER BY, etc.

    // --- 6. Append timeout enforcement step if configured ---
    if (info.timeout != null) {
      result.chain(new AccumulatingTimeoutStep(info.timeout, ctx, enableProfiling));
    }

    // --- 7. Store the assembled plan in the cache for future reuse ---
    if (useCache
        && !enableProfiling
        && statement.executinPlanCanBeCached(session)
        && result.canBeCached()
        && YqlExecutionPlanCache.getLastInvalidation(session) < planningStart) {
      // Stamp the plan with the placement this build read, still inside the scope. A build that read
      // none stamps nothing, so a lookup of that plan does no placement work.
      YqlExecutionPlanCache.put(
          cacheKey, result, session, session.getPlanNullPlacements().recorded());
    }
    return result;
  }

  /**
   * Assembles the tail of the execution plan covering projections, EXPAND, UNWIND,
   * ORDER BY, DISTINCT, SKIP and LIMIT.
   *
   * <p>The ordering of these steps depends on which SQL features are used. Three
   * mutually exclusive code paths handle all combinations:
   *
   * <pre>
   * Path A -- EXPAND / UNWIND / GROUP BY present:
   *   Projections -&gt; Expand -&gt; Unwind -&gt; OrderBy -&gt; Skip -&gt; Limit
   *   (ORDER BY must come AFTER expand/unwind because those change the row count)
   *
   * Path B -- DISTINCT / aggregation (no expand/unwind):
   *   OrderBy -&gt; Projections -&gt; Distinct -&gt; Skip -&gt; Limit
   *   (ORDER BY before projections so sort keys are still available)
   *
   * Path C -- simple query (no expand/unwind/distinct/aggregation):
   *   Skip -&gt; Limit -&gt; Projections
   *   (SKIP/LIMIT applied early to minimize projection work)
   * </pre>
   *
   * <p>In all paths, if an ORDER BY clause is present,
   * {@link #handleProjectionsBeforeOrderBy} is called first to ensure that ORDER BY
   * expressions that are not part of the user's SELECT list are temporarily added as
   * projections (they will be stripped later by {@code projectionAfterOrderBy}).
   */
  public static void handleProjectionsBlock(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean enableProfiling) {

    // Ensure ORDER BY expressions are available as projected columns.
    handleProjectionsBeforeOrderBy(result, info, ctx, enableProfiling);

    if (info.expand || info.unwind != null || info.groupBy != null) {
      // --- Path A: EXPAND / UNWIND / GROUP BY ---
      // Projections must be computed before expand/unwind because those operators
      // consume the projected field to produce new rows.  ORDER BY runs after
      // because the row cardinality has changed.
      handleProjections(result, info, ctx, enableProfiling);
      handleExpand(result, info, ctx, enableProfiling);
      handleUnwind(result, info, ctx, enableProfiling);
      handleOrderBy(result, info, ctx, enableProfiling);
      if (info.skip != null) {
        result.chain(new SkipExecutionStep(info.skip, ctx, enableProfiling));
      }
      if (info.limit != null) {
        result.chain(new LimitExecutionStep(info.limit, ctx, enableProfiling));
      }
    } else {
      // ORDER BY runs first so sort keys (which may be raw record fields
      // not present in the final projection) are still accessible.
      handleOrderBy(result, info, ctx, enableProfiling);

      if (info.distinct || info.groupBy != null || info.aggregateProjection != null) {
        // --- Path B: DISTINCT / aggregation ---
        // Note: info.groupBy != null is a defensive guard here. In practice, the outer
        // if-branch (Path A) already handles groupBy != null. It is retained to ensure
        // correct behavior if the Path A condition is ever modified.
        // Projections run after sorting; DISTINCT deduplicates the projected rows.
        handleProjections(result, info, ctx, enableProfiling);
        handleDistinct(result, info, ctx, enableProfiling);
        if (info.skip != null) {
          result.chain(new SkipExecutionStep(info.skip, ctx, enableProfiling));
        }
        if (info.limit != null) {
          result.chain(new LimitExecutionStep(info.limit, ctx, enableProfiling));
        }
      } else {
        // --- Path C: simple query ---
        // SKIP/LIMIT before projections to avoid computing projections on
        // rows that will be discarded anyway.
        if (info.skip != null) {
          result.chain(new SkipExecutionStep(info.skip, ctx, enableProfiling));
        }
        if (info.limit != null) {
          result.chain(new LimitExecutionStep(info.limit, ctx, enableProfiling));
        }
        handleProjections(result, info, ctx, enableProfiling);
      }
    }
  }

  /**
   * Rewrites legacy Lucene-style operators in the WHERE clause to the standard form.
   * Returns the (possibly mutated) WHERE clause, or {@code null} if the input was null.
   */
  @Nullable private static SQLWhereClause translateLucene(SQLWhereClause whereClause) {
    if (whereClause == null) {
      return null;
    }

    if (whereClause.getBaseExpression() != null) {
      whereClause.getBaseExpression().translateLuceneOperator();
    }
    return whereClause;
  }

  /**
   * For backward compatibility, translates the legacy {@code distinct(foo)} function
   * call syntax into the standard {@code SELECT DISTINCT foo} form. Returns a new
   * projection with {@code isDistinct() == true} and the inner expression unwrapped.
   * If the projection does not contain a legacy distinct() call, it is returned as-is.
   */
  public static SQLProjection translateDistinct(SQLProjection projection) {
    if (projection != null && projection.getItems().size() == 1) {
      if (isDistinct(projection.getItems().getFirst())) {
        projection = projection.copy();
        var item = projection.getItems().getFirst();
        var function =
            ((SQLBaseExpression) item.getExpression().getMathExpression())
                .getIdentifier()
                .getLevelZero()
                .getFunctionCall();
        var exp = function.getParams().getFirst();
        var resultItem = new SQLProjectionItem(-1);
        resultItem.setAlias(item.getAlias());
        resultItem.setExpression(exp.copy());
        var result = new SQLProjection(-1);
        result.setItems(new ArrayList<>());
        result.setDistinct(true);
        result.getItems().add(resultItem);
        return result;
      }
    }
    return projection;
  }

  /**
   * Returns {@code true} if the given projection item is a legacy {@code distinct(expr)}
   * function call. The new executor does not support {@code distinct()} as a function;
   * instead, the caller rewrites it to {@code SELECT DISTINCT expr} via
   * {@link #translateDistinct}.
   */
  private static boolean isDistinct(SQLProjectionItem item) {
    if (item.getExpression() == null) {
      return false;
    }
    if (item.getExpression().getMathExpression() == null) {
      return false;
    }
    if (!(item.getExpression().getMathExpression() instanceof SQLBaseExpression base)) {
      return false;
    }
    if (base.getIdentifier() == null) {
      return false;
    }
    if (base.getModifier() != null) {
      return false;
    }
    if (base.getIdentifier().getLevelZero() == null) {
      return false;
    }
    var function = base.getIdentifier().getLevelZero().getFunctionCall();
    if (function == null) {
      return false;
    }
    return function.getName().getStringValue().equalsIgnoreCase("distinct");
  }

  /**
   * Attempts to short-circuit the entire plan with a single optimized step when the
   * query is a simple {@code SELECT count(*) FROM ClassName} (optionally with a
   * single indexed equality condition).
   *
   * <pre>
   *  Case 1 -- bare count:   SELECT count(*) FROM Foo
   *    =&gt; CountFromClassStep  (O(1) metadata lookup, no record scan)
   *
   *  Case 2 -- indexed count: SELECT count(*) FROM Foo WHERE bar = ?
   *    =&gt; CountFromIndexWithKeyStep  (single index key count)
   * </pre>
   *
   * @return {@code true} if the optimization was applied and the plan is complete
   */
  private boolean handleHardwiredOptimizations(
      SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    return HardwiredCountOptimizations.tryApply(result, info, ctx, profilingEnabled);
  }

  /**
   * Returns {@code true} if the query has exactly one aggregate projection that is a
   * bare {@code count()} (with no modifier), and only one user-visible output column
   * (synthetic ORDER BY aliases are excluded from the count).
   */
  private static boolean isCountOnly(QueryPlanningInfo info) {
    if (info.aggregateProjection == null
        || info.projection == null
        || info.aggregateProjection.getItems().size() != 1
        || info.projection.getItems().stream()
            .filter(x -> !x.getProjectionAliasAsString().startsWith("_$$$ORDER_BY_ALIAS$$$_"))
            .count()
            != 1) {
      return false;
    }
    var item = info.aggregateProjection.getItems().getFirst();
    var exp = item.getExpression();
    if (exp.getMathExpression() != null
        && exp.getMathExpression() instanceof SQLBaseExpression base) {
      return base.isCount() && base.getModifier() == null;
    }
    return false;
  }

  /**
   * Appends an {@link UnwindStep} if the query contains an UNWIND clause.
   *
   * <p>UNWIND flattens a collection-valued field into multiple rows:
   * <pre>
   *   Input:  { name: "Alice", tags: ["a","b"] }
   *   Output: { name: "Alice", tags: "a" }
   *           { name: "Alice", tags: "b" }
   * </pre>
   */
  public static void handleUnwind(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.unwind != null) {
      result.chain(new UnwindStep(info.unwind, ctx, profilingEnabled));
    }
  }

  /** Appends a {@link DistinctExecutionStep} if the query uses DISTINCT. */
  private static void handleDistinct(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.distinct) {
      result.chain(new DistinctExecutionStep(ctx, profilingEnabled));
    }
  }

  /**
   * If an ORDER BY clause is present, projections are calculated early so that
   * sort keys derived from projected expressions are available to
   * {@link OrderByStep}. Without ORDER BY this is a no-op (projections are
   * deferred for efficiency).
   */
  private static void handleProjectionsBeforeOrderBy(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.orderBy != null) {
      handleProjections(result, info, ctx, profilingEnabled);
    }
  }

  /**
   * Builds the projection sub-pipeline (up to three steps) and appends it to the plan.
   *
   * <p>When aggregation is involved the projection is split into three phases
   * (see {@link #splitProjectionsForGroupBy}):
   * <pre>
   *   Phase 1 (optional): ProjectionCalculationStep(preAggregateProjection)
   *       -- evaluates per-row expressions needed by the aggregate
   *
   *   Phase 2 (optional): AggregateProjectionCalculationStep(aggregateProjection)
   *       -- accumulates aggregate functions, grouped by GROUP BY keys
   *       +  GuaranteeEmptyCountStep (only for bare count() without GROUP BY)
   *
   *   Phase 3 (always):   ProjectionCalculationStep(projection)
   *       -- computes the final output columns
   * </pre>
   *
   * <p>The {@code projectionsCalculated} flag prevents this method from appending
   * projection steps more than once (it can be called from both
   * {@link #handleProjectionsBeforeOrderBy} and the main projections block).
   *
   * <p>When no ORDER BY is present, a combined aggregation limit
   * ({@code SKIP + LIMIT}) is passed into the aggregation step so it can
   * stop accumulating early once enough groups have been produced.
   */
  private static void handleProjections(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (!info.projectionsCalculated && info.projection != null) {

      // Phase 1: pre-aggregate projections (per-row expressions feeding aggregates)
      if (info.preAggregateProjection != null) {
        result.chain(
            new ProjectionCalculationStep(info.preAggregateProjection, ctx, profilingEnabled));
      }

      // Phase 2: aggregate accumulation (GROUP BY + aggregate functions)
      if (info.aggregateProjection != null) {
        // When there is no ORDER BY, we can limit how many groups the aggregation
        // produces to (SKIP + LIMIT) -- an early-termination optimization.
        long aggregationLimit = -1;
        if (info.orderBy == null && info.limit != null) {
          aggregationLimit = info.limit.getValue(ctx);
          if (info.skip != null && info.skip.getValue(ctx) > 0) {
            aggregationLimit += info.skip.getValue(ctx);
          }
        }
        result.chain(
            new AggregateProjectionCalculationStep(
                info.aggregateProjection,
                info.groupBy,
                aggregationLimit,
                ctx,
                info.timeout != null ? info.timeout.getVal().longValue() : -1,
                profilingEnabled));

        // For a bare "SELECT count(*) FROM ..." (no GROUP BY), guarantee that
        // the result contains at least one row with count=0 even when the
        // upstream produces zero records.
        if (isCountOnly(info) && info.groupBy == null) {
          result.chain(
              new GuaranteeEmptyCountStep(
                  info.aggregateProjection.getItems().getFirst(), ctx, profilingEnabled));
        }
      }

      // Phase 3: final projection (maps accumulated / raw fields to output columns)
      result.chain(new ProjectionCalculationStep(info.projection, ctx, profilingEnabled));

      info.projectionsCalculated = true;
    }
  }

  /**
   * Master optimization pass that rewrites the mutable {@link QueryPlanningInfo} in-place.
   *
   * <p>The sub-passes run in a fixed order because each may depend on the output of
   * the previous one:
   * <pre>
   *  1. splitLet           -- separate global vs per-record LET items
   *  2. rewriteIndexChains -- convert chained index traversals to subqueries
   *  3. extractSubQueries  -- pull inline subqueries into LET variables
   *  4. detect expand()    -- extract EXPAND projection into a flag + alias
   *  5. flatten WHERE      -- convert OR/AND tree to a list of AND blocks
   *  6. equalities left    -- reorder each AND block: equalities first (index-friendly)
   *  7. splitProjections   -- split into pre-aggregate / aggregate / post-aggregate
   *  8. resolveCollations  -- pin the declared collation of each ORDER BY property
   *  9. addOrderByProjs    -- add synthetic projections for ORDER BY expressions
   * </pre>
   *
   * <p>After this method completes, {@code info.flattenedWhereClause} is a
   * {@code List<SQLAndBlock>} where each block represents one OR-branch, and within
   * each block the conditions are ordered with equalities first (which allows the
   * index selection logic to match index prefixes greedily).
   */
  public static void optimizeQuery(QueryPlanningInfo info, CommandContext ctx) {
    splitLet(info, ctx);
    rewriteIndexChainsAsSubqueries(info, ctx);
    extractSubQueries(info);

    // Detect and extract expand() from the projection
    if (info.projection != null && info.projection.isExpand()) {
      info.expand = true;
      info.expandAlias = info.projection.getExpandAlias();
      info.projection = info.projection.getExpandContent();
    }

    // Flatten the WHERE tree into a list of AND blocks (one per OR branch)
    // and reorder equalities to the front of each block for index matching.
    if (info.whereClause != null) {
      if (info.target == null) {
        info.flattenedWhereClause = info.whereClause.flatten(ctx, null);
      } else {
        info.flattenedWhereClause = info.whereClause.flatten(ctx,
            info.target.getSchemaClass(ctx.getDatabaseSession()));
      }
      // Move equality conditions to the left of each AND block so the index
      // selection logic can greedily match the longest index prefix.
      info.flattenedWhereClause = moveFlattenedEqualitiesLeft(info.flattenedWhereClause);
    }

    splitProjectionsForGroupBy(info, ctx);
    resolveOrderByCollations(info, ctx);
    addOrderByProjections(info);
  }

  /**
   * Pins the declared collation of each ORDER BY property onto its item, so the sort comparator uses
   * one rule per column instead of reading the schema per record.
   *
   * <p>Runs before {@link #addOrderByProjections}, which rewrites an item that the SELECT list does
   * not project into a synthetic alias and drops the property name with it. Resolving afterwards
   * would therefore see {@code _$$$ORDER_BY_ALIAS$$$_0} and find no property at all.
   *
   * <p>A null target leaves every item alone rather than resetting it. That is what lets the MATCH
   * planner resolve its own items against its alias-to-class map and then delegate the rest of the
   * projection pipeline here.
   */
  private static void resolveOrderByCollations(QueryPlanningInfo info, CommandContext ctx) {
    if (info.orderBy == null || info.target == null || ctx == null) {
      return;
    }
    var session = ctx.getDatabaseSession();
    if (session == null) {
      return;
    }
    OrderByCollationResolver.resolveOnTargetClass(
        info.orderBy, info.target.getSchemaClass(session), info.projection);
  }

  /**
   * Rewrites chained index traversals in the WHERE clause into subqueries.
   * For example, {@code WHERE friend.name = 'Alice'} where {@code friend} is a link
   * and {@code name} is indexed can be rewritten as a subquery that first resolves
   * the index lookup, then matches the link.
   */
  private static void rewriteIndexChainsAsSubqueries(QueryPlanningInfo info, CommandContext ctx) {
    if (ctx == null) {
      return;
    }

    var session = ctx.getDatabaseSession();
    if (session == null) {
      return;
    }

    if (info.whereClause != null
        && info.target != null) {
      var clazz = info.target.getSchemaClass(session);
      if (clazz != null) {
        info.whereClause.getBaseExpression().rewriteIndexChainsAsSubqueries(ctx, clazz);
      }
    }
  }

  /**
   * Splits the per-record LET clause into global and per-record items.
   *
   * <p>Items that can be evaluated once (before the main fetch) are promoted to
   * global LET. The promotion criteria are:
   * <ul>
   *   <li>Expression is early-calculable (does not depend on the current record)</li>
   *   <li>Expression is a set-combination function (unionAll, intersect, difference)</li>
   *   <li>Query subexpression does not reference {@code $parent} (no back-reference
   *       to the outer record)</li>
   * </ul>
   *
   * <p>Promoted items are removed from {@code info.perRecordLetClause} and added to
   * {@code info.globalLetClause}.
   */
  private static void splitLet(QueryPlanningInfo info, CommandContext ctx) {
    if (info.perRecordLetClause != null && info.perRecordLetClause.getItems() != null) {
      var iterator = info.perRecordLetClause.getItems().iterator();
      while (iterator.hasNext()) {
        var item = iterator.next();
        if (item.getExpression() != null
            && (item.getExpression().isEarlyCalculated(ctx)
                || isCombinationOfQueries(item.getExpression()))) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getExpression());
        } else if (item.getQuery() != null && !item.getQuery().refersToParent()) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getQuery());
        }
      }
    }
  }

  private static final Set<String> COMBINATION_FUNCTIONS =
      Set.of("unionall", "intersect", "difference");

  /**
   * Checks whether an expression is a set-combination function ({@code unionAll},
   * {@code intersect}, {@code difference}) that operates on query-result variables.
   * Such expressions are always promoted to global LET because they aggregate
   * multiple result sets and do not depend on individual records.
   */
  private static boolean isCombinationOfQueries(SQLExpression expression) {
    if (expression.getMathExpression() instanceof SQLBaseExpression exp) {
      if (exp.getIdentifier() != null
          && exp.getModifier() == null
          && exp.getIdentifier().getLevelZero() != null
          && exp.getIdentifier().getLevelZero().getFunctionCall() != null) {
        var fc = exp.getIdentifier().getLevelZero().getFunctionCall();
        if (COMBINATION_FUNCTIONS.stream()
            .anyMatch(fc.getName().getStringValue()::equalsIgnoreCase)) {
          for (var param : fc.getParams()) {
            if (!param.toString().isEmpty() && param.toString().charAt(0) == '$') {
              return true;
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Reorders the conditions within each AND block so that equality conditions
   * ({@code field = value}) appear before range/other conditions.
   *
   * <p>This is critical for index selection: indexes are matched by prefix, so having
   * equalities first maximizes the number of index fields that can be used.
   *
   * <pre>
   * Before: [age &gt; 20, name = 'Alice', city = 'NYC']
   * After:  [name = 'Alice', city = 'NYC', age &gt; 20]
   *          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  equality prefix for composite index
   * </pre>
   */
  @Nullable private static List<SQLAndBlock> moveFlattenedEqualitiesLeft(
      List<SQLAndBlock> flattenedWhereClause) {
    if (flattenedWhereClause == null) {
      return null;
    }

    List<SQLAndBlock> result = new ArrayList<>();
    for (var block : flattenedWhereClause) {
      List<SQLBooleanExpression> equalityExpressions = new ArrayList<>();
      List<SQLBooleanExpression> nonEqualityExpressions = new ArrayList<>();
      var newBlock = block.copy();
      for (var exp : newBlock.getSubBlocks()) {
        if (exp instanceof SQLBinaryCondition binCond) {
          if (binCond.getOperator() instanceof SQLEqualsOperator) {
            equalityExpressions.add(exp);
          } else {
            nonEqualityExpressions.add(exp);
          }
        } else {
          nonEqualityExpressions.add(exp);
        }
      }
      var newAnd = new SQLAndBlock(-1);
      newAnd.getSubBlocks().addAll(equalityExpressions);
      newAnd.getSubBlocks().addAll(nonEqualityExpressions);
      result.add(newAnd);
    }

    return result;
  }

  /**
   * Returns {@code true} when every OR branch in the flattened WHERE is provably unsatisfiable
   * at plan time — e.g. {@code field = 'a' AND field = 'b'} on the same property with different
   * early-calculable literals. The result set is guaranteed empty without scanning.
   */
  static boolean isUnsatisfiableWhere(
      @Nullable List<SQLAndBlock> flattenedWhereClause, CommandContext ctx,
      SchemaClass clazz) {
    if (flattenedWhereClause == null || flattenedWhereClause.isEmpty()) {
      return false;
    }
    for (var block : flattenedWhereClause) {
      if (!isUnsatisfiableAndBlock(block, ctx, clazz)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Detects that one AND block can never match. A block is provably empty when any of these holds
   * on the same property:
   *
   * <ul>
   *   <li>an equality against the SQL {@code null} literal ({@code field = null}) — always false,
   *       so a single such condition empties the block;
   *   <li>contradictory equality values ({@code field = 'a' AND field = 'b'}) — the property would
   *       have to hold two different values at once; and
   *   <li>a non-null equality and an {@code IS NULL} ({@code field = 'a' AND field IS NULL}) — the
   *       property would have to be both a value and null.
   * </ul>
   *
   * <p>The value operand of an equality is resolved at plan time when it is a literal or a bound
   * parameter — both of which the engine can evaluate up front; a per-row expression cannot be
   * resolved and is ignored. The property may sit on either side of the operator, so both
   * {@code field = 'a'} and {@code 'a' = field} are recognised.
   *
   * <p>Two equality values are contradictory only when they stay distinct after coercion to the
   * property's declared type and application of its collate, compared with the engine's own
   * type-aware equality. The runtime filter coerces each literal to the stored value's type, so
   * {@code n = '5' AND n = '05'} on an INTEGER property is satisfiable — both coerce to {@code 5} —
   * and must not be read as empty; likewise it applies the property's collate, so on a
   * case-insensitive STRING property {@code name = 'A' AND name = 'a'} is satisfiable and must not
   * be read as empty. Comparing the raw literals ({@code Objects.equals}, or the untyped equality)
   * would drop those rows. When the property type is unknown — a schemaless field whose property
   * is not declared — the coercion cannot be reproduced, so the equality-contradiction check is
   * skipped; the type-independent {@code = null} and {@code IS NULL} contradictions still apply.
   */
  static boolean isUnsatisfiableAndBlock(
      SQLAndBlock block, CommandContext ctx, SchemaClass clazz) {
    Map<String, List<Object>> literalEqualities = new HashMap<>();
    Set<String> nullRequiredProperties = new HashSet<>();
    for (var expr : block.getSubBlocks()) {
      // `field IS NULL` forces the property to be null; record it and cross-check against non-null
      // equalities on the same property after the loop.
      if (expr instanceof SQLIsNullCondition isNull) {
        var target = isNull.getExpression();
        if (target != null && target.isBaseIdentifier()) {
          nullRequiredProperties.add(target.getDefaultAlias().getStringValue());
        }
        continue;
      }
      if (!(expr instanceof SQLBinaryCondition cond)) {
        continue;
      }
      // `X = null` is always false, so one such condition makes the whole AND block empty.
      if (cond.isEqualityWithNullLiteral()) {
        return true;
      }
      if (!(cond.getOperator() instanceof SQLEqualsOperator)) {
        continue;
      }
      // The property can be on either side of '='; the literal is whatever the other side is,
      // provided it resolves at plan time. Skip conditions with no bare property (e.g. field =
      // field) or a non-literal operand.
      var left = cond.getLeft();
      var right = cond.getRight();
      String property;
      SQLExpression valueExpr;
      if (left != null && left.isBaseIdentifier() && right != null
          && right.isEarlyCalculated(ctx)) {
        property = left.getDefaultAlias().getStringValue();
        valueExpr = right;
      } else if (right != null && right.isBaseIdentifier()
          && left != null && left.isEarlyCalculated(ctx)) {
        property = right.getDefaultAlias().getStringValue();
        valueExpr = left;
      } else {
        continue;
      }
      // Resolve the value operand up front. A bound parameter that is unbound or bound to null
      // resolves to null and is recorded as an always-false `field = null` equality (matching the
      // runtime filter). A constant fold that throws when evaluated at plan time (e.g. `1 / 0`)
      // must not abort planning: on an empty or non-matching scan the runtime filter never
      // evaluates it, so leave the operand to normal planning rather than turning a query that
      // used to run into a plan-time failure.
      Object value;
      try {
        value = valueExpr.execute((Result) null, ctx);
      } catch (RuntimeException ignore) {
        continue;
      }
      literalEqualities.computeIfAbsent(property, k -> new ArrayList<>()).add(value);
    }
    var session = ctx.getDatabaseSession();
    for (var entry : literalEqualities.entrySet()) {
      var values = entry.getValue();
      if (values.size() < 2) {
        continue;
      }
      // The runtime filter coerces each literal to the property's stored type and applies the
      // property's collate before comparing, so two values only prove the block empty when they
      // stay distinct under that same coercion and collation. Without a declared type — a
      // schemaless field whose property is not declared — the coercion is unknown, so the
      // contradiction cannot be proven and the block is left to normal planning.
      var declared = clazz.getProperty(entry.getKey());
      if (declared == null) {
        continue;
      }
      var type = PropertyTypeInternal.convertFromPublicType(declared.getType());
      if (type == null) {
        continue;
      }
      var collate = declared.getCollate();
      var first = values.getFirst();
      for (var i = 1; i < values.size(); i++) {
        if (literalsProvablyDistinct(session, first, values.get(i), type, collate)) {
          return true;
        }
      }
    }
    // A property forced null by IS NULL cannot also satisfy an equality on the same property: a
    // non-null value contradicts IS NULL, and an equality whose value is null (a `= null` literal
    // returns above; a parameter bound to null reaches the map) matches no row on its own. Either
    // way, a property in both maps empties the block.
    for (var nullProperty : nullRequiredProperties) {
      if (literalEqualities.containsKey(nullProperty)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when {@code a} and {@code b} stay unequal after coercion to {@code type} and application
   * of {@code collate}, so no stored value of that type can equal both. Mirrors the runtime filter
   * ({@link com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBinaryCondition#evaluate}): the
   * property's collate is applied to each operand before the engine's type-aware equality, so a
   * case-insensitive property treats {@code 'A'} and {@code 'a'} as equal (satisfiable) rather than
   * distinct. A literal that cannot be coerced to {@code type}, or a collate that rejects the
   * value, matches no row at runtime, but doing that work here can throw; that case is treated as
   * "not provably distinct" so a query that used to run is never turned into a plan-time failure.
   */
  private static boolean literalsProvablyDistinct(
      @Nullable DatabaseSessionEmbedded session, Object a, Object b, PropertyTypeInternal type,
      @Nullable Collate collate) {
    try {
      if (collate != null) {
        // Guard nulls: `equals(x, null)` is already false, and not every Collate implementation is
        // null-safe in transform().
        if (a != null) {
          a = collate.transform(a);
        }
        if (b != null) {
          b = collate.transform(b);
        }
      }
      return !QueryOperatorEquals.equals(session, a, b, type);
    } catch (RuntimeException ignore) {
      return false;
    }
  }

  /**
   * Adds synthetic projection items for ORDER BY expressions that are not already
   * present in the user's SELECT list.
   *
   * <p>When a query sorts by an expression not in the projection (e.g.
   * {@code SELECT name FROM Person ORDER BY age}), the sort step needs access to
   * {@code age}. This method:
   * <ol>
   *   <li>Adds a temporary projection with alias {@code _$$$ORDER_BY_ALIAS$$$_N}</li>
   *   <li>Rewrites the ORDER BY item to reference this alias</li>
   *   <li>Creates a {@code projectionAfterOrderBy} that strips the temporary
   *       aliases after sorting is complete</li>
   * </ol>
   *
   * <pre>
   *  Before:  SELECT name FROM Person ORDER BY age
   *  After:   SELECT name, age AS _$$$ORDER_BY_ALIAS$$$_0 ... ORDER BY _$$$ORDER_BY_ALIAS$$$_0
   *           + projectionAfterOrderBy: SELECT name   (strips the temporary column)
   * </pre>
   */
  private static void addOrderByProjections(QueryPlanningInfo info) {
    if (info.orderApplied
        || info.expand
        || info.unwind != null
        || info.orderBy == null
        || info.orderBy.getItems().isEmpty()
        || info.projection == null
        || info.projection.getItems() == null
        || (info.projection.getItems().size() == 1 && info.projection.getItems().getFirst()
            .isAll())) {
      return;
    }

    var newOrderBy = info.orderBy == null ? null : info.orderBy.copy();
    var additionalOrderByProjections =
        calculateAdditionalOrderByProjections(info.projection.getAllAliases(), newOrderBy);
    if (!additionalOrderByProjections.isEmpty()) {
      info.orderBy = newOrderBy; // the ORDER BY has changed
    }
    if (!additionalOrderByProjections.isEmpty()) {
      info.projectionAfterOrderBy = new SQLProjection(-1);
      info.projectionAfterOrderBy.setItems(new ArrayList<>());
      for (var alias : info.projection.getAllAliases()) {
        info.projectionAfterOrderBy.getItems().add(projectionFromAlias(new SQLIdentifier(alias)));
      }

      for (var item : additionalOrderByProjections) {
        if (info.preAggregateProjection != null) {
          info.preAggregateProjection.getItems().add(item);
          info.aggregateProjection.getItems().add(projectionFromAlias(item.getAlias()));
          info.projection.getItems().add(projectionFromAlias(item.getAlias()));
        } else {
          info.projection.getItems().add(item);
        }
      }
    }
  }

  /**
   * Computes synthetic projection items for ORDER BY expressions not covered by existing
   * projection aliases.
   *
   * <p>For each ORDER BY item whose alias is not in {@code allAliases}, a new
   * {@link SQLProjectionItem} is created with a synthetic alias
   * ({@code _$$$ORDER_BY_ALIAS$$$_0}, {@code _$$$ORDER_BY_ALIAS$$$_1}, ...) and the
   * ORDER BY item is rewritten to reference this alias instead of the original expression.
   *
   * @param allAliases existing aliases in the user's projection
   * @param orderBy    the ORDER BY clause (mutated in-place: items are rewritten to use
   *                   the synthetic aliases)
   * @return synthetic projection items to append (empty if all ORDER BY expressions are
   *         already projected)
   */
  private static List<SQLProjectionItem> calculateAdditionalOrderByProjections(
      Set<String> allAliases, SQLOrderBy orderBy) {
    List<SQLProjectionItem> result = new ArrayList<>();
    var nextAliasCount = 0;
    if ((orderBy != null && orderBy.getItems() != null) || !orderBy.getItems().isEmpty()) {
      for (var item : orderBy.getItems()) {
        if (!allAliases.contains(item.getAlias())) {
          var newProj = new SQLProjectionItem(-1);
          if (item.getAlias() != null) {
            newProj.setExpression(
                new SQLExpression(new SQLIdentifier(item.getAlias()), item.getModifier()));
          } else if (item.getRecordAttr() != null) {
            var attr = new SQLRecordAttribute(-1);
            attr.setName(item.getRecordAttr());
            newProj.setExpression(new SQLExpression(attr, item.getModifier()));
          } else if (item.getRid() != null) {
            var exp = new SQLExpression(-1);
            exp.setRid(item.getRid().copy());
            newProj.setExpression(exp);
          }
          var newAlias = new SQLIdentifier("_$$$ORDER_BY_ALIAS$$$_" + nextAliasCount++);
          newProj.setAlias(newAlias);
          item.setAlias(newAlias.getStringValue());
          item.setModifier(null);
          result.add(newProj);
        }
      }
    }
    return result;
  }

  /**
   * Splits the user's SELECT-list projections into three phases to support SQL
   * aggregation with GROUP BY correctly.
   *
   * <pre>
   *  Example:
   *    SELECT city, count(*) AS cnt, max(price) AS mp FROM Product GROUP BY city
   *
   *  Split result:
   *    preAggregateProjection : [city]
   *        -- per-record fields consumed by the aggregation / grouping
   *    aggregateProjection    : [count(*), max(price)]
   *        -- aggregate accumulators, each producing one value per group
   *    projection (post)      : [city, cnt, mp]
   *        -- final output mapping from aliases to accumulated results
   *
   *  Non-aggregate items (e.g. "city") are forwarded through all three phases
   *  so they remain accessible after aggregation.
   *
   *  Pipeline built later:
   *    ... -&gt; ProjectionCalc(pre) -&gt; AggregateProjectionCalc -&gt; ProjectionCalc(post)
   * </pre>
   *
   * <p>If no aggregate functions are found the three-phase split is skipped entirely
   * (the {@code isSplitted} flag stays {@code false}).
   */
  private static void splitProjectionsForGroupBy(QueryPlanningInfo info, CommandContext ctx) {
    if (info.projection == null) {
      return;
    }

    var preAggregate = new SQLProjection(-1);
    preAggregate.setItems(new ArrayList<>());
    var aggregate = new SQLProjection(-1);
    aggregate.setItems(new ArrayList<>());
    var postAggregate = new SQLProjection(-1);
    postAggregate.setItems(new ArrayList<>());

    var isSplitted = false;

    var db = ctx.getDatabaseSession();
    // split for aggregate projections
    var result = new AggregateProjectionSplit();
    for (var item : info.projection.getItems()) {
      result.reset();
      if (isAggregate(db, item)) {
        isSplitted = true;
        var post = item.splitForAggregation(result, ctx);
        var postAlias = item.getProjectionAlias();
        postAlias = new SQLIdentifier(postAlias, true);
        post.setAlias(postAlias);
        postAggregate.getItems().add(post);
        aggregate.getItems().addAll(result.getAggregate());
        preAggregate.getItems().addAll(result.getPreAggregate());
      } else {
        preAggregate.getItems().add(item);
        // also push the alias forward in the chain
        var aggItem = new SQLProjectionItem(-1);
        aggItem.setExpression(new SQLExpression(item.getProjectionAlias()));
        aggregate.getItems().add(aggItem);
        postAggregate.getItems().add(aggItem);
      }
    }

    // bind split projections to the execution planner
    if (isSplitted) {
      info.preAggregateProjection = preAggregate;
      if (info.preAggregateProjection.getItems() == null
          || info.preAggregateProjection.getItems().isEmpty()) {
        info.preAggregateProjection = null;
      }
      info.aggregateProjection = aggregate;
      if (info.aggregateProjection.getItems() == null
          || info.aggregateProjection.getItems().isEmpty()) {
        info.aggregateProjection = null;
      }
      info.projection = postAggregate;

      addGroupByExpressionsToProjections(db, info);
    }
  }

  /** Delegates to the projection item's own aggregate detection logic. */
  private static boolean isAggregate(DatabaseSessionEmbedded session, SQLProjectionItem item) {
    return item.isAggregate(session);
  }

  /** Creates a pass-through projection item that simply references an existing alias by name. */
  private static SQLProjectionItem projectionFromAlias(SQLIdentifier oIdentifier) {
    var result = new SQLProjectionItem(-1);
    result.setExpression(new SQLExpression(oIdentifier));
    return result;
  }

  /**
   * Ensures that GROUP BY expressions are present in the pre-aggregate projections.
   *
   * <p>If a GROUP BY expression matches an existing pre-aggregate alias (by name), it
   * is reused directly. Otherwise, a synthetic projection with alias
   * {@code _$$$GROUP_BY_ALIAS$$$_N} is added to the pre-aggregate projection, and the
   * GROUP BY is rewritten to reference this alias.
   *
   * <pre>
   *  Example:
   *    SELECT count(*) FROM Product GROUP BY city * 2
   *
   *    city * 2 is not in the pre-aggregate projections, so:
   *      preAggregateProjection += [city * 2 AS _$$$GROUP_BY_ALIAS$$$_0]
   *      GROUP BY rewritten to: _$$$GROUP_BY_ALIAS$$$_0
   * </pre>
   */
  private static void addGroupByExpressionsToProjections(DatabaseSessionEmbedded session,
      QueryPlanningInfo info) {
    if (info.groupBy == null
        || info.groupBy.getItems() == null
        || info.groupBy.getItems().isEmpty()) {
      return;
    }
    // Build a new GROUP BY that references projection aliases instead of raw expressions.
    var newGroupBy = new SQLGroupBy(-1);
    var i = 0;
    for (var exp : info.groupBy.getItems()) {
      if (exp.isAggregate(session)) {
        throw new CommandExecutionException(session, "Cannot group by an aggregate function");
      }
      var found = false;
      if (info.preAggregateProjection != null) {
        for (var alias : info.preAggregateProjection.getAllAliases()) {
          // If the GROUP BY expression is a simple identifier matching an existing
          // projection alias, reuse it -- no need for a synthetic alias.
          if (alias.equals(exp.getDefaultAlias().getStringValue()) && exp.isBaseIdentifier()) {
            found = true;
            newGroupBy.getItems().add(exp);
            break;
          }
        }
      }
      if (!found) {
        // The GROUP BY expression is not already projected -- add a synthetic
        // projection (e.g. _$$$GROUP_BY_ALIAS$$$_0) and rewrite the GROUP BY
        // to reference this alias.
        var newItem = new SQLProjectionItem(-1);
        newItem.setExpression(exp);
        var groupByAlias = new SQLIdentifier("_$$$GROUP_BY_ALIAS$$$_" + i++);
        newItem.setAlias(groupByAlias);
        if (info.preAggregateProjection == null) {
          info.preAggregateProjection = new SQLProjection(-1);
        }
        if (info.preAggregateProjection.getItems() == null) {
          info.preAggregateProjection.setItems(new ArrayList<>());
        }
        info.preAggregateProjection.getItems().add(newItem);
        newGroupBy.getItems().add(new SQLExpression(groupByAlias));
      }

      // Replace the original GROUP BY with the rewritten version after each iteration.
      // This is inside the loop so that subsequent iterations see the updated groupBy.
      info.groupBy = newGroupBy;
    }
  }

  /**
   * Extracts inline subqueries from the WHERE, projection, ORDER BY, and GROUP BY
   * clauses and rewrites them as LET variables.
   *
   * <p>Each extracted subquery is assigned a synthetic alias (prefixed
   * {@code $$$SUBQUERY$$_}) and replaced in-place by a variable reference.
   * The subquery itself is added either to the global LET (when it does not
   * reference the current record) or to the per-record LET (when it uses
   * {@code $parent} or similar back-references).
   *
   * <pre>
   *  Before: SELECT * FROM Foo WHERE bar IN (SELECT id FROM Bar)
   *  After:  LET $$$SUBQUERY$$_0 = (SELECT id FROM Bar)
   *          SELECT * FROM Foo WHERE bar IN $$$SUBQUERY$$_0
   * </pre>
   */
  private static void extractSubQueries(QueryPlanningInfo info) {
    var collector = new SubQueryCollector();
    if (info.perRecordLetClause != null) {
      info.perRecordLetClause.extractSubQueries(collector);
    }
    var i = 0;
    var j = 0;
    for (var entry : collector.getSubQueries().entrySet()) {
      var alias = entry.getKey();
      var query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(info, alias, query, j++);
      } else {
        addGlobalLet(info, alias, query, i++);
      }
    }
    collector.reset();

    if (info.whereClause != null) {
      info.whereClause.extractSubQueries(collector);
    }
    if (info.projection != null) {
      info.projection.extractSubQueries(collector);
    }
    if (info.orderBy != null) {
      info.orderBy.extractSubQueries(collector);
    }
    if (info.groupBy != null) {
      info.groupBy.extractSubQueries(collector);
    }

    for (var entry : collector.getSubQueries().entrySet()) {
      var alias = entry.getKey();
      var query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(info, alias, query);
      } else {
        addGlobalLet(info, alias, query);
      }
    }
  }

  /** Appends an expression-based LET item to the global LET clause. */
  private static void addGlobalLet(QueryPlanningInfo info, SQLIdentifier alias, SQLExpression exp) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new SQLLetClause(-1);
    }
    var item = new SQLLetItem(-1);
    item.setVarName(alias);
    item.setExpression(exp);
    info.globalLetClause.addItem(item);
  }

  /** Appends a subquery-based LET item to the global LET clause. */
  private static void addGlobalLet(QueryPlanningInfo info, SQLIdentifier alias, SQLStatement stm) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new SQLLetClause(-1);
    }
    var item = new SQLLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.globalLetClause.addItem(item);
  }

  /** Inserts a subquery-based LET item at position {@code pos} in the global LET clause. */
  private static void addGlobalLet(
      QueryPlanningInfo info, SQLIdentifier alias, SQLStatement stm, int pos) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new SQLLetClause(-1);
    }
    var item = new SQLLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.globalLetClause.getItems().add(pos, item);
  }

  /** Appends a subquery-based LET item to the per-record LET clause. */
  private static void addRecordLevelLet(QueryPlanningInfo info, SQLIdentifier alias,
      SQLStatement stm) {
    if (info.perRecordLetClause == null) {
      info.perRecordLetClause = new SQLLetClause(-1);
    }
    var item = new SQLLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.perRecordLetClause.addItem(item);
  }

  /** Inserts a subquery-based LET item at position {@code pos} in the per-record LET clause. */
  private static void addRecordLevelLet(
      QueryPlanningInfo info, SQLIdentifier alias, SQLStatement stm, int pos) {
    if (info.perRecordLetClause == null) {
      info.perRecordLetClause = new SQLLetClause(-1);
    }
    var item = new SQLLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.perRecordLetClause.getItems().add(pos, item);
  }

  /**
   * Determines the data source for the SELECT and appends the appropriate fetch step(s).
   *
   * <p>Target resolution strategy (first match wins):
   * <pre>
   *  Target AST node         | Handler                     | Fetch step(s)
   *  ------------------------|-----------------------------|--------------------------
   *  null                    | handleNoTarget()            | EmptyDataGeneratorStep
   *  $variable               | handleVariableAsTarget()    | FetchFromVariableStep
   *  ClassName               | handleClassAsTarget()       | FetchFromClass/Index/...
   *  (SELECT subquery)       | handleSubqueryAsTarget()    | SubQueryStep
   *  :inputParam             | handleInputParamAsTarget()  | depends on param type
   *  multiple :params        | (parallel sub-plans)        | ParallelExecStep
   *  metadata:SCHEMA/...     | handleMetadataAsTarget()    | FetchFromRids/Metadata
   *  [#rid1, #rid2, ...]     | handleRidsAsTarget()        | FetchFromRidsStep
   * </pre>
   *
   * <p>For class targets, index-based optimizations are attempted before falling
   * back to a full class scan (see {@link #handleClassAsTarget}).
   */
  private void handleFetchFromTarget(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {

    var target = info.target == null ? null : info.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx, profilingEnabled);
    } else if (target.getIdentifier() != null) {
      var className = target.getIdentifier().getStringValue();
      if (!className.isEmpty() && className.charAt(0) == '$'
          && !ctx.getDatabaseSession()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .existsClass(className)) {
        handleVariableAsTarget(result, info, ctx, profilingEnabled);
      } else {
        var ridRangeConditions = extractRidRanges(info.flattenedWhereClause, ctx);
        if (!ridRangeConditions.isEmpty()) {
          info.ridRangeConditions = ridRangeConditions;
        }

        handleClassAsTarget(result, info, ctx, profilingEnabled);
      }
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(
          result, target.getStatement(), ctx, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(
          result,
          info,
          target.getInputParam(),
          ctx,
          profilingEnabled);
    } else if (target.getInputParams() != null && !target.getInputParams().isEmpty()) {
      List<InternalExecutionPlan> plans = new ArrayList<>();
      for (var param : target.getInputParams()) {
        var subPlan = new SelectExecutionPlan(ctx);
        handleInputParamAsTarget(
            subPlan,
            info,
            param,
            ctx,
            profilingEnabled);
        plans.add(subPlan);
      }
      result.chain(new ParallelExecStep(plans, ctx, profilingEnabled));
    } else if (target.getMetadata() != null) {
      handleMetadataAsTarget(result, target.getMetadata(), ctx, profilingEnabled);
    } else if (target.getRids() != null && !target.getRids().isEmpty()) {
      handleRidsAsTarget(result, target.getRids(), ctx, profilingEnabled);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Handles a context variable (e.g. {@code $myVar}) as the FROM target.
   * The variable's value is expected to be iterable (typically a result set
   * from a previous LET assignment).
   */
  private static void handleVariableAsTarget(
      SelectExecutionPlan plan,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    final var targetItem = info.target.getItem();
    if (targetItem.getModifier() != null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Modifiers cannot be used with variables: " + targetItem);
    }
    plan.chain(
        new FetchFromVariableStep(
            targetItem.getIdentifier().getStringValue(), ctx, profilingEnabled));
  }

  /**
   * Extracts RID range conditions (e.g. {@code @rid > #10:5 AND @rid < #10:100}) from
   * the flattened WHERE clause. These conditions are stored separately in
   * {@link QueryPlanningInfo#ridRangeConditions} and used to narrow the collection scan
   * range in {@link FetchFromClassExecutionStep}.
   */
  private static SQLAndBlock extractRidRanges(List<SQLAndBlock> flattenedWhereClause,
      CommandContext ctx) {
    var result = new SQLAndBlock(-1);

    if (flattenedWhereClause == null || flattenedWhereClause.size() != 1) {
      return result;
    }

    for (var booleanExpression : flattenedWhereClause.getFirst().getSubBlocks()) {
      if (isRidRange(booleanExpression, ctx)) {
        result.getSubBlocks().add(booleanExpression.copy());
      }
    }

    return result;
  }

  /**
   * Returns {@code true} if the given boolean expression is a range comparison on
   * {@code @rid} (e.g. {@code @rid > #10:5}).
   */
  private static boolean isRidRange(SQLBooleanExpression booleanExpression, CommandContext ctx) {
    if (booleanExpression instanceof SQLBinaryCondition cond) {
      var operator = cond.getOperator();
      if (operator.isRangeOperator() && cond.getLeft().toString().equalsIgnoreCase("@rid")) {
        Object obj;
        if (cond.getRight().getRid() != null) {
          obj = cond.getRight().getRid().toRecordId((Result) null, ctx);
        } else {
          obj = cond.getRight().execute((Result) null, ctx);
        }
        return obj instanceof Identifiable;
      }
    }
    return false;
  }

  /**
   * Handles a positional / named input parameter ({@code ?} or {@code :param}) as
   * the FROM target. The runtime value is inspected and dispatched to the
   * appropriate handler:
   * <ul>
   *   <li>{@code null} -- {@link EmptyStep} (no rows)</li>
   *   <li>{@link SchemaClass} or String -- treated as a class name</li>
   *   <li>{@link Identifiable} -- single RID fetch</li>
   *   <li>{@link Iterable} -- collection of RIDs</li>
   * </ul>
   */
  private void handleInputParamAsTarget(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      SQLInputParameter inputParam,
      CommandContext ctx,
      boolean profilingEnabled) {
    var session = ctx.getDatabaseSession();
    var paramValue = inputParam.getValue(ctx.getInputParameters());
    switch (paramValue) {
      case null -> result.chain(new EmptyStep(ctx, profilingEnabled)); // nothing to return
      case SchemaClass schemaClass -> {
        var from = new SQLFromClause(-1);
        var item = new SQLFromItem(-1);
        from.setItem(item);
        item.setIdentifier(new SQLIdentifier(schemaClass.getName()));
        handleClassAsTarget(result, from, info, ctx, profilingEnabled);
      }
      case String s -> {
        // strings are treated as classes
        var from = new SQLFromClause(-1);
        var item = new SQLFromItem(-1);
        from.setItem(item);
        item.setIdentifier(new SQLIdentifier(s));
        handleClassAsTarget(result, from, info, ctx, profilingEnabled);
      }
      case Identifiable identifiable -> {
        var orid = identifiable.getIdentity();

        var rid = new SQLRid(-1);
        var collection = new SQLInteger(-1);
        collection.setValue(orid.getCollectionId());
        var position = new SQLInteger(-1);
        position.setValue(orid.getCollectionPosition());
        rid.setLegacy(true);
        rid.setCollection(collection);
        rid.setPosition(position);
        handleRidsAsTarget(result, Collections.singletonList(rid), ctx, profilingEnabled);
      }
      case Iterable<?> iterable -> {
        // try list of RIDs
        List<SQLRid> rids = new ArrayList<>();
        for (var x : iterable) {
          if (!(x instanceof Identifiable id)) {
            throw new CommandExecutionException(session,
                "Cannot use colleciton as target: " + paramValue);
          }
          var orid = id.getIdentity();

          var rid = new SQLRid(-1);
          var collection = new SQLInteger(-1);
          collection.setValue(orid.getCollectionId());
          var position = new SQLInteger(-1);
          position.setValue(orid.getCollectionPosition());
          rid.setCollection(collection);
          rid.setPosition(position);
          rids.add(rid);
        }
        if (!rids.isEmpty()) {
          handleRidsAsTarget(result, rids, ctx, profilingEnabled);
        } else {
          result.chain(new EmptyStep(ctx, profilingEnabled)); // nothing to return
        }
      }
      default -> throw new CommandExecutionException(session, "Invalid target: " + paramValue);
    }
  }

  /**
   * Handles a SELECT without a FROM clause (e.g. {@code SELECT 1+1, sysdate()}).
   * Produces a single empty record so that the projection step can evaluate
   * expressions exactly once.
   */
  private static void handleNoTarget(
      SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, ctx, profilingEnabled));
  }

  /**
   * Handles {@code SELECT FROM metadata:SCHEMA}, {@code metadata:INDEXES},
   * {@code metadata:STORAGE}, or {@code metadata:DATABASE} targets.
   * Each is mapped to a specialized fetch step that reads the corresponding
   * metadata structures.
   */
  private static void handleMetadataAsTarget(
      SelectExecutionPlan plan,
      SQLMetadataIdentifier metadata,
      CommandContext ctx,
      boolean profilingEnabled) {
    var db = ctx.getDatabaseSession();
    String schemaRecordIdAsString;
    if (metadata.getName().equalsIgnoreCase("SCHEMA")) {
      schemaRecordIdAsString = db.getStorage().getSchemaRecordId();
      var schemaRid = RecordIdInternal.fromString(schemaRecordIdAsString, false);
      plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx, profilingEnabled));
    } else if (metadata.getName().equalsIgnoreCase("INDEXES")) {
      plan.chain(new FetchFromIndexManagerStep(ctx, profilingEnabled));
    } else if (metadata.getName().equalsIgnoreCase("STORAGE")) {
      plan.chain(new FetchFromStorageMetadataStep(ctx, profilingEnabled));
    } else if (metadata.getName().equalsIgnoreCase("DATABASE")) {
      plan.chain(new FetchFromDatabaseMetadataStep(ctx, profilingEnabled));
    } else {
      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
    }
  }

  /**
   * Handles an explicit list of RIDs as the FROM target
   * (e.g. {@code SELECT FROM [#10:3, #10:7]}).
   */
  private static void handleRidsAsTarget(
      SelectExecutionPlan plan, List<SQLRid> rids, CommandContext ctx, boolean profilingEnabled) {
    List<RecordIdInternal> actualRids = new ArrayList<>();
    for (var rid : rids) {
      actualRids.add(rid.toRecordId((Result) null, ctx));
    }
    // skipMissing=true: a query-level fetch over a set of RIDs - an explicit FROM [rids], a bound
    // RID-list param, or a MATCH @rid-promotion routed through here - must not truncate the set
    // when one RID is dangling (deleted / never allocated). It skips the hole and continues,
    // matching a class scan. The step default stays terminate-on-missing for low-level callers
    // such as DeleteEdge that construct FetchFromRidsStep directly.
    plan.chain(new FetchFromRidsStep(actualRids, ctx, profilingEnabled, /* skipMissing= */ true));
  }

  /**
   * Appends an {@link ExpandStep} when the projection uses {@code expand(field)}.
   * EXPAND takes a single link/collection field and expands each element into its
   * own result row (analogous to UNWIND but resolves links to full records).
   */
  private static void handleExpand(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.expand) {
      result.chain(new ExpandStep(ctx, profilingEnabled, info.expandAlias));
    }
  }

  /**
   * Appends global LET steps to the plan. Global LET items are evaluated exactly
   * once (before the main fetch) and their results are stored in the command context
   * for later use by per-record expressions.
   *
   * <p>Two step types are used:
   * <ul>
   *   <li>{@link GlobalLetExpressionStep} -- for simple expressions
   *       (e.g. {@code LET $x = 42})</li>
   *   <li>{@link GlobalLetQueryStep} -- for subquery expressions
   *       (e.g. {@code LET $x = (SELECT FROM Foo)})</li>
   * </ul>
   *
   * <p>The items are sorted to match the original LET declaration order
   * (important when one LET variable references a previous one).
   */
  private void handleGlobalLet(
      SelectExecutionPlan result,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.globalLetClause != null) {
      var items = info.globalLetClause.getItems();
      items = sortLet(items, this.statement.getLetClause());
      List<String> scriptVars = new ArrayList<>();
      for (var item : items) {
        if (item.getExpression() != null) {
          result.chain(
              new GlobalLetExpressionStep(
                  item.getVarName(), item.getExpression(), ctx, profilingEnabled));
        } else {
          result.chain(
              new GlobalLetQueryStep(
                  item.getVarName(), item.getQuery(), ctx, profilingEnabled, scriptVars));
        }
        scriptVars.add(item.getVarName().getStringValue());
        info.globalLetPresent = true;
      }
    }
  }

  /**
   * Pushes LET-independent WHERE conjuncts before per-record LET evaluation.
   *
   * <p>If the WHERE clause contains conjuncts that do not reference any per-record
   * LET variable (and do not reference {@code $parent}), those conjuncts are
   * split off and evaluated as an early {@link FilterStep} <em>before</em> the
   * per-record LET steps. This avoids executing expensive LET subqueries for
   * rows that will be discarded by the WHERE filter.
   *
   * <p>The remaining LET-dependent conjuncts (if any) stay in
   * {@code info.whereClause} for the subsequent {@link #handleWhere} call.
   * When the entire WHERE is LET-independent, it is fully pushed down and
   * {@code info.whereClause} is set to {@code null}.
   *
   * <p><b>Guards</b> (any one skips the optimization):
   * <ul>
   *   <li>No per-record LET clause, or its items list is empty</li>
   *   <li>No WHERE clause</li>
   *   <li>Last chained step is a {@link SubQueryStep} — inserting a
   *       FilterStep here would create a false
   *       {@code SubQueryStep → FilterStep} adjacency that
   *       {@link #tryPushDownFilterIntoExpand} would incorrectly match</li>
   * </ul>
   */
  private void handleLetPreFilter(
      SelectExecutionPlan plan,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    // Guard: must have both a per-record LET and a WHERE clause.
    if (info.perRecordLetClause == null
        || info.perRecordLetClause.getItems().isEmpty()) {
      return;
    }
    if (info.whereClause == null) {
      return;
    }

    // When the last chained step is a SubQueryStep, inserting a FilterStep
    // creates a SubQueryStep → FilterStep adjacency that
    // tryPushDownFilterIntoExpand() would incorrectly match. This is only
    // dangerous for mixed splits where info.whereClause retains a non-null
    // dependent part — tryPushDownFilterIntoExpand would try to push the
    // dependent WHERE (which references LET vars) into the subquery's expand.
    // For full push-downs (dependent == null), info.whereClause becomes null and
    // tryPushDownFilterIntoExpand bails out at its null check, so the
    // adjacency is harmless.
    var steps = plan.getSteps();
    boolean afterSubQuery =
        !steps.isEmpty() && steps.getLast() instanceof SubQueryStep;

    // Collect per-record LET variable names.
    var letVarNames = new HashSet<String>();
    for (var item : info.perRecordLetClause.getItems()) {
      var name = item.getVarName().getStringValue();
      if (name != null) {
        letVarNames.add(name);
      }
    }

    // splitByLetDependency never returns null; the outcome is encoded by the
    // nullability of independent()/dependent() (see LetSplitResult).
    var split = info.whereClause.splitByLetDependency(letVarNames);
    if (split.independent() == null) {
      // No LET-independent conjuncts to push — leave the WHERE for handleWhere.
      return;
    }

    long timeout =
        info.timeout != null ? info.timeout.getVal().longValue() : -1;

    if (split.dependent() == null) {
      // Entire WHERE is LET-independent — push it all. Safe even after a
      // SubQueryStep: info.whereClause becomes null, so
      // tryPushDownFilterIntoExpand bails at its null guard.
      plan.chain(new FilterStep(split.independent(), ctx, timeout, profilingEnabled));
      info.whereClause = null;
      return;
    }

    // Mixed split. Blocked after a SubQueryStep because info.whereClause would
    // stay non-null with the dependent part, creating a false
    // SubQueryStep -> FilterStep adjacency that tryPushDownFilterIntoExpand
    // would incorrectly match.
    if (afterSubQuery) {
      return;
    }
    plan.chain(new FilterStep(split.independent(), ctx, timeout, profilingEnabled));
    info.whereClause = split.dependent();
  }

  /**
   * Appends per-record LET steps. Unlike global LETs, these are evaluated once
   * for every record flowing through the pipeline.
   *
   * <p>Uses {@link LetExpressionStep} for expressions and {@link LetQueryStep}
   * for subqueries. Results are stored as metadata on the current result row
   * so they can be referenced by subsequent WHERE / projection expressions.
   *
   * <p>Note: this method may be called multiple times during planning (e.g.
   * from the indexed-function path that injects per-record LETs into sub-plans).
   */
  private void handleLet(
      SelectExecutionPlan plan,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.perRecordLetClause == null) {
      return;
    }
    var items = info.perRecordLetClause.getItems();
    items = sortLet(items, this.statement.getLetClause());

    var shared = detectSharedLetBases(items);
    var alreadyGrouped = new HashSet<SQLLetItem>();
    var letHostedPipeline = statementHasUserPerRecordLet(statement);

    for (var item : items) {
      if (alreadyGrouped.contains(item)) {
        continue;
      }
      if (item.getExpression() != null) {
        plan.chain(
            new LetExpressionStep(
                item.getVarName(), item.getExpression(), ctx, profilingEnabled));
        continue;
      }

      var group = shared.groups().get(item);
      if (group != null && group.size() > 1) {
        var sharedInner = shared.innerByItem().get(item);
        var commonFilter = shared.commonFilterByGroup().get(group.getFirst());
        var entries = new ArrayList<MaterializedLetGroupStep.LetEntry>();
        for (var grouped : group) {
          entries.add(new MaterializedLetGroupStep.LetEntry(
              grouped.getVarName(), grouped.getQuery()));
          alreadyGrouped.add(grouped);
        }
        plan.chain(new MaterializedLetGroupStep(
            sharedInner, commonFilter, entries, ctx, profilingEnabled, letHostedPipeline));
      } else {
        plan.chain(
            new LetQueryStep(
                item.getVarName(), item.getQuery(), ctx, profilingEnabled, letHostedPipeline));
      }
    }
  }

  /**
   * Groups per-record LET subquery items by their inner FROM subquery. Items
   * whose full query is a {@code SELECT ... FROM (innerSubquery) WHERE ...}
   * pattern are grouped by structural equality of the inner subquery.
   *
   * <p>Returns a map from each item to its group (list of items sharing the
   * same inner subquery). Items that don't match the pattern or have unique
   * inner subqueries map to a singleton list.
   */
  /**
   * Groups per-record LET subquery items by their shared inner FROM subquery
   * and extracts common WHERE conditions within each group.
   *
   * @param groups            maps each item to its group (all items sharing
   *                          the same inner subquery)
   * @param innerByItem       maps each item to its extracted inner FROM subquery
   * @param commonFilterByGroup common WHERE filter per group, keyed by the first
   *                          item of the group. Null value means no common filter.
   */
  private record SharedLetBases(
      Map<SQLLetItem, List<SQLLetItem>> groups,
      Map<SQLLetItem, SQLStatement> innerByItem,
      Map<SQLLetItem, SQLWhereClause> commonFilterByGroup) {
  }

  private static SharedLetBases detectSharedLetBases(
      List<SQLLetItem> items) {
    // Group subquery items by the string representation of their inner FROM
    // subquery. Using toString() instead of equals() because SQLStatement.equals()
    // may not work reliably across different AST instances parsed from the same
    // text (e.g. different object identities for $parent.$current references).
    var byInnerText = new LinkedHashMap<String, List<SQLLetItem>>();
    var innerByItem = new HashMap<SQLLetItem, SQLStatement>();
    for (var item : items) {
      if (item.getQuery() == null) {
        continue;
      }
      var inner = extractInnerFromSubquery(item.getQuery());
      if (inner == null) {
        continue;
      }
      innerByItem.put(item, inner);
      var key = inner.toString();
      byInnerText.computeIfAbsent(key, k -> new ArrayList<>()).add(item);
    }

    var groups = new HashMap<SQLLetItem, List<SQLLetItem>>();
    var commonFilterByGroup = new HashMap<SQLLetItem, SQLWhereClause>();
    for (var group : byInnerText.values()) {
      for (var item : group) {
        groups.put(item, group);
      }
      if (group.size() > 1) {
        var commonFilter = extractCommonFilter(group);
        if (commonFilter != null) {
          commonFilterByGroup.put(group.getFirst(), commonFilter);
        }
      }
    }
    return new SharedLetBases(groups, innerByItem, commonFilterByGroup);
  }

  /**
   * Extracts the common WHERE conditions shared by all entries in a group.
   * Decomposes each entry's WHERE into AND-level sub-blocks and computes
   * the intersection using structural {@code equals()} on
   * {@link SQLBooleanExpression} instances.
   *
   * @return a WHERE clause containing only the common conditions, or
   *         {@code null} if there is no common filter
   */
  @Nullable private static SQLWhereClause extractCommonFilter(List<SQLLetItem> group) {
    List<List<SQLBooleanExpression>> allConditions = new ArrayList<>();
    for (var item : group) {
      var conditions = extractAndConditions(item.getQuery());
      if (conditions == null || conditions.isEmpty()) {
        // An entry with no WHERE contributes an empty set to the
        // intersection — the result will be empty.
        return null;
      }
      allConditions.add(conditions);
    }

    // Compute intersection: start with the first entry's conditions,
    // then retain only those present in all other entries.
    var intersection = new ArrayList<>(allConditions.getFirst());
    for (int i = 1; i < allConditions.size(); i++) {
      var other = allConditions.get(i);
      intersection.removeIf(cond -> !containsExpression(other, cond));
    }

    if (intersection.isEmpty()) {
      return null;
    }

    // Build the common WHERE clause from intersection conditions.
    var commonWhere = buildWhereFromConditions(intersection);

    // Defensive guard: common filter must not reference $parent (impossible
    // by definition since $parent conditions vary per entry).
    if (commonWhere.refersToParent()) {
      return null;
    }
    return commonWhere;
  }

  /**
   * Extracts AND-level conditions from a LET entry's full query WHERE clause.
   * Returns {@code null} if the query has no WHERE or is not a SELECT statement.
   */
  @Nullable private static List<SQLBooleanExpression> extractAndConditions(
      SQLStatement query) {
    if (!(query instanceof SQLSelectStatement selectStmt)) {
      return null;
    }
    var where = selectStmt.getWhereClause();
    if (where == null) {
      return null;
    }
    var base = where.getBaseExpression();
    if (base == null) {
      return null;
    }

    // Unwrap: parser produces SQLOrBlock -> [SQLAndBlock -> [conditions]]
    if (base instanceof SQLOrBlock orBlock) {
      var orSubs = orBlock.getSubBlocks();
      if (orSubs.size() == 1 && orSubs.getFirst() instanceof SQLAndBlock andBlock) {
        // Single OR branch — unwrap to AND-level sub-blocks
        return new ArrayList<>(andBlock.getSubBlocks());
      }
      // Multiple OR branches — treat entire OR as a single opaque condition
      return List.of(base);
    }
    if (base instanceof SQLAndBlock andBlock) {
      return new ArrayList<>(andBlock.getSubBlocks());
    }
    // Single condition (e.g. @class = 'X') — wrap in a list
    return new ArrayList<>(List.of(base));
  }

  /**
   * Checks if a list of expressions contains one structurally equal to the
   * given expression, using {@link SQLBooleanExpression#equals(Object)}.
   */
  private static boolean containsExpression(
      List<SQLBooleanExpression> list, SQLBooleanExpression target) {
    for (var expr : list) {
      if (expr.equals(target)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a {@link SQLWhereClause} from a list of AND-conditions.
   */
  private static SQLWhereClause buildWhereFromConditions(
      List<SQLBooleanExpression> conditions) {
    SQLBooleanExpression base;
    if (conditions.size() == 1) {
      base = conditions.getFirst().copy();
    } else {
      var andBlock = new SQLAndBlock(-1);
      for (var cond : conditions) {
        andBlock.getSubBlocks().add(cond.copy());
      }
      base = andBlock;
    }
    var where = new SQLWhereClause(-1);
    where.setBaseExpression(base);
    return where;
  }

  /**
   * Extracts the inner FROM subquery from a LET query of the form
   * {@code SELECT ... FROM (innerSubquery) WHERE ...}. Returns {@code null}
   * if the query does not match this pattern.
   */
  @Nullable private static SQLStatement extractInnerFromSubquery(SQLStatement query) {
    if (!(query instanceof SQLSelectStatement selectStmt)) {
      return null;
    }
    var target = selectStmt.getTarget();
    if (target == null) {
      return null;
    }
    var fromItem = target.getItem();
    if (fromItem == null) {
      return null;
    }
    return fromItem.getStatement();
  }

  /**
   * Sorts LET items to match the order declared in the original SQL LET clause.
   * This is important because LET items may reference previously declared variables
   * (e.g. {@code LET $a = 1, $b = $a + 1}) so evaluation order must be preserved.
   *
   * <p>Items present in {@code items} but not in the original {@code letClause}
   * (e.g. synthetic variables from {@code extractSubQueries()}) are appended at
   * the end in their original insertion order.
   */
  private static List<SQLLetItem> sortLet(List<SQLLetItem> items, SQLLetClause letClause) {
    if (letClause == null) {
      return items;
    }
    List<SQLLetItem> i = new ArrayList<>(items);
    var result = new ArrayList<SQLLetItem>();
    for (var item : letClause.getItems()) {
      var var = item.getVarName().getStringValue();
      var iterator = i.iterator();
      while (iterator.hasNext()) {
        var x = iterator.next();
        if (x.getVarName().getStringValue().equals(var)) {
          iterator.remove();
          result.add(x);
          break;
        }
      }
    }
    result.addAll(i);
    return result;
  }

  /**
   * Appends a {@link FilterStep} for the WHERE clause (if present and not already
   * consumed by an index-based optimization).
   *
   * <p>Note: when index-based fetch is used, the planner may have already set
   * {@code info.whereClause = null} to indicate that the WHERE was fully satisfied
   * by the index. In that case this method is a no-op.
   */
  private void handleWhere(
      SelectExecutionPlan plan,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (info.whereClause != null) {
      plan.chain(
          new FilterStep(
              info.whereClause,
              ctx,
              this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
              profilingEnabled));
    }
  }

  /**
   * Appends an {@link OrderByStep} (and optionally a post-ORDER-BY projection) if:
   * <ul>
   *   <li>An ORDER BY clause exists</li>
   *   <li>The order has not already been satisfied by an index scan
   *       ({@code info.orderApplied == false})</li>
   * </ul>
   *
   * <p>The step loads all upstream records into memory and sorts them. When both SKIP
   * and LIMIT are specified (and no EXPAND/UNWIND invalidates them), the step is told
   * the maximum number of results needed ({@code SKIP + LIMIT}) so it can use a
   * bounded priority queue instead of a full sort.
   *
   * <p>Edge properties (e.g. {@code out_FriendOf}) are detected and flagged so the
   * comparator can handle LINKBAG values correctly.
   *
   * <p>If {@code projectionAfterOrderBy} is set (i.e. synthetic ORDER BY aliases were
   * added during planning), an additional projection step strips those temporary columns.
   */
  public static void handleOrderBy(
      SelectExecutionPlan plan,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    var session = ctx.getDatabaseSession();
    var skipSize = info.skip == null ? 0 : info.skip.getValue(ctx);
    if (skipSize < 0) {
      throw new CommandExecutionException(session, "Cannot execute a query with a negative SKIP");
    }
    var limitSize = info.limit == null ? -1 : info.limit.getValue(ctx);
    Integer maxResults = null;
    if (limitSize >= 0) {
      maxResults = skipSize + limitSize;
    }
    if (info.expand || info.unwind != null) {
      maxResults = null;
    }

    if (!info.orderApplied
        && info.orderBy != null
        && info.orderBy.getItems() != null
        && !info.orderBy.getItems().isEmpty()) {

      if (info.target != null) {
        var targetClass = info.target.getSchemaClass(session);
        if (targetClass != null) {
          info.orderBy
              .getItems()
              .forEach(
                  item -> {
                    var possibleEdgeProperty =
                        targetClass.getProperty("out_" + item.getAlias());
                    if (possibleEdgeProperty != null
                        && possibleEdgeProperty.getType() == PropertyType.LINKBAG) {
                      item.setEdge(true);
                    }
                  });
        }
      }
      plan.chain(
          new OrderByStep(
              info.orderBy,
              maxResults,
              info.primaryKeySortedInput,
              info.indexOrderedUpstream,
              ctx,
              info.timeout != null ? info.timeout.getVal().longValue() : -1,
              profilingEnabled));
      if (info.projectionAfterOrderBy != null) {
        plan.chain(
            new ProjectionCalculationStep(info.projectionAfterOrderBy, ctx, profilingEnabled));
      }
    }
  }

  /** Delegates to the full {@link #handleClassAsTarget} with the info's own target. */
  private void handleClassAsTarget(
      SelectExecutionPlan plan,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    handleClassAsTarget(plan, info.target, info, ctx, profilingEnabled);
  }

  /**
   * Central entry point for class-based data fetch. Tries optimizations in this order:
   *
   * <pre>
   *   1. handleClassAsTargetWithRidEquality       -- early-calculable @rid = / @rid IN
   *   2. handleClassAsTargetWithIndexedFunction   -- indexed function in WHERE
   *   3. handleClassAsTargetWithIndex             -- regular index lookup
   *   4. handleClassWithIndexForSortOnly          -- index used only for ORDER BY
   *   5. FetchFromClassExecutionStep              -- full class scan (fallback)
   * </pre>
   *
   * <p>After the fetch step, a {@link FilterByClassStep} is appended when an index was
   * used, because the index may cover a superclass and return records from sibling
   * classes that must be filtered out. The RID-equality fast path is the exception: it
   * filters class membership at plan time and chains no {@link FilterByClassStep}.
   *
   * <p>For the full-scan fallback, RID ordering (ASC/DESC) is pushed down to the
   * fetch step when the ORDER BY is simply {@code ORDER BY @rid}.
   */
  private void handleClassAsTarget(
      SelectExecutionPlan plan,
      SQLFromClause from,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    var identifier = from.getItem().getIdentifier();
    var targetClass = getSchemaFromContext(ctx).getClass(identifier.getStringValue());
    // Only short-circuit when the class resolves. A target whose name is not in the schema is left
    // to the downstream handlers, which reject it (throwing "Class not found") — so a missing class
    // errors consistently regardless of the WHERE, instead of a provably-empty predicate
    // (e.g. `field = null`) silently turning that error into an empty result set. Resolved classes
    // still short-circuit, including schemaless fields (class resolves, property undeclared).
    if (targetClass != null
        && isUnsatisfiableWhere(info.flattenedWhereClause, ctx, targetClass)) {
      plan.chain(new EmptyStep(ctx, profilingEnabled));
      // The predicate is provably empty. Clear the WHERE so handleWhere() does not append a dead
      // FilterStep over the already-empty stream, matching how the index paths consume the WHERE.
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return;
    }

    // Tried next, once the WHERE is known satisfiable: an early-calculable @rid = /
    // @rid IN under a class target compiles to a direct RID fetch instead of a full
    // class scan. Unlike the index handlers below, this branch chains NO
    // FilterByClassStep — the class-membership guard inside the handler (collection-id
    // intersection) already filters at plan time, whereas FilterByClassStep would
    // redundantly re-filter each loaded record.
    //
    // A top-level @rid predicate always wins over index selection here: when the WHERE is
    // `@rid IN [<list>] AND <indexed predicate>`, the list drives the fetch and the indexed
    // term becomes a post-filter. That is optimal for the intended small-list / equality shapes;
    // only a large IN list paired with a more selective indexed predicate would load more records
    // than an index-first plan.
    if (handleClassAsTargetWithRidEquality(plan, identifier, info, ctx, profilingEnabled)) {
      return;
    }

    if (handleClassAsTargetWithIndexedFunction(
        plan, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    if (handleClassAsTargetWithIndex(
        plan, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    if (info.orderBy != null
        && handleClassWithIndexForSortOnly(
            plan, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    Boolean orderByRidAsc = null; // null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info)) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc(info)) {
      orderByRidAsc = false;
    }
    var className = identifier.getStringValue();

    AbstractExecutionStep fetcher;
    // Reuse the class resolved at the top of the method for the empty-WHERE check instead of
    // looking it up again.
    if (targetClass != null) {
      fetcher =
          new FetchFromClassExecutionStep(
              className, null, info, ctx, orderByRidAsc, profilingEnabled);
    } else {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Class or View not present in the schema: " + className);
    }

    if (orderByRidAsc != null) {
      info.orderApplied = true;
    }
    plan.chain(fetcher);
  }

  /**
   * Attempts to compile an early-calculable {@code @rid = <value>} or
   * {@code @rid IN [<values>]} predicate under a class target into a direct RID
   * fetch, replacing the full class scan the predicate would otherwise survive as a
   * post-filter.
   *
   * <p>The body, in order:
   * <ol>
   *   <li>Cheap O(1) guards: bail when there is no WHERE, or when the WHERE flattens
   *       to more than one OR branch (the extraction primitives only handle a single
   *       OR branch).</li>
   *   <li>Pull at most one RID predicate via {@code extractRidEquality} then
   *       {@code extractRidInList}; if neither matches, return false so the
   *       caller falls through to the index / scan chain.</li>
   *   <li>Gate on {@code ridExpression.isPlanTimeResolvable(ctx)} — a literal or bound
   *       parameter resolves at plan time; a field reference, subquery, or internal
   *       LET-variable reference cannot, so return false and fall through.</li>
   *   <li>Evaluate the RID expression to candidate {@link RecordIdInternal}s (a
   *       singleton for {@code =}, the list elements for {@code IN}), mapping each
   *       result the way {@code SQLRid.toRecordId}'s switch does, then dedup.</li>
   *   <li>Membership-filter the candidates against the class's polymorphic
   *       collection-id set: keep a candidate iff its collection id is in the set.</li>
   *   <li>Survivors present → set {@code info.whereClause} to the extraction
   *       remainder and invalidate {@code info.flattenedWhereClause}, then chain a
   *       {@link FetchFromRidsStep}. No survivors (wrong-class RID, empty {@code IN []},
   *       or an all-non-member list) → chain an {@link EmptyStep}. Both cases return
   *       true so the caller short-circuits past the scan fallthrough.</li>
   * </ol>
   *
   * <p>The membership filter is a correctness requirement: {@link FetchFromRidsStep}
   * fetches by RID with no class check, so a bare fetch would let
   * {@code SELECT FROM A WHERE @rid = <rid-of-B>} wrongly return the B record. The
   * dedup is also a correctness requirement: the step iterates with no dedup, so a
   * duplicate {@code IN} list would otherwise return a record more than once, whereas
   * the scan-plus-filter it replaces returns it once.
   *
   * <p>Leaves {@code info.orderApplied} false so the downstream projections block
   * still assembles ORDER BY / SKIP / LIMIT / GROUP BY / DISTINCT.
   *
   * @return {@code true} if the query was compiled to a direct RID fetch (or an empty
   *     result), {@code false} to fall through to the existing handler chain
   */
  private boolean handleClassAsTargetWithRidEquality(
      SelectExecutionPlan plan,
      SQLIdentifier queryTarget,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (queryTarget == null || info.whereClause == null) {
      return false;
    }
    // More than one OR branch means the RID predicate is not the sole top-level
    // conjunction; the extraction primitives only handle a single OR branch.
    if (info.flattenedWhereClause != null && info.flattenedWhereClause.size() > 1) {
      return false;
    }

    // Pull at most one RID predicate: try equality first, then the IN list. Track which
    // extractor fired: the equality path must not expand a collection-valued RID value
    // (see the multi-value handling below), while the IN path expands all elements.
    var fromEquality = true;
    var extraction = info.whereClause.extractRidEquality();
    if (extraction == null) {
      fromEquality = false;
      extraction = info.whereClause.extractRidInList();
    }
    if (extraction == null) {
      return false;
    }

    // Plan-time RID fetch fires only for a literal or bound param. extractSubQueries() rewrites
    // `@rid IN (subquery)` into `@rid IN $$$SUBQUERY$$_N`. isEarlyCalculated() still reports
    // that as resolvable, but the value is bound only when the LET step runs; evaluating it
    // here yields empty and would wrongly collapse the plan to EmptyStep. isPlanTimeResolvable()
    // excludes it so those queries fall through to scan + filter after the LET.
    // Correlated `$parent` equality is the other non-plan-time case: the RID is known per
    // parent row at execution, so FetchFromCorrelatedRidStep evaluates it then instead of
    // scanning the class. The fetch is LET-hosted only ({@code LetQueryStep},
    // {@code MaterializedLetGroupStep}). IN-lists that refer to $parent stay on the scan path.
    //
    // The gate is ParentOnlyChain.isParentOnlyChain, a closed syntactic whitelist, and NOT
    // SQLExpression.refersToParent(). refersToParent() is existential — it fires when a $parent
    // reference occurs anywhere — so it also admits expressions whose value depends on the inner
    // row, for example `ifnull($parent.$current.missing, first(out('GE')))`. The correlated step
    // evaluates the expression once with a null current record, so such an expression can diverge
    // from the scan and rows are silently lost. refersToParent() is left untouched because its
    // other callers rely on the existential meaning.
    var ridExpression = extraction.ridExpression();
    if (!ridExpression.isPlanTimeResolvable(ctx)) {
      if (fromEquality
          && ctx.isLetHostedCorrelatedRidFetch()
          && ParentOnlyChain.isParentOnlyChain(ridExpression)) {
        var classCollectionIds =
            resolveClassToCollectionIds(queryTarget.getStringValue(), plan);
        if (classCollectionIds == null) {
          // Missing class: fall through so the scan path throws "Class or View not present",
          // matching the plan-time RID branch and the no-WHERE form of this query.
          return false;
        }
        var remaining = extraction.remainingWhere();
        info.whereClause = remaining;
        info.flattenedWhereClause = null;
        // Same IntSet membership shape as ExpandStep / MATCH pre-filter / plan-time RID path.
        plan.chain(
            new FetchFromCorrelatedRidStep(
                ridExpression, classCollectionIds, ctx, profilingEnabled));
        return true;
      }
      return false;
    }

    // Evaluate the RID expression and map its result(s) to RecordIdInternal, mapping
    // each the way SQLRid.toRecordId's switch does.
    var evaluated = ridExpression.execute((Result) null, ctx);
    if (evaluated == null) {
      // A plan-time-resolvable expression that yields null here is an unbound reference — most
      // importantly a batch-script LET variable, which is DECLARED during the script's up-front
      // planning pass (SqlScriptExecutor plans every statement before executing any) but not
      // BOUND until its LET statement runs. Evaluating it now reads null; chaining EmptyStep on
      // that would silently drop every row. Fall through to the scan, which re-evaluates the
      // predicate per row at execution, after the LET has run. A genuine `@rid = null` literal
      // never reaches here — isUnsatisfiableWhere routes it to EmptyStep upstream. info.whereClause
      // is still untouched at this point, so the scan sees the original predicate.
      return false;
    }
    var candidates = new LinkedHashSet<RecordIdInternal>();
    if (MultiValue.isMultiValue(evaluated)) {
      var iterable = MultiValue.getMultiValueIterable(evaluated);
      if (fromEquality) {
        // Scalar @rid = <value>: QueryOperatorEquals (the scan filter this replaces) unwraps only
        // a Collection, and only at size 1 — a size-0 or size-2+ Collection, or a non-Collection
        // multi-value (array, map), never matches a scalar @rid, so the scan returns empty. Keep
        // that parity: optimize only the size-1 Collection and fall through for anything else.
        // Falling through is safe because info.whereClause is not mutated until the success
        // branch, so the scan sees the original @rid = <value> predicate and returns empty.
        if (!(evaluated instanceof Collection<?>)) {
          return false;
        }
        var single = singleElementOrNull(iterable);
        if (single == null) {
          return false;
        }
        var rid = toRecordIdCandidate(single);
        if (rid != null) {
          candidates.add(rid);
        }
      } else if (iterable != null) {
        // IN list: expand every element into a candidate.
        for (var element : iterable) {
          var rid = toRecordIdCandidate(element);
          if (rid != null) {
            candidates.add(rid);
          }
        }
      }
    } else {
      var rid = toRecordIdCandidate(evaluated);
      if (rid != null) {
        candidates.add(rid);
      }
    }

    // Membership-filter: keep only candidates whose collection is in the target
    // class's polymorphic collection set. This is the class filter the scan gave for
    // free, applied here at plan time (see the no-FilterByClassStep note at the call
    // site).
    var classCollectionIds = resolveClassToCollectionIds(queryTarget.getStringValue(), plan);
    if (classCollectionIds == null) {
      // Null means the class does not exist. Fall through (return false) so the scan
      // path reaches its "Class or View not present" throw — matching the error the
      // no-WHERE / non-@rid-WHERE form of this query still raises. Treating null as
      // "no members" here would instead chain EmptyStep and silently swallow a typo'd
      // class name for this one query shape. info.whereClause is untouched up to this
      // point (only the survivors branch below mutates it), so the scan sees the
      // original predicate.
      return false;
    }
    List<RecordIdInternal> members = new ArrayList<>();
    for (var rid : candidates) {
      if (classCollectionIds.contains(rid.getCollectionId())) {
        members.add(rid);
      }
    }

    if (members.isEmpty()) {
      // Wrong-class RID, empty IN [], or an all-non-member list: no rows are possible.
      // Return an empty result rather than falling through to a full scan. Clear the WHERE so
      // handleWhere() does not append a dead FilterStep over the already-empty stream, matching
      // the unsatisfiable-WHERE sibling path at the top of handleClassAsTarget.
      info.whereClause = null;
      info.flattenedWhereClause = null;
      plan.chain(new EmptyStep(ctx, profilingEnabled));
      return true;
    }

    // Wire the extraction remainder as the WHERE for handleWhere to chain a single
    // FilterStep. Both fields must move together: mutating only whereClause and
    // leaving flattenedWhereClause stale violates the invariant the index handlers
    // uphold, so null both. A null remainder means the RID predicate was the sole
    // condition and no filter step is chained.
    info.whereClause = extraction.remainingWhere();
    info.flattenedWhereClause = null;
    // skipMissing=true: a caller-supplied IN list may name an in-class RID at a deleted
    // position, which passes the collection-id membership filter (existence is not checked
    // at plan time). Skipping it in the fetch preserves scan parity — a class scan never
    // visits a dangling position — instead of the default terminate-on-first-missing that
    // would drop every RID after the dangling one.
    plan.chain(new FetchFromRidsStep(members, ctx, profilingEnabled, /* skipMissing= */ true));
    return true;
  }

  /**
   * Returns the sole element of {@code iterable} when it contains exactly one element,
   * otherwise null (for a null iterable, an empty one, or one with two or more elements).
   * Used on the scalar-equality path to mirror {@code QueryOperatorEquals.equals}'s size-1
   * collection unwrap: a scalar {@code @rid} only matches a size-1 collection.
   */
  @Nullable static Object singleElementOrNull(@Nullable Iterable<?> iterable) {
    if (iterable == null) {
      return null;
    }
    var it = iterable.iterator();
    if (!it.hasNext()) {
      return null;
    }
    var first = it.next();
    if (it.hasNext()) {
      return null;
    }
    return first;
  }

  /**
   * Maps a single evaluated value to a {@link RecordIdInternal}, mirroring the switch
   * in {@code SQLRid.toRecordId}: an {@link Identifiable} yields its identity, a
   * {@link String} is parsed as a RID, anything else is skipped (null).
   */
  @Nullable static RecordIdInternal toRecordIdCandidate(Object value) {
    return switch (value) {
      case null -> null;
      // Drop an identity that is not a RecordIdInternal (e.g. a marker/tombstone RID) rather than
      // ClassCastException-ing at plan time, consistent with the no-throw contract of this method.
      case Identifiable identifiable ->
          identifiable.getIdentity() instanceof RecordIdInternal r ? r : null;
      // A malformed RID string (no #/: separator, non-numeric parts) makes fromString throw.
      // Yield null so the candidate is dropped rather than aborting the query: the scan-plus-
      // filter this path replaces returns empty for @rid = '<garbage>' (QueryOperatorEquals
      // swallows the conversion failure), so parity requires empty here, not a thrown error.
      case String s -> {
        try {
          yield RecordIdInternal.fromString(s, false);
        } catch (RuntimeException e) {
          yield null;
        }
      }
      default -> null;
    };
  }

  /**
   * Filters a class's polymorphic collection IDs to only those whose names appear in
   * the given set. Used when a subclass hierarchy query should only scan specific
   * collections.
   *
   * @param db                the database session (for resolving collection names)
   * @param clazz             the schema class whose polymorphic collection IDs are filtered
   * @param filterCollections the set of allowed collection names
   * @return an {@link IntArrayList} containing only the matching collection IDs
   */
  private static IntArrayList classCollectionsFiltered(
      DatabaseSessionEmbedded db, SchemaClass clazz, Set<String> filterCollections) {
    var ids = clazz.getPolymorphicCollectionIds();
    var filtered = new IntArrayList();
    for (var id : ids) {
      if (filterCollections.contains(db.getCollectionNameById(id))) {
        filtered.add(id);
      }
    }
    return filtered;
  }

  /**
   * Attempts to execute the query using indexed functions found in the WHERE clause
   * (e.g. spatial functions like {@code ST_Within()}).
   *
   * <p>For each OR-branch (AND block) in the flattened WHERE clause, the planner:
   * <ol>
   *   <li>Checks for indexed function conditions via
   *       {@code block.getIndexedFunctionConditions()}</li>
   *   <li>If found, picks the best candidate function and creates a
   *       {@link FetchFromIndexedFunctionStep}</li>
   *   <li>If not found, falls back to regular index lookup or full scan for that branch</li>
   *   <li>Remaining WHERE conditions (not covered by the function) are applied as
   *       post-fetch {@link FilterStep}</li>
   * </ol>
   *
   * <p>When the WHERE has multiple OR branches, each branch produces a sub-plan and
   * all are combined via {@link ParallelExecStep} + {@link DistinctExecutionStep}.
   *
   * <p>If this method succeeds, it clears {@code info.whereClause} and
   * {@code info.flattenedWhereClause} to signal that the WHERE has been fully handled.
   *
   * @return {@code true} if the query was handled via indexed functions
   */
  private boolean handleClassAsTargetWithIndexedFunction(
      SelectExecutionPlan plan,
      SQLIdentifier queryTarget,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    if (queryTarget == null) {
      return false;
    }
    var schema = getSchemaFromContext(ctx);
    var clazz = schema.getClassInternal(queryTarget.getStringValue());
    if (clazz == null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Class not found: " + queryTarget);
    }
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.isEmpty()) {
      return false;
    }

    List<InternalExecutionPlan> resultSubPlans = new ArrayList<>();

    var indexedFunctionsFound = false;

    for (var block : info.flattenedWhereClause) {
      var indexedFunctionConditions =
          block.getIndexedFunctionConditions(clazz, ctx.getDatabaseSession());

      indexedFunctionConditions =
          filterIndexedFunctionsWithoutIndex(indexedFunctionConditions, info.target, ctx);

      if (indexedFunctionConditions == null || indexedFunctionConditions.isEmpty()) {
        var bestIndex = findBestIndexFor(ctx,
            clazz.getIndexesInternal(),
            block, clazz);
        if (bestIndex != null) {

          var step = new FetchFromIndexStep(bestIndex, true, ctx, profilingEnabled);

          var subPlan = new SelectExecutionPlan(ctx);
          subPlan.chain(step);
          IntArrayList filterCollectionIds;

          filterCollectionIds = IntArrayList.of(clazz.getPolymorphicCollectionIds());
          subPlan.chain(new GetValueFromIndexEntryStep(ctx, filterCollectionIds, profilingEnabled));
          if (bestIndex.requiresDistinctStep()) {
            subPlan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
          }
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(subPlan, info, ctx, profilingEnabled);
            }
            subPlan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        } else {
          FetchFromClassExecutionStep step;
          step =
              new FetchFromClassExecutionStep(
                  clazz.getName(), null, ctx, true, profilingEnabled);

          var subPlan = new SelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(subPlan, info, ctx, profilingEnabled);
            }
            subPlan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
      } else {
        SQLBinaryCondition blockCandidateFunction = null;
        for (var cond : indexedFunctionConditions) {
          if (!cond.allowsIndexedFunctionExecutionOnTarget(info.target, ctx)) {
            if (!cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx)) {
              throw new CommandExecutionException(ctx.getDatabaseSession(),
                  "Cannot execute " + block + " on " + queryTarget);
            }
          }
          if (blockCandidateFunction == null) {
            blockCandidateFunction = cond;
          } else {
            var thisAllowsNoIndex =
                cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
            var prevAllowsNoIndex =
                blockCandidateFunction.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
            if (!thisAllowsNoIndex && !prevAllowsNoIndex) {
              // none of the functions allow execution without index, so cannot choose one
              throw new CommandExecutionException(ctx.getDatabaseSession(),
                  "Cannot choose indexed function between "
                      + cond
                      + " and "
                      + blockCandidateFunction
                      + ". Both require indexed execution");
            } else if (thisAllowsNoIndex && prevAllowsNoIndex) {
              // both can be calculated without index, choose the best one for index execution
              var thisEstimate = cond.estimateIndexed(info.target, ctx);
              var lastEstimate = blockCandidateFunction.estimateIndexed(info.target, ctx);
              if (thisEstimate > -1 && thisEstimate < lastEstimate) {
                blockCandidateFunction = cond;
              }
            } else if (prevAllowsNoIndex) {
              // choose current condition, because the other one can be calculated without index
              blockCandidateFunction = cond;
            }
          }
        }

        var step =
            new FetchFromIndexedFunctionStep(
                blockCandidateFunction, info.target, ctx, profilingEnabled);
        if (!blockCandidateFunction.executeIndexedFunctionAfterIndexSearch(info.target, ctx)) {
          block = block.copy();
          block.getSubBlocks().remove(blockCandidateFunction);
        }
        if (info.flattenedWhereClause.size() == 1) {
          plan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(plan, info, ctx, profilingEnabled);
            }
            plan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
        } else {
          var subPlan = new SelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
        indexedFunctionsFound = true;
      }
    }

    if (indexedFunctionsFound) {
      if (resultSubPlans.size()
          > 1) { // if resultSubPlans.size() == 1 the step was already chained (see above)
        plan.chain(new ParallelExecStep(resultSubPlans, ctx, profilingEnabled));
        plan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      // WHERE condition already applied
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns {@code true} if any of the boolean expressions reference a LET variable
   * (identifiable by a {@code $} prefix). When LET variables are referenced in a
   * WHERE sub-block, the per-record LET steps must be injected into the sub-plan
   * before the filter step.
   */
  private static boolean refersToLet(List<SQLBooleanExpression> subBlocks) {
    if (subBlocks == null) {
      return false;
    }
    for (var exp : subBlocks) {
      if (!exp.toString().isEmpty() && exp.toString().charAt(0) == '$') {
        return true;
      }
    }
    return false;
  }

  /**
   * Filters out indexed function conditions that cannot actually be executed via an
   * index on the given target. Conditions that can be evaluated without an index
   * (fallback mode) are silently excluded; conditions that require an index but
   * none exists cause an exception.
   */
  @Nullable private static List<SQLBinaryCondition> filterIndexedFunctionsWithoutIndex(
      List<SQLBinaryCondition> indexedFunctionConditions,
      SQLFromClause fromClause,
      CommandContext ctx) {
    if (indexedFunctionConditions == null) {
      return null;
    }
    List<SQLBinaryCondition> result = new ArrayList<>();
    for (var cond : indexedFunctionConditions) {
      if (cond.allowsIndexedFunctionExecutionOnTarget(fromClause, ctx)) {
        result.add(cond);
      } else if (!cond.canExecuteIndexedFunctionWithoutIndex(fromClause, ctx)) {
        throw new CommandExecutionException(ctx.getDatabaseSession(),
            "Cannot evaluate " + cond + ": no index defined");
      }
    }
    return result;
  }

  /**
   * Attempts to satisfy the ORDER BY clause using an index scan (without a WHERE
   * index lookup). When an index's field order matches the ORDER BY fields, the step
   * can iterate the index in the desired direction to produce pre-sorted results,
   * eliminating the need for an in-memory sort.
   *
   * <pre>
   *  Example:
   *    SELECT FROM Person ORDER BY lastName ASC
   *    Index on [lastName]
   *
   *    Pipeline:
   *      FetchFromIndexValuesStep(ASC) -&gt; GetValueFromIndexEntryStep -&gt; ...
   *      (no OrderByStep needed -- data is already sorted)
   * </pre>
   *
   * <p>An index that IGNORES NULL VALUES is never eligible here. Such an index holds no entry
   * for a record that lacks the property, so walking it in order would drop that record from the
   * result entirely rather than merely misplace it. The default configuration keeps null entries
   * (see {@code INDEX_IGNORE_NULL_VALUES_DEFAULT}), so only an explicitly configured index pays
   * the fallback to an in-memory sort.
   *
   * @return {@code true} if an index was used for sorting (plan is updated);
   *         {@code false} if no suitable index was found (caller should fall back)
   */
  private boolean handleClassWithIndexForSortOnly(
      SelectExecutionPlan plan,
      SQLIdentifier queryTarget,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    var schema = getSchemaFromContext(ctx);
    var clazz = schema.getClassInternal(queryTarget.getStringValue());
    if (clazz == null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Class not found: " + queryTarget);
    }

    for (var idx : clazz.getIndexesInternal().stream()
        .filter(i -> i.getDefinition() != null)
        // An index that ignores null values does not cover the records that lack the
        // property, so it cannot order the class without losing them.
        .filter(i -> !i.getDefinition().isNullValuesIgnored())
        .toList()) {
      var indexFields = idx.getDefinition().getProperties();
      if (indexFields.size() < info.orderBy.getItems().size()) {
        continue;
      }
      if (IndexOrderedPlanner.isMultiValueDefinition(idx.getDefinition())
          || !IndexOrderedPlanner.isDefaultCollate(idx.getDefinition().getCollate())) {
        continue;
      }
      var indexFound = true;
      String orderType = null;
      for (var i = 0; i < info.orderBy.getItems().size(); i++) {
        var orderItem = info.orderBy.getItems().get(i);
        if (orderItem.getCollate() != null
            || !IndexOrderedPlanner.isDefaultCollate(orderItem.getDeclaredCollate())) {
          indexFound = false;
          break;
        }
        var indexField = indexFields.get(i);
        if (i == 0) {
          orderType = orderItem.getType();
        } else {
          if (orderType == null || !orderType.equals(orderItem.getType())) {
            indexFound = false;
            break; // ASC/DESC interleaved, cannot be used with index.
          }
        }
        if (!(indexField.equals(orderItem.getAlias())
            || isInOriginalProjection(indexField, orderItem.getAlias()))) {
          indexFound = false;
          break;
        }
      }
      if (indexFound && orderType != null) {
        var orderAsc = orderType.equals(SQLOrderByItem.ASC);
        var placements = ctx.getDatabaseSession().getPlanNullPlacements().resolve();
        // Null-key stream placement follows the first ORDER BY item. An explicit clause wins over
        // the direction-specific setting. A single-property index can move its null bucket.
        // Composite keys keep null components inline and can only provide natural placement.
        var nullsFirst = info.orderBy.getItems().getFirst().nullsFirstFor(placements);
        if (!canProduceNullPlacement(idx.getDefinition(), info.orderBy, orderAsc, placements)) {
          continue;
        }
        plan.chain(
            new FetchFromIndexValuesStep(
                new IndexSearchDescriptor(idx),
                orderAsc,
                nullsFirst,
                ctx,
                profilingEnabled));
        IntArrayList filterCollectionIds;
        filterCollectionIds = IntArrayList.of(clazz.getPolymorphicCollectionIds());
        plan.chain(new GetValueFromIndexEntryStep(ctx, filterCollectionIds, profilingEnabled));
        info.orderApplied = true;
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if {@code alias} is a projected alias for an expression
   * that equals {@code indexField}. This is needed to match ORDER BY items that
   * reference a projection alias rather than the raw field name.
   */
  private boolean isInOriginalProjection(String indexField, String alias) {
    if (info.projection == null) {
      return false;
    }
    if (info.projection.getItems() == null) {
      return false;
    }
    return info.projection.getItems().stream()
        .filter(proj -> proj.getExpression().toString().equals(indexField))
        .filter(proj -> proj.getAlias() != null)
        .anyMatch(proj -> proj.getAlias().getStringValue().equals(alias));
  }

  /**
   * Attempts to satisfy the entire WHERE clause using index lookups on the target
   * class. If the target class itself has no suitable index but is the root of a
   * class hierarchy with subclasses that do, the planner recursively tries each
   * subclass and combines results via {@link ParallelExecStep}.
   *
   * <p>If successful, clears {@code info.whereClause} and
   * {@code info.flattenedWhereClause}.
   *
   * @return {@code true} if index-based fetch was set up for all OR-branches
   */
  private boolean handleClassAsTargetWithIndex(
      SelectExecutionPlan plan,
      SQLIdentifier targetClass,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {

    var result =
        handleClassAsTargetWithIndex(
            targetClass.getStringValue(), null, info, ctx,
            profilingEnabled, true);

    if (result != null) {
      result.forEach(plan::chain);
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    }

    var schema = getSchemaFromContext(ctx);
    var clazz = schema.getClassInternal(targetClass.getStringValue());

    if (clazz == null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Class not found: " + targetClass);
    }

    var session = ctx.getDatabaseSession();
    if (clazz.approximateCount(session, false) != 0 || clazz.getSubclasses().isEmpty()
        || isDiamondHierarchy(clazz)) {
      return false;
    }
    // try subclasses

    var subclasses = clazz.getSubclasses();

    List<InternalExecutionPlan> subclassPlans = new ArrayList<>();
    for (var subClass : subclasses) {
      var subSteps =
          handleClassAsTargetWithIndexRecursive(
              subClass.getName(), null, info, ctx, profilingEnabled);
      if (subSteps == null || subSteps.isEmpty()) {
        return false;
      }
      var subPlan = new SelectExecutionPlan(ctx);
      subSteps.forEach(subPlan::chain);
      subclassPlans.add(subPlan);
    }
    if (!subclassPlans.isEmpty()) {
      plan.chain(new ParallelExecStep(subclassPlans, ctx, profilingEnabled));
      return true;
    }
    return false;
  }

  /**
   * Returns {@code true} if the class is the root of a diamond inheritance hierarchy
   * (i.e. two or more subclasses share a common descendant). Diamond hierarchies
   * prevent per-subclass index plans because a record could appear in multiple
   * subclass scans, leading to incorrect duplicate results.
   *
   * <pre>
   *    A         &lt;-- clazz
   *   / \
   *  B   C       &lt;-- A's subclasses
   *   \ /
   *    D         &lt;-- diamond: D is reachable from both B and C
   * </pre>
   */
  private static boolean isDiamondHierarchy(SchemaClass clazz) {
    Set<SchemaClass> traversed = new HashSet<>();
    List<SchemaClass> stack = new ArrayList<>();
    stack.add(clazz);
    while (!stack.isEmpty()) {
      var current = stack.removeFirst();
      traversed.add(current);
      for (var sub : current.getSubclasses()) {
        if (traversed.contains(sub)) {
          return true;
        }
        stack.add(sub);
        traversed.add(sub);
      }
    }
    return false;
  }

  /**
   * Recursively tries to find index-based fetch steps for the given class or its
   * subclasses. Used when the parent class has no records of its own (abstract
   * hierarchy root) but subclasses may have their own indexes.
   *
   * @return list of steps if successful, {@code null} if any branch cannot be indexed
   */
  @Nullable private List<ExecutionStepInternal> handleClassAsTargetWithIndexRecursive(
      String targetClass,
      Set<String> filterCollections,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled) {
    var result =
        handleClassAsTargetWithIndex(targetClass, filterCollections, info, ctx, profilingEnabled,
            false);
    var session = ctx.getDatabaseSession();
    if (result == null) {
      result = new ArrayList<>();
      var clazz = getSchemaFromContext(ctx).getClassInternal(targetClass);
      if (clazz == null) {
        throw new CommandExecutionException(ctx.getDatabaseSession(),
            "Cannot find class " + targetClass);
      }
      if (clazz.approximateCount(session, false) != 0
          || clazz.getSubclasses().isEmpty()
          || isDiamondHierarchy(clazz)) {
        return null;
      }

      var subclasses = clazz.getSubclasses();

      List<InternalExecutionPlan> subclassPlans = new ArrayList<>();
      for (var subClass : subclasses) {
        var subSteps =
            handleClassAsTargetWithIndexRecursive(
                subClass.getName(), filterCollections, info, ctx, profilingEnabled);
        if (subSteps == null || subSteps.isEmpty()) {
          return null;
        }
        var subPlan = new SelectExecutionPlan(ctx);
        subSteps.forEach(subPlan::chain);
        subclassPlans.add(subPlan);
      }
      if (!subclassPlans.isEmpty()) {
        result.add(new ParallelExecStep(subclassPlans, ctx, profilingEnabled));
      }
    }
    return result.isEmpty() ? null : result;
  }

  /**
   * Core index lookup logic for a single class. For each OR-branch in the flattened
   * WHERE, calls {@link #findBestIndexFor} to select the optimal index. If all branches
   * can be covered by indexes, assembles the fetch steps.
   *
   * <pre>
   *  WHERE (a = 1 AND b = 2) OR (c = 3)
   *  flattenedWhereClause: [AND(a=1,b=2), AND(c=3)]
   *
   *  Each AND block is matched independently:
   *    AND(a=1,b=2) -&gt; idx_a_b (composite index)
   *    AND(c=3)      -&gt; idx_c  (single-field index)
   *
   *  If both succeed -&gt; combine via commonFactor + executionStepFromIndexes
   *  If any fails    -&gt; return null (cannot use indexes for this class)
   * </pre>
   *
   * @param isHierarchyRoot if true, ORDER BY can be satisfied from the index;
   *                        for subclass branches this is false
   * @return list of execution steps, or {@code null} if indexes cannot cover all branches
   */
  @Nullable private List<ExecutionStepInternal> handleClassAsTargetWithIndex(
      String targetClass,
      Set<String> filterCollections,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled,
      boolean isHierarchyRoot) {
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.isEmpty()) {
      return null;
    }

    var clazz = getSchemaFromContext(ctx).getClassInternal(targetClass);
    if (clazz == null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Cannot find class " + targetClass);
    }

    var indexes = clazz.getIndexesInternal();

    final SchemaClass c = clazz;
    var indexSearchDescriptors =
        info.flattenedWhereClause.stream()
            .map(x -> findBestIndexFor(ctx, indexes, x, c))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    if (indexSearchDescriptors.size() != info.flattenedWhereClause.size()) {
      return null; // some blocks could not be managed with an index
    }

    var optimumIndexSearchDescriptors =
        commonFactor(indexSearchDescriptors);

    return executionStepFromIndexes(
        filterCollections,
        clazz,
        info,
        ctx,
        profilingEnabled,
        optimumIndexSearchDescriptors,
        isHierarchyRoot);
  }

  /**
   * Converts a list of {@link IndexSearchDescriptor}s into concrete execution steps.
   *
   * <p>Single-descriptor case: creates a linear chain:
   * <pre>
   *   FetchFromIndexStep -&gt; GetValueFromIndexEntryStep [-&gt; DistinctStep] [-&gt; FilterStep]
   * </pre>
   *
   * <p>Multi-descriptor case: creates parallel sub-plans merged with
   * {@link ParallelExecStep} + {@link DistinctExecutionStep} to deduplicate results
   * from overlapping index ranges.
   */
  private List<ExecutionStepInternal> executionStepFromIndexes(
      Set<String> filterCollections,
      SchemaClass clazz,
      QueryPlanningInfo info,
      CommandContext ctx,
      boolean profilingEnabled,
      List<IndexSearchDescriptor> optimumIndexSearchDescriptors,
      boolean isHierarchyRoot) {
    List<ExecutionStepInternal> result;
    if (optimumIndexSearchDescriptors.size() == 1) {
      var desc = optimumIndexSearchDescriptors.getFirst();
      result = new ArrayList<>();
      var orderAsc = getOrderDirection(info);
      var ascending = !Boolean.FALSE.equals(orderAsc);
      // When ORDER BY is present, place the null-key stream per the first item's resolved null
      // ordering so a fullySorted index plan matches in-memory null placement. Without ORDER BY,
      // keep the shipped placement for the direction and read no placement at all.
      var nullsFirst =
          (info.orderBy != null && !info.orderBy.getItems().isEmpty())
              ? info.orderBy
                  .getItems()
                  .getFirst()
                  .nullsFirstFor(ctx.getDatabaseSession().getPlanNullPlacements().resolve())
              : ascending;
      result.add(
          new FetchFromIndexStep(desc, ascending, nullsFirst, ctx, profilingEnabled));
      IntArrayList filterCollectionIds;
      if (filterCollections != null) {
        filterCollectionIds = classCollectionsFiltered(ctx.getDatabaseSession(), clazz,
            filterCollections);
      } else {
        filterCollectionIds = IntArrayList.of(clazz.getPolymorphicCollectionIds());
      }
      result.add(new GetValueFromIndexEntryStep(ctx, filterCollectionIds, profilingEnabled));
      if (desc.requiresDistinctStep()) {
        result.add(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      // at the moment, we allow this optimization only for root classes in the hierarchy.
      // I.e. For B and C that are subclasses of A, `select from A where aField > 10` will
      // apply this optimization only if `aField` is indexed in the root class A.
      if (isHierarchyRoot
          && orderAsc != null
          && info.orderBy != null
          && fullySorted(
              info.orderBy, desc, ctx.getDatabaseSession().getPlanNullPlacements().resolve())) {
        info.orderApplied = true;
      }
      if (desc.getRemainingCondition() != null && !desc.getRemainingCondition().isEmpty()) {
        if ((info.perRecordLetClause != null
            && refersToLet(Collections.singletonList(desc.getRemainingCondition())))) {
          var stubPlan = new SelectExecutionPlan(ctx);
          handleLet(stubPlan, info, ctx, profilingEnabled);
          for (var step : stubPlan.getSteps()) {
            result.add((ExecutionStepInternal) step);
          }
        }
        result.add(
            new FilterStep(
                createWhereFrom(desc.getRemainingCondition()),
                ctx,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                profilingEnabled));
      }
    } else {
      result = new ArrayList<>();
      result.add(
          createParallelIndexFetch(
              optimumIndexSearchDescriptors, filterCollections, ctx, profilingEnabled));
      if (optimumIndexSearchDescriptors.size() > 1) {
        result.add(new DistinctExecutionStep(ctx, profilingEnabled));
      }
    }
    return result;
  }

  /** Returns the immutable schema snapshot from the session for thread-safe class lookups. */
  private static SchemaInternal getSchemaFromContext(CommandContext ctx) {
    return ctx.getDatabaseSession().getMetadata().getImmutableSchemaSnapshot();
  }

  /**
   * Returns {@code true} if the ORDER BY is fully covered by the index field order
   * in the given descriptor (i.e. no in-memory sort is needed).
   */
  private static boolean fullySorted(
      SQLOrderBy orderBy, IndexSearchDescriptor desc,
      ResolvedOrderByNullsPlacement placements) {
    if (orderBy.ordersWithCollate() || !orderBy.ordersSameDirection()) {
      return false;
    }
    var definition = desc.getIndex().getDefinition();
    // Every item shares one direction here, and an item with no declared type sorts ascending.
    var orderAsc = !SQLOrderByItem.DESC.equals(orderBy.getItems().getFirst().getType());
    if (IndexOrderedPlanner.isMultiValueDefinition(definition)
        || !IndexOrderedPlanner.isDefaultCollate(definition.getCollate())
        || !canProduceNullPlacement(definition, orderBy, orderAsc, placements)) {
      return false;
    }
    for (var item : orderBy.getItems()) {
      if (!IndexOrderedPlanner.isDefaultCollate(item.getDeclaredCollate())) {
        return false;
      }
    }
    return desc.fullySorted(orderBy.getProperties());
  }

  /**
   * Whether the index can produce the requested null placement for every item of {@code orderBy}.
   *
   * <p>A single-property index owns a separate null bucket, which the fetch step concatenates before
   * or after the ordered keys, so it can produce either placement and the check passes at once.
   *
   * <p>A composite index keeps a null component inside the key, so its scan yields only the natural
   * placement, which is first for an ascending scan and last for a descending one. Every item the
   * shortcut would satisfy must request that natural placement, because one item asking for the
   * other end would need a bucket move a composite key cannot do. Reading the leading item alone
   * would keep the shortcut for {@code ORDER BY a ASC, b ASC NULLS LAST} and return a wrong order.
   */
  private static boolean canProduceNullPlacement(
      IndexDefinition definition,
      SQLOrderBy orderBy,
      boolean orderAsc,
      ResolvedOrderByNullsPlacement placements) {
    if (definition.getProperties().size() < 2) {
      return true;
    }
    for (var item : orderBy.getItems()) {
      if (item.nullsFirstFor(placements) != orderAsc) {
        return false;
      }
    }
    return true;
  }

  /**
   * returns TRUE if all the order clauses are ASC, FALSE if all are DESC, null otherwise
   *
   * @return TRUE if all the order clauses are ASC, FALSE if all are DESC, null otherwise
   */
  @Nullable private static Boolean getOrderDirection(QueryPlanningInfo info) {
    if (info.orderBy == null) {
      return null;
    }
    String result = null;
    for (var item : info.orderBy.getItems()) {
      if (result == null) {
        result = item.getType() == null ? SQLOrderByItem.ASC : item.getType();
      } else {
        var newType = item.getType() == null ? SQLOrderByItem.ASC : item.getType();
        if (!newType.equals(result)) {
          return null;
        }
      }
    }
    return result == null || result.equals(SQLOrderByItem.ASC);
  }

  /**
   * Creates a {@link ParallelExecStep} that runs one index fetch sub-plan per
   * {@link IndexSearchDescriptor}. Each sub-plan is:
   * {@code FetchFromIndex -> GetValueFromIndexEntry [-> Distinct] [-> Filter]}.
   */
  private ExecutionStepInternal createParallelIndexFetch(
      List<IndexSearchDescriptor> indexSearchDescriptors,
      Set<String> filterCollections,
      CommandContext ctx,
      boolean profilingEnabled) {
    List<InternalExecutionPlan> subPlans = new ArrayList<>();
    for (var desc : indexSearchDescriptors) {
      var subPlan = new SelectExecutionPlan(ctx);
      subPlan.chain(new FetchFromIndexStep(desc, true, ctx, profilingEnabled));
      IntArrayList filterCollectionIds = null;
      if (filterCollections != null) {
        filterCollectionIds = IntArrayList.of(
            ctx.getDatabaseSession().getCollectionsIds(filterCollections));
      }
      subPlan.chain(new GetValueFromIndexEntryStep(ctx, filterCollectionIds, profilingEnabled));
      if (desc.requiresDistinctStep()) {
        subPlan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      if (desc.getRemainingCondition() != null && !desc.getRemainingCondition().isEmpty()) {
        subPlan.chain(
            new FilterStep(
                createWhereFrom(desc.getRemainingCondition()),
                ctx,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                profilingEnabled));
      }
      subPlans.add(subPlan);
    }
    return new ParallelExecStep(subPlans, ctx, profilingEnabled);
  }

  /** Wraps a single boolean expression in a {@link SQLWhereClause} for use with {@link FilterStep}. */
  private static SQLWhereClause createWhereFrom(SQLBooleanExpression remainingCondition) {
    var result = new SQLWhereClause(-1);
    result.setBaseExpression(remainingCondition);
    return result;
  }

  /**
   * Whether an index has a built engine and so is usable to accelerate a query. An index created
   * inside the still-open transaction that is now querying has no engine yet: its engine id is
   * negative until the commit builds it, and reading an engine-less index throws. Skipping it here
   * lets the WHERE block fall through to a full class scan, which returns the correct merged
   * transaction view (committed rows plus the transaction's own updates minus its deletes). Once the
   * transaction commits and the engine is built the same query accelerates through the index.
   */
  private static boolean isIndexBuilt(Index index) {
    return index.getIndexId() >= 0;
  }

  /**
   * Selects the best index from the given candidates to satisfy as many conditions
   * as possible within the AND block. The selection algorithm works in four stages:
   *
   * <pre>
   *  Stage 1: Build candidates
   *    - For each equality/range index: buildIndexSearchDescriptor()
   *    - For each FULLTEXT index:       buildIndexSearchDescriptorForFulltext()
   *
   *  Stage 2: Prune redundant candidates
   *    - removeGenericIndexes(): prefer target-class index over superclass index
   *    - removePrefixIndexes():  if [a,b] and [a] both match, discard [a]
   *
   *  Stage 3: Sort by cost
   *    - IndexSearchDescriptor.cost() estimates I/O cost
   *    - Keep only candidates tied for lowest cost
   *
   *  Stage 4: Pick the widest
   *    - Among equal-cost candidates, pick the one covering the most fields
   * </pre>
   *
   * <p>An index whose engine is not built yet (a transaction-created index queried inside the same
   * transaction) is excluded up front through {@link #isIndexBuilt}, so a query inside the creating
   * transaction falls through to a full class scan rather than reading an engine-less index.
   *
   * @param indexes all indexes defined on the target class
   * @param block   a single AND block from the flattened WHERE clause
   * @param clazz   the target schema class
   * @return the best index descriptor, or {@code null} if no index can be used
   */
  @Nullable static IndexSearchDescriptor findBestIndexFor(
      CommandContext ctx, Set<Index> indexes, SQLAndBlock block, SchemaClass clazz) {
    // get all valid index descriptors
    var descriptors =
        indexes.stream()
            .filter(SelectExecutionPlanner::isIndexBuilt)
            .filter(Index::canBeUsedInEqualityOperators)
            .map(index -> buildIndexSearchDescriptor(ctx, index, block, clazz))
            .filter(Objects::nonNull)
            .filter(x -> x.getKeyCondition() != null)
            .filter(x -> x.blockCount() > 0)
            .collect(Collectors.toList());

    var fullTextIndexDescriptors =
        indexes.stream()
            .filter(SelectExecutionPlanner::isIndexBuilt)
            .filter(idx -> idx.getType().equalsIgnoreCase("FULLTEXT"))
            .filter(idx -> !idx.getAlgorithm().equalsIgnoreCase("LUCENE"))
            .map(idx -> buildIndexSearchDescriptorForFulltext(idx, block))
            .filter(Objects::nonNull)
            .filter(x -> x.getKeyCondition() != null)
            .filter(x -> x.blockCount() > 0)
            .toList();

    descriptors.addAll(fullTextIndexDescriptors);

    descriptors = removeGenericIndexes(descriptors, clazz);

    // remove the redundant descriptors (eg. if I have one on [a] and one on [a, b], the first one
    // is redundant, just discard it)
    descriptors = removePrefixIndexes(descriptors);

    // sort by cost
    var sortedDescriptors =
        descriptors.stream().map(x -> new PairIntegerObject<>(x.cost(ctx), x)).sorted().toList();

    // get only the descriptors with the lowest cost
    if (sortedDescriptors.isEmpty()) {
      descriptors = Collections.emptyList();
    } else {
      descriptors =
          sortedDescriptors.stream()
              .filter(x -> x.key == sortedDescriptors.getFirst().key)
              .map(x -> x.value)
              .collect(Collectors.toList());
    }

    // sort remaining by the number of indexed fields
    descriptors =
        descriptors.stream()
            .sorted(Comparator.comparingInt(IndexSearchDescriptor::blockCount))
            .collect(Collectors.toList());

    // get the one that has more indexed fields
    return descriptors.isEmpty() ? null : descriptors.getLast();
  }

  /**
   * If between the index candidates there are for the same property target class index and super
   * class index prefer the target class.
   */
  private static List<IndexSearchDescriptor> removeGenericIndexes(
      List<IndexSearchDescriptor> descriptors, SchemaClass clazz) {
    List<IndexSearchDescriptor> results = new ArrayList<>();
    for (var desc : descriptors) {
      IndexSearchDescriptor matching = null;
      for (var result : results) {
        if (desc.isSameCondition(result)) {
          matching = result;
          break;
        }
      }
      if (matching != null) {
        if (clazz.getName().equals(desc.getIndex().getDefinition().getClassName())) {
          results.remove(matching);
          results.add(desc);
        }
      } else {
        results.add(desc);
      }
    }
    return results;
  }

  /**
   * Removes index descriptors that are strict prefixes of other descriptors in the list.
   * When two indexes cover overlapping condition prefixes, the longer (more specific) one
   * is preferred because it narrows the result set further.
   */
  private static List<IndexSearchDescriptor> removePrefixIndexes(
      List<IndexSearchDescriptor> descriptors) {
    List<IndexSearchDescriptor> result = new ArrayList<>();
    for (var desc : descriptors) {
      if (result.isEmpty()) {
        result.add(desc);
      } else {
        var prefixes = findPrefixes(desc, result);
        if (prefixes.isEmpty()) {
          if (!isPrefixOfAny(desc, result)) {
            result.add(desc);
          }
        } else {
          result.removeAll(prefixes);
          result.add(desc);
        }
      }
    }
    return result;
  }

  /** Returns {@code true} if {@code desc} is a condition prefix of any descriptor in the list. */
  private static boolean isPrefixOfAny(IndexSearchDescriptor desc,
      List<IndexSearchDescriptor> result) {
    for (var item : result) {
      if (desc.isPrefixOf(item)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns all descriptors in {@code descriptors} whose condition blocks are a prefix
   * of (or equal to) {@code desc}'s condition blocks. For example, if {@code desc}
   * covers conditions [a, b] and the list contains a descriptor covering [a], that
   * descriptor is returned because [a] is a prefix of [a, b].
   */
  private static List<IndexSearchDescriptor> findPrefixes(
      IndexSearchDescriptor desc, List<IndexSearchDescriptor> descriptors) {
    List<IndexSearchDescriptor> result = new ArrayList<>();
    for (var item : descriptors) {
      if (item.isPrefixOf(desc)) {
        result.add(item);
      }
    }
    return result;
  }

  /**
   * Builds an {@link IndexSearchDescriptor} that describes how to use the given index
   * to evaluate as many conditions as possible from the AND block.
   *
   * <p>The algorithm walks the index's property list (which defines the composite key
   * order) and greedily matches WHERE conditions:
   *
   * <pre>
   *  Index on [city, age, name]
   *  WHERE city = 'NYC' AND age &gt; 20 AND name = 'Alice' AND salary &gt; 50000
   *
   *  Matching:
   *    city  -&gt; city = 'NYC'     (equality, continue to next field)
   *    age   -&gt; age &gt; 20         (range, STOP -- range can only be on last field)
   *    name  -&gt; not reachable (after range)
   *
   *  Result:
   *    indexKeyValue:      [city = 'NYC', age &gt; 20]
   *    remainingCondition: name = 'Alice' AND salary &gt; 50000
   * </pre>
   *
   * <p>For a single indexed property with two conditions on the same field (e.g.
   * {@code age >= 20 AND age < 30}), the method attempts to merge them into a single
   * between-style range condition for the index key.
   *
   * <p>Hash indexes (which do not support ordered iteration) require all key fields
   * to be present; if only a prefix matches, the descriptor is rejected.
   *
   * @return a descriptor, or {@code null} if the index cannot be used for this block
   */
  @Nullable private static IndexSearchDescriptor buildIndexSearchDescriptor(
      CommandContext ctx, Index index, SQLAndBlock block, SchemaClass clazz) {
    var indexProperties = index.getDefinition().getProperties();

    //copy as we will modify the list of expressions
    var blockCopy = block.copy();

    var indexKeyValue = new SQLAndBlock(-1);

    //used if we need to generate a range query instead of a point query and applied to an end range
    //interval of the key to search in index.
    SQLBinaryCondition additionalRangeCondition = null;

    var booleanExpressions = blockCopy.getSubBlocks();
    var propertyNameBooleanExpressionMap =
        new HashMap<String, List<SQLBooleanExpression>>(booleanExpressions.size());

    //group all boolean expressions by indexed property they test,
    // all SQL expressions should be already flattened at this moment
    // so will use only a single property in the expression
    for (var booleanExpression : booleanExpressions) {
      //skip expressions that do not use properties we will apply them later on post filtering
      var indexPropertyName = booleanExpression.getRelatedIndexPropertyName();
      if (indexPropertyName != null) {
        var list = propertyNameBooleanExpressionMap.computeIfAbsent(indexPropertyName,
            k -> new ArrayList<>());
        list.add(booleanExpression);
      }
    }

    //Flag is used to indicate the situation when applied expressions make usage of indexes
    //impossible to use.
    //One of the most typical is the usage of several range conditions as we can apply only
    //one.
    var invalidConditions = new boolean[1];
    //Range condition should always go after equality condition, so sort all expressions by
    //the type of operator they use.
    //Then we will merge all expressions for the same property name that have more than two
    //conditions as we can apply only single equals and single range condition for each property.
    propertyNameBooleanExpressionMap.forEach((indexPropertyName, expressions) -> {
      if (expressions.size() > 1) {
        //merge all tail expressions as we can support only one range condition per index
        //try to mere first condition with the rest of the conditions too
        var resultingExpressions = new ArrayList<SQLBooleanExpression>(2);

        // The merge collapses two equalities on the same property into one index key. Pass the
        // property collate so values that are equal only under it (e.g. 'A' and 'a' on a
        // case-insensitive property) still merge and keep using the index instead of dropping to a
        // full scan.
        var mergeProperty = clazz.getProperty(indexPropertyName);
        var mergeCollate = mergeProperty == null ? null : mergeProperty.getCollate();

        var firstExpression = expressions.getFirst();
        var expressionToMerge = expressions.get(1);

        var mergedExpression = firstExpression.mergeUsingAnd(expressionToMerge, ctx, mergeCollate);
        if (mergedExpression != null) {
          expressionToMerge = mergedExpression;
        } else {
          resultingExpressions.add(firstExpression);
        }

        if (expressions.size() > 2) {
          for (var i = 2; i < expressions.size(); i++) {
            var nextBlockToMerge = expressions.get(i);
            expressionToMerge =
                expressionToMerge.mergeUsingAnd(nextBlockToMerge, ctx, mergeCollate);

            //unable to merge expressions
            if (expressionToMerge == null) {
              invalidConditions[0] = true;
              return;
            }
          }
        }

        resultingExpressions.add(expressionToMerge);

        expressions.clear();
        expressions.addAll(resultingExpressions);
      }
    });

    //there are more than two boolean expressions for the same property, skip the current index
    if (invalidConditions[0]) {
      return null;
    }

    for (var indexProperty : indexProperties) {
      var propertyExpressions = propertyNameBooleanExpressionMap.get(indexProperty);
      if (propertyExpressions == null) {
        break;
      }

      if (propertyExpressions.size() > 2) {
        break;
      }

      var info =
          new IndexSearchInfo(
              indexProperty,
              true,
              isMap(clazz, indexProperty),
              isIndexByKey(index, indexProperty),
              isIndexByValue(index, indexProperty),
              clazz, ctx);

      var firstPropertyExpression = propertyExpressions.getFirst();
      if (firstPropertyExpression.isRangeExpression()) {
        if (!info.allowsRangeQueries()) {
          break;
        }
      }

      if (propertyExpressions.size() == 2) {
        var secondPropertyExpression = propertyExpressions.get(1);
        if (secondPropertyExpression.isIndexAware(info, ctx)) {
          if (secondPropertyExpression.canCreateRangeWith(firstPropertyExpression)) {
            additionalRangeCondition = (SQLBinaryCondition) secondPropertyExpression;
          } else {
            break;
          }
        } else {
          return null;
        }
      }

      if (firstPropertyExpression.isIndexAware(info, ctx)) {
        indexKeyValue.getSubBlocks().add(firstPropertyExpression.copy());
        if (firstPropertyExpression.isRangeExpression()) {
          //we can have only a single range condition per index
          break;
        }
      } else {
        break;
      }
    }

    return new IndexSearchDescriptor(index, indexKeyValue, additionalRangeCondition, blockCopy);
  }

  /**
   * Builds an {@link IndexSearchDescriptor} for a FULLTEXT (non-Lucene) index.
   *
   * <p>Iterates over the index's field list and, for each field, looks for a matching
   * CONTAINSTEXT condition in the AND block. Matched conditions become the index key;
   * unmatched conditions remain as post-fetch filters.
   *
   * <p>Like {@link #buildIndexSearchDescriptor}, hash-type fulltext indexes require a
   * complete key match (all fields present).
   *
   * @return a descriptor, or {@code null} if no CONTAINSTEXT match was found
   */
  @Nullable private static IndexSearchDescriptor buildIndexSearchDescriptorForFulltext(
      Index index, SQLAndBlock block) {
    var indexFields = index.getDefinition().getProperties();
    var found = false;

    var blockCopy = block.copy();
    Iterator<SQLBooleanExpression> blockIterator;

    var indexKeyValue = new SQLAndBlock(-1);

    for (var indexField : indexFields) {
      blockIterator = blockCopy.getSubBlocks().iterator();
      var indexFieldFound = false;
      while (blockIterator.hasNext()) {
        var singleExp = blockIterator.next();
        if (singleExp.isFullTextIndexAware(indexField)) {
          found = true;
          indexFieldFound = true;
          indexKeyValue.getSubBlocks().add(singleExp.copy());
          blockIterator.remove();
          break;
        }
      }
      if (!indexFieldFound) {
        break;
      }
    }

    if (found) {
      return new IndexSearchDescriptor(index, indexKeyValue, null, blockCopy);
    }
    return null;
  }

  /** Returns {@code true} if the given field is indexed "by key" (for map-type indexes). */
  private static boolean isIndexByKey(Index index, String field) {
    var def = index.getDefinition();
    for (var o : def.getFieldsToIndex()) {
      if (o.equalsIgnoreCase(field + " by key")) {
        return true;
      }
    }
    return false;
  }

  /** Returns {@code true} if the given field is indexed "by value" (for map-type indexes). */
  private static boolean isIndexByValue(Index index, String field) {
    var def = index.getDefinition();
    for (var o : def.getFieldsToIndex()) {
      if (o.equalsIgnoreCase(field + " by value")) {
        return true;
      }
    }
    return false;
  }

  /** Returns {@code true} if the given field's type is {@link PropertyType#EMBEDDEDMAP}. */
  private static boolean isMap(SchemaClass clazz,
      String indexField) {
    var prop = clazz.getProperty(indexField);
    if (prop == null) {
      return false;
    }
    return prop.getType() == PropertyType.EMBEDDEDMAP;
  }

  /**
   * Aggregates multiple {@link IndexSearchDescriptor}s that share the same index and
   * key condition into a single descriptor with an OR-combined remaining filter.
   *
   * <p>This handles the case where multiple OR-branches of the WHERE clause share the
   * same index key but differ in their residual filter:
   * <pre>
   *  WHERE (a = 1 AND b = 2) OR (a = 1 AND b = 3)
   *           same index key: a=1
   *           residual: b=2 OR b=3  (combined into a single SQLOrBlock)
   * </pre>
   *
   * @param indexSearchDescriptors one descriptor per OR-branch
   * @return deduplicated descriptors with OR-combined residual filters
   */
  private static List<IndexSearchDescriptor> commonFactor(
      List<IndexSearchDescriptor> indexSearchDescriptors) {
    // index, key condition, additional filter (to aggregate in OR)
    Map<Index, Map<IndexCondPair, SQLOrBlock>> aggregation = new HashMap<>();
    for (var item : indexSearchDescriptors) {
      var filtersForIndex = aggregation.computeIfAbsent(item.getIndex(), k -> new HashMap<>());
      var extendedCond =
          new IndexCondPair(item.getKeyCondition(), item.getAdditionalRangeCondition());

      var existingAdditionalConditions = filtersForIndex.get(extendedCond);
      if (existingAdditionalConditions == null) {
        existingAdditionalConditions = new SQLOrBlock(-1);
        filtersForIndex.put(extendedCond, existingAdditionalConditions);
      }
      existingAdditionalConditions.getSubBlocks().add(item.getRemainingCondition());
    }
    List<IndexSearchDescriptor> result = new ArrayList<>();
    for (var item : aggregation.entrySet()) {
      for (var filters : item.getValue().entrySet()) {
        result.add(
            new IndexSearchDescriptor(
                item.getKey(),
                filters.getKey().mainCondition,
                filters.getKey().additionalRange,
                filters.getValue()));
      }
    }
    return result;
  }

  /**
   * Wraps a subquery statement as the FROM target using a {@link SubQueryStep}.
   * A child {@link BasicCommandContext} is created for the subquery so that its
   * variables do not leak into the outer query scope.
   */
  private static void handleSubqueryAsTarget(
      SelectExecutionPlan plan,
      SQLStatement subQuery,
      CommandContext ctx,
      boolean profilingEnabled) {
    var subCtx = new BasicCommandContext();
    subCtx.setDatabaseSession(ctx.getDatabaseSession());
    subCtx.setParent(ctx);
    if (ctx.isLetHostedCorrelatedRidFetch()) {
      subCtx.setLetHostedCorrelatedRidFetch(true);
    }
    var subExecutionPlan =
        subQuery.createExecutionPlan(subCtx, profilingEnabled);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx, profilingEnabled));
  }

  /**
   * Attempts to push the outer WHERE filter down into a subquery's expand() step.
   *
   * <p>Detects the pattern:
   * <pre>
   *   SELECT FROM (SELECT expand(...) FROM ...) WHERE &lt;predicate&gt;
   * </pre>
   * and moves the predicate into the ExpandStep so it filters during iteration
   * rather than after all elements are materialized. This avoids loading records
   * that would be discarded by the outer WHERE.
   *
   * <p>The push-down is only applied when:
   * <ul>
   *   <li>The plan chain is: SubQueryStep → FilterStep</li>
   *   <li>The subquery plan contains an ExpandStep as its last step</li>
   *   <li>The WHERE clause does not reference parent scope ({@code $parent})</li>
   * </ul>
   */
  private void tryPushDownFilterIntoExpand(
      SelectExecutionPlan plan, QueryPlanningInfo info) {
    // Materialized LET per-entry plans set this flag to preserve the outer
    // FilterStep — push-down would move filters into the SubQueryStep which
    // is later replaced with ListSourceStep, silently losing the filter.
    if (plan.getContext().isSkipExpandPushDown()) {
      return;
    }
    var steps = plan.steps;
    if (steps.size() < 2 || info.whereClause == null) {
      return;
    }

    for (var i = 0; i < steps.size() - 1; i++) {
      if (!(steps.get(i) instanceof SubQueryStep subQueryStep)) {
        continue;
      }
      if (!(steps.get(i + 1) instanceof FilterStep filterStep)) {
        continue;
      }

      if (!(subQueryStep.subExecutionPlan instanceof SelectExecutionPlan innerPlan)) {
        continue;
      }

      ExpandStep expandStep = null;
      int expandIndex = -1;
      for (var j = 0; j < innerPlan.steps.size(); j++) {
        if (innerPlan.steps.get(j) instanceof ExpandStep es) {
          expandStep = es;
          expandIndex = j;
        }
      }
      if (expandStep == null) {
        continue;
      }

      // Step 1: Extract @class = 'X' from the WHERE
      it.unimi.dsi.fastutil.ints.IntSet classFilter = null;
      String className = null;
      SQLWhereClause remainingWhere = info.whereClause;
      var classExtraction = info.whereClause.extractClassEquality();
      if (classExtraction != null) {
        classFilter = resolveClassToCollectionIds(classExtraction.className(), plan);
        if (classFilter != null) {
          className = classExtraction.className();
          remainingWhere = classExtraction.remainingWhere();
        }
      }

      // Step 2: Extract out/in('EdgeClass').@rid = <expr>
      RidFilterDescriptor ridFilter = null;
      if (remainingWhere != null) {
        var edgeExtraction = remainingWhere.extractEdgeRidLookup();
        if (edgeExtraction != null) {
          ridFilter = new RidFilterDescriptor.EdgeRidLookup(
              edgeExtraction.edgeClassName(),
              edgeExtraction.traversalDirection(),
              edgeExtraction.targetRidExpression(),
              false);
          remainingWhere = edgeExtraction.remainingWhere();
        }
      }

      // Step 3: Extract @rid = <expr>
      if (ridFilter == null && remainingWhere != null) {
        var ridExtraction = remainingWhere.extractRidEquality();
        if (ridExtraction != null) {
          ridFilter = new RidFilterDescriptor.DirectRid(ridExtraction.ridExpression());
          remainingWhere = ridExtraction.remainingWhere();
        }
      }

      // Step 4: Split remaining by $parent reference
      SQLWhereClause pushDownWhere = null;
      SQLWhereClause outerWhere = null;
      if (remainingWhere != null) {
        var parentSplit = remainingWhere.splitByParentReference();
        if (parentSplit != null) {
          outerWhere = parentSplit.parentReferencing();
          pushDownWhere = parentSplit.nonParentReferencing();
        } else {
          pushDownWhere = remainingWhere;
        }
      }

      // Step 4b: Infer target class from edge schema if not found via @class.
      // Only needed when there is a push-down filter — without a filter there
      // is no index to look up, so the schema traversal is unnecessary.
      if (className == null && pushDownWhere != null) {
        className = inferTargetClassFromExpandEdgeSchema(info, plan);
      }

      // Step 5: Try to find an index for the push-down filter
      IndexSearchDescriptor indexDescriptor = null;
      if (pushDownWhere != null && className != null) {
        indexDescriptor = tryBuildExpandIndexDescriptor(
            pushDownWhere, className, plan.getContext());
      }

      if (classFilter == null && ridFilter == null
          && pushDownWhere == null && indexDescriptor == null) {
        continue;
      }

      var pushedDown = new ExpandStep(
          innerPlan.getContext(), expandStep.isProfilingEnabled(),
          expandStep.expandAlias, pushDownWhere, classFilter,
          ridFilter, indexDescriptor);
      pushedDown.setPrevious(expandStep.prev);
      innerPlan.steps.set(expandIndex, pushedDown);

      if (outerWhere != null) {
        var newFilter = new FilterStep(
            outerWhere, plan.getContext(), 0, filterStep.isProfilingEnabled());
        newFilter.setPrevious(filterStep.prev);
        steps.set(i + 1, newFilter);
        if (i + 2 < steps.size()) {
          steps.get(i + 2).setPrevious(newFilter);
        }
      } else {
        steps.remove(i + 1);
        if (i + 1 < steps.size()) {
          steps.get(i + 1).setPrevious(subQueryStep);
        }
      }

      return;
    }
  }

  /**
   * Attempts to build an {@link IndexSearchDescriptor} for the push-down filter
   * by looking up indexes on the target class.
   *
   * <p>First tries the full WHERE clause. If that fails because the WHERE
   * flattens to multiple OR branches (e.g. due to graph-navigation CONTAINS
   * conditions), falls back to extracting only the "indexable" subset of
   * AND conditions — those that flatten to a single branch — and attempts
   * the index lookup on that subset. The full WHERE is still applied as a
   * post-filter on the ExpandStep, so correctness is preserved.
   *
   * @param pushDownWhere the WHERE clause to analyze (must not reference $parent)
   * @param className     the target class name (from @class or edge schema)
   * @param ctx           command context
   * @return an index descriptor, or {@code null} if no suitable index exists
   */
  @Nullable private IndexSearchDescriptor tryBuildExpandIndexDescriptor(
      SQLWhereClause pushDownWhere, String className, CommandContext ctx) {
    var result = TraversalPreFilterHelper.findIndexForFilter(
        pushDownWhere, className, ctx);
    if (result != null) {
      return result;
    }

    // The full WHERE could not be used (likely multi-branch flatten from
    // graph navigation conditions). Try the indexable subset only.
    var schema = ctx.getDatabaseSession().getMetadata().getImmutableSchemaSnapshot();
    var schemaClass = schema.getClassInternal(className);
    if (schemaClass == null) {
      return null;
    }
    var indexablePart = pushDownWhere.extractIndexablePart(ctx, schemaClass);
    if (indexablePart == null) {
      return null;
    }
    return TraversalPreFilterHelper.findIndexForFilter(
        indexablePart, className, ctx);
  }

  /**
   * Infers the target vertex class from the edge schema when the outer WHERE does
   * not contain an explicit {@code @class = 'X'} filter.
   *
   * <p>For a subquery like {@code SELECT expand(in('HAS_CREATOR')) FROM Person},
   * the edge class {@code HAS_CREATOR} may declare linked classes on its {@code out}
   * and {@code in} properties. The target class of the expansion is the endpoint
   * opposite to the traversal direction:
   * <ul>
   *   <li>{@code in('X')} follows incoming edges → targets are the {@code out}
   *       endpoint → {@code edgeClass.getPropertyInternal("out").getLinkedClass()}</li>
   *   <li>{@code out('X')} follows outgoing edges → targets are the {@code in}
   *       endpoint → {@code edgeClass.getPropertyInternal("in").getLinkedClass()}</li>
   * </ul>
   *
   * @return the inferred class name, or {@code null} if inference is not possible
   */
  @Nullable private static String inferTargetClassFromExpandEdgeSchema(
      QueryPlanningInfo info, SelectExecutionPlan plan) {
    var funcCall = extractExpandTraversalFunction(info);
    if (funcCall == null) {
      return null;
    }

    var directionName = extractTraversalDirection(funcCall);
    if (directionName == null) {
      return null;
    }

    var edgeClassName = extractEdgeClassName(funcCall);
    if (edgeClassName == null) {
      return null;
    }

    return lookupLinkedClassName(edgeClassName, directionName, plan);
  }

  /**
   * Extracts the {@code in(...)} or {@code out(...)} function call from the inner
   * subquery's expand projection. Returns {@code null} if the query structure does
   * not match the expected {@code SELECT expand(in/out('EdgeClass')) FROM ...} pattern.
   */
  @Nullable private static SQLFunctionCall extractExpandTraversalFunction(
      QueryPlanningInfo info) {
    if (info.target == null) {
      return null;
    }
    var fromItem = info.target.getItem();
    if (fromItem == null || fromItem.getStatement() == null) {
      return null;
    }
    if (!(fromItem.getStatement() instanceof SQLSelectStatement selectStmt)) {
      return null;
    }
    var projection = selectStmt.getProjection();
    if (projection == null || !projection.isExpand()) {
      return null;
    }

    var expandExpr = projection.getExpandContent();
    if (expandExpr == null
        || expandExpr.getItems() == null
        || expandExpr.getItems().isEmpty()) {
      return null;
    }
    var contentExpr = expandExpr.getItems().getFirst().getExpression();
    if (contentExpr == null) {
      return null;
    }
    if (!(contentExpr.getMathExpression() instanceof SQLBaseExpression base)) {
      return null;
    }
    var identifier = base.getIdentifier();
    if (identifier == null || identifier.getLevelZero() == null) {
      return null;
    }
    return identifier.getLevelZero().getFunctionCall();
  }

  /**
   * Returns the traversal direction ({@code "in"} or {@code "out"}) from the
   * function call, or {@code null} if the function is not a recognized traversal.
   */
  @Nullable private static String extractTraversalDirection(
      SQLFunctionCall funcCall) {
    if (funcCall.getName() == null) {
      return null;
    }
    var name = funcCall.getName().getStringValue();
    if (name == null) {
      return null;
    }
    name = name.toLowerCase(Locale.ROOT);
    if ("in".equals(name) || "out".equals(name)) {
      return name;
    }
    return null;
  }

  /**
   * Extracts the edge class name string from the first parameter of the traversal
   * function call (e.g. {@code 'HAS_CREATOR'} from {@code in('HAS_CREATOR')}).
   * Returns {@code null} if the parameter cannot be evaluated to a string.
   */
  @Nullable private static String extractEdgeClassName(SQLFunctionCall funcCall) {
    if (funcCall.getParams() == null || funcCall.getParams().isEmpty()) {
      return null;
    }
    try {
      var value = funcCall.getParams().getFirst()
          .execute((Result) null, new BasicCommandContext());
      return value instanceof String s ? s : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Looks up the linked class name from the edge schema. For {@code in('X')},
   * the targets are the {@code out} endpoint; for {@code out('X')}, the targets
   * are the {@code in} endpoint.
   */
  @Nullable private static String lookupLinkedClassName(
      String edgeClassName, String direction, SelectExecutionPlan plan) {
    var ctx = plan.getContext();
    if (ctx == null || ctx.getDatabaseSession() == null) {
      return null;
    }
    var schema = ctx.getDatabaseSession().getMetadata().getImmutableSchemaSnapshot();
    if (schema == null) {
      return null;
    }
    var edgeClass = schema.getClassInternal(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    var targetPropName = "in".equals(direction) ? "out" : "in";
    var prop = edgeClass.getPropertyInternal(targetPropName);
    if (prop == null || prop.getLinkedClass() == null) {
      return null;
    }
    return prop.getLinkedClass().getName();
  }

  /**
   * Resolves a class name to the set of collection (cluster) IDs for that class
   * and all its subclasses. Returns {@code null} if the class is not found.
   */
  @Nullable private static it.unimi.dsi.fastutil.ints.IntSet resolveClassToCollectionIds(
      String className, SelectExecutionPlan plan) {
    var ctx = plan.getContext();
    if (ctx == null || ctx.getDatabaseSession() == null) {
      return null;
    }
    var schema = ctx.getDatabaseSession().getMetadata().getImmutableSchemaSnapshot();
    var schemaClass = schema.getClass(className);
    if (schemaClass == null) {
      return null;
    }
    return TraversalPreFilterHelper.collectionIdsForClass(schemaClass);
  }

  /** Returns {@code true} if the ORDER BY is exactly {@code ORDER BY @rid DESC}. */
  private static boolean isOrderByRidDesc(QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info)) {
      return false;
    }

    if (info.orderBy == null) {
      return false;
    }
    if (info.orderBy.getItems().size() == 1) {
      var item = info.orderBy.getItems().getFirst();
      var recordAttr = item.getRecordAttr();
      return recordAttr != null
          && recordAttr.equalsIgnoreCase("@rid")
          && SQLOrderByItem.DESC.equals(item.getType());
    }
    return false;
  }

  /** Returns {@code true} if the ORDER BY is exactly {@code ORDER BY @rid ASC} (or default). */
  private static boolean isOrderByRidAsc(QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info)) {
      return false;
    }

    if (info.orderBy == null) {
      return false;
    }
    if (info.orderBy.getItems().size() == 1) {
      var item = info.orderBy.getItems().getFirst();
      var recordAttr = item.getRecordAttr();
      return recordAttr != null
          && recordAttr.equalsIgnoreCase("@rid")
          && (item.getType() == null || SQLOrderByItem.ASC.equals(item.getType()));
    }
    return false;
  }

  /**
   * Returns {@code true} if the target is a class/identifier (as opposed to a subquery
   * or variable), which means records can be scanned in RID order.
   */
  private static boolean hasTargetWithSortedRids(QueryPlanningInfo info) {
    if (info.target == null) {
      return false;
    }
    if (info.target.getItem() == null) {
      return false;
    }

    return info.target.getItem().getIdentifier() != null;
  }

  /**
   * Returns {@code true} when the statement carries at least one user LET alias (not a
   * {@link SubQueryCollector} synthetic {@code $$$SUBQUERY$$_} alias). Such statements host
   * per-row parent context and admit correlated fetch in nested subqueries, including
   * projection subqueries fed from a LET variable source.
   */
  private static boolean statementHasUserPerRecordLet(SQLSelectStatement selectStatement) {
    var letClause = selectStatement.getLetClause();
    if (letClause == null || letClause.getItems() == null) {
      return false;
    }
    for (var item : letClause.getItems()) {
      var varName = item.getVarName();
      if (varName != null
          && !varName.getStringValue().startsWith(SubQueryCollector.GENERATED_ALIAS_PREFIX)) {
        return true;
      }
    }
    return false;
  }
}
