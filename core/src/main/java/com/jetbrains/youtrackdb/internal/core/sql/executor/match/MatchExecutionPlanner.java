package com.jetbrains.youtrackdb.internal.core.sql.executor.match;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.util.PairLongObject;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Direction;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Identifiable;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.id.RecordIdInternal;
import com.jetbrains.youtrackdb.internal.core.index.engine.SelectivityEstimator;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Schema;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.sql.executor.AbstractExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.CartesianProductStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.CostModel;
import com.jetbrains.youtrackdb.internal.core.sql.executor.DistinctExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.EmptyStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ExecutionStepInternal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.HardwiredCountOptimizations;
import com.jetbrains.youtrackdb.internal.core.sql.executor.IndexSearchDescriptor;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.executor.LimitExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.OrderByStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ProjectionCalculationStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.QueryPlanningInfo;
import com.jetbrains.youtrackdb.internal.core.sql.executor.RidFilterDescriptor;
import com.jetbrains.youtrackdb.internal.core.sql.executor.SelectExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.executor.SelectExecutionPlanner;
import com.jetbrains.youtrackdb.internal.core.sql.executor.SkipExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.TraversalPreFilterHelper;
import com.jetbrains.youtrackdb.internal.core.sql.executor.UnwindStep;
import com.jetbrains.youtrackdb.internal.core.sql.parser.OrderByCollationResolver;
import com.jetbrains.youtrackdb.internal.core.sql.parser.Pattern;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBaseExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBinaryCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLEqualsOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLFromItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGroupBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLInCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLInteger;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLimit;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchFilter;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMathExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMethodCall;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMultiMatchPathItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNeOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNestedProjection;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNotBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNotInCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLProjectionItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLRecordAttribute;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLRid;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSkip;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLUnwind;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YqlExecutionPlanCache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a parsed `MATCH` statement into a physical execution plan.
 *
 * <p>## Overview
 *
 * <p>The `MATCH` statement is YouTrackDB's graph pattern-matching query language construct,
 * inspired by the pattern-matching semantics of graph query languages such as Cypher.
 * A typical `MATCH` query defines one or more **patterns** — chains of nodes connected by
 * directed or bidirectional edges — along with optional `WHERE` filters, `WHILE` recursive
 * traversal conditions, and a `RETURN` projection.
 *
 * <p>### Example
 *
 * <p>```sql
 * MATCH {class: Person, as: p, where: (name = 'Alice')}
 *         .out('Knows') {as: friend}
 *         .out('Lives') {as: city, where: (name = 'Berlin')}
 * RETURN p.name, friend.name, city.name
 * ```
 *
 * <p>This planner takes the AST produced by the SQL parser ({@link SQLMatchStatement}) and
 * produces a {@link SelectExecutionPlan} composed of execution steps that evaluate the
 * pattern at runtime.
 *
 * <p>## End-to-end flow
 *
 * <p><pre>
 *   SQL text ──→ Parser ──→ SQLMatchStatement (AST)
 *                                   │
 *                          MatchExecutionPlanner
 *                                   │
 *                  ┌────────────────┼────────────────────┐
 *                  │                │                    │
 *           buildPatterns()   estimateRoots()   topologicalSort()
 *            (Pattern graph)  (cardinality map)  (EdgeTraversal schedule)
 *                  │                │                    │
 *                  └────────────────┼────────────────────┘
 *                                   │
 *                          Step generation:
 *                  MatchPrefetchStep (small aliases)
 *                  MatchFirstStep (initial scan)
 *                  MatchStep / OptionalMatchStep (per edge)
 *                  FilterNotMatchPatternStep (NOT patterns)
 *                  RemoveEmptyOptionalsStep (optional cleanup)
 *                  ReturnMatch*Step (projection)
 *                                   │
 *                                   ▼
 *                        SelectExecutionPlan (ready to execute)
 * </pre>
 *
 * <p>## Planning phases
 *
 * <p>The phases below correspond to the inline comments in
 * {@link #createExecutionPlan(CommandContext, boolean)}:
 *
 * <p>1. **Build the pattern graph** — converts the list of {@link SQLMatchExpression}s into a
 *    {@link Pattern} (an adjacency structure of {@link PatternNode}s and
 *    {@link PatternEdge}s), and extracts per-alias metadata such as class constraints,
 *    RID constraints, and `WHERE` filters.
 *
 * <p>2. **Split disjoint sub-patterns** — if the `MATCH` contains multiple disconnected
 *    sub-graphs (e.g. `MATCH {as: a}.out(){as: b}, {as: x}.out(){as: y}`), each connected
 *    component is planned independently and later joined via a
 *    {@link CartesianProductStep}.
 *
 * <p>3. **Estimate root cardinalities** — for every aliased node that has a class or RID
 *    constraint, estimate the number of root records using schema statistics. Aliases
 *    whose estimated cardinality falls below {@link #THRESHOLD} are **prefetched** and
 *    cached in the execution context so that the traversal can start from the smallest
 *    set. Aliases whose filters reference `$matched` cannot be prefetched because their
 *    filter depends on values produced during traversal (see
 *    {@link #dependsOnExecutionContext}).
 *
 * <p>4. **Prefetch small alias sets** — for each alias below the threshold, a
 *    {@link MatchPrefetchStep} eagerly loads all matching records into the execution
 *    context under a well-known key.
 *
 * <p>5. **Topological scheduling and step generation** — edges in the pattern graph are
 *    ordered via a cost-driven depth-first traversal
 *    ({@link #getTopologicalSortedSchedule}). The algorithm picks the cheapest root node
 *    first, then greedily expands outward, respecting dependency constraints introduced
 *    by `$matched` references in `WHERE` clauses. The sorted list of
 *    {@link EdgeTraversal}s is mapped to execution steps:
 *    - The first node becomes a {@link MatchFirstStep} (initial record scan/lookup).
 *    - Each subsequent edge becomes either a {@link MatchStep} or an
 *      {@link OptionalMatchStep}.
 *
 * <p>6. **NOT patterns** — any `NOT { … }` sub-patterns are appended as
 *    {@link FilterNotMatchPatternStep}s that discard rows matching the negative pattern.
 *
 * <p>7. **Optional cleanup** — if the query contains optional nodes, a
 *    {@link RemoveEmptyOptionalsStep} replaces sentinel
 *    {@link OptionalMatchEdgeTraverser#EMPTY_OPTIONAL} markers with `null`.
 *
 * <p>8. **Return projection** — depending on the `RETURN` clause, one of several
 *    projection steps is appended (`$elements`, `$paths`, `$patterns`,
 *    `$pathElements`, or a custom expression list).
 *
 * <p>## Result row evolution
 *
 * <p>As each step in the pipeline processes a row, the row's property map grows.
 * The diagram below shows a concrete example for the query:
 *
 * <p>```sql
 * MATCH {class: Person, as: p, where: (name = 'Alice')}
 *         .out('Knows') {as: friend, optional: true}
 *         .out('Lives') {as: city}
 * RETURN p.name, friend.name, city.name
 * ```
 *
 * <p><pre>
 *   Step                        Row contents
 *   ─────────────────────────   ──────────────────────────────────────────────
 *   MatchFirstStep (p)          {p: Person#1}
 *   OptionalMatchStep (→friend) {p: Person#1, friend: Person#2}
 *                            or {p: Person#1, friend: EMPTY_OPTIONAL}
 *   MatchStep (→city)           {p: Person#1, friend: Person#2, city: City#3}
 *   RemoveEmptyOptionalsStep    {p: Person#1, friend: null,     city: City#3}
 *                                                    ^^ sentinel replaced
 *   ReturnMatch*Step / project  {p.name: "Alice", friend.name: null, city.name: "Berlin"}
 * </pre>
 *
 * <p>When a `depthAlias` or `pathAlias` is declared on a WHILE edge, the
 * corresponding metadata is also stored as a top-level property:
 * `{p: Person#1, friend: Person#2, depth: 2, path: [Person#1, ...]}`.
 *
 * <p>## Cartesian product for disjoint sub-patterns
 *
 * <p>When the MATCH query contains multiple disconnected sub-graphs (e.g.
 * `MATCH {as: a}.out(){as: b}, {as: x}.out(){as: y}`), each connected
 * component is planned independently via {@link #createPlanForPattern} and
 * produces its own stream of partial rows. A {@link CartesianProductStep}
 * then joins these streams by computing the Cartesian (cross) product:
 * every row from sub-pattern 1 is combined with every row from sub-pattern 2,
 * yielding a merged row that contains all aliases from both sub-patterns.
 *
 * <p><pre>
 *   Sub-pattern 1: {a: A#1, b: B#2}   Sub-pattern 2: {x: X#5, y: Y#6}
 *                  {a: A#3, b: B#4}                   {x: X#7, y: Y#8}
 *
 *   CartesianProductStep output:
 *     {a: A#1, b: B#2, x: X#5, y: Y#6}
 *     {a: A#1, b: B#2, x: X#7, y: Y#8}
 *     {a: A#3, b: B#4, x: X#5, y: Y#6}
 *     {a: A#3, b: B#4, x: X#7, y: Y#8}
 * </pre>
 *
 * @see MatchStep
 * @see MatchFirstStep
 * @see MatchEdgeTraverser
 * @see Pattern
 * @see PatternNode
 * @see PatternEdge
 * @see EdgeTraversal
 */
public class MatchExecutionPlanner {

  private static final Logger logger =
      LoggerFactory.getLogger(MatchExecutionPlanner.class);

  /**
   * Prefix prepended to auto-generated aliases for pattern nodes that the user did not
   * name explicitly. Properties whose name starts with this prefix are treated as
   * **internal** and are stripped from the final result set by
   * {@link ReturnMatchPatternsStep} and {@link ReturnMatchElementsStep}.
   */
  static final String DEFAULT_ALIAS_PREFIX = "$YOUTRACKDB_DEFAULT_ALIAS_";

  /** The original parsed `MATCH` statement, used for execution plan caching. */
  private SQLMatchStatement statement;

  /** Positive `MATCH` expressions (the main graph pattern). */
  protected List<SQLMatchExpression> matchExpressions;

  /** Negative `NOT MATCH` expressions that filter out matching rows. */
  protected List<SQLMatchExpression> notMatchExpressions;

  /** Expressions to evaluate in the `RETURN` clause. */
  protected List<SQLExpression> returnItems;

  /** User-specified aliases for each return expression (may contain `null` entries). */
  protected List<SQLIdentifier> returnAliases;

  /** Optional nested projections applied to return items. */
  protected List<SQLNestedProjection> returnNestedProjections;

  /** `true` when the user wrote `RETURN $elements` — unrolls matched nodes. */
  boolean returnElements;

  /** `true` when the user wrote `RETURN $paths` — returns full matched paths. */
  boolean returnPaths;

  /** `true` when the user wrote `RETURN $patterns` — returns matched patterns. */
  boolean returnPatterns;

  /** `true` when the user wrote `RETURN $pathElements` — unrolls *all* path nodes. */
  boolean returnPathElements;

  /** `true` when the `RETURN` clause contains the `DISTINCT` keyword. */
  private boolean returnDistinct;

  protected SQLSkip skip;
  private final SQLGroupBy groupBy;
  private final SQLOrderBy orderBy;
  private final SQLUnwind unwind;
  protected SQLLimit limit;

  // ---- Post-parsing state (populated lazily by buildPatterns / splitDisjointPatterns) ----

  /** The complete pattern graph built from {@link #matchExpressions}. */
  private Pattern pattern;

  /** Connected components of {@link #pattern}, one per disjoint sub-graph. */
  private List<Pattern> subPatterns;

  /**
   * Accumulated `WHERE` filters per alias.  When the same alias appears in multiple
   * `MATCH` expressions, all its `WHERE` predicates are AND-ed together.
   */
  private Map<String, SQLWhereClause> aliasFilters;

  /** Maps each alias to the schema class name it is constrained to. */
  private Map<String, String> aliasClasses;

  /**
   * Maps each alias to pinned RIDs from the pattern ({@code {as: a, rid: #1:2}})
   * or promoted from static {@code @rid = ...} / {@code @rid IN [...]} WHERE filters.
   * Singleton pins use {@code List.of(rid)}; multi-RID pins use the promoted list.
   */
  private Map<String, List<SQLRid>> aliasPinnedRids;

  /**
   * The subset of {@link #aliasPinnedRids} keys this planner promoted out of a static
   * {@code @rid} WHERE term, as opposed to reading from a pattern {@code {as: a, rid: #1:2}} slot.
   * {@link #fetchFilterFor} needs the distinction: only for a promoted alias is the surviving
   * {@code @rid} term known to enumerate the same records the fetch target does.
   */
  private Set<String> promotedRidAliases = Set.of();

  /**
   * When true, {@link #buildPatterns} promotes the pre-built {@code @rid} / {@code @rid IN}
   * filters into {@link #aliasPinnedRids} on the additive (pre-built-pattern) path. Set only by
   * the {@link MatchPlanInputs} constructor — the Gremlin-to-MATCH translator's path — so the GQL
   * 3-arg constructor, which shares the same early return in {@code buildPatterns}, keeps its
   * existing plans.
   */
  private boolean promoteFilterRidsOnBuild = false;

  /**
   * Aliases whose class was inferred from edge LINK schema rather than
   * explicitly declared. Inferred aliases must NOT outcompete explicit
   * roots during scheduling — a low-cardinality inferred class can cause
   * the scheduler to reverse traversal direction across while steps,
   * producing 0 results. Their estimates are inflated to {@code
   * Long.MAX_VALUE} so they sort last in root selection while remaining
   * available for prefetching and collection-ID filtering.
   */
  private Set<String> inferredWhileExprAliases = Set.of();

  /** Set to `true` if at least one node in the pattern is marked `optional: true`. */
  private boolean foundOptional = false;

  /**
   * Aliases with an estimated record count below this threshold are **prefetched** into
   * memory before the traversal starts. This avoids repeated scans of very small sets
   * during the nested-loop pattern matching.
   */
  static final long THRESHOLD = 100;

  /**
   * Maximum chain depth for the MATCH cost fold, bounded to
   * {@code [0, MAX_CHAIN_FOLD_HOPS]}. Zero disables the fold. Configurable
   * via {@link GlobalConfiguration#QUERY_MATCH_CHAIN_FOLD_MAX_HOPS};
   * out-of-range values clamp silently, as they do for the sibling knobs
   * below.
   */
  static int getChainFoldMaxHops() {
    return Math.min(
        MAX_CHAIN_FOLD_HOPS,
        Math.max(0,
            GlobalConfiguration.QUERY_MATCH_CHAIN_FOLD_MAX_HOPS.getValueAsInteger()));
  }

  /**
   * Maximum estimated build-side cardinality for which the planner will choose a
   * hash-based join (anti-join, semi-join, inner join) over nested-loop evaluation.
   * If the estimated NOT-pattern result set exceeds this threshold, the planner
   * falls back to {@link FilterNotMatchPatternStep}. Configurable via
   * {@link GlobalConfiguration#QUERY_MATCH_HASH_JOIN_THRESHOLD}.
   */
  static long getHashJoinThreshold() {
    return Math.max(0, GlobalConfiguration.QUERY_MATCH_HASH_JOIN_THRESHOLD.getValueAsLong());
  }

  /**
   * Minimum upstream (probe-side) cardinality for hash join to be worthwhile.
   * When set to 0, both the upstream check (Guard 1) and cost-based comparison
   * (Guard 2) are bypassed — only the build-side threshold applies. Configurable
   * via {@link GlobalConfiguration#QUERY_MATCH_HASH_JOIN_UPSTREAM_MIN}.
   */
  static long getHashJoinUpstreamMin() {
    return GlobalConfiguration.QUERY_MATCH_HASH_JOIN_UPSTREAM_MIN.getValueAsLong();
  }

  /**
   * Memory weight ratio of INNER_JOIN vs SEMI_JOIN entries. INNER_JOIN materializes
   * full ResultInternal rows (~100 + aliasCount × 80 bytes each) into a
   * HashMap&lt;JoinKey, List&lt;Result&gt;&gt;, while SEMI_JOIN stores only JoinKey entries
   * in a HashSet (~72 bytes each). The INNER_JOIN is roughly 7× heavier per entry
   * and should use a proportionally tighter threshold.
   */
  private static final int INNER_JOIN_MEMORY_WEIGHT = 7;

  // FANOUT_PER_HOP removed — fan-out is now computed per-edge via
  // EdgeFanOutEstimator.estimateFanOut() using schema statistics
  // (edgeCount / sourceCount). Falls back to GlobalConfiguration
  // .QUERY_STATS_DEFAULT_FAN_OUT when schema metadata is unavailable.

  /** Pattern for detecting $matched.ALIAS.@rid correlation in WHERE clauses. */
  private static final java.util.regex.Pattern MATCHED_RID_PATTERN =
      java.util.regex.Pattern.compile("\\$matched\\.(\\w+)\\.@rid");

  /** Pattern for counting @rid occurrences in a filter string. */
  private static final java.util.regex.Pattern RID_PATTERN =
      java.util.regex.Pattern.compile("@rid");

  /** Pattern for validating edge class names as valid identifiers. */
  private static final java.util.regex.Pattern VALID_EDGE_LABEL =
      java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  /**
   * Hard upper bound applied to {@code QUERY_MATCH_CHAIN_FOLD_MAX_HOPS} when
   * it is read for plan construction. The chain-fold walk terminates
   * structurally at the pattern graph's branch points and visited edges, so
   * any knob value larger than the deepest linear chain a MATCH can encode
   * behaves the same as this cap. Bounding it anyway keeps the walk's loop
   * counter in a range no later arithmetic on it can overflow. Realistic
   * MATCH patterns top out below 30 linear hops, so 1000 leaves a wide
   * margin.
   */
  private static final int MAX_CHAIN_FOLD_HOPS = 1000;

  /**
   * Creates a planner from a pre-built pattern IR. Bypasses SQL AST parsing entirely:
   * {@link #buildPatterns} becomes a no-op because {@code pattern} is already set.
   * Intended for non-SQL front-ends (e.g. GQL) that build the match IR directly.
   *
   * @param pattern      the pattern graph (nodes and edges)
   * @param aliasClasses maps each alias to the schema class name it is constrained to
   */
  public MatchExecutionPlanner(Pattern pattern, Map<String, String> aliasClasses) {
    this(pattern, aliasClasses, Map.of());
  }

  /**
   * Creates a planner from a pre-built pattern IR with per-alias WHERE filters.
   * Intended for GQL front-ends that provide inline property filters
   * (e.g. {@code MATCH (a:Person {name: 'Karl'})}).
   *
   * @param pattern      the pattern graph (nodes and edges)
   * @param aliasClasses maps each alias to the schema class name it is constrained to
   * @param aliasFilters per-alias WHERE clauses built from inline property filters
   */
  public MatchExecutionPlanner(Pattern pattern, Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters) {
    this.matchExpressions = List.of();
    this.notMatchExpressions = List.of();
    this.returnItems = List.of();
    this.returnAliases = List.of();
    this.returnNestedProjections = List.of();
    this.groupBy = null;
    this.orderBy = null;
    this.unwind = null;

    this.pattern = pattern;
    this.aliasClasses = aliasClasses;
    // Defensive copy: aliasFilters may be immutable (e.g. Map.of() from GQL).
    // detectNotInAntiJoin() mutates this map to strip NOT IN conditions.
    this.aliasFilters = new HashMap<>(aliasFilters);
    this.aliasPinnedRids = Map.of();
  }

  /**
   * Creates a planner by **deep-copying** every mutable component from the parsed
   * statement so that planning can freely mutate state (e.g. assign default aliases,
   * merge filters) without affecting the original AST.
   *
   * @param stm the parsed `MATCH` statement AST node
   */
  public MatchExecutionPlanner(SQLMatchStatement stm) {
    this.statement = stm;
    // Deep-copy all mutable AST components to allow safe in-place mutation during planning
    this.matchExpressions =
        stm.getMatchExpressions().stream().map(SQLMatchExpression::copy)
            .collect(Collectors.toList());
    this.notMatchExpressions =
        stm.getNotMatchExpressions().stream().map(SQLMatchExpression::copy)
            .collect(Collectors.toList());
    this.returnItems =
        stm.getReturnItems().stream().map(SQLExpression::copy).collect(Collectors.toList());
    this.returnAliases =
        stm.getReturnAliases().stream()
            .map(x -> x == null ? null : x.copy())
            .collect(Collectors.toList());
    this.returnNestedProjections =
        stm.getReturnNestedProjections().stream()
            .map(x -> x == null ? null : x.copy())
            .collect(Collectors.toList());
    this.limit = stm.getLimit() == null ? null : stm.getLimit().copy();
    this.skip = stm.getSkip() == null ? null : stm.getSkip().copy();

    this.returnElements = stm.returnsElements();
    this.returnPaths = stm.returnsPaths();
    this.returnPatterns = stm.returnsPatterns();
    this.returnPathElements = stm.returnsPathElements();
    this.returnDistinct = stm.isReturnDistinct();
    this.groupBy = stm.getGroupBy() == null ? null : stm.getGroupBy().copy();
    this.orderBy = stm.getOrderBy() == null ? null : stm.getOrderBy().copy();
    this.unwind = stm.getUnwind() == null ? null : stm.getUnwind().copy();
  }

  /**
   * Creates a planner from pre-built post-parse inputs. Intended for non-SQL front-ends that
   * produce the complete MATCH input set directly (currently the Gremlin-to-MATCH translator).
   *
   * <p>This constructor is purely additive; the three pre-existing constructors are unchanged.
   * It field-by-field defensive-copies the mutable working maps (and shallow-copies the AST
   * lists) so the planner can freely mutate {@code aliasClasses} and {@code aliasFilters} during
   * planning without affecting the caller's record &mdash; {@code aliasClasses} during class
   * inference into chained edges, and {@code aliasFilters} during NOT-IN anti-join detection
   * (which strips the rewritten conditions from the per-alias filter map). {@code aliasPinnedRids}
   * starts empty and is filled by {@link #buildPatterns}, which promotes the pre-built
   * {@code @rid} / {@code @rid IN} filters so RID-pinned sources take the same fast path as on the
   * SQL path.
   *
   * <p><b>Caching precondition.</b> The inherited {@code statement} field stays {@code null}
   * because no SQL AST is available, so callers MUST invoke
   * {@link #createExecutionPlan(CommandContext, boolean, boolean)} with {@code useCache=false}.
   * Otherwise the cache lookup at the start of {@code createExecutionPlan} dereferences
   * {@code statement} and throws a {@link NullPointerException}. Plan caching for non-SQL
   * front-ends would need a separate cache-key mechanism keyed off the front-end's own
   * fingerprint (e.g. Gremlin bytecode), which is intentionally
   * out of scope here.
   *
   * @param inputs the pre-built post-parse inputs (must not be null)
   */
  public MatchExecutionPlanner(@Nonnull MatchPlanInputs inputs) {
    this.pattern = inputs.pattern();
    // Defensive copies of the three working maps. The planner mutates aliasClasses (for
    // class inference into chained edges) and aliasFilters (for NOT-IN anti-join detection),
    // so without copies a second invocation would observe mutated state from the first.
    this.aliasClasses = new HashMap<>(inputs.aliasClasses());
    this.aliasFilters = new HashMap<>(inputs.aliasFilters());
    // aliasPinnedRids starts empty and mutable; buildPatterns promotes the pre-built @rid /
    // @rid IN filters into it on this additive path (see promoteFilterRidsOnBuild), so RID-pinned
    // sources take the same fast path as the SQL path.
    this.aliasPinnedRids = new LinkedHashMap<>();
    this.promoteFilterRidsOnBuild = true;
    // Shallow copies of the AST lists. Translator-built front-ends produce fresh AST
    // elements that are not shared with anyone else, so element-level deep copy (as the
    // (SQLMatchStatement) ctor does) is unnecessary; the list-level copy guards against
    // the caller mutating its own list reference after construction.
    this.matchExpressions = new ArrayList<>(inputs.matchExpressions());
    this.notMatchExpressions = new ArrayList<>(inputs.notMatchExpressions());
    this.returnItems = new ArrayList<>(inputs.returnItems());
    this.returnAliases = new ArrayList<>(inputs.returnAliases());
    this.returnNestedProjections = new ArrayList<>(inputs.returnNestedProjections());
    // Single-value AST fields and primitive flags pass through as-is.
    this.groupBy = inputs.groupBy();
    this.orderBy = inputs.orderBy();
    this.unwind = inputs.unwind();
    this.limit = inputs.limit();
    this.skip = inputs.skip();
    this.returnDistinct = inputs.returnDistinct();
    this.returnElements = inputs.returnElements();
    this.returnPaths = inputs.returnPaths();
    this.returnPatterns = inputs.returnPatterns();
    this.returnPathElements = inputs.returnPathElements();
  }

  /**
   * Builds the complete physical execution plan for this `MATCH` query.
   *
   * <p>The plan is assembled as a pipeline of {@link ExecutionStepInternal} instances chained
   * inside a {@link SelectExecutionPlan}. See the class-level Javadoc for the full list
   * of planning phases (1–8). The inline comments in this method reference those same
   * phase numbers.
   *
   * @param context          the command execution context (holds the database session,
   *                         variables, etc.)
   * @param enableProfiling  when `true`, each step will collect timing/count statistics
   * @param useCache         when `true`, attempts to retrieve/store the plan from/to the
   *                         {@link YqlExecutionPlanCache}
   * @return the assembled execution plan, ready to be {@linkplain InternalExecutionPlan#start()
   *         started}
   */
  public InternalExecutionPlan createExecutionPlan(
      CommandContext context, boolean enableProfiling, boolean useCache) {
    var scopeSession = context.getDatabaseSession();
    if (scopeSession == null) {
      // A context with no session reads no configuration, so there is no placement to scope.
      return buildExecutionPlan(context, enableProfiling, useCache);
    }
    // Bracket the whole build in a null placement scope. The index-ordered traversal freezes a
    // placement into its step, and the plan published below carries that very pair as its stamp.
    var placements = scopeSession.getPlanNullPlacements();
    placements.open();
    try {
      return buildExecutionPlan(context, enableProfiling, useCache);
    } finally {
      placements.close();
    }
  }

  /** Runs the MATCH planning phases inside an open null placement scope. */
  private InternalExecutionPlan buildExecutionPlan(
      CommandContext context, boolean enableProfiling, boolean useCache) {

    var session = context.getDatabaseSession();

    // --- Check the plan cache before doing any work ---
    if (useCache && !enableProfiling && statement.executinPlanCanBeCached(session)) {
      var plan = YqlExecutionPlanCache.get(statement.getOriginalStatement(), context, session);
      if (plan != null) {
        return (InternalExecutionPlan) plan;
      }
    }

    // Record the timestamp so we can avoid caching a stale plan if the schema
    // was modified concurrently during planning.
    var planningStart = System.nanoTime();

    // Phase 1: Build the pattern graph and extract per-alias metadata
    buildPatterns(context);
    // Phase 1b: Pin the declared collation of each ordered property, now that buildPatterns has
    // filled the alias-to-class map. Done here, once, because both ORDER BY paths below need it:
    // the built-in RETURN modes chain their own OrderByStep, and a custom RETURN delegates to the
    // SELECT planner, which resolves nothing for a MATCH because a MATCH carries no single target.
    resolveOrderByCollations(context);
    // Phase 2: Identify disconnected sub-graphs that must be joined via Cartesian product
    splitDisjointPatterns();

    var result = new SelectExecutionPlan(context);

    // Shared count short-circuit (SELECT + MATCH): single-node count(*) with no edges/filters
    // becomes CountFromClassStep before the MATCH scan pipeline is built.
    if (tryHardwiredMatchCount(result, context, enableProfiling)) {
      if (useCache
          && !enableProfiling
          && statement != null
          && statement.executinPlanCanBeCached(session)
          && result.canBeCached()
          && YqlExecutionPlanCache.getLastInvalidation(session) < planningStart) {
        YqlExecutionPlanCache.put(
            statement.getOriginalStatement(),
            result,
            session,
            session.getPlanNullPlacements().recorded());
      }
      return result;
    }

    // Phase 3: Estimate how many root records each aliased node will produce.
    var estimatedRootEntries =
        estimateRootEntries(aliasClasses, aliasPinnedRids, aliasFilters, context);
    // Inflate estimates for inferred-class aliases so they never outcompete
    // explicitly declared roots. A low-cardinality inferred class can cause
    // the scheduler to reverse traversal direction across while steps.
    // The alias stays in the map for prefetching; only root priority changes.
    for (var alias : inferredWhileExprAliases) {
      if (estimatedRootEntries.containsKey(alias)) {
        estimatedRootEntries.put(alias, Long.MAX_VALUE);
      }
    }

    // Aliases with fewer records than THRESHOLD and no dependency on $matched are prefetched
    var aliasesToPrefetch =
        estimatedRootEntries.entrySet().stream()
            .filter(x -> x.getValue() < THRESHOLD)
            .filter(x -> !dependsOnExecutionContext(x.getKey()))
            .map(Entry::getKey)
            .collect(Collectors.toSet());

    // Short-circuit: if any non-optional alias has zero estimated records, the query
    // is guaranteed to produce no results, so skip pattern scheduling and return an empty plan.
    //
    // Bare RETURN count(*) (no GROUP BY) still exits here, but not empty-handed: SQL semantics
    // require a single 0 row (same as SELECT count(*)), so the projection block goes on top of
    // the EmptyStep and its GuaranteeEmptyCountStep synthesises that row. Returning the bare
    // EmptyStep made a filtered MATCH count on an empty-but-existing class emit zero rows
    // instead of {count: 0}, which broke Gremlin g.V().has(...).count().next() after a
    // rolled-back addV left the class behind; letting the count shape fall through to full
    // planning fixed that but gave up the O(1) exit, which bites on a disjoint pattern where
    // CartesianProductStep restarts the empty sub-plan once per row of the other side.
    for (var entry : estimatedRootEntries.entrySet()) {
      if (entry.getValue() == 0L && !isOptional(entry.getKey())) {
        result.chain(new EmptyStep(context, enableProfiling));
        if (isBareCountStarWithoutGroupBy()) {
          appendCustomReturnProjection(result, context, enableProfiling);
        }
        return result;
      }
    }

    // Phase 4b: Detect index-ordered MATCH traversal opportunity.
    // Only applicable for single connected patterns (no Cartesian product).
    // Detected BEFORE prefetch so we can exclude the target alias from prefetching —
    // prefetching it would pre-bind the target, causing IndexOrderedEdgeStep to
    // reject all index results via the isAlreadyBoundAndDifferent check.
    //
    // The schedule computed here is reused by Phase 5 (see precomputedSortedEdges
    // parameter of createPlanForPattern) to avoid a duplicate
    // getTopologicalSortedSchedule call on every planning invocation — measured
    // ~5% CPU savings on queries where plan cache misses (e.g. IS7, where
    // $matched.X.@rid back-reference blocks caching).
    IndexOrderedPlanner.IndexOrderedCandidate indexOrderedCandidate = null;
    List<EdgeTraversal> probeEdges = null;
    if (subPatterns.size() == 1 && orderBy != null) {
      probeEdges = getTopologicalSortedSchedule(
          estimatedRootEntries, pattern, aliasClasses, aliasFilters,
          context.getDatabaseSession());
      indexOrderedCandidate = detectIndexOrderedCandidate(
          probeEdges, context, estimatedRootEntries);
    }

    // Phase 4: Prefetch small alias sets into the context variable map (see class Javadoc)
    if (indexOrderedCandidate != null) {
      // Exclude the target alias — it will be bound by IndexOrderedEdgeStep
      aliasesToPrefetch.remove(indexOrderedCandidate.targetAlias());
    }
    addPrefetchSteps(result, aliasesToPrefetch, context, enableProfiling);

    // Phase 5: Topological scheduling + step generation for each connected component
    if (subPatterns.size() > 1) {
      // Multiple disjoint sub-patterns → Cartesian product of their independent results
      var step = new CartesianProductStep(context, enableProfiling);
      for (var subPattern : subPatterns) {
        step.addSubPlan(
            createPlanForPattern(
                subPattern, context, estimatedRootEntries, aliasesToPrefetch,
                null, null, enableProfiling));
      }
      result.chain(step);
    } else {
      // Single connected pattern → inline the steps directly into the main plan.
      // probeEdges was computed in Phase 4b for this same pattern; reuse it so
      // createPlanForPattern does not recompute the identical schedule.
      var plan =
          createPlanForPattern(
              pattern, context, estimatedRootEntries, aliasesToPrefetch,
              indexOrderedCandidate, probeEdges, enableProfiling);
      for (var step : plan.getSteps()) {
        result.chain((ExecutionStepInternal) step);
      }
    }

    // Phase 6: Append NOT-pattern filter steps (nested-loop or hash anti-join)
    manageNotPatterns(
        result, pattern, notMatchExpressions, aliasClasses, aliasFilters,
        aliasPinnedRids, context, enableProfiling);

    // Phase 7: If optional nodes were encountered, replace EMPTY_OPTIONAL sentinels with null
    if (foundOptional) {
      result.chain(new RemoveEmptyOptionalsStep(context, enableProfiling));
    }

    // Phase 8: Append return projection and post-processing steps
    if (returnElements || returnPaths || returnPatterns || returnPathElements) {
      // Built-in return modes ($elements, $paths, $patterns, $pathElements)
      addReturnStep(result, context, enableProfiling);

      if (this.returnDistinct) {
        result.chain(new DistinctExecutionStep(context, enableProfiling));
      }
      if (groupBy != null) {
        throw new CommandExecutionException(context.getDatabaseSession(),
            "Cannot execute GROUP BY in MATCH query with RETURN $elements, $pathElements, $patterns"
                + " or $paths");
      }

      if (this.unwind != null) {
        result.chain(new UnwindStep(unwind, context, enableProfiling));
      }

      if (this.orderBy != null) {
        Integer maxResults = null;
        if (this.limit != null && this.limit.getValue(context) >= 0) {
          var skipSize = (this.skip != null && this.skip.getValue(context) >= 0)
              ? this.skip.getValue(context) : 0;
          maxResults = skipSize + this.limit.getValue(context);
        }
        // Multi-field + candidate → primary key cutoff hint for early
        // termination in the bounded heap.
        // Disabled when RETURN DISTINCT: early termination stops reading
        // when primary key worsens, but the bounded heap may contain
        // duplicates that DistinctStep will remove — producing fewer
        // than LIMIT distinct results. Without the hint, the bounded
        // heap still reads all upstream rows (no early termination),
        // matching the pre-existing behavior.
        SQLOrderByItem primaryHint = null;
        if (indexOrderedCandidate != null
            && indexOrderedCandidate.multiFieldOrderBy()
            && !this.returnDistinct) {
          primaryHint = orderBy.getItems().getFirst();
        }
        // indexOrderedUpstream: OrderByStep checks runtime context variable
        // to pass through when IndexOrderedEdgeStep produces sorted output.
        // Safe with RETURN DISTINCT: DistinctExecutionStep is a streaming
        // filter that preserves input order (RidSet-based dedup), and runs
        // AFTER OrderByStep in the pipeline.
        var indexOrderedUpstream = indexOrderedCandidate != null;
        result.chain(new OrderByStep(
            orderBy, maxResults, primaryHint, indexOrderedUpstream,
            context, -1, enableProfiling));
      }

      if (this.skip != null && skip.getValue(context) >= 0) {
        result.chain(new SkipExecutionStep(skip, context, enableProfiling));
      }
      if (this.limit != null && limit.getValue(context) >= 0) {
        result.chain(new LimitExecutionStep(limit, context, enableProfiling));
      }
    } else {
      // Custom RETURN expressions — delegate to the SELECT planner for projection,
      // GROUP BY, ORDER BY, UNWIND, SKIP, LIMIT handling
      var info = new QueryPlanningInfo();
      List<SQLProjectionItem> items = new ArrayList<>();
      for (var i = 0; i < this.returnItems.size(); i++) {
        var item =
            new SQLProjectionItem(
                returnItems.get(i), this.returnAliases.get(i), returnNestedProjections.get(i));
        items.add(item);
      }
      info.projection = new SQLProjection(items, returnDistinct);

      info.projection = SelectExecutionPlanner.translateDistinct(info.projection);
      info.distinct = info.projection != null && info.projection.isDistinct();
      if (info.projection != null) {
        info.projection.setDistinct(false);
      }

      info.groupBy = this.groupBy;
      info.orderBy = this.orderBy;
      info.unwind = this.unwind;
      info.skip = this.skip;
      info.limit = this.limit;

      // When index-ordered traversal is active AND no GROUP BY:
      // pass indexOrderedUpstream flag so OrderByStep can detect pre-sorted
      // input at runtime and pass through without sorting.
      // Safe with RETURN DISTINCT: DistinctExecutionStep is a streaming
      // filter that preserves input order and runs after OrderByStep.
      if (indexOrderedCandidate != null
          && this.groupBy == null) {
        info.indexOrderedUpstream = true;
        if (indexOrderedCandidate.multiFieldOrderBy()
            && !this.returnDistinct) {
          info.primaryKeySortedInput = orderBy.getItems().getFirst();
        }
      }

      SelectExecutionPlanner.optimizeQuery(info, context);
      SelectExecutionPlanner.handleProjectionsBlock(result, info, context, enableProfiling);
    }

    // --- Store the assembled plan in the cache for future reuse ---
    if (useCache
        && !enableProfiling
        && statement.executinPlanCanBeCached(session)
        && result.canBeCached()
        && YqlExecutionPlanCache.getLastInvalidation(session) < planningStart) {
      // Stamp the plan with the placement this build read, still inside the scope.
      YqlExecutionPlanCache.put(
          statement.getOriginalStatement(),
          result,
          session,
          session.getPlanNullPlacements().recorded());
    }

    return result;
  }

  /**
   * Pins the declared collation of each ordered property onto its ORDER BY item, reading the class
   * of the item's alias out of the alias-to-class map that {@code buildPatterns} just filled.
   *
   * <p>A MATCH item names its property as {@code <alias>.<property>}, so the alias decides which
   * class the declaration comes from. An alias with no class constraint, or an item that is not one
   * plain property, takes the default collation.
   */
  private void resolveOrderByCollations(CommandContext context) {
    if (orderBy == null || aliasClasses == null || aliasClasses.isEmpty()) {
      return;
    }
    var session = context.getDatabaseSession();
    if (session == null) {
      return;
    }
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    if (schema == null) {
      return;
    }
    var projectionItems = new ArrayList<SQLProjectionItem>();
    for (var i = 0; i < returnItems.size(); i++) {
      projectionItems.add(new SQLProjectionItem(returnItems.get(i), returnAliases.get(i),
          returnNestedProjections.get(i)));
    }
    var projection = new SQLProjection(projectionItems, returnDistinct);
    OrderByCollationResolver.resolveOnAliasClasses(
        orderBy,
        alias -> {
          var className = aliasClasses.get(alias);
          return className == null ? null : schema.getClass(className);
        },
        projection);
  }

  /**
   * Appends the projection pipeline for custom RETURN expressions — projection, GROUP BY, ORDER BY,
   * UNWIND, SKIP, LIMIT — by delegating to the SELECT planner. Shared by the normal end of planning
   * and by the zero-estimate short-circuit, which needs the identical pipeline (specifically its
   * {@link GuaranteeEmptyCountStep}) on top of an {@link EmptyStep} so a bare {@code count(*)} over
   * nothing still answers {@code 0}.
   */
  private void appendCustomReturnProjection(
      SelectExecutionPlan result, CommandContext context, boolean enableProfiling) {
    var info = new QueryPlanningInfo();
    List<SQLProjectionItem> items = new ArrayList<>();
    for (var i = 0; i < this.returnItems.size(); i++) {
      var item =
          new SQLProjectionItem(
              returnItems.get(i), this.returnAliases.get(i), returnNestedProjections.get(i));
      items.add(item);
    }
    info.projection = new SQLProjection(items, returnDistinct);

    info.projection = SelectExecutionPlanner.translateDistinct(info.projection);
    info.distinct = info.projection != null && info.projection.isDistinct();
    if (info.projection != null) {
      info.projection.setDistinct(false);
    }

    info.groupBy = this.groupBy;
    info.orderBy = this.orderBy;
    info.unwind = this.unwind;
    info.skip = this.skip;
    info.limit = this.limit;

    SelectExecutionPlanner.optimizeQuery(info, context);
    SelectExecutionPlanner.handleProjectionsBlock(result, info, context, enableProfiling);
  }

  /**
   * {@code true} when RETURN is a bare {@code count(*)} with no GROUP BY — the shape that must emit
   * a synthetic 0 row on empty input ({@link GuaranteeEmptyCountStep}), so the zero-estimate
   * {@link EmptyStep} short-circuit must carry the projection pipeline with it rather than return
   * an empty plan.
   */
  private boolean isBareCountStarWithoutGroupBy() {
    return groupBy == null && isCountStarReturn();
  }

  /**
   * {@code true} when RETURN is exactly one bare {@code count(*)} item. Both the empty-count
   * guarantee ({@link #isBareCountStarWithoutGroupBy}) and the class-size short-circuit ({@link
   * #tryHardwiredMatchCount}) turn on this shape, and one deciding it differently from the other is
   * what let a filtered count on an empty class return no row at all — so the literal lives here
   * once.
   */
  private boolean isCountStarReturn() {
    if (returnItems == null || returnItems.size() != 1) {
      return false;
    }
    return "count(*)".equalsIgnoreCase(returnItems.getFirst().toString().trim());
  }

  /**
   * Single-node {@code RETURN count(*)} with no edges and no NOT patterns maps to {@link
   * com.jetbrains.youtrackdb.internal.core.sql.executor.CountFromClassStep} via the shared helper.
   * Unfiltered patterns count polymorphically; a lone exact {@code @class = className} filter
   * counts leaf-exact (Gremlin non-polymorphic {@code hasLabel}). Other filters / multi-node /
   * grouped counts fall through to the generic MATCH aggregate path (or, for declined Gremlin
   * shapes, to {@code YTDBGraphCountStrategy}).
   */
  private boolean tryHardwiredMatchCount(
      SelectExecutionPlan result, CommandContext context, boolean enableProfiling) {
    if (!isCountStarReturn()) {
      return false;
    }
    if (returnDistinct
        || groupBy != null
        || orderBy != null
        || unwind != null
        || skip != null
        || limit != null) {
      // A count fast path reads the whole-class count and ignores SKIP/LIMIT, so any pagination
      // clause must stay on the generic aggregate path where SKIP/LIMIT apply after the count row.
      return false;
    }
    if (pattern == null || pattern.numOfEdges != 0 || pattern.aliasToNode.size() != 1) {
      return false;
    }
    if (notMatchExpressions != null && !notMatchExpressions.isEmpty()) {
      return false;
    }
    if (subPatterns != null && subPatterns.size() > 1) {
      return false;
    }
    var alias = pattern.aliasToNode.keySet().iterator().next();
    if (aliasPinnedRids != null
        && aliasPinnedRids.get(alias) != null
        && !aliasPinnedRids.get(alias).isEmpty()) {
      return false;
    }
    var className = aliasClasses == null ? null : aliasClasses.get(alias);
    if (className == null || className.isBlank()) {
      return false;
    }
    // No filter → polymorphic count (MATCH/GQL default; bare g.V()/g.E() and poly hasLabel).
    // Exact @class = className alone → non-polymorphic (Gremlin hasLabel under poly=false).
    // Any other filter stays on the generic aggregate path; indexed-equality COUNT is still a
    // SELECT-only Phase-1 deferral (design §"Non-fast-path counts").
    var polymorphic = true;
    var filter = aliasFilters == null ? null : aliasFilters.get(alias);
    if (filter != null) {
      if (!HardwiredCountOptimizations.isExactClassEqualsOnly(filter, className)) {
        return false;
      }
      polymorphic = false;
    }
    var resultAlias =
        returnAliases != null
            && !returnAliases.isEmpty()
            && returnAliases.getFirst() != null
                ? returnAliases.getFirst().getStringValue()
                : "count(*)";
    return HardwiredCountOptimizations.tryMatchCountFromClass(
        result, className, resultAlias, polymorphic, context, enableProfiling);
  }

  /**
   * Checks whether the filter for the given alias references runtime context variables
   * such as `$matched`, which means the alias cannot be prefetched because its filter
   * depends on values produced during pattern traversal.
   */
  private boolean dependsOnExecutionContext(String key) {
    return filterDependsOnContext(aliasFilters.get(key));
  }

  private boolean isOptional(String key) {
    var node = this.pattern.aliasToNode.get(key);
    return node != null && node.isOptionalNode();
  }

  /**
   * Converts each `NOT { … }` expression into a {@link FilterNotMatchPatternStep} and
   * appends it to the plan. The NOT pattern reuses the same traversal mechanics as
   * positive patterns, but wraps them in a filter that **discards** rows for which the
   * pattern matches.
   *
   * <p>### Constraints (current implementation)
   *
   * <p>- The first alias in a NOT expression **must** already exist in the positive pattern.
   * - `WHERE` conditions on the origin node of a NOT expression are not yet supported.
   * - Multi-path items ({@link SQLMultiMatchPathItem}) inside NOT are not yet supported.
   *
   * @param result               the plan being assembled
   * @param pattern              the positive pattern (used to validate alias references)
   * @param notMatchExpressions  the list of negative match expressions
   * @param aliasClasses         per-alias class names (needed for hash join build-side)
   * @param aliasFilters         per-alias WHERE clauses
   * @param aliasPinnedRids            per-alias RID constraints
   * @param context              the command context
   * @param enableProfiling      whether to enable step profiling
   */
  private static void manageNotPatterns(
      SelectExecutionPlan result,
      Pattern pattern,
      List<SQLMatchExpression> notMatchExpressions,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context,
      boolean enableProfiling) {
    for (var exp : notMatchExpressions) {
      if (pattern.aliasToNode.get(exp.getOrigin().getAlias()) == null) {
        throw new CommandExecutionException(context.getDatabaseSession(),
            "This kind of NOT expression is not supported (yet). "
                + "The first alias in a NOT expression has to be present in the positive pattern");
      }

      if (exp.getOrigin().getFilter() != null) {
        throw new CommandExecutionException(context.getDatabaseSession(),
            "This kind of NOT expression is not supported (yet): "
                + "WHERE condition on the initial alias");
        // TODO implement this
      }

      // Build the NOT pattern's MatchStep chain (shared by both strategies)
      var lastFilter = exp.getOrigin();
      List<AbstractExecutionStep> matchSteps = new ArrayList<>();
      for (var item : exp.getItems()) {
        if (item instanceof SQLMultiMatchPathItem) {
          throw new CommandExecutionException(context.getDatabaseSession(),
              "This kind of NOT expression is not supported (yet): " + item);
        }
        var edge = new PatternEdge();
        edge.item = item;
        edge.out = new PatternNode();
        edge.out.alias = lastFilter.getAlias();
        edge.in = new PatternNode();
        edge.in.alias = item.getFilter().getAlias();
        var traversal = new EdgeTraversal(edge, true);
        var step = new MatchStep(context, traversal, enableProfiling);
        matchSteps.add(step);
        lastFilter = item.getFilter();
      }

      if (canUseHashJoin(
          exp, aliasClasses, aliasFilters, aliasPinnedRids, context)) {
        // Hash anti-join path: materialize NOT sub-pattern, probe per upstream row
        var buildPlan = buildNotPatternPlan(
            exp, matchSteps, aliasClasses, aliasFilters, aliasPinnedRids,
            context, enableProfiling);
        var sharedAliases = findSharedAliases(exp, pattern);
        result.chain(new HashJoinMatchStep(
            context, buildPlan, sharedAliases, JoinMode.ANTI_JOIN, enableProfiling));
      } else {
        // Fallback: nested-loop evaluation via FilterNotMatchPatternStep
        result.chain(new FilterNotMatchPatternStep(matchSteps, context, enableProfiling));
      }
    }
  }

  /**
   * Checks whether a NOT expression depends on the current execution context
   * ({@code $matched} or {@code $parent} references). If any filter in the NOT
   * expression references these variables, the pattern cannot be independently
   * materialized and must use the nested-loop {@link FilterNotMatchPatternStep}.
   *
   * <p>Inspects the origin filter and all intermediate path-item filters, checking
   * each WHERE clause for {@code refersToParent()} and string-level
   * {@code $matched.} references (matching the approach in
   * {@link #dependsOnExecutionContext}).
   *
   * @param exp the NOT match expression to inspect
   * @return {@code true} if any filter depends on execution context
   */
  static boolean notPatternDependsOnMatched(SQLMatchExpression exp) {
    // Check origin filter (currently always null per parser validation in
    // manageNotPatterns, but check defensively in case that constraint is relaxed)
    if (filterDependsOnContext(exp.getOrigin().getFilter())) {
      return true;
    }
    // Check each intermediate path item's filter
    for (var item : exp.getItems()) {
      var filter = item.getFilter();
      if (filter != null && filterDependsOnContext(filter.getFilter())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether a single WHERE clause references execution context variables.
   */
  private static boolean filterDependsOnContext(@Nullable SQLWhereClause where) {
    if (where == null) {
      return false;
    }
    if (where.refersToParent()) {
      return true;
    }
    return where.toString().toLowerCase(Locale.ROOT).contains("$matched.");
  }

  /**
   * Finds aliases shared between a NOT expression and the positive pattern. These
   * shared aliases form the join key for hash-based evaluation.
   *
   * <p>The origin alias is guaranteed to be shared (enforced by the origin-alias
   * validation in {@code manageNotPatterns}). Additional shared aliases may exist if intermediate path items
   * reference aliases that also appear in the positive pattern.
   *
   * @param exp     the NOT match expression
   * @param pattern the positive pattern
   * @return shared alias names, origin first, then others in NOT-expression traversal order;
   *     never empty
   */
  static List<String> findSharedAliases(SQLMatchExpression exp, Pattern pattern) {
    // Collect all alias names from the NOT expression
    var notAliases = new LinkedHashSet<String>();
    var originAlias = exp.getOrigin().getAlias();
    assert originAlias != null : "NOT expression origin must have an alias";
    notAliases.add(originAlias);
    for (var item : exp.getItems()) {
      var filter = item.getFilter();
      if (filter != null) {
        var alias = filter.getAlias();
        if (alias != null) {
          notAliases.add(alias);
        }
      }
    }

    // Intersect with positive pattern aliases, preserving origin-first order
    var positiveAliases = pattern.aliasToNode.keySet();
    var result = new ArrayList<String>();
    for (var alias : notAliases) {
      if (positiveAliases.contains(alias)) {
        result.add(alias);
      }
    }

    assert !result.isEmpty()
        : "NOT expression must share at least the origin alias with the positive pattern";
    return result;
  }

  /**
   * Estimates the cardinality of a NOT pattern's build side (the materialized result
   * set used for hash-based evaluation). The estimate drives the planner's decision
   * to use hash join vs. nested-loop.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Get the origin alias's base cardinality via {@link #estimateRootEntries}.
   *       If the origin alias has no estimable class/RID/filter, returns
   *       {@code Long.MAX_VALUE} to force fallback to nested-loop.</li>
   *   <li>For each edge, multiply by schema-based fan-out via
   *       {@link EdgeFanOutEstimator#estimateFanOut}.</li>
   *   <li>For each intermediate filter (non-null WHERE clause), apply 0.5 selectivity.</li>
   *   <li>Cap at {@code Long.MAX_VALUE} to avoid overflow.</li>
   * </ol>
   *
   * @return estimated cardinality, or {@code Long.MAX_VALUE} if not estimable
   */
  static long estimateNotPatternCardinality(
      SQLMatchExpression exp,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    var originAlias = exp.getOrigin().getAlias();
    assert originAlias != null : "NOT expression origin must have an alias";

    // If the origin alias has no class, RID, or filter, we can't estimate —
    // return MAX_VALUE to force fallback to nested-loop.
    if (aliasClasses.get(originAlias) == null
        && !aliasPinnedRids.containsKey(originAlias)
        && aliasFilters.get(originAlias) == null) {
      return Long.MAX_VALUE;
    }
    var session = context.getDatabaseSession();
    long estimate = estimateAliasCardinality(
        originAlias, aliasClasses, aliasFilters, aliasPinnedRids, context);
    var currentClass = aliasClasses.get(originAlias);
    // Fresh per-call memo of class-name → approximateCount: fan-out values
    // are unchanged, only repeated counts for the same class are elided.
    Map<String, Long> classCountCache = new HashMap<>();
    for (var item : exp.getItems()) {
      // Estimate fan-out from schema statistics (edgeCount / sourceCount)
      // or fall back to default if schema metadata is unavailable
      var method = item.getMethod();
      double fanOut = estimateMethodFanOut(method, currentClass, session,
          classCountCache);
      long fanOutLong = Math.max(1, Math.round(fanOut));
      if (estimate > Long.MAX_VALUE / fanOutLong) {
        return Long.MAX_VALUE; // overflow guard
      }
      estimate *= fanOutLong;

      // Apply selectivity for intermediate filters — use histogram-based
      // estimation if an index is available, otherwise fall back to default.
      var filter = item.getFilter();
      if (filter != null && filter.getFilter() != null) {
        double selectivity = estimateFilterSelectivity(
            filter.getFilter(), currentClass, context);
        estimate = Math.max(1, Math.round(estimate * selectivity));
      }
      // Track current class for next hop's fan-out estimation
      if (filter != null && filter.getClassName(context) != null) {
        currentClass = filter.getClassName(context);
      }
    }
    return estimate;
  }

  /**
   * Determines whether a NOT expression is eligible for hash-based anti-join evaluation.
   * Returns {@code true} iff all three conditions are met:
   * <ol>
   *   <li>No filter in the NOT expression references {@code $matched} or
   *       {@code $parent}.</li>
   *   <li>The origin alias has a known class in {@code aliasClasses} (needed to
   *       construct the build-side scan).</li>
   *   <li>The estimated build-side cardinality does not exceed
   *       {@link #getHashJoinThreshold()}.</li>
   * </ol>
   */
  static boolean canUseHashJoin(
      SQLMatchExpression exp,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    if (notPatternDependsOnMatched(exp)) {
      return false;
    }
    var originAlias = exp.getOrigin().getAlias();
    if (originAlias == null || !aliasClasses.containsKey(originAlias)) {
      return false;
    }
    var estimatedCardinality = estimateNotPatternCardinality(
        exp, aliasClasses, aliasFilters, aliasPinnedRids, context);
    return estimatedCardinality <= getHashJoinThreshold();
  }

  /**
   * Determines which pattern aliases are referenced downstream of the MATCH traversal —
   * in the RETURN clause, GROUP BY, ORDER BY, and UNWIND. This is needed to decide if
   * a branch's non-shared aliases can be elided via semi-join (i.e., if they are not
   * referenced downstream, the branch only needs to be probed for existence).
   *
   * <p>If any wildcard return mode is active ({@code $elements}, {@code $paths},
   * {@code $patterns}, {@code $pathElements}), all pattern aliases are implicitly
   * needed and the method returns the full {@code allPatternAliases} set.
   *
   * @param returnItems         expressions in the RETURN clause
   * @param groupBy             GROUP BY clause (nullable)
   * @param orderBy             ORDER BY clause (nullable)
   * @param unwind              UNWIND clause (nullable)
   * @param allPatternAliases   all aliases defined in the pattern graph
   * @return the subset of aliases that are referenced downstream
   */
  Set<String> collectDownstreamAliases(
      List<SQLExpression> returnItems,
      @Nullable SQLGroupBy groupBy,
      @Nullable SQLOrderBy orderBy,
      @Nullable SQLUnwind unwind,
      Set<String> allPatternAliases) {
    // Wildcard return modes implicitly reference all aliases
    if (returnElements || returnPaths || returnPatterns || returnPathElements) {
      return Set.copyOf(allPatternAliases);
    }

    // Defensive: empty RETURN items shouldn't happen, but return all aliases
    if (returnItems == null || returnItems.isEmpty()) {
      return Set.copyOf(allPatternAliases);
    }

    var referenced = new HashSet<String>();

    // Pre-compile word-boundary patterns for all aliases (avoids repeated
    // regex compilation when scanning multiple expressions)
    var compiled = new HashMap<String, java.util.regex.Pattern>();
    for (var alias : allPatternAliases) {
      compiled.put(alias, java.util.regex.Pattern.compile(
          "\\b" + java.util.regex.Pattern.quote(alias) + "\\b"));
    }

    // Scan RETURN expressions
    for (var expr : returnItems) {
      collectAliasesFromText(expr.toString(), allPatternAliases, referenced, compiled);
    }

    // Scan GROUP BY expressions
    if (groupBy != null) {
      for (var expr : groupBy.getItems()) {
        collectAliasesFromText(
            expr.toString(), allPatternAliases, referenced, compiled);
      }
    }

    // Scan ORDER BY items — use getAlias() which holds the expression text
    // (e.g., "friend.name"), not Object.toString() which is not overridden
    if (orderBy != null && orderBy.getItems() != null) {
      for (var item : orderBy.getItems()) {
        var alias = item.getAlias();
        if (alias != null) {
          collectAliasesFromText(alias, allPatternAliases, referenced, compiled);
        }
      }
    }

    // Scan UNWIND identifiers
    if (unwind != null) {
      for (var ident : unwind.getItems()) {
        collectAliasesFromText(
            ident.toString(), allPatternAliases, referenced, compiled);
      }
    }

    return referenced;
  }

  /**
   * Scans a text string for alias references using pre-compiled word-boundary patterns.
   * An alias is considered referenced if it appears as a standalone word (e.g.,
   * {@code friend} or {@code friend.name}, but not as a substring of a longer
   * identifier like {@code friendship}).
   */
  private static void collectAliasesFromText(
      String text,
      Set<String> allPatternAliases,
      Set<String> result,
      Map<String, java.util.regex.Pattern> compiledPatterns) {
    if (text == null || text.isEmpty()) {
      return;
    }
    for (var alias : allPatternAliases) {
      var pattern = compiledPatterns.get(alias);
      if (pattern == null) {
        pattern = java.util.regex.Pattern.compile(
            "\\b" + java.util.regex.Pattern.quote(alias) + "\\b");
      }
      if (pattern.matcher(text).find()) {
        result.add(alias);
      }
    }
  }

  /**
   * Describes a secondary branch in the pattern graph that is eligible for hash join
   * optimization. Depending on whether intermediate aliases are referenced downstream,
   * the branch is executed as a {@link JoinMode#SEMI_JOIN} (existence check only) or
   * {@link JoinMode#INNER_JOIN} (intermediate bindings merged into result rows).
   *
   * @param sharedAliases       aliases shared between the branch and the main path (join keys)
   * @param branchEdges         the edge traversals forming this branch (in schedule order),
   *                            including the consistency-check edge
   * @param intermediateAliases aliases visited exclusively by this branch
   * @param estimatedCardinality estimated number of rows the branch produces
   * @param joinMode            the join mode: SEMI_JOIN if no intermediates are downstream,
   *                            INNER_JOIN if any intermediate is referenced downstream
   * @param scanAlias           the alias to scan from in the build-side plan — must have a
   *                            known class or RID in {@code aliasClasses}/{@code aliasPinnedRids}
   */
  record HashJoinBranch(
      List<String> sharedAliases,
      List<EdgeTraversal> branchEdges,
      Set<String> intermediateAliases,
      long estimatedCardinality,
      JoinMode joinMode,
      String scanAlias) {
  }

  /**
   * Identifies secondary branches in the pattern graph that are eligible for hash join
   * optimization (semi-join or inner join). A hash join branch is a contiguous sub-sequence
   * of scheduled edges that:
   * <ul>
   *   <li>ends with a <b>consistency-check edge</b> — an edge whose target alias was already
   *       visited before this edge (both endpoints known, cost 0 in the scheduler)</li>
   *   <li>no filter on any branch node depends on {@code $matched} or {@code $parent}</li>
   *   <li>estimated cardinality does not exceed {@link #getHashJoinThreshold()}</li>
   *   <li>no intermediate alias uses an auto-generated name ({@link #DEFAULT_ALIAS_PREFIX})</li>
   * </ul>
   *
   * <p>Branches are classified as {@link JoinMode#SEMI_JOIN} when no intermediate alias
   * is referenced downstream, or {@link JoinMode#INNER_JOIN} when at least one intermediate
   * alias is referenced downstream (requiring build-side row merge into the result).
   *
   * @param scheduledEdges     the edge schedule from {@code getTopologicalSortedSchedule()}
   * @param downstreamAliases  aliases referenced downstream of the MATCH traversal
   * @param aliasClasses       per-alias class names
   * @param aliasFilters       per-alias WHERE clauses
   * @param aliasPinnedRids          per-alias RID constraints
   * @param context            the command context
   * @return list of eligible hash join branches (may be empty)
   */
  static List<HashJoinBranch> identifyHashJoinBranches(
      List<EdgeTraversal> scheduledEdges,
      Set<String> downstreamAliases,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    if (scheduledEdges.size() < 2) {
      // Need at least 2 edges: one branch edge + one consistency-check edge
      return List.of();
    }

    // Build a per-edge visited snapshot: for each edge index i, we know which
    // aliases were visited by edges [0..i-1]. This lets traceBackwardBranch
    // distinguish "main path" aliases from "branch" aliases.
    var visited = new LinkedHashSet<String>();
    visited.add(sourceAlias(scheduledEdges.get(0)));

    // visitedBefore[i] = set of aliases visited before edge i is processed
    @SuppressWarnings("unchecked")
    var visitedBefore = new Set[scheduledEdges.size()];
    for (int i = 0; i < scheduledEdges.size(); i++) {
      visitedBefore[i] = Set.copyOf(visited);
      visited.add(sourceAlias(scheduledEdges.get(i)));
      visited.add(targetAlias(scheduledEdges.get(i)));
    }

    var result = new ArrayList<HashJoinBranch>();
    // Track claimed edges to prevent overlapping branches from sharing edges.
    // If two branches share an edge, the same edge would be skipped once in the
    // main loop but included in two build plans, leading to duplicated traversal.
    var claimedEdges = new HashSet<EdgeTraversal>();

    for (int i = 0; i < scheduledEdges.size(); i++) {
      var target = targetAlias(scheduledEdges.get(i));
      @SuppressWarnings("unchecked")
      Set<String> beforeThis = visitedBefore[i];

      if (beforeThis.contains(target)) {
        // This is a consistency-check edge — target was already visited.
        var branch = traceBackwardBranch(
            scheduledEdges, i, visitedBefore, downstreamAliases,
            aliasClasses, aliasFilters, aliasPinnedRids, context);
        if (branch != null) {
          // Discard branch if any of its edges overlap with an already-claimed branch
          boolean overlaps = false;
          for (var edge : branch.branchEdges()) {
            if (claimedEdges.contains(edge)) {
              overlaps = true;
              break;
            }
          }
          if (!overlaps) {
            claimedEdges.addAll(branch.branchEdges());
            result.add(branch);
          }
        }
      }
    }

    return result;
  }

  /**
   * Returns the source alias of an edge traversal (the alias that is already matched).
   */
  private static String sourceAlias(EdgeTraversal edge) {
    return edge.out ? edge.edge.out.alias : edge.edge.in.alias;
  }

  /**
   * Returns the target alias of an edge traversal (the alias being traversed to).
   */
  private static String targetAlias(EdgeTraversal edge) {
    return edge.out ? edge.edge.in.alias : edge.edge.out.alias;
  }

  /**
   * Traces backward from a consistency-check edge at position {@code checkIdx} to find
   * the branch's edges. The branch is the contiguous sub-sequence of edges ending at the
   * consistency-check edge, starting from a "branch root" shared alias. Branches with
   * downstream intermediates are classified as {@link JoinMode#INNER_JOIN} (build-side
   * row merge); branches without are classified as {@link JoinMode#SEMI_JOIN}.
   *
   * @return a {@link HashJoinBranch} if eligible, or {@code null} if not
   */
  @Nullable private static HashJoinBranch traceBackwardBranch(
      List<EdgeTraversal> scheduledEdges,
      int checkIdx,
      Set<String>[] visitedBefore,
      Set<String> downstreamAliases,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    var checkEdge = scheduledEdges.get(checkIdx);
    var checkTarget = targetAlias(checkEdge);
    var checkSource = sourceAlias(checkEdge);

    // Phase 1: Walk backward to collect branch edges and intermediate aliases
    var trace = traceBackwardEdges(
        scheduledEdges, checkIdx, visitedBefore, checkSource, checkTarget);
    if (trace == null) {
      return null;
    }

    // Phase 2: Eligibility checks (optional nodes, context dependencies,
    // external $matched dependencies on intermediates)
    var joinMode = checkBranchEligibility(
        trace.branchEdges, trace.intermediateAliases, scheduledEdges,
        downstreamAliases, aliasFilters);
    if (joinMode == null) {
      return null;
    }

    // Also check shared aliases (checkTarget, checkSource, branchRoot) — their
    // filters are used in the build-side plan's scan or leftFilter, and $matched
    // is not populated in the isolated context.
    if (filterDependsOnContext(aliasFilters.get(checkTarget))
        || filterDependsOnContext(aliasFilters.get(checkSource))
        || filterDependsOnContext(aliasFilters.get(trace.branchRoot))) {
      return null;
    }

    // Phase 3: Cardinality estimation and cost-based guards
    long cardinality = estimateBranchCardinality(
        trace.branchRoot, trace.branchEdges, aliasClasses, aliasFilters,
        aliasPinnedRids, context);
    long threshold = getHashJoinThreshold();
    if (cardinality > threshold) {
      return null;
    }

    // INNER_JOIN materializes full ResultInternal rows (~7× heavier per entry
    // than SEMI_JOIN's lightweight JoinKey entries). Apply a tighter threshold.
    if (joinMode == JoinMode.INNER_JOIN
        && cardinality > threshold / INNER_JOIN_MEMORY_WEIGHT) {
      return null;
    }

    // Guards 1 & 2 are only active when upstreamMin > 0. Setting upstreamMin to 0
    // bypasses both guards — only the build-side threshold applies.
    long upstreamMin = getHashJoinUpstreamMin();
    if (upstreamMin > 0) {
      // Guard 1: Skip hash join when the upstream (probe side) is small.
      long upstreamCardinality = estimateUpstreamCardinality(
          scheduledEdges, checkIdx, trace.branchEdges,
          aliasClasses, aliasFilters, aliasPinnedRids, context);
      if (upstreamCardinality < upstreamMin) {
        return null;
      }

      // Guard 2: Cost-based comparison.
      //   hashJoinCost  = build_cardinality + upstream
      //   nestedLoopCost = upstream × branchFanOut × numHops
      double branchFanOut = estimateBranchFanOut(
          trace.branchEdges, trace.branchRoot, aliasClasses, aliasFilters,
          context);
      int numBranchHops = Math.max(1, trace.branchEdges.size() - 1);
      double nestedLoopCost = upstreamCardinality * branchFanOut * numBranchHops;
      double hashJoinCost = (double) cardinality + upstreamCardinality;
      if (hashJoinCost >= nestedLoopCost) {
        return null;
      }
    }

    // Shared aliases: the branch root and the check edge's non-intermediate endpoint
    var otherShared = trace.intermediateAliases.contains(checkSource)
        ? checkTarget : checkSource;
    var sharedAliases = trace.branchRoot.equals(otherShared)
        ? List.of(trace.branchRoot) : List.of(otherShared, trace.branchRoot);

    // The build plan needs a scan alias with a known class or RID.
    var scanAlias = findScanAlias(
        otherShared, trace.branchRoot, trace.intermediateAliases, aliasClasses,
        aliasPinnedRids);
    if (scanAlias == null) {
      return null;
    }

    return new HashJoinBranch(
        sharedAliases, trace.branchEdges, trace.intermediateAliases, cardinality,
        joinMode, scanAlias);
  }

  /** Intermediate result from backward edge tracing in {@link #traceBackwardBranch}. */
  private record BranchTrace(
      List<EdgeTraversal> branchEdges,
      Set<String> intermediateAliases,
      String branchRoot) {
  }

  /**
   * Walks backward from the consistency-check edge at {@code checkIdx}, collecting
   * consecutive edges whose target was not yet visited when that edge was processed
   * (i.e., edges that introduced new intermediate aliases). Stops when the branch
   * root (an already-visited source alias) is found.
   *
   * @return the traced branch edges, intermediate aliases, and branch root; or
   *         {@code null} if no valid branch was found
   */
  @SuppressWarnings("unchecked")
  @Nullable private static BranchTrace traceBackwardEdges(
      List<EdgeTraversal> scheduledEdges,
      int checkIdx,
      Set<String>[] visitedBefore,
      String checkSource,
      String checkTarget) {
    var checkEdge = scheduledEdges.get(checkIdx);
    var branchEdges = new ArrayList<EdgeTraversal>();
    branchEdges.add(checkEdge);
    var intermediateAliases = new HashSet<String>();

    String currentAlias = null;
    for (int j = checkIdx - 1; j >= 0; j--) {
      var prevEdge = scheduledEdges.get(j);
      var prevTarget = targetAlias(prevEdge);
      var prevSource = sourceAlias(prevEdge);
      Set<String> visitedBeforeJ = visitedBefore[j];

      // This edge introduced prevTarget if prevTarget was NOT in visitedBefore[j]
      if (!visitedBeforeJ.contains(prevTarget)) {
        if (currentAlias == null) {
          // First branch edge: must connect to checkSource or checkTarget
          if (prevTarget.equals(checkSource) || prevTarget.equals(checkTarget)) {
            intermediateAliases.add(prevTarget);
            branchEdges.add(0, prevEdge);
            currentAlias = prevSource;
            if (visitedBeforeJ.contains(prevSource)) {
              break;
            }
          }
        } else if (prevTarget.equals(currentAlias)) {
          // Continues the branch chain
          intermediateAliases.add(prevTarget);
          branchEdges.add(0, prevEdge);
          currentAlias = prevSource;
          if (visitedBeforeJ.contains(prevSource)) {
            break;
          }
        }
      }
    }

    if (intermediateAliases.isEmpty() || currentAlias == null
        || branchEdges.size() < 2) {
      return null;
    }
    return new BranchTrace(branchEdges, intermediateAliases, currentAlias);
  }

  /**
   * Checks whether a traced branch is eligible for hash join optimization.
   * Rejects branches with optional nodes, auto-generated internal aliases,
   * context-dependent filters ({@code $matched}/{@code $parent}), or external
   * edges that depend on branch intermediates via {@code $matched}.
   *
   * @return the {@link JoinMode} if eligible, or {@code null} if not
   */
  @Nullable private static JoinMode checkBranchEligibility(
      List<EdgeTraversal> branchEdges,
      Set<String> intermediateAliases,
      List<EdgeTraversal> scheduledEdges,
      Set<String> downstreamAliases,
      Map<String, SQLWhereClause> aliasFilters) {
    // Check for auto-generated internal aliases and classify join mode
    boolean hasDownstreamIntermediate = false;
    for (var alias : intermediateAliases) {
      if (alias.startsWith(DEFAULT_ALIAS_PREFIX)) {
        return null;
      }
      if (downstreamAliases.contains(alias)) {
        hasDownstreamIntermediate = true;
      }
    }
    var joinMode = hasDownstreamIntermediate
        ? JoinMode.INNER_JOIN : JoinMode.SEMI_JOIN;

    // Check that no branch edge involves an optional node
    for (var edgeT : branchEdges) {
      if (edgeT.edge.out.isOptionalNode() || edgeT.edge.in.isOptionalNode()) {
        return null;
      }
    }

    // Check that no branch node's filter depends on $matched/$parent
    for (var alias : intermediateAliases) {
      if (filterDependsOnContext(aliasFilters.get(alias))) {
        return null;
      }
    }

    // Check that no NON-BRANCH edge references a branch intermediate via $matched.
    // If an edge outside the branch depends on an intermediate, moving the branch
    // to the end would break execution (the alias wouldn't be bound).
    var branchEdgeSet = new HashSet<>(branchEdges);
    for (var scheduled : scheduledEdges) {
      if (branchEdgeSet.contains(scheduled)) {
        continue;
      }
      var outFilter = aliasFilters.get(scheduled.edge.out.alias);
      var inFilter = aliasFilters.get(scheduled.edge.in.alias);
      var outStr = outFilter != null ? outFilter.toString() : null;
      var inStr = inFilter != null ? inFilter.toString() : null;
      for (var interAlias : intermediateAliases) {
        if (outStr != null && outStr.contains("$matched." + interAlias)) {
          return null;
        }
        if (inStr != null && inStr.contains("$matched." + interAlias)) {
          return null;
        }
      }
    }

    return joinMode;
  }

  /**
   * Estimates the cardinality of a hash join branch. Starts from the branch root's
   * estimated record count and multiplies by schema-based fan-out per edge
   * (via {@link EdgeFanOutEstimator}), applying 0.5 selectivity for WHERE filters.
   */
  private static long estimateBranchCardinality(
      String branchRoot,
      List<EdgeTraversal> branchEdges,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    var session = context.getDatabaseSession();
    // Start with branch root cardinality
    long rows = estimateAliasCardinality(
        branchRoot, aliasClasses, aliasFilters, aliasPinnedRids, context);

    // Skip the last edge (consistency-check edge) — it doesn't expand cardinality,
    // it's a filter verifying the target alias matches an already-visited node (cost 0).
    int edgeCount = Math.max(0, branchEdges.size() - 1);
    var currentClass = aliasClasses.get(branchRoot);
    // Fresh per-call class-count memo (pure optimisation).
    Map<String, Long> classCountCache = new HashMap<>();
    for (int i = 0; i < edgeCount; i++) {
      var edgeT = branchEdges.get(i);
      var method = edgeT.edge.item != null ? edgeT.edge.item.getMethod() : null;
      double fanOut = estimateMethodFanOut(method, currentClass, session,
          classCountCache);
      long fanOutLong = Math.max(1, Math.round(fanOut));
      if (rows > Long.MAX_VALUE / fanOutLong) {
        return Long.MAX_VALUE; // overflow guard
      }
      rows *= fanOutLong;
      var target = targetAlias(edgeT);
      // Apply selectivity — histogram-based if index available, default otherwise
      var targetFilter = aliasFilters.get(target);
      if (targetFilter != null) {
        double selectivity = estimateFilterSelectivity(
            targetFilter, currentClass, context);
        rows = Math.max(1, Math.round(rows * selectivity));
      }
      // Track current class for next hop
      var targetClass = aliasClasses.get(target);
      if (targetClass != null) {
        currentClass = targetClass;
      }
    }

    return rows;
  }

  /**
   * Estimates the upstream (probe-side) cardinality at the point where a hash join
   * branch diverges from the main path. Walks the main-path edges from the scan root
   * up to (but not including) the branch edges, multiplying by fan-out and selectivity.
   *
   * <p>Main-path edges are all edges in {@code scheduledEdges[0..checkIdx-1]} that are
   * NOT in {@code branchEdges}. The upstream cardinality is:
   * <pre>
   *   rootCardinality × Π(fanOut_i × selectivity_i) for each main-path edge i
   * </pre>
   *
   * @param scheduledEdges full edge schedule
   * @param checkIdx       index of the consistency-check edge
   * @param branchEdges    edges belonging to the hash join branch
   * @return estimated upstream row count, or {@link Long#MAX_VALUE} if not estimable
   */
  private static long estimateUpstreamCardinality(
      List<EdgeTraversal> scheduledEdges,
      int checkIdx,
      List<EdgeTraversal> branchEdges,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    var session = context.getDatabaseSession();
    var branchEdgeSet = new HashSet<>(branchEdges);

    // Find the scan root alias (source of the first scheduled edge)
    var rootAlias = sourceAlias(scheduledEdges.get(0));
    long rows = estimateAliasCardinality(
        rootAlias, aliasClasses, aliasFilters, aliasPinnedRids, context);

    var currentClass = aliasClasses.get(rootAlias);
    // Fresh per-call class-count memo (pure optimisation).
    Map<String, Long> classCountCache = new HashMap<>();
    for (int i = 0; i < checkIdx; i++) {
      var edgeT = scheduledEdges.get(i);
      if (branchEdgeSet.contains(edgeT)) {
        continue; // Skip branch edges — they don't contribute to upstream
      }
      var method = edgeT.edge.item != null ? edgeT.edge.item.getMethod() : null;
      double fanOut = estimateMethodFanOut(method, currentClass, session,
          classCountCache);
      long fanOutLong = Math.max(1, Math.round(fanOut));
      if (rows > Long.MAX_VALUE / fanOutLong) {
        return Long.MAX_VALUE;
      }
      rows *= fanOutLong;
      var target = targetAlias(edgeT);
      var targetFilter = aliasFilters.get(target);
      if (targetFilter != null) {
        double selectivity = estimateFilterSelectivity(
            targetFilter, currentClass, context);
        rows = Math.max(1, Math.round(rows * selectivity));
      }
      var targetClass = aliasClasses.get(target);
      if (targetClass != null) {
        currentClass = targetClass;
      }
    }
    return rows;
  }

  /**
   * Estimates the per-row branch fan-out — the average number of rows a single upstream
   * row produces when traversing the branch via nested-loop. This is the product of
   * fanOut × selectivity for each branch edge, excluding the last consistency-check edge
   * (which is a free RID equality check, cost 0).
   *
   * @return the estimated per-row fan-out (≥ 1.0)
   */
  private static double estimateBranchFanOut(
      List<EdgeTraversal> branchEdges,
      String branchRoot,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      CommandContext context) {
    var session = context.getDatabaseSession();
    // Exclude the last edge (consistency-check) — it's a free RID match
    int edgeCount = Math.max(0, branchEdges.size() - 1);
    double fanOut = 1.0;
    var currentClass = aliasClasses.get(branchRoot);
    // Fresh per-call class-count memo (pure optimisation).
    Map<String, Long> classCountCache = new HashMap<>();
    for (int i = 0; i < edgeCount; i++) {
      var edgeT = branchEdges.get(i);
      var method = edgeT.edge.item != null ? edgeT.edge.item.getMethod() : null;
      fanOut *= estimateMethodFanOut(method, currentClass, session,
          classCountCache);
      var target = targetAlias(edgeT);
      var targetFilter = aliasFilters.get(target);
      if (targetFilter != null) {
        double selectivity = estimateFilterSelectivity(
            targetFilter, currentClass, context);
        fanOut *= selectivity;
      }
      var targetClass = aliasClasses.get(target);
      if (targetClass != null) {
        currentClass = targetClass;
      }
    }
    return Math.max(1.0, fanOut);
  }

  /**
   * Estimates the cardinality of a single alias — the number of records it can produce.
   * Delegates to {@link #estimateRootEntries} with single-alias maps, matching the
   * approach used in {@link #estimateNotPatternCardinality}.
   */
  private static long estimateAliasCardinality(
      String alias,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    var cls = aliasClasses.get(alias);
    var filter = aliasFilters.get(alias);
    var singleClasses = cls != null ? Map.of(alias, cls) : Map.<String, String>of();
    var singleFilters = filter != null ? Map.of(alias, filter) : Map.<String, SQLWhereClause>of();
    var singlePinnedRids = aliasPinnedRids.containsKey(alias)
        ? Map.of(alias, aliasPinnedRids.get(alias))
        : Map.<String, List<SQLRid>>of();

    var rootEstimates = estimateRootEntries(
        singleClasses, singlePinnedRids, singleFilters, context);
    var count = rootEstimates.get(alias);
    return count != null ? Math.max(1, count) : THRESHOLD;
  }

  /**
   * Selects the best alias to scan from in the build-side plan. Tries shared aliases
   * first ({@code otherShared}, then {@code branchRoot}), then intermediates. Returns
   * the first alias that has a known class or RID, or {@code null} if none qualifies.
   */
  @Nullable private static String findScanAlias(
      String otherShared,
      String branchRoot,
      Set<String> intermediateAliases,
      Map<String, String> aliasClasses,
      Map<String, List<SQLRid>> aliasPinnedRids) {
    // Prefer shared aliases — they anchor the build plan at a join endpoint
    if (aliasClasses.get(otherShared) != null
        || aliasPinnedRids.containsKey(otherShared)) {
      return otherShared;
    }
    if (aliasClasses.get(branchRoot) != null
        || aliasPinnedRids.containsKey(branchRoot)) {
      return branchRoot;
    }
    // Fall back to intermediates (relevant for INNER_JOIN where intermediates have classes)
    for (var alias : intermediateAliases) {
      if (aliasClasses.get(alias) != null
          || aliasPinnedRids.containsKey(alias)) {
        return alias;
      }
    }
    return null;
  }

  /**
   * Constructs the build-side {@link SelectExecutionPlan} for a NOT pattern's hash
   * anti-join. The plan scans the origin alias's class and chains the NOT pattern's
   * {@link MatchStep}s to traverse the negative edges.
   *
   * @param exp              the NOT match expression
   * @param matchSteps       the pre-built MatchStep chain for the NOT pattern's edges
   * @param aliasClasses     per-alias class names
   * @param aliasFilters     per-alias WHERE clauses
   * @param aliasPinnedRids    per-alias pinned RID lists
   * @param context          the command context
   * @param enableProfiling  whether to enable step profiling
   * @return a complete build-side execution plan
   */
  private static SelectExecutionPlan buildNotPatternPlan(
      SQLMatchExpression exp,
      List<AbstractExecutionStep> matchSteps,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context,
      boolean enableProfiling) {
    var originAlias = exp.getOrigin().getAlias();
    var originClass = aliasClasses.get(originAlias);
    var originRids = pinnedRidsForAlias(originAlias, aliasPinnedRids);
    var originFilter = aliasFilters.get(originAlias);

    // Build the origin scan: SELECT FROM <class> [WHERE ...]
    // Copy the WHERE clause to prevent mutable filter corruption (same as
    // buildHashJoinBranchPlan and addStepsFor).
    //
    // Unlike addStepsFor, this scan is built whether or not the origin alias is prefetched, and
    // it must be: HashJoinMatchStep.internalStart materializes the build side before it calls
    // prev.start(), so the upstream MatchPrefetchStep has not run and the context carries no
    // cache for MatchFirstStep to read. Handing the sub-plan-free constructor to a prefetched
    // origin alias here makes the anti-join throw on its first row.
    var select = createSelectStatement(
        originClass, originRids, originFilter == null ? null : originFilter.copy());

    // Create PatternNode for the origin alias
    var originNode = new PatternNode();
    originNode.alias = originAlias;

    var buildPlan = new SelectExecutionPlan(context);
    buildPlan.chain(new MatchFirstStep(
        context, originNode, select.createExecutionPlan(context, enableProfiling),
        enableProfiling));

    // Chain the NOT pattern's MatchSteps
    for (var step : matchSteps) {
      buildPlan.chain(step);
    }

    return buildPlan;
  }

  /**
   * Constructs the build-side {@link SelectExecutionPlan} for a hash join branch.
   * The plan scans from {@link HashJoinBranch#scanAlias()} (the first shared or
   * intermediate alias with a known class) and chains {@link MatchStep}s for all
   * branch edges, building a directed path through the branch's aliases.
   *
   * @param branch          the branch descriptor
   * @param context         the command context
   * @param profilingEnabled whether to enable step profiling
   * @return a complete build-side execution plan for the branch, or null if path is incomplete
   */
  private SelectExecutionPlan buildHashJoinBranchPlan(
      HashJoinBranch branch,
      CommandContext context,
      boolean profilingEnabled) {
    // The build plan scans from the branch's scan alias (determined by
    // traceBackwardBranch — the first shared/intermediate alias with a known
    // class) and traverses through all branch edges to produce rows containing
    // all branch aliases.
    var scanAlias = branch.scanAlias();
    var scanClass = aliasClasses.get(scanAlias);
    var scanRids = pinnedRidsForAlias(scanAlias);
    var scanFilter = aliasFilters.get(scanAlias);

    // Copy the WHERE clause to prevent mutable state from the main plan's
    // execution corrupting the build-side filter (matches addStepsFor behavior).
    //
    // The scan is built whether or not the scan alias is prefetched, for the same reason as in
    // buildNotPatternPlan: the build side is materialized before HashJoinMatchStep starts its
    // upstream, so the prefetch cache does not exist yet and this sub-plan is the only source of
    // build rows.
    var select = createSelectStatement(
        scanClass, scanRids, scanFilter == null ? null : scanFilter.copy());
    var scanNode = new PatternNode();
    scanNode.alias = scanAlias;

    // Use a sub-context with parent linkage for the SELECT sub-plan, matching
    // the pattern in addStepsFor(). Without parent linkage, variable lookups
    // (e.g., input parameters) would fail in the sub-plan's steps.
    var subCtx = new BasicCommandContext();
    subCtx.setParentWithoutOverridingChild(context);

    var buildPlan = new SelectExecutionPlan(context);
    buildPlan.chain(new MatchFirstStep(
        context, scanNode, select.createExecutionPlan(subCtx, profilingEnabled),
        profilingEnabled));

    // Build a directed path from scanAlias through intermediates to the other
    // shared alias (branchRoot). For each branch edge, determine the correct
    // traversal direction so the path connects.
    var edges = branch.branchEdges();
    var current = scanAlias;
    // We need to traverse branch edges in an order that forms a path from
    // scanAlias. Try each unused edge that has 'current' as one endpoint.
    var used = new boolean[edges.size()];
    for (int step = 0; step < edges.size(); step++) {
      boolean found = false;
      for (int j = 0; j < edges.size(); j++) {
        if (used[j]) {
          continue;
        }
        var orig = edges.get(j);
        var outAlias = orig.edge.out.alias;
        var inAlias = orig.edge.in.alias;
        if (outAlias.equals(current)) {
          // Forward traversal: current → inAlias
          var traversal = new EdgeTraversal(orig.edge, true);
          // Left constraints = edge.out node's constraints (same as createPlanForPattern)
          traversal.setLeftClass(aliasClasses.get(outAlias));
          traversal.setLeftFilter(aliasFilters.get(outAlias));
          traversal.setLeftRid(singletonPinnedRid(aliasPinnedRids.get(outAlias)));
          buildPlan.chain(new MatchStep(context, traversal, profilingEnabled));
          current = inAlias;
          used[j] = true;
          found = true;
          break;
        } else if (inAlias.equals(current)) {
          // Reverse traversal: current → outAlias
          var traversal = new EdgeTraversal(orig.edge, false);
          // Left constraints = edge.out node's constraints, NOT the current (in) node.
          // MatchReverseEdgeTraverser uses leftClass/leftFilter/leftRid as TARGET
          // constraints — the target in reverse mode is edge.out.
          traversal.setLeftClass(aliasClasses.get(outAlias));
          traversal.setLeftFilter(aliasFilters.get(outAlias));
          traversal.setLeftRid(singletonPinnedRid(aliasPinnedRids.get(outAlias)));
          buildPlan.chain(new MatchStep(context, traversal, profilingEnabled));
          current = outAlias;
          used[j] = true;
          found = true;
          break;
        }
      }
      if (!found) {
        break; // Can't continue the path
      }
    }

    // Verify all edges were used — if the path is incomplete, the build plan
    // would produce incorrect results (missing alias bindings for the join key).
    for (boolean u : used) {
      if (!u) {
        // Incomplete path — fall back by returning null. The caller must check.
        return null;
      }
    }

    return buildPlan;
  }

  /**
   * Appends the appropriate return-projection step based on the `RETURN` mode
   * (`$elements`, `$paths`, `$patterns`, or `$pathElements`).
   */
  private void addReturnStep(
      SelectExecutionPlan result, CommandContext context, boolean profilingEnabled) {
    if (returnElements) {
      result.chain(new ReturnMatchElementsStep(context, profilingEnabled));
    } else if (returnPaths) {
      result.chain(new ReturnMatchPathsStep(context, profilingEnabled));
    } else if (returnPatterns) {
      result.chain(new ReturnMatchPatternsStep(context, profilingEnabled));
    } else if (returnPathElements) {
      result.chain(new ReturnMatchPathElementsStep(context, profilingEnabled));
    } else {
      var projection = new SQLProjection(-1);
      projection.setItems(new ArrayList<>());
      for (var i = 0; i < returnAliases.size(); i++) {
        var item = new SQLProjectionItem(-1);
        item.setExpression(returnItems.get(i));
        item.setAlias(returnAliases.get(i));
        item.setNestedProjection(returnNestedProjections.get(i));
        projection.getItems().add(item);
      }
      result.chain(new ProjectionCalculationStep(projection, context, profilingEnabled));
    }
  }

  /**
   * Creates an execution plan for a single connected pattern (sub-graph).
   *
   * <p>The method first computes a topological traversal order via
   * {@link #getTopologicalSortedSchedule}, then emits one execution step per edge.
   * If the pattern has no edges (a single isolated node), a standalone
   * {@link MatchFirstStep} is emitted instead.
   *
   * @param pattern              the connected pattern to plan
   * @param context              the command context
   * @param estimatedRootEntries per-alias cardinality estimates used to pick the
   *                             cheapest starting node
   * @param prefetchedAliases    aliases whose records have already been prefetched
   * @param profilingEnabled     whether to collect execution statistics
   * @return an execution plan for this sub-pattern
   */
  private InternalExecutionPlan createPlanForPattern(
      Pattern pattern,
      CommandContext context,
      Map<String, Long> estimatedRootEntries,
      Set<String> prefetchedAliases,
      @Nullable IndexOrderedPlanner.IndexOrderedCandidate candidate,
      @Nullable List<EdgeTraversal> precomputedSortedEdges,
      boolean profilingEnabled) {
    var plan = new SelectExecutionPlan(context);
    // Reuse the schedule computed by Phase 4b (index-ordered probe) when available.
    // Inputs to getTopologicalSortedSchedule are deterministic for (pattern,
    // estimatedRootEntries, aliasClasses, aliasFilters, session), so the probe's
    // result is identical to what we would compute here.
    var sortedEdges = precomputedSortedEdges != null
        ? precomputedSortedEdges
        : getTopologicalSortedSchedule(estimatedRootEntries, pattern,
            aliasClasses, aliasFilters, context.getDatabaseSession());

    var first = true;
    if (!sortedEdges.isEmpty()) {
      // optimizeScheduleWithIntersections is the sole producer of IndexLookup
      // descriptors. It reports back whether any edge ended up with one — the
      // single signal that gates the (expensive) forecast pass below, so we
      // never have to re-scan the schedule for IndexLookup presence.
      var hasIndexLookup = optimizeScheduleWithIntersections(sortedEdges, context);

      // Re-bind filters after optimization: detectNotInAntiJoin() may have
      // stripped NOT IN conditions from aliasFilters, so we must push the
      // updated filters to the match expression AST nodes. Without this,
      // the MatchStep would still evaluate the original un-stripped filter.
      rebindFilters(aliasFilters);

      // Annotate each edge traversal with the source node's class/RID/filter
      // constraints (post-optimization, so stripped filters are reflected).
      // MatchReverseEdgeTraverser uses these when traversing in reverse.
      // Also propagate the profile flag so the per-build nanoTime pair in
      // EdgeTraversal#materializeAndCache only fires when PROFILE asked for
      // it — see EdgeTraversal#profilingEnabled.
      for (var edge : sortedEdges) {
        edge.setProfilingEnabled(profilingEnabled);
        if (edge.edge.out.alias != null) {
          edge.setLeftClass(aliasClasses.get(edge.edge.out.alias));
          edge.setLeftRid(singletonPinnedRid(aliasPinnedRids.get(edge.edge.out.alias)));
          edge.setLeftFilter(aliasFilters.get(edge.edge.out.alias));
        }
      }

      // Single schedule walk that stamps both per-edge collection-ID class
      // filters (always needed by the traverser) and BUILD_EAGER row-count
      // forecasts (only when an IndexLookup descriptor exists — otherwise
      // the forecast result is unread). Replaces the previous two-loop
      // structure (stampEdgeForecasts + attachCollectionIdFilters) with one
      // pass; the forecast-specific state is allocated lazily inside the
      // method when hasIndexLookup is true.
      stampEdgeMetadata(sortedEdges, estimatedRootEntries, hasIndexLookup, context);

      // Hash join optimization: detect secondary branches that can be evaluated as
      // build-side hash joins (semi-join if intermediates not downstream, inner join
      // if intermediates are referenced downstream).
      var downstreamAliases = collectDownstreamAliases(
          returnItems, groupBy, orderBy, unwind, pattern.aliasToNode.keySet());
      var hashJoinBranches = identifyHashJoinBranches(
          sortedEdges, downstreamAliases, aliasClasses, aliasFilters,
          aliasPinnedRids, context);

      // Collect edges that belong to hash join branches — skip them in the main loop.
      // Guards:
      // - If ALL edges would be claimed, the main plan would have no source step.
      // - If the FIRST edge is claimed, the main plan loses its scan root and
      //   subsequent edges may get an incorrect MatchFirstStep.
      var branchEdgeSet = new HashSet<PatternEdge>();
      for (var branch : hashJoinBranches) {
        for (var branchEdge : branch.branchEdges()) {
          branchEdgeSet.add(branchEdge.edge);
        }
      }
      if (branchEdgeSet.size() >= sortedEdges.size()
          || branchEdgeSet.contains(sortedEdges.getFirst().edge)) {
        // All edges claimed or first edge claimed — fall back to normal execution
        branchEdgeSet.clear();
        hashJoinBranches = List.of();
      }

      for (var edge : sortedEdges) {
        if (branchEdgeSet.contains(edge.edge)) {
          continue; // Skip edges handled by hash join
        }
        addStepsFor(plan, edge, context, prefetchedAliases, first, candidate, profilingEnabled);
        first = false;
      }

      // Append HashJoinMatchSteps for each hash join branch
      for (var branch : hashJoinBranches) {
        var branchPlan = buildHashJoinBranchPlan(branch, context, profilingEnabled);
        if (branchPlan == null) {
          // Build plan construction failed (incomplete path) — re-add the branch's
          // edges back into the main plan by not skipping them. Since we already
          // skipped them above, we need to add them now.
          for (var branchEdge : branch.branchEdges()) {
            addStepsFor(plan, branchEdge, context, prefetchedAliases, first, null,
                profilingEnabled);
            first = false;
          }
        } else {
          plan.chain(new HashJoinMatchStep(
              context, branchPlan, branch.sharedAliases(),
              branch.joinMode(), profilingEnabled));
        }
      }
    } else {
      // No edges → single isolated node. Use prefetched data if available, otherwise
      // build a SELECT execution plan to scan/fetch the node's records.
      var node = pattern.getAliasToNode().values().iterator().next();
      if (prefetchedAliases.contains(node.alias)) {
        plan.chain(new MatchFirstStep(context, node, profilingEnabled));
      } else {
        var clazz = aliasClasses.get(node.alias);
        var pinnedRids = pinnedRidsForAlias(node.alias);
        var filter = fetchFilterFor(node.alias, pinnedRids);
        var select = createSelectStatement(clazz, pinnedRids, filter);
        plan.chain(
            new MatchFirstStep(
                context,
                node,
                select.createExecutionPlan(context, profilingEnabled),
                profilingEnabled));
      }
    }
    return plan;
  }

  /**
   * Computes the **edge schedule** — the order in which pattern edges will be traversed
   * at runtime.
   *
   * <p>The algorithm is a cost-driven, dependency-aware, depth-first graph traversal:
   *
   * <p>1. Compute per-alias dependencies from `$matched` references in `WHERE` clauses.
   * 2. Sort candidate root nodes by their estimated cardinality (ascending) so that the
   *    traversal starts from the smallest set.
   * 3. Repeatedly pick the cheapest unvisited, dependency-free root node and perform a
   *    depth-first expansion, appending each discovered edge to the schedule.
   * 4. Continue until all edges have been scheduled.
   *
   * <p>If the algorithm stalls before scheduling all edges (e.g. due to circular
   * `$matched` dependencies), a {@link CommandExecutionException} is thrown.
   *
   * <p><pre>
   * ┌────────────────────────────────────────────────────────────────┐
   * │ getTopologicalSortedSchedule()                                │
   * │                                                               │
   * │ 1. Compute dependency map: alias → {aliases it depends on}    │
   * │    (from $matched references in WHERE clauses)                │
   * │                                                               │
   * │ 2. Sort candidate roots by estimated cardinality (ascending)  │
   * │    [cheapest first]                                           │
   * │                                                               │
   * │ 3. Main loop (while unscheduled edges remain):                │
   * │    a. Pick cheapest unvisited root with no unmet dependencies │
   * │    b. DFS from that root (updateScheduleStartingAt):          │
   * │       ┌────────────────────────────────────────────┐          │
   * │       │ Mark node visited, clear it from deps      │          │
   * │       │ For each outgoing/bidirectional edge:      │          │
   * │       │   neighbor has unmet deps? → skip          │          │
   * │       │   neighbor visited, edge not? → add edge   │          │
   * │       │   neighbor unvisited? → add edge, recurse  │          │
   * │       └────────────────────────────────────────────┘          │
   * │    c. Repeat until no more expansions possible               │
   * │                                                               │
   * │ 4. If edges remain unscheduled → circular dependency error    │
   * └────────────────────────────────────────────────────────────────┘
   *
   * Worked example:
   *
   *   Query: MATCH {class:A, as:a}.out(){as:b}.out(){as:c}
   *
   *   Estimated roots: {a: 50, b: 10000, c: 500}
   *   Sorted roots:    [a(50), c(500), b(10000)]
   *
   *   Pass 1: start at 'a' (cheapest, no unmet deps)
   *     DFS: visit a
   *            → edge(a→b): b unvisited, deps met → add edge(a→b, fwd), visit b
   *              → edge(b→c): c unvisited, deps met → add edge(b→c, fwd), visit c
   *     Schedule: [edge(a→b, fwd), edge(b→c, fwd)]
   *
   *   All 2 edges scheduled → done.
   * </pre>
   *
   * @param estimatedRootEntries per-alias cardinality estimates
   * @param pattern              the pattern graph to schedule
   * @param session              the database session (used for error reporting)
   * @return an ordered list of {@link EdgeTraversal}s representing the traversal schedule
   * @throws CommandExecutionException if the pattern contains unresolvable circular
   *                                    dependencies
   */
  private List<EdgeTraversal> getTopologicalSortedSchedule(
      Map<String, Long> estimatedRootEntries, Pattern pattern,
      Map<String, String> aliasClasses, Map<String, SQLWhereClause> aliasFilters,
      DatabaseSessionEmbedded session) {
    List<EdgeTraversal> resultingSchedule = new ArrayList<>();
    var remainingDependencies = getDependencies(pattern);
    Set<PatternNode> visitedNodes = new HashSet<>();
    Set<PatternEdge> visitedEdges = new HashSet<>();

    // Read the chain-fold knob once at the top of plan construction. Every
    // candidate edge in the sort loop consults it, so re-reading the volatile
    // configuration field per edge would spread an O(edges) lookup across the
    // planning phase; reading once also stops a mid-plan reconfiguration from
    // splitting one plan across two knob values.
    int chainFoldMaxHops = getChainFoldMaxHops();
    // Per-plan structural-detection cache for the chain fold: the answer is
    // determined by the pattern graph plus the per-plan {@code aliasClasses}
    // and session, all stable for the lifetime of one plan. The visited-edge
    // check is dynamic and is performed at each call site separately, so the
    // cache stores only the structural shape. {@code containsKey} disambiguates
    // a known-not-chain ({@code null} value) from an uncached entry. The key is
    // a composite of (edge, neighbor-direction) because detectChainShape's
    // answer depends on which side of the edge the neighbor is on; keying by
    // edge alone would let one direction's result poison the other.
    Map<ChainShapeKey, ChainedTarget> chainShapeCache = new HashMap<>();
    // Per-plan memo of class-name → approximateCount, shared by the edge-cost
    // estimator and the chain fold so each distinct class's O(1) count read
    // happens once per plan. Pure memo of a deterministic call: values are
    // identical to the un-cached path. Same lifetime/scope as chainShapeCache.
    Map<String, Long> classCountCache = new HashMap<>();

    // Sort the possible root vertices in order of estimated size, since we want to start with a
    // small vertex set.
    List<PairLongObject<String>> rootWeights = new ArrayList<>();
    for (var root : estimatedRootEntries.entrySet()) {
      rootWeights.add(new PairLongObject<>(root.getValue(), root.getKey()));
    }
    Collections.sort(rootWeights);

    // Add the starting vertices, in the correct order, to an ordered set.
    Set<String> remainingStarts = new LinkedHashSet<>();
    for (var item : rootWeights) {
      remainingStarts.add(item.getValue());
    }
    // Add all the remaining aliases after all the suggested start points.
    remainingStarts.addAll(pattern.aliasToNode.keySet());

    while (resultingSchedule.size() < pattern.numOfEdges) {
      // Start a new depth-first pass, adding all nodes with satisfied dependencies.
      // 1. Find a starting vertex for the depth-first pass.
      PatternNode startingNode = null;
      List<String> startsToRemove = new ArrayList<>();
      for (var currentAlias : remainingStarts) {
        var currentNode = pattern.aliasToNode.get(currentAlias);

        if (visitedNodes.contains(currentNode)) {
          // If a previous traversal already visited this alias, remove it from further
          // consideration.
          startsToRemove.add(currentAlias);
        } else if (remainingDependencies.get(currentAlias) == null
            || remainingDependencies.get(currentAlias).isEmpty()) {
          // If it hasn't been visited, and has all dependencies satisfied, visit it.
          startsToRemove.add(currentAlias);
          startingNode = currentNode;
          break;
        }
      }
      startsToRemove.forEach(remainingStarts::remove);

      if (startingNode == null) {
        // We didn't manage to find a valid root, and yet we haven't constructed a complete
        // schedule.
        // This means there must be a cycle in our dependency graph, or all dependency-free nodes
        // are optional.
        // Therefore, the query is invalid.
        throw new CommandExecutionException(session,
            "This query contains MATCH conditions that cannot be evaluated, "
                + "like an undefined alias or a circular dependency on a $matched condition.");
      }

      // 2. Having found a starting vertex, traverse its neighbors depth-first,
      //    adding any non-visited ones with satisfied dependencies to our schedule.
      updateScheduleStartingAt(
          startingNode, visitedNodes, visitedEdges, remainingDependencies,
          resultingSchedule, estimatedRootEntries, aliasClasses, aliasFilters,
          session, chainFoldMaxHops, chainShapeCache, classCountCache);
    }

    if (resultingSchedule.size() != pattern.numOfEdges) {
      throw new AssertionError(
          "Incorrect number of edges: " + resultingSchedule.size() + " vs " + pattern.numOfEdges);
    }

    return resultingSchedule;
  }

  /**
   * Start a depth-first traversal from the starting node, adding all viable unscheduled edges and
   * vertices.
   *
   * @param startNode             the node from which to start the depth-first traversal
   * @param visitedNodes          set of nodes that are already visited (mutated in this function)
   * @param visitedEdges          set of edges that are already visited and therefore don't need to
   *                              be scheduled (mutated in this function)
   * @param remainingDependencies dependency map including only the dependencies that haven't yet
   *                              been satisfied (mutated in this function)
   * @param resultingSchedule     the schedule being computed i.e. appended to (mutated in this
   *                              function)
   */
  private static void updateScheduleStartingAt(
      PatternNode startNode,
      Set<PatternNode> visitedNodes,
      Set<PatternEdge> visitedEdges,
      Map<String, Set<String>> remainingDependencies,
      List<EdgeTraversal> resultingSchedule,
      Map<String, Long> estimatedRootEntries,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      DatabaseSessionEmbedded session,
      int chainFoldMaxHops,
      Map<ChainShapeKey, ChainedTarget> chainShapeCache,
      Map<String, Long> classCountCache) {
    // YouTrackDB requires the schedule to contain all edges present in the query, which is a stronger
    // condition
    // than simply visiting all nodes in the query. Consider the following example query:
    //     MATCH {
    //         class: A,
    //         as: foo
    //     }.in() {
    //         as: bar
    //     }, {
    //         class: B,
    //         as: bar
    //     }.out() {
    //         as: foo
    //     } RETURN $matches
    // The schedule for the above query must have two edges, even though there are only two nodes
    // and they can both
    // be visited with the traversal of a single edge.
    //
    // To satisfy it, we obey the following for each non-optional node:
    // - ignore edges to neighboring nodes which have unsatisfied dependencies;
    // - for visited neighboring nodes, add their edge if it wasn't already present in the schedule,
    // but do not
    //   recurse into the neighboring node;
    // - for unvisited neighboring nodes with satisfied dependencies, add their edge and recurse
    // into them.
    visitedNodes.add(startNode);
    for (var dependencies : remainingDependencies.values()) {
      dependencies.remove(startNode.alias);
    }

    // Build candidate edges with estimated costs, sorted cheapest first.
    List<Map.Entry<PatternEdge, Boolean>> sortedEdges = new ArrayList<>();
    for (var outEdge : startNode.out) {
      sortedEdges.add(Map.entry(outEdge, true));
    }
    for (var inEdge : startNode.in) {
      if (inEdge.item.isBidirectional()) {
        sortedEdges.add(Map.entry(inEdge, false));
      } else if (visitedNodes.contains(inEdge.out)) {
        // Non-bidirectional incoming edge from an already-visited node.
        // This happens when a node with $matched dependencies becomes a start
        // node after its dependencies are resolved by a different branch of the
        // DFS. The edge must still be scheduled (from the visited source toward
        // this node), even though we cannot traverse it in reverse.
        //
        // isOutbound=false: from startNode's perspective, this edge is incoming.
        // In the traversal direction logic below, for non-optional
        // non-bidirectional edges this yields traversalDirection=false (reverse),
        // which causes MatchReverseEdgeTraverser to start at edge.in (startNode)
        // and call executeReverse() to reach edge.out (the visited node). Both
        // endpoints are already visited, so this is a join/verification step.
        // For optional nodes, the direction is flipped to !isOutbound=true
        // (outbound from the visited source toward the optional start node).
        sortedEdges.add(Map.entry(inEdge, false));
      }
    }

    // Sort by estimated edge traversal cost (cheapest first).
    // Already-visited neighbors get cost 0.0 (just a join, no traversal).
    // When estimates are unavailable (MAX_VALUE), preserve original order.
    var sourceAlias = startNode.alias;
    // Use THRESHOLD as fallback for unestimated nodes — consistent with the
    // prefetch threshold used elsewhere in the planner. A moderate value avoids
    // making unknown-cost edges appear artificially cheap or expensive.
    long sourceRows = estimatedRootEntries.getOrDefault(sourceAlias, THRESHOLD);
    // Pre-compute costs to avoid redundant schema lookups during sort comparisons.
    // The cost combines fan-out-based traversal cost with target node selectivity:
    // edges whose target has a selective WHERE clause (low estimated cardinality)
    // are cheaper because they produce fewer intermediate results to join against.
    var edgeCosts = new HashMap<PatternEdge, Double>(sortedEdges.size());
    for (var entry : sortedEdges) {
      var neighbor = entry.getValue() ? entry.getKey().in : entry.getKey().out;
      double cost;
      if (visitedNodes.contains(neighbor)) {
        cost = 0.0;
      } else {
        cost = estimateEdgeCost(
            entry.getKey(), sourceAlias, sourceRows, aliasClasses, session,
            classCountCache);
        if (cost < Double.MAX_VALUE) {
          cost = applyTargetSelectivity(
              cost, neighbor.alias, entry.getKey(), entry.getValue(),
              aliasClasses, aliasFilters, estimatedRootEntries, session);
          // Edge-method chain detection: when the current edge is the first hop of
          // an outE→inV / inE→outV / bothE→bothV sequence (possibly extended into
          // a multi-hop linear chain), fold each downstream vertex's WHERE and
          // each subsequent edge step's fan-out into the first edge's cost. The
          // intermediate edge alias carries no vertex WHERE, so without this
          // fold the branch sorts as "no selectivity" and loses to broader
          // branches. Each fold call short-circuits to baseCost when its alias
          // has no filter/class/row-estimate, so adding more hops does not
          // double-count when those hops have no filter.
          if (chainFoldMaxHops >= 1) {
            cost = applyChainFold(
                cost, entry.getKey(), neighbor, chainFoldMaxHops, visitedEdges,
                aliasClasses, aliasFilters, estimatedRootEntries, session,
                chainShapeCache, classCountCache);
          }
          cost = applyDepthMultiplier(cost, entry.getKey());
        }
      }
      edgeCosts.put(entry.getKey(), cost);
    }
    // TimSort is stable: equal-cost edges (including those with MAX_VALUE when
    // cost cannot be estimated) retain their insertion order, preserving the
    // original out-first-then-bidirectional-in ordering as a tiebreaker.
    sortedEdges.sort(Comparator.comparingDouble(
        entry -> edgeCosts.getOrDefault(entry.getKey(), Double.MAX_VALUE)));

    // Process edges in cost order, retrying any that were skipped due to unsatisfied
    // $matched dependencies. Traversing cheaper edges first may resolve dependencies
    // for more expensive edges (e.g., visiting 'author' before 'knowsCheck' which
    // references $matched.author). We loop until no further progress is made.
    var pending = new ArrayList<>(sortedEdges);
    boolean progress = true;
    while (progress && !pending.isEmpty()) {
      progress = false;
      var deferred = new ArrayList<Map.Entry<PatternEdge, Boolean>>();

      for (var edgeData : pending) {
        var edge = edgeData.getKey();
        boolean isOutbound = edgeData.getValue();
        var neighboringNode = isOutbound ? edge.in : edge.out;

        var deps = remainingDependencies.get(neighboringNode.alias);
        if (!deps.isEmpty()) {
          // Unsatisfied dependencies — defer to a later pass.
          if (logger.isTraceEnabled()) {
            logger.trace("Deferred edge {} to {} due to unsatisfied dependencies: {}",
                edge, neighboringNode.alias, deps);
          }
          deferred.add(edgeData);
          continue;
        }

        if (visitedNodes.contains(neighboringNode)) {
          if (!visitedEdges.contains(edge)) {
            // If we are executing in this block, we are in the following situation:
            // - the startNode has not been visited yet;
            // - it has a neighboringNode that has already been visited;
            // - the edge between the startNode and the neighboringNode has not been
            //   scheduled yet.
            //
            // The isOutbound value shows us whether the edge is outbound from the point
            // of view of the startNode. However, if there are edges to the startNode, we
            // must visit the startNode from an already-visited neighbor, to preserve the
            // validity of the traversal. Therefore, we negate the value of isOutbound to
            // ensure that the edge is always scheduled in the direction from the
            // already-visited neighbor toward the startNode. Notably, this is also the
            // case when evaluating "optional" nodes -- we always visit the optional node
            // from its non-optional and already-visited neighbor.
            //
            // The only exception to the above is when we have edges with "while"
            // conditions. We are not allowed to flip their directionality, so we leave
            // them as-is.
            //
            // Example: bidirectional edge between visited node B and new node A
            //
            //   Pattern:    (A) ──both('Knows')──> (B)
            //                        edge.out=A, edge.in=B
            //
            //   DFS state at this point:
            //     - B is already in visitedNodes (it was reached earlier)
            //     - A is the startNode being processed for the first time
            //     - isOutbound = true (edge goes out from A's perspective)
            //
            //   Without flip:  schedule as EdgeTraversal(edge, fwd)  -> A is source
            //     Problem: A hasn't been produced by any prior step yet, so
            //     MatchStep cannot look up A in the upstream row.
            //
            //   With flip:     schedule as EdgeTraversal(edge, rev)  -> B is source
            //     Correct: B was already matched in a previous step, so
            //     MatchReverseEdgeTraverser starts from B and traverses to A.
            //
            // The flip is applied when:
            //   - startNode is optional (must be reached FROM a visited node), or
            //   - edge is bidirectional (direction is arbitrary; pick the valid one)
            // The flip is NOT applied for WHILE edges because recursive traversal
            // semantics depend on the original syntactic direction.
            boolean traversalDirection;
            if (startNode.optional || edge.item.isBidirectional()) {
              traversalDirection = !isOutbound;
            } else {
              traversalDirection = isOutbound;
            }

            visitedEdges.add(edge);
            resultingSchedule.add(new EdgeTraversal(edge, traversalDirection));
            progress = true;
          }
        } else if (!startNode.optional
            || isOptionalChain(startNode, edge, neighboringNode)) {
          // If the neighboring node wasn't visited, we don't expand the optional node
          // into it, hence the above check. Instead, we'll allow the neighboring node
          // to add the edge we failed to visit, via the above block.
          if (visitedEdges.contains(edge)) {
            // Should never happen.
            throw new AssertionError(
                "The edge was visited, but the neighboring vertex was not: "
                    + edge
                    + " "
                    + neighboringNode);
          }

          visitedEdges.add(edge);
          resultingSchedule.add(new EdgeTraversal(edge, isOutbound));
          updateScheduleStartingAt(
              neighboringNode, visitedNodes, visitedEdges, remainingDependencies,
              resultingSchedule, estimatedRootEntries, aliasClasses, aliasFilters,
              session, chainFoldMaxHops, chainShapeCache, classCountCache);
          progress = true;
        }
      }

      pending = deferred;
    }
  }

  /**
   * Determines whether the given edge connects two nodes that belong to a fully-optional
   * chain — i.e. every node reachable through outgoing edges from the start node is
   * optional. If so, the scheduler is allowed to expand into the neighboring node even
   * though the start node is itself optional.
   */
  private static boolean isOptionalChain(
      PatternNode startNode, PatternEdge edge, PatternNode neighboringNode) {
    return isOptionalChain(startNode, edge, neighboringNode, new HashSet<>());
  }

  private static boolean isOptionalChain(
      PatternNode startNode,
      PatternEdge edge,
      PatternNode neighboringNode,
      Set<PatternEdge> visitedEdges) {
    if (!startNode.isOptionalNode() || !neighboringNode.isOptionalNode()) {
      return false;
    }

    visitedEdges.add(edge);

    if (neighboringNode.out != null) {
      for (var patternEdge : neighboringNode.out) {
        if (!visitedEdges.contains(patternEdge)
            && !isOptionalChain(neighboringNode, patternEdge, patternEdge.in, visitedEdges)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Estimates the cost of traversing {@code edge} from the node whose alias is
   * {@code sourceAlias}, using the edge's fan-out and the source's estimated
   * cardinality.
   *
   * @param edge         the pattern edge to traverse
   * @param sourceAlias  alias of the node we are traversing FROM
   * @param sourceRows   estimated rows for the source alias
   * @param aliasClasses alias → class name mapping
   * @param session      database session for schema access
   * @return estimated cost (lower is better), or {@link Double#MAX_VALUE} if
   *         the cost cannot be estimated
   */
  static double estimateEdgeCost(
      PatternEdge edge,
      String sourceAlias,
      long sourceRows,
      Map<String, String> aliasClasses,
      DatabaseSessionEmbedded session) {
    // Backward-compatible overload for callers with no per-plan cache in
    // scope: a fresh memo yields values identical to the un-cached path.
    return estimateEdgeCost(
        edge, sourceAlias, sourceRows, aliasClasses, session, new HashMap<>());
  }

  /**
   * Cache-aware variant of {@link #estimateEdgeCost(PatternEdge, String, long,
   * Map, DatabaseSessionEmbedded)} that threads a per-plan class-count memo
   * into {@link EdgeFanOutEstimator#estimateFanOut}. The memo is a pure
   * optimisation: estimated costs are identical to the un-cached path.
   */
  static double estimateEdgeCost(
      PatternEdge edge,
      String sourceAlias,
      long sourceRows,
      Map<String, String> aliasClasses,
      DatabaseSessionEmbedded session,
      Map<String, Long> classCountCache) {
    var method = edge.item.getMethod();
    if (method == null) {
      return Double.MAX_VALUE;
    }

    // Extract direction from the method name (out/in/both).
    Direction direction = parseDirection(method.getMethodNameString());
    if (direction == null) {
      return Double.MAX_VALUE;
    }

    // Extract edge class name from the method's first parameter, if present.
    // e.g., .out('Knows') → edgeClassName = "Knows"
    String edgeClassName = extractEdgeClassName(method);

    // Determine OUT/IN vertex classes from the edge schema (if available).
    String outVertexClass = null;
    String inVertexClass = null;
    if (edgeClassName != null) {
      var schema = session.getMetadata().getImmutableSchemaSnapshot();
      if (schema != null) {
        var edgeClass = schema.getClassInternal(edgeClassName);
        if (edgeClass != null) {
          var outProp = edgeClass.getPropertyInternal("out");
          if (outProp != null && outProp.getLinkedClass() != null) {
            outVertexClass = outProp.getLinkedClass().getName();
          }
          var inProp = edgeClass.getPropertyInternal("in");
          if (inProp != null && inProp.getLinkedClass() != null) {
            inVertexClass = inProp.getLinkedClass().getName();
          }
        }
      }
    }

    String sourceClassName = aliasClasses.get(sourceAlias);

    double fanOut = EdgeFanOutEstimator.estimateFanOut(
        session, edgeClassName, sourceClassName, direction,
        outVertexClass, inVertexClass, classCountCache);

    return CostModel.edgeTraversalCost(sourceRows, fanOut);
  }

  /**
   * Estimates the fan-out for a single edge traversal using schema statistics
   * via {@link EdgeFanOutEstimator}. Falls back to
   * {@link EdgeFanOutEstimator#defaultFanOut()} if schema metadata is
   * unavailable.
   *
   * @param method         the edge's traversal method (e.g., out('KNOWS'))
   * @param sourceClassName class of the vertex we traverse FROM, or null
   * @param session        database session for schema access
   * @param classCountCache per-call/per-plan memo of class-name →
   *                        approximateCount, threaded into
   *                        {@link EdgeFanOutEstimator#estimateFanOut}; a pure
   *                        optimisation that does not change the returned value
   * @return estimated fan-out (avg neighbors per source vertex)
   */
  static double estimateMethodFanOut(
      SQLMethodCall method,
      @Nullable String sourceClassName,
      DatabaseSessionEmbedded session,
      Map<String, Long> classCountCache) {
    if (method == null) {
      return EdgeFanOutEstimator.defaultFanOut();
    }
    Direction direction = parseDirection(method.getMethodNameString());
    if (direction == null) {
      return EdgeFanOutEstimator.defaultFanOut();
    }
    String edgeClassName = extractEdgeClassName(method);
    String outVertexClass = null;
    String inVertexClass = null;
    if (edgeClassName != null) {
      var schema = session.getMetadata().getImmutableSchemaSnapshot();
      if (schema != null) {
        var edgeClass = schema.getClassInternal(edgeClassName);
        if (edgeClass != null) {
          var outProp = edgeClass.getPropertyInternal("out");
          if (outProp != null && outProp.getLinkedClass() != null) {
            outVertexClass = outProp.getLinkedClass().getName();
          }
          var inProp = edgeClass.getPropertyInternal("in");
          if (inProp != null && inProp.getLinkedClass() != null) {
            inVertexClass = inProp.getLinkedClass().getName();
          }
        }
      }
    }
    return EdgeFanOutEstimator.estimateFanOut(
        session, edgeClassName, sourceClassName, direction,
        outVertexClass, inVertexClass, classCountCache);
  }

  /**
   * Estimates the selectivity of a WHERE clause for cardinality estimation.
   * Uses the existing {@link TraversalPreFilterHelper#findIndexForFilter} +
   * {@link IndexSearchDescriptor} pipeline to get histogram-based estimates
   * when an index exists. Falls back to
   * {@link SelectivityEstimator#defaultSelectivity()} otherwise.
   *
   * @param where      the WHERE clause to estimate
   * @param className  the class the filter applies to, or null
   * @param ctx        the command context
   * @return selectivity in (0.0, 1.0]
   */
  private static double estimateFilterSelectivity(
      @Nullable SQLWhereClause where,
      @Nullable String className,
      CommandContext ctx) {
    if (where == null || className == null) {
      return SelectivityEstimator.defaultSelectivity();
    }
    // Try to find an index that covers this filter and use its statistics
    var indexDesc = TraversalPreFilterHelper.findIndexForFilter(where, className, ctx);
    if (indexDesc != null) {
      var session = ctx.getDatabaseSession();
      var stats = indexDesc.getIndex().getStatistics(session);
      if (stats != null && stats.totalCount() > 0) {
        var histogram = indexDesc.getIndex().getHistogram(session);
        // Extract the leading condition and estimate selectivity via histogram
        var baseExpr = where.getBaseExpression();
        if (baseExpr instanceof SQLBinaryCondition bc
            && bc.getRight().isEarlyCalculated(ctx)) {
          var value = bc.getRight().execute((Result) null, ctx);
          if (value != null) {
            double sel = SelectivityEstimator.estimateForOperator(
                bc.getOperator(), stats, histogram, value);
            if (sel >= 0) {
              return Math.max(0.001, sel); // clamp to avoid zero
            }
          }
        }
      }
    }
    return SelectivityEstimator.defaultSelectivity();
  }

  /**
   * Adjusts the traversal cost of an edge by the selectivity of the target node's
   * WHERE clause. Uses two complementary strategies:
   *
   * <ol>
   *   <li><b>Filter-shape heuristic</b> — inspects the AST of the target's WHERE clause
   *       to classify it as an equality ({@code name = :value} → selectivity ≈ 1/classCount)
   *       or inequality ({@code name <> :value} → selectivity ≈ (classCount−1)/classCount).
   *       This works regardless of table size and does not require an index.</li>
   *   <li><b>Estimated cardinality ratio</b> — when the heuristic cannot classify the
   *       filter, falls back to the ratio of estimated filtered rows
   *       ({@code estimatedRootEntries}) to total class count.</li>
   * </ol>
   *
   * <p>If the target node has no explicit {@code class:} constraint, the method infers
   * the class from the edge schema's linked vertex property (e.g., {@code HAS_TAG.in}
   * linked to {@code Tag}).
   *
   * <p>This method is used by the sort loop for single-hop edges such as
   * {@code .out('X')}. For the edge-method chain pattern {@code .outE('X').inV()}
   * (and its {@code inE→outV} / {@code bothE→bothV} variants), the sort loop adds
   * a second, chain-aware call using
   * {@link #applyTargetSelectivityWithResolvedClass} below, which bypasses
   * {@link #resolveTargetClass} and applies the downstream vertex's filter on
   * top of the intermediate edge alias's filter — see the call site in
   * {@link #updateScheduleStartingAt}.
   *
   * @param baseCost             the fan-out-based traversal cost from
   *                             {@link #estimateEdgeCost}
   * @param targetAlias          alias of the target (neighbor) node
   * @param edge                 the pattern edge being evaluated
   * @param isOutbound           whether the edge is outbound from the source node
   * @param aliasClasses         alias → class name mapping
   * @param aliasFilters         alias → WHERE clause mapping
   * @param estimatedRootEntries estimated cardinality per alias
   * @param session              database session for schema access
   * @return adjusted cost (lower when the target's WHERE is more selective)
   */
  static double applyTargetSelectivity(
      double baseCost,
      String targetAlias,
      PatternEdge edge,
      boolean isOutbound,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, Long> estimatedRootEntries,
      DatabaseSessionEmbedded session) {
    double factor = targetSelectivityFactor(
        targetAlias, edge, isOutbound,
        aliasClasses, aliasFilters, estimatedRootEntries, null, session);
    return baseCost * factor;
  }

  /**
   * Pure-scalar variant of {@link #applyTargetSelectivity}: returns the
   * selectivity multiplier without applying it to a cost. Returns {@code 1.0}
   * (no narrowing) when target class cannot be resolved, schema is missing,
   * {@code approximateCount} returns ≤ 0, no usable filter heuristic exists,
   * and no per-alias estimate is registered.
   *
   * <p>Used by the plan-time row-estimate propagation walk that stamps
   * {@code forecastN} on each {@link EdgeTraversal}.
   */
  static double targetSelectivityFactor(
      String targetAlias,
      PatternEdge edge,
      boolean isOutbound,
      Map<String, String> aliasClasses,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, Long> estimatedRootEntries,
      @Nullable Map<String, Long> classCountCache,
      DatabaseSessionEmbedded session) {
    var targetClass = resolveTargetClass(
        targetAlias, edge, isOutbound, aliasClasses, session);
    if (targetClass == null) {
      return 1.0;
    }
    return applyClassSelectivity(
        targetAlias, targetClass,
        aliasFilters, estimatedRootEntries, classCountCache, session);
  }

  /**
   * Class-forced sibling of {@link #applyTargetSelectivity} used by the
   * sort-loop's edge-method chain fold. Bypasses {@link #resolveTargetClass}
   * — the caller ({@link #updateScheduleStartingAt} via
   * {@link #resolveChainedTarget}) has already computed the downstream
   * vertex class using chain-aware precedence (aliasClasses first, then
   * direction-aware edge-schema derivation), so re-inferring with the outer
   * edge's direction would pick the wrong endpoint for {@code inE→outV}.
   *
   * <p>Short-circuits to {@code baseCost} when {@code preResolvedTargetClass}
   * is {@code null} (e.g. {@code bothE→bothV} without an explicit
   * {@code class:} annotation) — matching the behaviour of
   * {@link #applyTargetSelectivity} when {@link #resolveTargetClass} returns
   * {@code null}.
   *
   * @param baseCost             the fan-out-based traversal cost from
   *                             {@link #estimateEdgeCost}, already adjusted
   *                             by the intermediate alias's filter (if any)
   *                             via the preceding {@link #applyTargetSelectivity}
   *                             call
   * @param targetAlias          the downstream vertex alias (from
   *                             {@link ChainedTarget#effectiveTargetAlias})
   * @param preResolvedTargetClass class name resolved by the chain helper,
   *                             or {@code null} when inference failed
   * @param aliasFilters         alias → WHERE clause mapping
   * @param estimatedRootEntries estimated cardinality per alias
   * @param session              database session for schema access
   * @return adjusted cost
   */
  static double applyTargetSelectivityWithResolvedClass(
      double baseCost,
      String targetAlias,
      @Nullable String preResolvedTargetClass,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, Long> estimatedRootEntries,
      DatabaseSessionEmbedded session) {
    // Backward-compatible overload with no class-count memo.
    return applyTargetSelectivityWithResolvedClass(
        baseCost, targetAlias, preResolvedTargetClass,
        aliasFilters, estimatedRootEntries, null, session);
  }

  /**
   * Cache-aware variant of
   * {@link #applyTargetSelectivityWithResolvedClass(double, String, String,
   * Map, Map, DatabaseSessionEmbedded)} that threads a per-plan class-count
   * memo into {@link #applyClassSelectivity}. The memo is a pure optimisation
   * (deterministic {@code approximateCount} keyed by class name); returned
   * costs are identical to the un-cached path.
   */
  static double applyTargetSelectivityWithResolvedClass(
      double baseCost,
      String targetAlias,
      @Nullable String preResolvedTargetClass,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, Long> estimatedRootEntries,
      @Nullable Map<String, Long> classCountCache,
      DatabaseSessionEmbedded session) {
    if (preResolvedTargetClass == null) {
      return baseCost;
    }
    return baseCost * applyClassSelectivity(
        targetAlias, preResolvedTargetClass,
        aliasFilters, estimatedRootEntries, classCountCache, session);
  }

  /**
   * Shared factor body of {@link #targetSelectivityFactor} and
   * {@link #applyTargetSelectivityWithResolvedClass}: given a non-null
   * target class, look it up in the schema and return the selectivity
   * multiplier from either (a) the filter-shape heuristic on the target's
   * WHERE clause, or (b) the estimated cardinality ratio. Returns {@code 1.0}
   * (no narrowing) when the schema or class is missing, {@code approximateCount}
   * returns ≤ 0, no usable filter heuristic exists, and no per-alias estimate is
   * registered. Callers scale their own {@code baseCost} by the returned factor.
   * All null-guards on the target class are the callers' responsibility; this
   * helper assumes {@code targetClass != null}.
   *
   * <p>When {@code classCountCache} is non-null the class row count is memoized
   * across calls within a single forecast pass.
   */
  private static double applyClassSelectivity(
      String targetAlias,
      String targetClass,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, Long> estimatedRootEntries,
      @Nullable Map<String, Long> classCountCache,
      DatabaseSessionEmbedded session) {
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    if (schema == null) {
      return 1.0;
    }
    var schemaClass = schema.getClassInternal(targetClass);
    if (schemaClass == null) {
      return 1.0;
    }

    // targetClass is safe as a cache key: the lookup above returned non-null,
    // and the schema keys its class map by the exact declared name, so this
    // string is the canonical one. Every writer into the map resolves its key
    // the same way, which is what lets one plan share a single count per class.
    long classCount = classCountCache != null
        ? classCountCache.computeIfAbsent(
            targetClass, k -> schemaClass.approximateCount(session))
        : schemaClass.approximateCount(session);
    if (classCount <= 0) {
      return 1.0;
    }

    var filter = aliasFilters != null ? aliasFilters.get(targetAlias) : null;
    if (filter != null) {
      double heuristic = estimateFilterSelectivity(
          filter, classCount, schemaClass, session);
      if (heuristic >= 0.0) {
        return heuristic;
      }
    }

    var targetEstimate = estimatedRootEntries.get(targetAlias);
    if (targetEstimate == null) {
      return 1.0;
    }
    return (double) targetEstimate / classCount;
  }

  /**
   * Single-pass schedule walk that stamps two independent pieces of plan-time
   * metadata on each {@link EdgeTraversal}:
   *
   * <ol>
   *   <li><b>Collection-ID class filter</b> (always) — resolves the target
   *       node's class constraint to collection IDs. The traverser applies
   *       this as a zero-I/O class filter on the link bag, skipping vertices
   *       whose collection ID does not match.</li>
   *   <li><b>Row-count forecast</b> (only when {@code stampForecasts} is
   *       {@code true} — i.e. at least one edge carries a
   *       {@link RidFilterDescriptor.IndexLookup} descriptor). Stamps
   *       {@code forecastN} and {@code rootSourceRows} on each edge,
   *       feeding {@code EdgeTraversal.resolveWithCache}'s {@code BUILD_EAGER}
   *       vs {@code DEFERRED_WITH_NET} amortization decision. When no edge
   *       has an IndexLookup the forecast would be unread, so the
   *       per-edge cost (a {@link #targetSelectivityFactor} call and the
   *       propagation bookkeeping) is skipped.</li>
   * </ol>
   *
   * <p>Replaces an earlier two-method pair that walked the schedule
   * independently; the merged loop keeps the same conditional gating on the
   * forecast block but removes the second pass entirely.
   *
   * <p>Forecast-pass state ({@code classCountCache}, {@code aliasRowEstimate},
   * {@code aliasRootSampleSize}) is allocated lazily inside the method only
   * when {@code stampForecasts} is {@code true}. The {@code Long.MAX_VALUE}
   * input strip is a scheduler-priority hack: inferred-class aliases get
   * MAX_VALUE in {@code estimatedRootEntries} as a sentinel that must not
   * propagate into the forecast (it would cascade-multiply to MAX_VALUE and
   * force BUILD_EAGER regardless of the real cost-model verdict).
   */
  private void stampEdgeMetadata(
      List<EdgeTraversal> schedule,
      Map<String, Long> estimatedRootEntries,
      boolean stampForecasts,
      CommandContext ctx) {
    var session = ctx.getDatabaseSession();
    if (session == null) {
      return;
    }
    var schema = session.getMetadata().getImmutableSchemaSnapshot();

    // Forecast-pass state — materialized only when at least one edge has an
    // IndexLookup descriptor; otherwise none of these maps are read.
    Map<String, Long> classCountCache = null;
    Map<String, Long> aliasRowEstimate = null;
    Map<String, Long> aliasRootSampleSize = null;
    if (stampForecasts) {
      classCountCache = new HashMap<>();
      aliasRowEstimate = new HashMap<>(estimatedRootEntries.size());
      for (var entry : estimatedRootEntries.entrySet()) {
        if (entry.getValue() != null && entry.getValue() != Long.MAX_VALUE) {
          aliasRowEstimate.put(entry.getKey(), entry.getValue());
        }
      }
      // The CLT-relevant n is the original root, not the propagated expected
      // count along the way — see EdgeTraversal#rootSourceRows.
      aliasRootSampleSize = new HashMap<>(aliasRowEstimate);
    }

    for (var et : schedule) {
      var sourceAlias = et.out ? et.edge.out.alias : et.edge.in.alias;
      var targetAlias = et.out ? et.edge.in.alias : et.edge.out.alias;

      // --- Collection-ID class filter (always) ---
      if (targetAlias != null && schema != null) {
        var className = aliasClasses.get(targetAlias);
        if (className != null) {
          var schemaClass = schema.getClassInternal(className);
          if (schemaClass != null) {
            et.setAcceptedCollectionIds(
                TraversalPreFilterHelper.collectionIdsForClass(schemaClass));
          }
        }
      }

      // --- Row-count forecast (only when any edge has an IndexLookup) ---
      if (!stampForecasts) {
        continue;
      }
      stampForecastFor(et, sourceAlias, targetAlias,
          aliasRowEstimate, aliasRootSampleSize, classCountCache, session);
    }
  }

  /**
   * Per-edge forecast stamping. Computes {@code forecastN = sourceRows ×
   * estimateMethodFanOut} and propagates the row estimate to the target
   * alias for downstream edges. Non-positive or non-finite intermediate
   * values short-circuit to an absent forecast ({@code -1}); saturated
   * forecasts (clamped to {@code Long.MAX_VALUE}) are also treated as
   * absent — the defense mirrors the {@code MAX_VALUE} input strip at
   * {@link #stampEdgeMetadata}'s entry boundary.
   */
  private void stampForecastFor(
      EdgeTraversal et,
      @Nullable String sourceAlias,
      @Nullable String targetAlias,
      Map<String, Long> aliasRowEstimate,
      Map<String, Long> aliasRootSampleSize,
      Map<String, Long> classCountCache,
      DatabaseSessionEmbedded session) {
    if (sourceAlias == null || targetAlias == null) {
      et.setForecastN(-1L);
      et.setRootSourceRows(-1L);
      return;
    }

    Long sourceRows = aliasRowEstimate.get(sourceAlias);
    Long sourceRootSize = aliasRootSampleSize.get(sourceAlias);
    // Stamp the root-lineage sample size on every scheduled edge,
    // independent of whether the forecast itself ends up usable.
    et.setRootSourceRows(sourceRootSize == null ? -1L : sourceRootSize);

    if (sourceRows == null || sourceRows <= 0) {
      et.setForecastN(-1L);
      return;
    }

    var method = et.edge.item.getMethod();
    String sourceClass = aliasClasses.get(sourceAlias);
    double fanOut = estimateMethodFanOut(method, sourceClass, session,
        classCountCache);
    if (!(fanOut > 0) || !Double.isFinite(fanOut)) {
      et.setForecastN(-1L);
      return;
    }

    double forecastDouble = sourceRows * fanOut;
    if (!Double.isFinite(forecastDouble) || forecastDouble <= 0) {
      et.setForecastN(-1L);
      return;
    }
    // Saturated forecast (clamp to Long.MAX_VALUE) is treated as absent.
    // Otherwise the cost-model break-even is trivially satisfied by a
    // saturated value, which re-introduces the inflation-driven
    // BUILD_EAGER bug that the MAX_VALUE input strip closes at the
    // entry boundary. Saturation here is rare in practice (it requires
    // sourceRows × fanOut > 9.2e18 in double arithmetic) but the
    // defensive {@code -1} mirrors the input strip's intent.
    long forecastNLong = forecastDouble >= (double) Long.MAX_VALUE
        ? -1L : (long) Math.ceil(forecastDouble);
    et.setForecastN(forecastNLong);

    // Propagate to the target alias for downstream edges. Skip propagation
    // when the existing entry is already at least as constraining as our
    // forecast — fresh estimates from {@code estimateRootEntries} should
    // not be overwritten by a looser-derived value. Inflation of the
    // propagated row count is bounded by the {@code Long.MAX_VALUE}
    // saturation guard above and by the CLT gate downstream
    // ({@link EdgeTraversal#MIN_FOR_CLT}) which routes low-confidence
    // forecasts to {@code DEFERRED_WITH_NET}.
    double targetSel = targetSelectivityFactor(
        targetAlias, et.edge, et.out,
        aliasClasses, aliasFilters, aliasRowEstimate, classCountCache, session);
    double targetDouble = forecastDouble * targetSel;
    if (Double.isFinite(targetDouble) && targetDouble > 0) {
      long targetRows = (long) Math.min(
          (double) Long.MAX_VALUE, Math.ceil(targetDouble));
      Long existing = aliasRowEstimate.get(targetAlias);
      if (existing == null || targetRows < existing) {
        aliasRowEstimate.put(targetAlias, targetRows);
        // Propagate the source's root sample size. If the target already
        // had a fresher root sample size from {@code estimatedRootEntries}
        // (e.g. it's itself a class-rooted alias), keep that — fresh
        // class-based statistics outrank inherited lineage from a tiny
        // upstream sample.
        Long existingRoot = aliasRootSampleSize.get(targetAlias);
        if (existingRoot == null && sourceRootSize != null) {
          aliasRootSampleSize.put(targetAlias, sourceRootSize);
        }
      }
    }
  }

  /**
   * Resolves the target vertex class for an edge traversal. First checks
   * {@code aliasClasses} for an explicit constraint; if absent, infers the class
   * from the edge schema's linked vertex property.
   *
   * <p>For {@code .out('HAS_TAG')} the target is the "in" vertex of the edge class,
   * so we read the linked class of the {@code in} property, and vice versa.
   */
  @Nullable private static String resolveTargetClass(
      String targetAlias,
      PatternEdge edge,
      boolean isOutbound,
      Map<String, String> aliasClasses,
      DatabaseSessionEmbedded session) {
    var explicit = aliasClasses.get(targetAlias);
    if (explicit != null) {
      return explicit;
    }
    // Infer target class from edge schema's linked vertex property.
    // Chain of null-safe lookups: method → edgeClassName → schema → edgeClass → linkedProp.
    var method = edge.item.getMethod();
    var edgeClassName = method != null ? extractEdgeClassName(method) : null;
    var schema = edgeClassName != null
        ? session.getMetadata().getImmutableSchemaSnapshot() : null;
    var edgeClass = schema != null ? schema.getClassInternal(edgeClassName) : null;
    if (edgeClass == null) {
      return null;
    }
    var linkedPropName = isOutbound ? "in" : "out";
    var linkedProp = edgeClass.getPropertyInternal(linkedPropName);
    return (linkedProp != null && linkedProp.getLinkedClass() != null)
        ? linkedProp.getLinkedClass().getName() : null;
  }

  /**
   * Describes the downstream vertex that a recognised edge-method chain
   * (e.g. {@code .outE('X').inV()}) folds onto for cost-model purposes.
   *
   * <p>Returned by {@link #resolveChainedTarget}. The planner's sort loop
   * uses this to apply the downstream vertex's {@code WHERE} selectivity to
   * the first edge's cost, instead of the synthetic intermediate edge alias
   * that carries no filter.
   *
   * @param effectiveTargetAlias  the downstream vertex alias (the real target
   *                              of the chain, e.g. {@code tag} in
   *                              {@code .outE('VIHasTag').inV(){as: tag}})
   * @param effectiveTargetClass  the downstream vertex class name, or
   *                              {@code null} when it cannot be inferred
   *                              (e.g. {@code bothE→bothV} without an
   *                              explicit {@code class:} annotation)
   */
  record ChainedTarget(
      String effectiveTargetAlias, @Nullable String effectiveTargetClass) {
  }

  /**
   * Composite key for {@code chainShapeCache}. {@link #detectChainShape}'s
   * answer depends not only on the first {@link PatternEdge} but also on which
   * side of it the neighbor sits (forward: {@code neighbor == edge.in};
   * reverse: {@code neighbor == edge.out}) — reverse traversals are rejected by
   * the structural rule while forward ones may match. Keying by edge alone
   * would let a cached reverse {@code null} poison the forward lookup (and vice
   * versa); the {@code neighborIsIn} flag keeps the two directions distinct,
   * and a {@code null} result stays cached per (edge, direction).
   *
   * <p>Package-visible so a test can build a shared cache and assert that the
   * two directions of one edge occupy separate entries. Keyed on
   * {@link PatternEdge} identity: the class declares no
   * {@code equals}/{@code hashCode}, and two distinct edges of the same shape
   * must not share a cache slot.
   */
  record ChainShapeKey(PatternEdge edge, boolean neighborIsIn) {
  }

  /**
   * Detects the edge-method chain pattern {@code .outE(X).inV()} (and its
   * {@code inE→outV} / {@code bothE→bothV} variants) that appears as two
   * consecutive {@link PatternEdge}s in the pattern graph.
   *
   * <p>The pattern graph models {@code .outE('X').inV()} as
   * <pre>
   *   source ── outE('X') ──▶ intermediate(edge alias) ── inV() ──▶ target
   * </pre>
   * Cost is computed per {@link PatternEdge}. The first edge's target is the
   * synthetic intermediate alias (no filter, no class), so
   * {@link #applyTargetSelectivity} returns the base cost unchanged and the
   * {@code WHERE} on the real downstream vertex never affects scheduling.
   *
   * <p>This helper recognises the chain structurally and returns the
   * downstream vertex so the caller can fold its selectivity into the first
   * edge's cost. The pattern graph and runtime execution are left untouched.
   *
   * <p><b>Structural rule</b> (all must hold):
   * <ol>
   *   <li>{@code edge.item} and {@code edge.item.getMethod()} are non-null;</li>
   *   <li>first edge method name (lower-cased, {@link Locale#ENGLISH}) is
   *       one of {@code oute}, {@code ine}, {@code bothe};</li>
   *   <li>{@code neighbor.out} has exactly one edge, and it is not in
   *       {@code visitedEdges};</li>
   *   <li>{@code neighbor.in} has exactly one edge, and it <b>is</b>
   *       {@code edge} (identity comparison — guards against a user-named
   *       intermediate alias joined from a second MATCH fragment);</li>
   *   <li>the downstream edge's method name (lower-cased) is {@code inv},
   *       {@code outv}, or {@code bothv}.</li>
   * </ol>
   *
   * <p><b>Reverse traversals</b> (where {@code neighbor = edge.out}) are
   * rejected naturally by clause 3 — the reverse neighbor has no
   * {@code inV}/{@code outV}/{@code bothV} continuation.
   *
   * <p>The helper is pure — no schema mutation, no side effects — so it is
   * unit-testable in isolation.
   *
   * @param edge           the candidate first edge of the chain
   * @param neighbor       the direction-dependent target already computed
   *                       by the sort loop
   *                       ({@code entry.getValue() ? edge.in : edge.out})
   * @param visitedEdges   DFS state — the chain is only detected while the
   *                       intermediate edge's follow-up is still unscheduled
   * @param aliasClasses   alias → class map; may be {@code null}, in which
   *                       case the helper falls directly to the
   *                       edge-schema-derivation fallback
   * @param session        database session for schema access; may be
   *                       {@code null}, in which case the fallback returns
   *                       {@code null} for the class field
   * @return the downstream vertex descriptor when the chain signature
   *         matches, otherwise {@code null}
   * @apiNote Package-visible test seam. The full structural rule — including
   *          the clause-4 intermediate-edge identity guard — covers pattern
   *          shapes the runtime DFS cannot construct today; this seam lets
   *          unit tests exercise those guards directly. Production folds go
   *          through {@link #applyChainFold} / {@link #lookupOrDetectChainShape}.
   */
  @Nullable static ChainedTarget resolveChainedTarget(
      PatternEdge edge,
      PatternNode neighbor,
      Set<PatternEdge> visitedEdges,
      @Nullable Map<String, String> aliasClasses,
      @Nullable DatabaseSessionEmbedded session) {
    // Route through the cached path (with a throwaway per-call cache) so the
    // test seam exercises the exact structural detection the sort loop uses,
    // then apply the shared dynamic visited-edge rule.
    var shape = lookupOrDetectChainShape(
        edge, neighbor, aliasClasses, session, new HashMap<>());
    if (shape == null || chainVertexStepVisited(neighbor, visitedEdges)) {
      return null;
    }
    return shape;
  }

  /**
   * Shared visited-edge rule for the edge-method chain fold: the chain is only
   * foldable while its vertex-step edge (the single edge out of {@code neighbor},
   * guaranteed to exist by {@link #detectChainShape}'s
   * {@code neighbor.out.size() == 1} check) has not yet been scheduled. Folding
   * an already-visited vertex-step edge would double-count its downstream
   * selectivity through the cached structural answer.
   *
   * <p>Single source of truth for the dynamic guard shared by the cached
   * sort-loop path ({@link #applyChainFold}) and the package-visible test seam
   * ({@link #resolveChainedTarget}).
   */
  private static boolean chainVertexStepVisited(
      PatternNode neighbor, Set<PatternEdge> visitedEdges) {
    return visitedEdges.contains(neighbor.out.iterator().next());
  }

  /**
   * Pure structural part of the chain-detection rule: applies all clauses
   * of {@link #resolveChainedTarget} except the dynamic visited-edge check.
   * The result depends only on the pattern graph and the per-plan
   * {@code aliasClasses} / {@code session}, all stable for the lifetime of
   * one plan, which makes it safe to cache by {@link PatternEdge} identity
   * (see the {@code chainShapeCache} threaded through
   * {@link #updateScheduleStartingAt}).
   *
   * <p>Callers that need the visited check (the sort loop, the multi-hop
   * walk) apply it on top of this helper's result.
   */
  @Nullable private static ChainedTarget detectChainShape(
      PatternEdge edge,
      PatternNode neighbor,
      @Nullable Map<String, String> aliasClasses,
      @Nullable DatabaseSessionEmbedded session) {
    if (edge.item == null || edge.item.getMethod() == null) {
      return null;
    }
    var rawFirstName = edge.item.getMethod().getMethodNameString();
    // Sort-loop hot path: invoked on every candidate edge, including the very
    // common single-step methods (.out / .in / .both). Reject those without
    // allocating a lower-cased copy — String.equalsIgnoreCase short-circuits
    // on length mismatch, so .out (len 3) vs .outE (len 4) bails out before
    // any character comparison. Only on a positive match do we cache the
    // lower-cased canonical form for `linkedVertexClassForVertexStep`.
    if (rawFirstName == null
        || (!"outE".equalsIgnoreCase(rawFirstName)
            && !"inE".equalsIgnoreCase(rawFirstName)
            && !"bothE".equalsIgnoreCase(rawFirstName))) {
      return null;
    }

    if (neighbor.out.size() != 1 || neighbor.in.size() != 1) {
      return null;
    }
    var downstreamEdge = neighbor.out.iterator().next();
    // Defense-in-depth identity check. The size==1 guard above already
    // rejects the common fragment-join case (a user reusing {as: e} across
    // two MATCH fragments makes neighbor.in.size() >= 2). This check pins
    // the residual case — a pattern graph whose intermediate has a single
    // incoming edge that is not `edge` — which the DFS does not produce
    // today but may via future refactorings of pattern construction.
    if (neighbor.in.iterator().next() != edge) {
      return null;
    }

    if (downstreamEdge.item == null || downstreamEdge.item.getMethod() == null) {
      return null;
    }
    var rawSecondName = downstreamEdge.item.getMethod().getMethodNameString();
    if (rawSecondName == null
        || (!"inV".equalsIgnoreCase(rawSecondName)
            && !"outV".equalsIgnoreCase(rawSecondName)
            && !"bothV".equalsIgnoreCase(rawSecondName))) {
      return null;
    }

    // effectiveTargetAlias is the downstream vertex alias — NOT neighbor.alias,
    // which is the intermediate edge alias and carries no filter.
    var effectiveTargetAlias = downstreamEdge.in.alias;

    // Class-inference precedence:
    //   1. aliasClasses.get(effectiveTargetAlias) — the normal path for
    //      outE→inV / inE→outV because {@code addAliases} pre-populates via
    //      {@code inferClassFromEdgeSchema}. Also the only path for
    //      bothE→bothV when the user wrote {class: ...}.
    //   2. Defensive fallback for while-expression aliases skipped by
    //      {@code addAliases}'s whileAliases filter: derive from the first
    //      edge's class name + direction. outE→inV uses the edge class's
    //      {@code in} linked vertex class; inE→outV uses {@code out};
    //      bothE→bothV cannot infer (returns null).
    String effectiveTargetClass = aliasClasses != null
        ? aliasClasses.get(effectiveTargetAlias) : null;
    if (effectiveTargetClass == null) {
      // Precedence-2 fallback: derive class from the edge schema using the
      // SECOND method (inV/outV/bothV). The vertex step decides which side of
      // the edge is downstream, regardless of whether we entered via outE,
      // inE, or bothE — so this mirrors how addAliases.inferClassFromEdgeSchema
      // resolves a stand-alone inV/outV. bothV cannot be disambiguated from
      // schema alone and falls through to null.
      effectiveTargetClass = linkedVertexClassForVertexStep(
          rawSecondName, extractEdgeClassName(edge.item.getMethod()), session);
    }
    return new ChainedTarget(effectiveTargetAlias, effectiveTargetClass);
  }

  /**
   * Resolves the linked vertex class for a TinkerPop vertex step ({@code inV},
   * {@code outV}, {@code bothV}) on a known edge class, using the edge's
   * {@code in}/{@code out} LINK schema.
   *
   * <p>{@code inV} reads the edge's {@code in} linked vertex class (target
   * side); {@code outV} reads {@code out} (source side); {@code bothV} cannot
   * be resolved from schema alone and returns {@code null}.
   *
   * <p>Shared by {@link #resolveChainedTarget} (precedence-2 fallback during
   * cost-fold class inference) and the {@code inV}/{@code outV} branch of
   * {@link #inferClassFromEdgeSchema} (during plan construction in
   * {@code addAliases}). Centralising the mapping here keeps the two paths in
   * lock-step: any future addition (e.g. a new vertex-step variant) only
   * needs editing once.
   *
   * @param vertexStepName the vertex step name as read from the parsed
   *                       method call ({@code inV}/{@code outV}/{@code bothV},
   *                       case-insensitive); {@code null} returns {@code null}
   * @param edgeClassName  the edge class whose schema supplies the linked
   *                       vertex class; {@code null} returns {@code null}
   * @param session        database session for schema access; {@code null}
   *                       returns {@code null}
   * @return the linked vertex class name, or {@code null} when any input is
   *         missing or the step is {@code bothV}/unrecognized
   */
  @Nullable static String linkedVertexClassForVertexStep(
      @Nullable String vertexStepName,
      @Nullable String edgeClassName,
      @Nullable DatabaseSessionEmbedded session) {
    if (edgeClassName == null || vertexStepName == null) {
      return null;
    }
    String prop;
    if ("inV".equalsIgnoreCase(vertexStepName)) {
      prop = "in";
    } else if ("outV".equalsIgnoreCase(vertexStepName)) {
      prop = "out";
    } else {
      // bothV or any unrecognized step: cannot disambiguate from schema
      return null;
    }
    return lookupLinkedVertexClass(edgeClassName, prop, session);
  }

  /**
   * Describes a single sub-chain in a multi-hop linear chain extension. A
   * sub-chain is one (edge step → vertex step) pair, e.g. {@code outE('B')
   * → inV()} appearing AFTER an initial {@code outE('A').inV()} that started
   * the chain. The first sub-chain is handled by the existing single-hop
   * fold and is not represented here — only the additional hops are.
   *
   * @param edgeStep         the {@code outE}/{@code inE}/{@code bothE} step
   *                         that starts this sub-chain. Its fan-out from
   *                         {@code edgeStepSourceClass} is multiplied into
   *                         the running cost (the first edge step is part of
   *                         {@code estimateEdgeCost}'s baseCost; subsequent
   *                         steps are not, so we add them here)
   * @param edgeStepSourceClass class of the vertex feeding {@code edgeStep},
   *                         used as the cardinality denominator for fan-out
   *                         estimation. May be {@code null} if class
   *                         inference failed for the previous hop's
   *                         downstream vertex
   * @param intermediateAlias the alias of the intermediate edge alias node
   *                         (e.g. user's {@code as: e2}). Selectivity is
   *                         applied to it the same way the first hop does
   *                         in the sort loop's primary
   *                         {@code applyTargetSelectivity} call
   * @param downstreamAlias  the alias of the vertex reached by this hop's
   *                         vertex step. Its WHERE drives the per-hop
   *                         selectivity contribution
   * @param downstreamClass  class of the downstream vertex, or {@code null}
   *                         when it cannot be inferred
   */
  record ChainHop(
      PatternEdge edgeStep,
      @Nullable String edgeStepSourceClass,
      String intermediateAlias,
      String downstreamAlias,
      @Nullable String downstreamClass) {
  }

  /**
   * Applies the multi-hop chain fold to {@code baseCost} starting from
   * {@code firstEdge → firstNeighbor}. This is the unified entry point used
   * by the sort loop: it composes {@link #resolveChainedTarget} (the
   * single-hop rule) with {@link #walkLinearChainExtension} (the iterative
   * extension into subsequent linear sub-chains, bounded by
   * {@code maxHops}).
   *
   * <p>For the first hop only the downstream vertex's WHERE selectivity is
   * applied — the first edge's fan-out is already in {@code baseCost} and
   * the immediate intermediate alias was folded by the sort loop's primary
   * {@code applyTargetSelectivity} call before this method runs.
   *
   * <p>For each additional hop (hop 2…N): multiply by the edge step's
   * fan-out, apply the intermediate edge alias's WHERE (rare, only when the
   * user named it with {@code as:} and gave it a filter), then apply the
   * downstream vertex's WHERE.
   *
   * <p>{@code maxHops} caps the total number of (edge, vertex) sub-chains
   * folded. Two layers gate it. The sort-loop call site skips
   * {@code applyChainFold} entirely when {@code chainFoldMaxHops < 1},
   * reproducing the schedule the planner produced before the fold existed
   * and allocating nothing. The {@code maxHops <= 1} short-circuit below
   * returns after the first hop's fold without entering the multi-hop walk.
   * At {@code maxHops == 1} either layer alone confines the fold to one hop;
   * only the outer layer can switch it off completely. Both are deliberate
   * defense-in-depth, so a caller may rely on either.
   *
   * <p>{@code chainShapeCache} memoizes the structural part of the chain
   * detection per (first-edge, neighbor-direction) key, so repeat sort-loop
   * invocations of the same edge skip the lower-case method-name comparisons
   * and the schema lookups. The dynamic visited-edge check is performed inline
   * via {@link #chainVertexStepVisited}. {@code classCountCache} is the shared
   * per-plan memo of class-name → approximateCount; threading it here keeps the
   * fold's fan-out and selectivity lookups in lock-step with the sort loop's
   * edge-cost estimator so each distinct class is counted once per plan.
   *
   * @return the cost adjusted for all detected chain hops up to
   *         {@code maxHops}; equals {@code baseCost} when no chain is
   *         detected
   */
  private static double applyChainFold(
      double baseCost,
      PatternEdge firstEdge,
      PatternNode firstNeighbor,
      int maxHops,
      Set<PatternEdge> visitedEdges,
      @Nullable Map<String, String> aliasClasses,
      @Nullable Map<String, SQLWhereClause> aliasFilters,
      @Nullable Map<String, Long> estimatedRootEntries,
      DatabaseSessionEmbedded session,
      Map<ChainShapeKey, ChainedTarget> chainShapeCache,
      Map<String, Long> classCountCache) {
    var firstHop = lookupOrDetectChainShape(
        firstEdge, firstNeighbor, aliasClasses, session, chainShapeCache);
    // Shared dynamic visited-edge rule (see chainVertexStepVisited): a chain
    // whose vertex-step edge is already scheduled would double-count its
    // selectivity through the cached structural answer.
    if (firstHop == null || chainVertexStepVisited(firstNeighbor, visitedEdges)) {
      return baseCost;
    }
    // detectChainShape verified firstNeighbor.out.size() == 1, so this
    // iterator yields the chain's vertex-step edge — used as the source of
    // the first hop's downstream vertex when seeding the multi-hop walk.
    var firstHopVertexStepEdge = firstNeighbor.out.iterator().next();
    double cost = applyTargetSelectivityWithResolvedClass(
        baseCost,
        firstHop.effectiveTargetAlias(),
        firstHop.effectiveTargetClass(),
        aliasFilters,
        estimatedRootEntries,
        classCountCache,
        session);

    if (maxHops <= 1) {
      return cost;
    }

    // Walk forward from the first hop's downstream vertex into any
    // additional linear sub-chains, capped at maxHops - 1 extra hops.
    var extraHops = walkLinearChainExtension(
        firstHopVertexStepEdge.in,
        firstHop.effectiveTargetClass(),
        maxHops - 1,
        visitedEdges,
        firstEdge,
        firstHopVertexStepEdge,
        aliasClasses,
        session,
        chainShapeCache);
    for (var hop : extraHops) {
      cost *= estimateMethodFanOut(
          hop.edgeStep().item.getMethod(),
          hop.edgeStepSourceClass(),
          session,
          classCountCache);
      // Intermediate edge alias is mostly auto-generated (no WHERE), so this
      // call is typically a no-op via the no-filter short-circuit. The class
      // is the EDGE class of this hop's edge step (e.g. 'Friend' for
      // .outE('Friend'){as: e}); without it, the helper would short-circuit
      // unconditionally and ignore user-named intermediate aliases that
      // carry their own WHERE (e.g. {as: e, where: weight > 5}).
      cost = applyTargetSelectivityWithResolvedClass(
          cost,
          hop.intermediateAlias(),
          extractEdgeClassName(hop.edgeStep().item.getMethod()),
          aliasFilters,
          estimatedRootEntries,
          classCountCache,
          session);
      cost = applyTargetSelectivityWithResolvedClass(
          cost,
          hop.downstreamAlias(),
          hop.downstreamClass(),
          aliasFilters,
          estimatedRootEntries,
          classCountCache,
          session);
    }
    return cost;
  }

  /**
   * Iteratively extends a linear chain past its first hop. Starts from
   * {@code currentVertex} (the downstream vertex of the first hop) and
   * walks forward as long as:
   * <ul>
   *   <li>{@code currentVertex.out.size() == 1} (no branch point);</li>
   *   <li>that single outgoing edge starts another valid sub-chain per
   *       {@link #resolveChainedTarget}'s structural rule;</li>
   *   <li>neither edge in the new sub-chain has been visited (back-edge
   *       guard against pattern loops).</li>
   * </ul>
   * Walk terminates as soon as any condition fails, or when
   * {@code remainingHops} reaches zero. The structural rule handles
   * fragment-join, branch-point, and visited-edge rejection identically to
   * the single-hop case.
   *
   * <p>Visited tracking uses two disjoint sets to avoid copying the DFS
   * state: {@code initialVisitedEdges} is consulted read-only for the
   * back-edge guard against already-scheduled edges, while a small local
   * {@code chainEdges} set tracks just the edges this walk has crossed
   * (the first hop's two edges plus each sub-chain's pair). Each
   * {@code contains} check queries both sets. The walk's loop counter
   * {@code remainingHops} is upper-bounded by {@code MAX_CHAIN_FOLD_HOPS}
   * via the knob clamp at the read site, so {@code chainEdges} grows to
   * at most {@code 2 * MAX_CHAIN_FOLD_HOPS + 2} entries — but in practice
   * the structural termination at branch points keeps it well under 30
   * for any realistic MATCH pattern. The caller's DFS-level
   * {@code visitedEdges} is not mutated.
   *
   * @return list of additional hops (may be empty); each hop is one
   *         (edge step, vertex step) sub-chain
   * @apiNote Package-visible so unit tests can drive the walk directly. Its
   *          four break conditions (branch point, back-edge into
   *          {@code initialVisitedEdges}, loop back into {@code chainEdges},
   *          non-chain continuation) and the {@code remainingHops} cap are
   *          each reachable from a synthesised pattern graph but not from
   *          every MATCH query.
   */
  // Known and accepted cost: this walk enumerates only structural hops, and
  // both of its lookups are memoized. Per-hop WHERE selectivity is applied by
  // the caller through estimateFilterSelectivity, which does uncached
  // index/histogram reads. A chain of N filtered hops therefore costs O(N^2)
  // per plan. At the maximum knob (1000) with hundreds of filtered hops that
  // would matter; at the default (10), where the realistic worst case stays
  // under ~30 hops, it does not, so there is no per-hop selectivity cache.
  static List<ChainHop> walkLinearChainExtension(
      PatternNode currentVertex,
      @Nullable String currentVertexClass,
      int remainingHops,
      Set<PatternEdge> initialVisitedEdges,
      PatternEdge firstHopEdge,
      PatternEdge firstHopVertexStepEdge,
      @Nullable Map<String, String> aliasClasses,
      @Nullable DatabaseSessionEmbedded session,
      Map<ChainShapeKey, ChainedTarget> chainShapeCache) {
    if (remainingHops <= 0) {
      return List.of();
    }
    // Fast-path: terminal vertices and branch points cannot extend the
    // chain. Bail out before allocating the chainEdges HashSet — the
    // common case in pattern graphs whose tail vertex has no outgoing
    // edges or fans out to multiple neighbors.
    if (currentVertex.out.size() != 1) {
      return List.of();
    }
    var hops = new ArrayList<ChainHop>();
    // Default-sized HashSet (capacity 16) is sufficient: the walk
    // terminates structurally at branch points or visited edges, so
    // chainEdges typically holds well under 16 entries even for
    // MAX_CHAIN_FOLD_HOPS-sized knobs. The default knob (10) walks at
    // most 20 entries, costing one rehash to capacity 32 — negligible
    // in plan-construction time. Explicit pre-sizing was removed to
    // eliminate any int-overflow surface in the sizing arithmetic; the
    // knob is already clamped to [0, MAX_CHAIN_FOLD_HOPS] at the read
    // site, so every entry in the loop's bookkeeping stays bounded.
    var chainEdges = new HashSet<PatternEdge>();
    chainEdges.add(firstHopEdge);
    chainEdges.add(firstHopVertexStepEdge);

    var sourceClass = currentVertexClass;
    while (hops.size() < remainingHops) {
      if (currentVertex.out.size() != 1) {
        break;
      }
      var nextEdgeStep = currentVertex.out.iterator().next();
      // Two-set check: nextEdgeStep is rejected if it's already in the
      // DFS schedule (back-edge into scheduled territory) OR already in
      // this walk (chain loops back on itself).
      if (initialVisitedEdges.contains(nextEdgeStep)
          || chainEdges.contains(nextEdgeStep)) {
        break;
      }
      var nextIntermediate = nextEdgeStep.in;
      // Reuse the structural-detection cache: identical pattern shape and
      // class-resolution inputs across plan iterations. The visited-edge
      // check uses the two-set guard rather than the cached value.
      var nextSubChain = lookupOrDetectChainShape(
          nextEdgeStep, nextIntermediate, aliasClasses, session, chainShapeCache);
      if (nextSubChain == null) {
        break;
      }
      // detectChainShape verified nextIntermediate.out.size() == 1.
      var vertexStepEdge = nextIntermediate.out.iterator().next();
      if (initialVisitedEdges.contains(vertexStepEdge)
          || chainEdges.contains(vertexStepEdge)) {
        break;
      }
      chainEdges.add(nextEdgeStep);
      chainEdges.add(vertexStepEdge);

      hops.add(new ChainHop(
          nextEdgeStep,
          sourceClass,
          nextIntermediate.alias,
          nextSubChain.effectiveTargetAlias(),
          nextSubChain.effectiveTargetClass()));

      currentVertex = vertexStepEdge.in;
      sourceClass = nextSubChain.effectiveTargetClass();
    }
    return hops;
  }

  /**
   * Cache-aware wrapper around {@link #detectChainShape}. {@code computeIfAbsent}
   * cannot be used here because we want to memoize {@code null} results
   * (known-not-chain edges) too; the {@code containsKey}/{@code put} pair
   * disambiguates a missing entry from a cached negative.
   *
   * <p>The cache is keyed by ({@code edge}, neighbor-direction) rather than by
   * {@code edge} alone: {@link #detectChainShape}'s answer depends on which
   * side of the edge the neighbor sits, so a {@code null} cached for one
   * direction must not be returned for (or poison) the opposite direction.
   *
   * @apiNote Package-visible test seam. Production callers ({@link #applyChainFold},
   *          {@link #walkLinearChainExtension}) always pass the per-plan cache
   *          allocated in {@link #getTopologicalSortedSchedule}; tests pass their
   *          own map so they can inspect what was memoized under which key.
   */
  @Nullable static ChainedTarget lookupOrDetectChainShape(
      PatternEdge edge,
      PatternNode neighbor,
      @Nullable Map<String, String> aliasClasses,
      @Nullable DatabaseSessionEmbedded session,
      Map<ChainShapeKey, ChainedTarget> chainShapeCache) {
    var key = new ChainShapeKey(edge, neighbor == edge.in);
    if (chainShapeCache.containsKey(key)) {
      return chainShapeCache.get(key);
    }
    var shape = detectChainShape(edge, neighbor, aliasClasses, session);
    chainShapeCache.put(key, shape);
    return shape;
  }

  /**
   * Classifies a WHERE clause by inspecting its AST to produce a selectivity
   * estimate. When the filter is a simple binary condition on an indexed
   * property, uses {@code distinctCount} from index statistics for accuracy:
   *
   * <ul>
   *   <li>{@code field = value} → {@code 1.0 / distinctCount}</li>
   *   <li>{@code field <> value} → {@code (distinctCount - 1.0) / distinctCount}</li>
   * </ul>
   *
   * <p>Falls back to {@code classCount} when no index statistics are available.
   * Returns {@code -1.0} for compound or unrecognizable filters to signal that
   * the caller should fall back to the cardinality-ratio estimate.
   */
  static double estimateFilterSelectivity(
      SQLWhereClause filter,
      long classCount,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    var base = classCount > 0 ? filter.getBaseExpression() : null;
    var condition = base != null ? unwrapSingleCondition(base) : null;
    if (condition == null) {
      return -1.0;
    }

    // Compound AND: multiply individual selectivities (independence assumption).
    // For example, creationDate >= X AND creationDate < Y → sel(>=X) * sel(<Y).
    if (condition instanceof SQLAndBlock andBlock && andBlock.getSubBlocks().size() > 1) {
      return estimateCompoundAndSelectivity(
          andBlock, classCount, schemaClass, session);
    }

    // Compound OR: inclusion-exclusion (independence assumption).
    // sel(A OR B) = 1 - (1 - sel(A)) * (1 - sel(B))
    if (condition instanceof SQLOrBlock orBlock && orBlock.getSubBlocks().size() > 1) {
      return estimateCompoundOrSelectivity(
          orBlock, classCount, schemaClass, session);
    }

    if (!(condition instanceof SQLBinaryCondition binary)) {
      return -1.0;
    }
    return estimateSingleConditionSelectivity(
        binary, classCount, schemaClass, session);
  }

  /**
   * Estimates selectivity of a compound AND filter by multiplying individual
   * condition selectivities (independence assumption). Returns -1.0 if no
   * sub-condition could be estimated.
   */
  private static double estimateCompoundAndSelectivity(
      SQLAndBlock andBlock, long classCount,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    double combined = 1.0;
    boolean anyEstimated = false;
    for (var sub : andBlock.getSubBlocks()) {
      double sel = estimateSubExpression(sub, classCount, schemaClass, session);
      if (sel >= 0.0) {
        combined *= sel;
        anyEstimated = true;
      }
    }
    return anyEstimated ? combined : -1.0;
  }

  /**
   * Estimates selectivity of a compound OR filter using the inclusion-exclusion
   * principle (independence assumption):
   * {@code sel(A OR B) = 1 - (1 - sel(A)) * (1 - sel(B))}.
   * Returns -1.0 if no sub-condition could be estimated.
   */
  private static double estimateCompoundOrSelectivity(
      SQLOrBlock orBlock, long classCount,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    double complementProduct = 1.0;
    boolean anyEstimated = false;
    for (var sub : orBlock.getSubBlocks()) {
      double sel = estimateSubExpression(sub, classCount, schemaClass, session);
      if (sel >= 0.0) {
        complementProduct *= (1.0 - sel);
        anyEstimated = true;
      }
    }
    return anyEstimated ? 1.0 - complementProduct : -1.0;
  }

  /**
   * Estimates selectivity of a single sub-expression within a compound
   * AND/OR block. Handles nested AND blocks, OR blocks, and leaf binary
   * conditions via recursive dispatch.
   */
  private static double estimateSubExpression(
      SQLBooleanExpression sub, long classCount,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    var unwrapped = unwrapSingleCondition(sub);
    if (unwrapped instanceof SQLBinaryCondition binary) {
      return estimateSingleConditionSelectivity(
          binary, classCount, schemaClass, session);
    }
    if (unwrapped instanceof SQLAndBlock nested && nested.getSubBlocks().size() > 1) {
      return estimateCompoundAndSelectivity(
          nested, classCount, schemaClass, session);
    }
    if (unwrapped instanceof SQLOrBlock nested && nested.getSubBlocks().size() > 1) {
      return estimateCompoundOrSelectivity(
          nested, classCount, schemaClass, session);
    }
    return -1.0;
  }

  /**
   * Estimates selectivity for a single binary condition using a three-tier
   * approach:
   * <ol>
   *   <li>{@code @class = 'X'} — class count ratio (meta-attribute)</li>
   *   <li>Histogram-aware estimation via {@link SelectivityEstimator}</li>
   *   <li>Uniform-distribution fallback using {@code distinctCount}</li>
   * </ol>
   */
  private static double estimateSingleConditionSelectivity(
      SQLBinaryCondition binary, long classCount,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    // 1. @class = 'X' — meta-attribute, not index-backed
    double classSel = estimateClassAttributeSelectivity(
        binary, classCount, schemaClass, session);
    if (classSel >= 0.0) {
      return classSel;
    }

    // 2. Histogram-aware estimation (all operators: =, <>, >, <, >=, <=, IN)
    double histogramSel = estimateViaHistogram(binary, schemaClass, session);
    if (histogramSel >= 0.0) {
      return histogramSel;
    }

    // 3. Fallback: uniform-distribution using distinctCount
    var op = binary.getOperator();
    long divisor = resolveDistinctCount(binary, schemaClass, session);
    if (divisor <= 0) {
      divisor = classCount;
    }
    if (op instanceof SQLEqualsOperator) {
      return 1.0 / divisor;
    } else if (op instanceof SQLNeOperator) {
      return (divisor - 1.0) / divisor;
    }
    return -1.0;
  }

  /**
   * Attempts histogram-aware selectivity estimation by looking up index statistics
   * and histogram for the property in the binary condition, then delegating to
   * {@link SelectivityEstimator#estimateForOperator}.
   *
   * @return selectivity in [0.0, 1.0], or -1.0 if estimation is not possible
   *     (no index, no statistics, or value cannot be resolved at plan time)
   */
  private static double estimateViaHistogram(
      SQLBinaryCondition binary,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    if (schemaClass == null || session == null) {
      return -1.0;
    }
    var propName = binary.getRelatedIndexPropertyName();
    if (propName == null) {
      return -1.0;
    }
    var indexes = schemaClass.getInvolvedIndexesInternal(session, propName);
    if (indexes == null) {
      return -1.0;
    }
    // Try to resolve the comparison value at plan time. Only literal values
    // can be resolved — parameterized queries (e.g. :startDate) depend on
    // runtime input parameters which are not available during planning.
    // Catches RuntimeException (not just CommandExecutionException) because
    // execute() with a null Result/context can also throw NPE, ClassCastException,
    // etc. for expressions that reference runtime state. All such failures
    // are non-fatal — they simply mean the value cannot be resolved at plan time.
    Object value;
    try {
      value = binary.getRight().execute(
          (com.jetbrains.youtrackdb.internal.core.query.Result) null, null);
    } catch (RuntimeException e) {
      value = null;
    }
    if (value == null) {
      return -1.0;
    }
    // Pick the most selective index (lowest selectivity estimate) to avoid
    // random plan jumps when multiple indexes cover the same property.
    double bestSel = -1.0;
    for (var index : indexes) {
      var stats = index.getStatistics(session);
      if (stats == null || stats.totalCount() <= 0) {
        continue;
      }
      var histogram = index.getHistogram(session);
      var sel = SelectivityEstimator.estimateForOperator(
          binary.getOperator(), stats, histogram, value);
      if (sel >= 0.0 && (bestSel < 0.0 || sel < bestSel)) {
        bestSel = sel;
      }
    }
    return bestSel;
  }

  /**
   * Attempts to resolve the number of distinct values for the property
   * referenced in a binary condition by looking up index statistics. Returns
   * {@code -1} if no indexed property can be identified or no statistics are
   * available.
   */
  private static long resolveDistinctCount(
      SQLBinaryCondition binary,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    var propName = (schemaClass != null && session != null)
        ? binary.getRelatedIndexPropertyName() : null;
    var indexes = propName != null
        ? schemaClass.getInvolvedIndexesInternal(session, propName) : null;
    if (indexes != null) {
      // Pick the most selective index (lowest selectivity estimate) and return
      // its distinct count. Same criterion as resolveSelectivity() — ensures
      // both methods use the same index for a given property.
      Object value;
      try {
        value = binary.getRight().execute(
            (com.jetbrains.youtrackdb.internal.core.query.Result) null, null);
      } catch (RuntimeException e) {
        value = null;
      }
      double bestSel = -1.0;
      long bestDistinct = -1;
      for (var index : indexes) {
        var stats = index.getStatistics(session);
        if (stats == null || stats.distinctCount() <= 0) {
          continue;
        }
        if (value != null && stats.totalCount() > 0) {
          var histogram = index.getHistogram(session);
          var sel = SelectivityEstimator.estimateForOperator(
              binary.getOperator(), stats, histogram, value);
          if (sel >= 0.0 && (bestSel < 0.0 || sel < bestSel)) {
            bestSel = sel;
            bestDistinct = stats.distinctCount();
          }
        } else if (bestDistinct < 0) {
          // Fallback when value cannot be resolved: take first available.
          bestDistinct = stats.distinctCount();
        }
      }
      return bestDistinct;
    }
    return -1;
  }

  /**
   * Adjusts edge cost based on the maximum traversal depth specified in
   * a WHILE clause. Edges with {@code $depth < N} (or explicit maxDepth)
   * produce intermediate results proportional to the depth limit. A
   * lower maxDepth means fewer hops and cheaper traversal.
   *
   * <p>When no depth limit is set (simple one-hop edge), the cost is
   * unchanged. For WHILE edges without maxDepth, a default multiplier
   * of {@value #DEFAULT_WHILE_DEPTH} is applied to reflect the
   * potentially unbounded recursive expansion.
   *
   * @param baseCost the cost computed from fan-out and target selectivity
   * @param edge     the pattern edge to inspect for depth limits
   * @return adjusted cost, multiplied by the depth factor
   */
  static double applyDepthMultiplier(double baseCost, PatternEdge edge) {
    var filter = edge.item.getFilter();
    if (filter == null) {
      return baseCost;
    }
    var whileCondition = filter.getWhileCondition();
    if (whileCondition == null) {
      return baseCost;
    }
    var maxDepth = filter.getMaxDepth();
    if (maxDepth != null && maxDepth > 0) {
      return baseCost * maxDepth;
    }
    return baseCost * DEFAULT_WHILE_DEPTH;
  }

  private static final int DEFAULT_WHILE_DEPTH = 10;

  /**
   * Handles the {@code @class = 'ClassName'} selectivity heuristic. When the
   * left side of the condition is a record attribute ({@code @class}) and the
   * operator is equality, the selectivity is the ratio of the subclass record
   * count to the total target class count:
   * {@code subclassCount / targetClassCount}.
   *
   * <p>For example, with target class {@code Message} (10000 records) and
   * filter {@code @class = 'Post'} where {@code Post} has 3000 records,
   * selectivity = 3000/10000 = 0.3.
   *
   * @return selectivity in [0.0, 1.0], or {@code -1.0} if the condition
   *     is not a {@code @class = 'X'} pattern
   */
  private static double estimateClassAttributeSelectivity(
      SQLBinaryCondition binary,
      long targetClassCount,
      @Nullable SchemaClassInternal schemaClass,
      @Nullable DatabaseSessionEmbedded session) {
    if (schemaClass == null || session == null || targetClassCount <= 0) {
      return -1.0;
    }
    // Verify left side is @class, evaluate right side, and look up subclass — all
    // in a single guard chain. Any null/mismatch returns -1.0 to signal "not applicable".
    var recAttr = extractRecordAttribute(binary.getLeft());
    var isClassAttr = recAttr != null && "@class".equalsIgnoreCase(recAttr.getName());
    var subclassName = isClassAttr ? evaluateAsString(binary.getRight()) : null;
    var schema = subclassName != null
        ? session.getMetadata().getImmutableSchemaSnapshot() : null;
    if (schema == null || !schema.existsClass(subclassName)) {
      return -1.0;
    }
    long subclassCount = schema.getClassInternal(subclassName).approximateCount(session);
    return (double) subclassCount / targetClassCount;
  }

  /** Extracts the SQLRecordAttribute from an expression's left side, or null. */
  @Nullable private static SQLRecordAttribute extractRecordAttribute(
      @Nullable SQLExpression expr) {
    if (expr == null || expr.getMathExpression() == null) {
      return null;
    }
    if (!(expr.getMathExpression() instanceof SQLBaseExpression base)) {
      return null;
    }
    var identifier = base.getIdentifier();
    if (identifier == null || identifier.getSuffix() == null) {
      return null;
    }
    return identifier.getSuffix().getRecordAttribute();
  }

  /**
   * Evaluates an expression and returns the result as a String, or null.
   *
   * <p>Catches {@link RuntimeException} (not just {@link CommandExecutionException})
   * because {@code execute()} is called at plan time with a null {@code Result} and
   * a bare {@link BasicCommandContext}. Expressions that reference runtime state
   * ({@code $matched}, {@code $parent}, input parameters, record fields) may throw
   * {@code NullPointerException}, {@code ClassCastException}, or other unchecked
   * exceptions in addition to {@code CommandExecutionException}. All such failures
   * are non-fatal — they simply mean the value cannot be resolved at plan time.
   */
  @Nullable private static String evaluateAsString(@Nullable SQLExpression expr) {
    if (expr == null) {
      return null;
    }
    try {
      var value = expr.execute(
          (com.jetbrains.youtrackdb.internal.core.query.Result) null,
          new BasicCommandContext());
      return value instanceof String s ? s : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Unwraps single-element AND/OR blocks to find the innermost condition.
   * Returns the expression as-is if it is already a leaf condition or if
   * there are multiple sub-blocks (compound filter).
   */
  @Nullable private static SQLBooleanExpression unwrapSingleCondition(SQLBooleanExpression expr) {
    if (expr instanceof SQLAndBlock and) {
      if (and.getSubBlocks().size() == 1) {
        return unwrapSingleCondition(and.getSubBlocks().getFirst());
      }
    } else if (expr instanceof SQLOrBlock or) {
      if (or.getSubBlocks().size() == 1) {
        return unwrapSingleCondition(or.getSubBlocks().getFirst());
      }
    } else if (expr instanceof SQLNotBlock not) {
      if (!not.isNegate()) {
        return unwrapSingleCondition(not.getSub());
      }
    }
    return expr;
  }

  /**
   * Parses "out", "in", "both" (and their E-variants "outE", "inE", "bothE")
   * into a {@link Direction}. Returns {@code null} for unrecognized methods.
   */
  static Direction parseDirection(String methodName) {
    if (methodName == null) {
      return null;
    }
    return switch (methodName.toLowerCase(Locale.ENGLISH)) {
      case "out", "oute" -> Direction.OUT;
      case "in", "ine" -> Direction.IN;
      case "both", "bothe" -> Direction.BOTH;
      default -> null;
    };
  }

  /**
   * Extracts the edge class name from the method call's first parameter.
   * Tries {@code execute()} first to get the evaluated literal value;
   * falls back to stripping surrounding quotes from {@code toString()}
   * if execution returns null (e.g., context-dependent expressions).
   *
   * @return the edge class name, or {@code null} if no parameter is present
   *     or the value cannot be resolved to a string
   */
  static String extractEdgeClassName(SQLMethodCall method) {
    var params = method.getParams();
    if (params == null || params.isEmpty()) {
      return null;
    }
    var firstParam = params.getFirst();

    // Try evaluating the expression first (handles all literal types cleanly).
    // Catches RuntimeException: execute() with null Result / bare context can
    // throw NPE, ClassCastException, etc. — not just CommandExecutionException.
    try {
      var value = firstParam.execute((Result) null, new BasicCommandContext());
      if (value instanceof String s && !s.isEmpty()) {
        return s;
      }
    } catch (RuntimeException e) {
      if (logger.isTraceEnabled()) {
        logger.trace("Could not evaluate edge class parameter, "
            + "falling back to toString", e);
      }
    }

    // Fallback: strip surrounding quotes from the string representation
    if (firstParam.getMathExpression() instanceof SQLBaseExpression base) {
      var raw = base.toString();
      if (raw != null && raw.length() >= 2) {
        char first = raw.charAt(0);
        char last = raw.charAt(raw.length() - 1);
        if ((first == '"' && last == '"')
            || (first == '\'' && last == '\'')) {
          return raw.substring(1, raw.length() - 1);
        }
      }
      return raw;
    }
    return null;
  }

  /**
   * Post-scheduling optimization pass: detects back-reference and index-based
   * pre-filter opportunities and attaches {@link RidFilterDescriptor}s
   * to the appropriate edges.
   *
   * <p><b>Back-reference detection</b>: when edge_j's target filter contains
   * {@code @rid = $matched.X.@rid}, the intermediate node (edge_j's source)
   * must be in {@code X.reverse(edge_j)}. A {@link
   * RidFilterDescriptor.EdgeRidLookup} is attached to the preceding edge
   * (edge_i) that produces the intermediate node, so that edge_i's traversal
   * results are intersected with the pre-computed RidSet.
   *
   * <p><b>Index pre-filter detection</b>: when an edge's target node has an
   * indexable condition that does not reference {@code $matched}, a {@link
   * RidFilterDescriptor.IndexLookup} is attached to the edge.
   */
  private boolean optimizeScheduleWithIntersections(
      List<EdgeTraversal> schedule, CommandContext ctx) {
    // Tracks whether at least one IndexLookup descriptor was attached during
    // this pass. Returned to the caller so it can short-circuit the forecast
    // pass (the sole consumer of BUILD_EAGER amortization data) without
    // having to re-scan the schedule after the fact.
    var hasIndexLookup = false;
    // Build a map: target alias → edge index, so we can find the producing edge
    Map<String, Integer> targetAliasToEdgeIndex = new HashMap<>();
    for (var i = 0; i < schedule.size(); i++) {
      var et = schedule.get(i);
      var targetAlias = et.out ? et.edge.in.alias : et.edge.out.alias;
      if (targetAlias != null) {
        targetAliasToEdgeIndex.put(targetAlias, i);
      }
    }

    // Track aliases that are bound (visited) before each edge. Built
    // incrementally: the source alias is added at the start of each
    // iteration (it was bound by a preceding edge or is the root), and
    // the target alias is added at the end (it becomes bound during this
    // edge's execution). This ensures that semi-join candidacy checks
    // only see aliases that are actually available at execution time.
    Set<String> boundAliases = new HashSet<>();
    for (var j = 0; j < schedule.size(); j++) {
      var edgeJ = schedule.get(j);
      var sourceAliasJ = edgeJ.out ? edgeJ.edge.out.alias : edgeJ.edge.in.alias;
      var targetAliasJ = edgeJ.out ? edgeJ.edge.in.alias : edgeJ.edge.out.alias;

      // Source alias is bound before this edge executes (it is either
      // the root alias or the target of a preceding edge).
      if (sourceAliasJ != null) {
        boundAliases.add(sourceAliasJ);
      }
      if (targetAliasJ == null) {
        continue;
      }

      var targetFilter = aliasFilters.get(targetAliasJ);
      if (targetFilter == null) {
        boundAliases.add(targetAliasJ);
        continue;
      }

      // --- RID equality detection ---
      // Check if target filter contains @rid = <expr>.
      // Uses findRidEquality() rather than extractRidEquality(): this path needs only the RID
      // expression, not a remainder clause, and findRidEquality() recurses through the AND/OR
      // double-nesting addAliases() wraps MATCH filters in — which extractRidEquality() does not.
      var ridExpr = targetFilter.findRidEquality();
      if (ridExpr != null) {
        var involvedAliases = ridExpr.getMatchPatternInvolvedAliases();
        if (involvedAliases != null && !involvedAliases.isEmpty()) {
          // Back-reference: @rid = $matched.X.@rid → EdgeRidLookup
          var edgeClass = getEdgeClassName(edgeJ);
          var edgeDirection = getEdgeDirection(edgeJ);
          var collectEdgeRids = false;

          // .inV()/.outV() steps have no edge class — propagate from the
          // preceding .outE('CLASS')/.inE('CLASS') step if present.
          // The preceding edge iterates edge RIDs, so collectEdgeRids=true.
          if (edgeClass == null && j > 0) {
            var prevEdge = schedule.get(j - 1);
            var prevMethodName = getMethodName(prevEdge);
            if ("oute".equals(prevMethodName) || "ine".equals(prevMethodName)) {
              edgeClass = getEdgeClassName(prevEdge);
              // Normalize direction: "oute" -> "out", "ine" -> "in"
              edgeDirection = getEdgeDirection(prevEdge);
              if (edgeDirection != null && edgeDirection.endsWith("e")) {
                edgeDirection =
                    edgeDirection.substring(0, edgeDirection.length() - 1);
              }
              collectEdgeRids = true;
            }
          }

          if (edgeClass != null && edgeDirection != null) {

            // --- Semi-join candidacy check (Pattern A) ---
            // Only when the edge class belongs to the current edge (not
            // propagated from a preceding outE/inE). When collectEdgeRids
            // is true, the class/direction came from the previous edge —
            // Pattern B detection handles that case below.
            //
            // Any residual WHERE terms on the target alias (beyond the
            // {@code @rid = $matched.X.@rid} equality) are extracted here
            // and passed to {@link BackRefHashJoinStep} for post-load
            // evaluation. Pattern A is rejected only when the residual
            // cannot be extracted safely — either because the RID equality
            // is nested too deep for the flat-block extractor, or because
            // the residual references {@code $matched}/{@code $currentMatch}
            // which build phase cannot resolve.
            var residualExtraction = extractTargetResidual(targetFilter);
            if (!collectEdgeRids
                && !edgeJ.edge.in.isOptionalNode()
                && isSemiJoinCandidate(edgeDirection, involvedAliases,
                    boundAliases)
                && residualExtraction.safe()) {
              var backRefAlias = involvedAliases.getFirst();
              var descriptor = new SingleEdgeSemiJoin(
                  edgeClass, edgeDirection, ridExpr,
                  sourceAliasJ, backRefAlias, targetAliasJ,
                  residualExtraction.residual());
              edgeJ.setSemiJoinDescriptor(descriptor);
              logger.debug(
                  "MATCH pre-filter: BackRefHashJoin on edge[{}] "
                      + "({}({}) semi-join via $matched.{})",
                  j, edgeDirection, edgeClass, backRefAlias);
            } else {
              // Fallback: attach EdgeRidLookup on the producing edge
              var producingEdgeIdx = targetAliasToEdgeIndex.get(sourceAliasJ);
              if (producingEdgeIdx != null) {
                var edgeI = schedule.get(producingEdgeIdx);
                edgeI.addIntersectionDescriptor(
                    new RidFilterDescriptor.EdgeRidLookup(
                        edgeClass, edgeDirection, ridExpr, collectEdgeRids));
                logger.debug(
                    "MATCH pre-filter: EdgeRidLookup on edge[{}] "
                        + "({}({}) back-ref from alias '{}', edgeRids={})",
                    producingEdgeIdx, edgeDirection, edgeClass, targetAliasJ,
                    collectEdgeRids);
              }
              // --- Pattern B: outE('E').inV() chain semi-join ---
              // When edge class was propagated from the preceding edge, also
              // try chain semi-join which collapses both edges into one step.
              if (collectEdgeRids) {
                tryAttachChainSemiJoin(
                    schedule, j, edgeJ, involvedAliases, ridExpr,
                    targetAliasJ, boundAliases, ctx);
              }
            }
          } else if (j > 0) {
            // --- Pattern B: outE('E').inV() chain semi-join ---
            // edge_j is .inV() (no edge class/direction). Check if the
            // preceding edge is .outE('E') or .inE('E') with a recognized
            // edge class. If so, collapse both into a ChainSemiJoin.
            tryAttachChainSemiJoin(
                schedule, j, edgeJ, involvedAliases, ridExpr,
                targetAliasJ, boundAliases, ctx);
          }
        } else {
          // Literal or parameter RID: @rid = #12:0 or @rid = :param
          // → DirectRid singleton set for zero-waste link bag filtering.
          // A singleton RID cannot benefit from further index intersection,
          // so skip the index detection below.
          edgeJ.addIntersectionDescriptor(
              new RidFilterDescriptor.DirectRid(ridExpr));
          logger.debug(
              "MATCH pre-filter: DirectRid on edge[{}] for alias '{}'",
              j, targetAliasJ);
          boundAliases.add(targetAliasJ);
          continue;
        }
      }

      // --- Pattern D: NOT IN anti-semi-join detection ---
      // Check if the target's WHERE clause contains
      // $currentMatch NOT IN $matched.X.out('E')
      // Pattern D detection follows below
      if (edgeJ.getSemiJoinDescriptor() == null) {
        var antiDesc = detectNotInAntiJoin(
            targetFilter, targetAliasJ, boundAliases);
        if (antiDesc != null) {
          edgeJ.setSemiJoinDescriptor(antiDesc);
          logger.debug(
              "MATCH pre-filter: AntiSemiJoin on edge[{}] "
                  + "(NOT IN $matched.{}.{}('{}'))",
              j, antiDesc.anchorAlias(),
              antiDesc.traversalDirection(), antiDesc.traversalEdgeClass());
        }
      }

      // --- Index pre-filter detection ---
      var targetClass = aliasClasses.get(targetAliasJ);
      if (targetClass == null) {
        boundAliases.add(targetAliasJ);
        continue;
      }

      // Split the filter: only the non-$matched part can use an index.
      SQLWhereClause indexableFilter = targetFilter;
      var matchedSplit = targetFilter.splitByMatchedReference();
      if (matchedSplit != null) {
        indexableFilter = matchedSplit.nonMatchedReferencing();
      }
      if (indexableFilter == null) {
        boundAliases.add(targetAliasJ);
        continue;
      }

      var indexDesc = TraversalPreFilterHelper.findIndexForFilter(
          indexableFilter, targetClass, ctx);
      if (indexDesc != null) {
        edgeJ.addIntersectionDescriptor(
            new RidFilterDescriptor.IndexLookup(indexDesc));
        hasIndexLookup = true;
        logger.debug(
            "MATCH pre-filter: IndexLookup on edge[{}] "
                + "(class '{}' for alias '{}')",
            j, targetClass, targetAliasJ);
      }

      // Target alias becomes bound after this edge executes
      boundAliases.add(targetAliasJ);
    }
    return hasIndexLookup;
  }

  /**
   * Checks if a back-reference edge qualifies for a semi-join hash table
   * optimization (Pattern A). The edge must be a vertex-level traversal
   * ({@code out('E')} or {@code in('E')}, not {@code outE('E')} or
   * {@code inE('E')}), and the back-referenced alias must be already bound
   * earlier in the schedule.
   *
   * @param edgeDirection    the traversal direction (e.g., "out", "in", "oute")
   * @param involvedAliases  the aliases referenced by the back-ref expression
   * @param boundAliases     all aliases bound (visited) in the schedule
   * @return true if the edge is a semi-join candidate
   */
  private static boolean isSemiJoinCandidate(
      String edgeDirection,
      List<String> involvedAliases,
      Set<String> boundAliases) {
    // Only vertex-level traversals qualify (not outE/inE/bothE/both)
    if (!"out".equals(edgeDirection) && !"in".equals(edgeDirection)) {
      return false;
    }
    // The back-ref must reference exactly one alias
    if (involvedAliases.size() != 1) {
      return false;
    }
    // The back-referenced alias must be already bound in the schedule
    var backRefAlias = involvedAliases.getFirst();
    if (!boundAliases.contains(backRefAlias)) {
      return false;
    }
    // Check threshold is enabled (0 disables hash join)
    var threshold = getHashJoinThreshold();
    return threshold > 0;
  }

  /**
   * Structural decomposition of a {@code $matched.<alias>.<out|in>('E')}
   * traversal expression — the RHS shape required by Pattern D anti-joins.
   */
  private record MatchedTraversal(
      String anchorAlias, String direction, String edgeClass) {
  }

  /**
   * Extracts {@link MatchedTraversal} from an RHS AST node or returns
   * {@code null} when the shape does not match. Walks the AST directly
   * instead of parsing {@link SQLMathExpression#toString}, which would
   * rely on the generated parser's serialization format staying stable
   * and would mis-handle back-quoted identifiers, bound parameters, and
   * edge-direction variants like {@code outE}.
   */
  @Nullable private static MatchedTraversal extractMatchedTraversal(
      @Nullable SQLMathExpression rhs) {
    if (!(rhs instanceof SQLBaseExpression base)) {
      return null;
    }
    var identifier = base.getIdentifier();
    if (identifier == null || !"$matched".equals(identifier.toString())) {
      return null;
    }
    var firstMod = base.getModifier();
    if (firstMod == null) {
      return null;
    }
    // First segment must be `.X` (suffix identifier, no method call).
    var suffix = firstMod.getSuffix();
    if (suffix == null || suffix.getIdentifier() == null
        || firstMod.getMethodCall() != null) {
      return null;
    }
    var anchorAlias = suffix.getIdentifier().getStringValue();
    if (anchorAlias == null) {
      return null;
    }
    // Second segment must be `.<dir>(<edgeClass>)` — a method call.
    var secondMod = firstMod.getNext();
    if (secondMod == null) {
      return null;
    }
    var method = secondMod.getMethodCall();
    if (method == null) {
      return null;
    }
    // There must not be further modifiers (reject `.out('E').somethingElse`).
    if (secondMod.getNext() != null) {
      return null;
    }
    var methodName = method.getMethodNameString();
    if (methodName == null) {
      return null;
    }
    // Vertex-level traversals only: out / in. Reject outE, inE, bothV, etc.
    var direction = methodName.toLowerCase(Locale.ROOT);
    if (!"out".equals(direction) && !"in".equals(direction)) {
      return null;
    }
    var params = method.getParams();
    if (params == null || params.size() != 1) {
      return null;
    }
    var paramMath = params.getFirst().getMathExpression();
    if (!(paramMath instanceof SQLBaseExpression paramBase)) {
      return null;
    }
    // Only accept a plain string literal for the edge class — reject bound
    // parameters (:edge), identifiers, and compound expressions whose
    // value cannot be determined at plan time.
    var edgeClass = paramBase.getStringLiteralValue();
    if (edgeClass == null) {
      return null;
    }
    return new MatchedTraversal(anchorAlias, direction, edgeClass);
  }

  /**
   * Detects Pattern D: a {@code $currentMatch NOT IN $matched.X.out('E')}
   * condition in the target node's WHERE clause. If found, removes the
   * NOT IN condition from the WHERE clause and returns an
   * {@link AntiSemiJoin} descriptor. Any remaining conditions in the AND
   * block stay as residual filter on the {@link MatchEdgeTraverser}.
   *
   * @param targetFilter  the WHERE clause on the target node
   * @param targetAlias   the alias of the target node
   * @param boundAliases  all aliases bound in the schedule
   * @return an AntiSemiJoin descriptor, or null if the pattern is not found
   */
  @Nullable private AntiSemiJoin detectNotInAntiJoin(
      SQLWhereClause targetFilter,
      String targetAlias,
      Set<String> boundAliases) {
    var threshold = getHashJoinThreshold();
    if (threshold <= 0) {
      return null;
    }

    var baseExpr = targetFilter.getBaseExpression();
    if (baseExpr == null) {
      return null;
    }

    // Descend through the transparent wrappers MATCH adds around filters
    // (addAliases wraps original WHERE in an outer AND, the grammar in turn
    // wraps AND blocks in single-element ORs) to the AND block whose
    // sub-blocks are the user's visible conjuncts. Without this, a compound
    // WHERE like "NOT IN AND name='n4'" sits two wrappers deep — iterating
    // the top-level AND's single sub-block (an OR wrapping the inner AND
    // with two terms) never reaches the NOT IN.
    var andBlock = findConjunctsAnd(baseExpr);
    if (andBlock == null) {
      return null;
    }
    var andSubBlocks = andBlock.getSubBlocks();
    if (andSubBlocks == null || andSubBlocks.isEmpty()) {
      return null;
    }
    // Scan for SQLNotInCondition nodes whose LHS is $currentMatch and whose
    // RHS matches $matched.X.out('E') or $matched.X.in('E'). The grammar
    // wraps conditions in multiple transparent layers (OrBlock → AndBlock →
    // NotBlock → ConditionBlock). We unwrap single-element wrappers before
    // checking the inner type, then inspect the LHS/RHS AST nodes directly.
    for (int i = 0; i < andSubBlocks.size(); i++) {
      var sub = andSubBlocks.get(i);
      var inner = unwrapToNotInCondition(sub);
      if (inner == null) {
        continue;
      }

      // Check LHS is $currentMatch via the AST node
      var lhs = inner.getLeft();
      if (lhs == null || !"$currentMatch".equals(lhs.toString().trim())) {
        continue;
      }

      // Check RHS is $matched.X.out('E') or $matched.X.in('E') by walking
      // the AST structure directly — immune to toString() format drift,
      // back-quoted identifiers, and bound parameters for the edge class.
      var traversal = extractMatchedTraversal(inner.getRightMathExpression());
      if (traversal == null) {
        continue;
      }

      // Anchor alias must be already bound
      if (!boundAliases.contains(traversal.anchorAlias())) {
        continue;
      }

      // Strip the NOT IN from the target alias's WHERE clause so that the
      // preceding MatchStep does not re-evaluate it per row (the expensive
      // O(degree) link bag traversal). The stripped condition is stored in
      // the AntiSemiJoin descriptor for runtime fallback: if the hash table
      // build fails, BackRefHashJoinStep evaluates it per row.
      //
      // Both sides use independent AST copies: buildWhereWithoutTerm already
      // deep-copies the sub-blocks it keeps, and we deep-copy {@code sub}
      // before stashing it in the descriptor. The original andBlock is kept
      // intact by the planner (rebindFilters, etc.) and cached plans may
      // re-execute with re-bound parameters — any shared sub-block reference
      // would let an AST rewrite on one side corrupt the other.
      var strippedFilter =
          SQLWhereClause.buildWhereWithoutTerm(andBlock, i);
      if (strippedFilter != null) {
        aliasFilters.put(targetAlias, strippedFilter);
      } else {
        // NOT IN was the only condition — remove the filter entirely
        aliasFilters.remove(targetAlias);
      }

      return new AntiSemiJoin(
          traversal.anchorAlias(), traversal.edgeClass(),
          traversal.direction(), targetAlias, sub.copy());
    }
    return null;
  }

  /**
   * Descends through single-element {@link SQLOrBlock} and {@link SQLAndBlock}
   * wrappers to find the AND block whose sub-blocks are the user's top-level
   * conjuncts.
   *
   * <p>The MATCH planner wraps each alias's WHERE clause in at least one
   * extra AND block (see {@code addAliases}), and the grammar itself wraps
   * the user's AND inside a single-element OR. For a compound WHERE like
   * {@code NOT IN AND name='n4'} the resulting structure is
   * {@code AND[OR[AND[<notIn>, <name='n4'>]]]} — the inner AND holds the
   * actual conjuncts. Returns {@code null} when a multi-branch OR is
   * encountered (that would require disjunctive processing) or when no AND
   * is reachable.
   */
  @Nullable private static SQLAndBlock findConjunctsAnd(
      SQLBooleanExpression expr) {
    SQLAndBlock lastAnd = null;
    var current = expr;
    while (true) {
      if (current instanceof SQLOrBlock or) {
        var subs = or.getSubBlocks();
        if (subs == null || subs.size() != 1) {
          return null;
        }
        current = subs.getFirst();
        continue;
      }
      if (current instanceof SQLAndBlock and) {
        lastAnd = and;
        var subs = and.getSubBlocks();
        if (subs == null || subs.isEmpty()) {
          return and;
        }
        // If this AND already has multiple conjuncts, it IS the user's list.
        if (subs.size() > 1) {
          return and;
        }
        // Single-element AND may be a wrapper around another AND/OR layer.
        var only = subs.getFirst();
        if (only instanceof SQLOrBlock || only instanceof SQLAndBlock) {
          current = only;
          continue;
        }
        // The lone sub-block is a leaf condition — this AND is the deepest
        // one reachable and holds that single conjunct.
        return and;
      }
      // Not an AND/OR at the top — return the deepest AND we've seen.
      return lastAnd;
    }
  }

  /**
   * Unwraps the transparent AST wrappers the grammar produces around condition
   * blocks (OrBlock → AndBlock → NotBlock → ConditionBlock) to reach the inner
   * {@link SQLNotInCondition}, if present. Returns {@code null} if the expression
   * is not a NOT IN condition or if any wrapper is non-transparent (e.g., OR with
   * multiple branches, or NOT with {@code negate=true}).
   */
  @Nullable private static SQLNotInCondition unwrapToNotInCondition(
      SQLBooleanExpression expr) {
    var inner = expr;
    // Unwrap single-element SQLOrBlock
    if (inner instanceof SQLOrBlock or && or.getSubBlocks() != null
        && or.getSubBlocks().size() == 1) {
      inner = or.getSubBlocks().getFirst();
    }
    // Unwrap single-element SQLAndBlock
    if (inner instanceof SQLAndBlock and && and.getSubBlocks() != null
        && and.getSubBlocks().size() == 1) {
      inner = and.getSubBlocks().getFirst();
    }
    // Unwrap transparent SQLNotBlock (negate=false)
    if (inner instanceof SQLNotBlock not && !not.isNegate()) {
      inner = not.getSub();
    }
    return inner instanceof SQLNotInCondition notIn ? notIn : null;
  }

  /**
   * Detects Pattern B: an {@code .outE('E').inV()} chain where the
   * {@code .inV()} target has a back-reference {@code @rid = $matched.X.@rid}.
   * Returns a {@link ChainSemiJoin} descriptor if the pattern qualifies,
   * or {@code null} otherwise.
   *
   * <p>The edge class and direction are extracted from edge_j-1 (the
   * {@code .outE('E')} edge), not from edge_j ({@code .inV()}).
   */
  @Nullable private ChainSemiJoin detectChainSemiJoin(
      List<EdgeTraversal> schedule,
      int j,
      List<String> involvedAliases,
      SQLExpression ridExpr,
      String targetAliasJ,
      Set<String> boundAliases,
      CommandContext ctx) {
    // The back-ref must reference exactly one alias that is already bound
    if (involvedAliases.size() != 1) {
      return null;
    }
    var backRefAlias = involvedAliases.getFirst();
    if (!boundAliases.contains(backRefAlias)) {
      return null;
    }
    var threshold = getHashJoinThreshold();
    if (threshold <= 0) {
      return null;
    }

    var edgeJ = schedule.get(j);
    // Mirror the Pattern A guard: an optional target node must pass through
    // rows with nulls when no match is found, which BackRefHashJoinStep does
    // not implement. Worse, addStepsFor dispatches on the optional branch
    // before consulting getSemiJoinDescriptor() — so if Pattern B fired here
    // the predecessor edge would be marked consumed and silently dropped
    // from the plan, producing wrong results.
    if (edgeJ.edge.in.isOptionalNode() || edgeJ.edge.out.isOptionalNode()) {
      return null;
    }

    // Check preceding edge (j-1) is an edge-level traversal (outE/inE)
    var edgePrev = schedule.get(j - 1);
    // The intermediate alias (edgePrev endpoint) must also be non-optional:
    // it would otherwise be "skipped" in the traversal yet still bound by
    // the collapsed BackRefHashJoinStep, diverging from the documented
    // semantics of optional nodes.
    if (edgePrev.edge.in.isOptionalNode() || edgePrev.edge.out.isOptionalNode()) {
      return null;
    }
    var prevDirection = getEdgeDirection(edgePrev);
    if (prevDirection == null) {
      return null;
    }
    // Must be edge-level: "oute" or "ine"
    if (!"oute".equals(prevDirection) && !"ine".equals(prevDirection)) {
      return null;
    }
    var prevEdgeClass = getEdgeClassName(edgePrev);
    if (prevEdgeClass == null) {
      return null;
    }

    // Extract aliases. The source alias is the source of edge_j-1 (outE),
    // not edge_j (inV). The intermediate alias is the target of edge_j-1.
    var sourceAlias = edgePrev.out
        ? edgePrev.edge.out.alias : edgePrev.edge.in.alias;
    var intermediateAlias = edgePrev.out
        ? edgePrev.edge.in.alias : edgePrev.edge.out.alias;

    // Map oute→out, ine→in for the reverse link bag direction
    var direction = prevDirection.startsWith("out") ? "out" : "in";

    // The intermediate edge may have a WHERE filter. Correctness requires
    // that every edge added to the hash table actually passes the whole
    // WHERE clause — the consumed predecessor's MatchStep, which would
    // normally evaluate it, is skipped in the collapsed plan.
    //
    // Strategy (both optional, both applied in BackRefHashJoinStep):
    //   1. indexFilter — RidSet intersection, rejects non-candidate edges
    //      without loading them. Partial cover is fine here: its only role
    //      is pre-filtering for performance.
    //   2. edgeFilter — the complete WHERE clause, re-evaluated on every
    //      loaded edge. Authoritative correctness check; catches any terms
    //      the index didn't cover.
    //
    // Rejection case kept at plan time:
    //   * Filter references $matched / $currentMatch — build phase has no
    //     per-row scope for those variables, so runtime evaluation would
    //     see stale values. Safer to fall back to the standard chain.
    //
    // Multi-branch OR in the intermediate filter is not rejected here: any
    // terms findIndexForFilter cannot cover are re-verified post-load by
    // edgeFilter on every loaded edge, so correctness does not depend on
    // indexability.
    var intermediateFilter = aliasFilters.get(intermediateAlias);
    IndexSearchDescriptor indexFilter = null;
    SQLWhereClause edgeFilter = null;
    if (intermediateFilter != null) {
      var baseExpr = intermediateFilter.getBaseExpression();
      if (baseExpr != null) {
        var refAliases = baseExpr.getMatchPatternInvolvedAliases();
        if (refAliases != null && !refAliases.isEmpty()) {
          // Filter uses $matched.<alias> — cannot evaluate at build phase.
          return null;
        }
      }
      if (refersToCurrentMatch(intermediateFilter)) {
        return null;
      }
      // Deep-copy the filter so the ChainSemiJoin descriptor owns its own
      // AST subtree, independent of aliasFilters. The planner may still
      // rewrite the original (rebindFilters, etc.) and cached plans may
      // re-execute with re-bound parameters — shared references would let
      // an AST rewrite on one side corrupt the other.
      edgeFilter = intermediateFilter.copy();

      var intermediateClass = aliasClasses.get(intermediateAlias);
      if (intermediateClass != null) {
        indexFilter = TraversalPreFilterHelper.findIndexForFilter(
            intermediateFilter, intermediateClass, ctx);
      }
      // indexFilter may be null (no index) or a partial cover — both are OK
      // because edgeFilter will re-verify every edge post-load.
    }

    return new ChainSemiJoin(
        prevEdgeClass, direction, ridExpr,
        sourceAlias, backRefAlias, intermediateAlias,
        targetAliasJ, indexFilter, edgeFilter);
  }

  /**
   * Returns {@code true} if the given WHERE clause references the
   * {@code $currentMatch} identifier anywhere in its AST.
   *
   * <p>Uses a hybrid strategy: recurse structurally through block types
   * ({@link SQLOrBlock}, {@link SQLAndBlock}, {@link SQLNotBlock}) and
   * fall back to a quote-aware token scan for anything else — leaf
   * condition types (binary / NOT IN / BETWEEN / ...) plus opaque
   * wrappers whose inner structure is not publicly accessible. The
   * token scan skips text enclosed in single or double quotes, so a
   * literal like {@code name = '$currentMatchThing'} no longer
   * false-positives and forces the Pattern A / B optimization to bail
   * unnecessarily.
   *
   * <p>The identifier check is anchored at a word boundary (the next
   * character must not be an identifier part), so {@code $currentMatchX}
   * — a hypothetical unrelated variable — does not match either.
   */
  private static boolean refersToCurrentMatch(SQLWhereClause filter) {
    var base = filter.getBaseExpression();
    return base != null && refersToCurrentMatch(base);
  }

  private static boolean refersToCurrentMatch(SQLBooleanExpression expr) {
    if (expr instanceof SQLOrBlock or) {
      var subs = or.getSubBlocks();
      if (subs != null) {
        for (var sub : subs) {
          if (refersToCurrentMatch(sub)) {
            return true;
          }
        }
      }
      return false;
    }
    if (expr instanceof SQLAndBlock and) {
      var subs = and.getSubBlocks();
      if (subs != null) {
        for (var sub : subs) {
          if (refersToCurrentMatch(sub)) {
            return true;
          }
        }
      }
      return false;
    }
    if (expr instanceof SQLNotBlock not) {
      return not.getSub() != null && refersToCurrentMatch(not.getSub());
    }
    return leafContainsCurrentMatchIdentifier(expr.toString());
  }

  /**
   * Returns {@code true} if {@code serialized} contains the token
   * {@code $currentMatch} outside of any single- or double-quoted string
   * literal, at a word boundary. The serialized form of a leaf boolean
   * expression is re-entrant (its toString is a valid YQL fragment), so
   * string literals use standard SQL quoting and every {@code $}-prefixed
   * identifier is emitted literally.
   */
  private static boolean leafContainsCurrentMatchIdentifier(String serialized) {
    if (serialized == null) {
      return false;
    }
    var len = serialized.length();
    var token = "$currentMatch";
    var inSingle = false;
    var inDouble = false;
    for (var i = 0; i < len; i++) {
      var c = serialized.charAt(i);
      if (inSingle) {
        if (c == '\\' && i + 1 < len) {
          i++;
        } else if (c == '\'') {
          inSingle = false;
        }
        continue;
      }
      if (inDouble) {
        if (c == '\\' && i + 1 < len) {
          i++;
        } else if (c == '"') {
          inDouble = false;
        }
        continue;
      }
      if (c == '\'') {
        inSingle = true;
        continue;
      }
      if (c == '"') {
        inDouble = true;
        continue;
      }
      if (c == '$' && serialized.regionMatches(i, token, 0, token.length())) {
        var end = i + token.length();
        if (end >= len || !isIdentifierPart(serialized.charAt(end))) {
          return true;
        }
        i = end - 1;
      }
    }
    return false;
  }

  private static boolean isIdentifierPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Result of splitting a Pattern A target WHERE into the hash-joinable
   * {@code @rid} equality and everything else.
   *
   * @param safe     {@code true} when Pattern A may fire — either the target
   *                 has no residual, or the residual has been cleanly
   *                 extracted and does not reference any build-phase-unsafe
   *                 variables
   * @param residual the residual to re-evaluate on the loaded target entity,
   *                 or {@code null} when there is nothing to re-evaluate
   */
  record ResidualExtraction(boolean safe, @Nullable SQLWhereClause residual) {
  }

  private static final ResidualExtraction RESIDUAL_SAFE_NONE =
      new ResidualExtraction(true, null);
  private static final ResidualExtraction RESIDUAL_UNSAFE =
      new ResidualExtraction(false, null);

  /**
   * Extracts the non-{@code @rid} residual of a Pattern A target WHERE clause.
   * Returns {@link #RESIDUAL_UNSAFE} when the clause cannot be flattened to
   * a single conjunction (e.g. multi-branch OR) or when the residual
   * references {@code $matched}/{@code $currentMatch} — those variables have
   * no defined value at hash-build time, so evaluating them per loaded
   * target would read stale context state.
   *
   * <p>Handles the MATCH planner's typical double-wrapping
   * ({@code AND[OR[AND[@rid, ...]]]}) by flattening all transparent
   * single-element OR/AND wrappers before locating the {@code @rid} term,
   * matching the recursive behavior of {@link SQLWhereClause#findRidEquality}.
   */
  private static ResidualExtraction extractTargetResidual(
      @Nullable SQLWhereClause targetFilter) {
    if (targetFilter == null) {
      return RESIDUAL_SAFE_NONE;
    }
    var base = targetFilter.getBaseExpression();
    if (base == null) {
      return RESIDUAL_SAFE_NONE;
    }
    var atoms = new ArrayList<SQLBooleanExpression>();
    if (!flattenConjunction(base, atoms)) {
      return RESIDUAL_UNSAFE;
    }
    var residualAtoms = new ArrayList<SQLBooleanExpression>(atoms.size());
    var foundRid = false;
    for (var atom : atoms) {
      if (!foundRid && isRidEqualityAtom(atom)) {
        foundRid = true;
      } else {
        residualAtoms.add(atom);
      }
    }
    if (!foundRid) {
      return RESIDUAL_UNSAFE;
    }
    if (residualAtoms.isEmpty()) {
      return RESIDUAL_SAFE_NONE;
    }
    // Deep-copy each atom so the residual owns its own AST subtree, independent
    // of the original targetFilter. The original stays in place for other
    // planner passes (rebindFilters, index detection) and the residual is
    // stashed on the SingleEdgeSemiJoin descriptor, which may be re-used when
    // a cached plan re-executes with re-bound parameters — shared references
    // would let an AST rewrite on one side corrupt the other. Mirrors the
    // policy applied in SQLWhereClause.buildWhereWithoutTerm.
    var newAnd = new SQLAndBlock(-1);
    for (var atom : residualAtoms) {
      newAnd.getSubBlocks().add(atom.copy());
    }
    var newOr = new SQLOrBlock(-1);
    newOr.getSubBlocks().add(newAnd);
    var residual = new SQLWhereClause(-1);
    residual.setBaseExpression(newOr);

    var refs = newAnd.getMatchPatternInvolvedAliases();
    if (refs != null && !refs.isEmpty()) {
      return RESIDUAL_UNSAFE;
    }
    if (refersToCurrentMatch(residual)) {
      return RESIDUAL_UNSAFE;
    }
    return new ResidualExtraction(true, residual);
  }

  /**
   * Flattens a conjunction expression, peeling single-element OR/AND
   * wrappers recursively and collecting leaf boolean terms into
   * {@code atoms}. Returns {@code false} when a multi-branch OR is
   * encountered (not a pure conjunction) or the expression is otherwise
   * not flattenable.
   */
  private static boolean flattenConjunction(
      SQLBooleanExpression expr, List<SQLBooleanExpression> atoms) {
    if (expr instanceof SQLOrBlock or) {
      if (or.getSubBlocks().size() != 1) {
        return false;
      }
      return flattenConjunction(or.getSubBlocks().getFirst(), atoms);
    }
    if (expr instanceof SQLAndBlock and) {
      for (var sub : and.getSubBlocks()) {
        if (!flattenConjunction(sub, atoms)) {
          return false;
        }
      }
      return true;
    }
    atoms.add(expr);
    return true;
  }

  /**
   * Returns {@code true} if the given already-flattened atomic term is a
   * {@code @rid = <expr>} equality. Wraps the atom in a single-term WHERE
   * clause and delegates to {@link SQLWhereClause#findRidEquality} to avoid
   * duplicating the RID-matching logic.
   */
  private static boolean isRidEqualityAtom(SQLBooleanExpression atom) {
    var tmpAnd = new SQLAndBlock(-1);
    tmpAnd.getSubBlocks().add(atom);
    var tmpWhere = new SQLWhereClause(-1);
    tmpWhere.setBaseExpression(tmpAnd);
    return tmpWhere.findRidEquality() != null;
  }

  /**
   * Attempts to detect and attach a Pattern B (ChainSemiJoin) descriptor on
   * {@code edgeJ}. If detection succeeds, the predecessor edge is marked as
   * consumed so {@code addStepsFor()} skips it.
   */
  private void tryAttachChainSemiJoin(
      List<EdgeTraversal> schedule,
      int j,
      EdgeTraversal edgeJ,
      List<String> involvedAliases,
      SQLExpression ridExpr,
      String targetAliasJ,
      Set<String> boundAliases,
      CommandContext ctx) {
    var chainDesc = detectChainSemiJoin(
        schedule, j, involvedAliases, ridExpr, targetAliasJ,
        boundAliases, ctx);
    if (chainDesc != null) {
      var consumed = schedule.get(j - 1);
      edgeJ.setSemiJoinDescriptor(chainDesc);
      consumed.setConsumed(true);
      edgeJ.setConsumedPredecessor(consumed);
      logger.debug(
          "MATCH pre-filter: ChainSemiJoin on edge[{},{}] "
              + "({}({}) chain semi-join via $matched.{})",
          j - 1, j, chainDesc.direction(), chainDesc.edgeClass(),
          chainDesc.backRefAlias());
    }
  }

  /**
   * Detects whether an optional edge with a {@code $matched.X.@rid} correlation
   * can be replaced with a correlated hash lookup. The pattern is:
   * {@code .out('LABEL'){where: (@rid = $matched.ALIAS.@rid), optional: true}}
   *
   * @return descriptor or null if the pattern is not detected
   */
  record CorrelatedOptionalDesc(
      String correlatedAlias, String probeAlias, String targetAlias,
      String edgeLabel, boolean edgeOut) {
  }

  @Nullable private CorrelatedOptionalDesc detectCorrelatedOptionalJoin(EdgeTraversal edge) {
    // Must be optional
    if (!edge.edge.in.isOptionalNode()) {
      return null;
    }

    var filter = edge.edge.item.getFilter();
    if (filter == null) {
      return null;
    }

    // No WHILE or maxDepth
    if (filter.getWhileCondition() != null || filter.getMaxDepth() != null) {
      return null;
    }

    // Must have a WHERE filter
    var whereClause = filter.getFilter();
    if (whereClause == null) {
      return null;
    }

    // The WHERE filter must be a simple RID correlation: @rid = $matched.X.@rid
    // Use regex to extract the correlated alias robustly (handles whitespace,
    // both operand orders, and rejects complex multi-condition filters).
    var filterStr = whereClause.toString();
    var matcher = MATCHED_RID_PATTERN.matcher(filterStr);
    if (!matcher.find()) {
      return null;
    }
    var correlatedAlias = matcher.group(1);

    // Reject if there are multiple $matched references (complex filter)
    if (matcher.find()) {
      return null;
    }

    // Verify the filter also references @rid on the other side (simple equality)
    // Count @rid occurrences — must be exactly 2 (one for each side of =)
    long ridCount = RID_PATTERN.matcher(filterStr).results().count();
    if (ridCount != 2) {
      return null;
    }

    // The correlated alias must already be visited (it's in aliasClasses or known)
    if (aliasClasses.get(correlatedAlias) == null
        && !aliasPinnedRids.containsKey(correlatedAlias)
        && !pattern.aliasToNode.containsKey(correlatedAlias)) {
      return null;
    }

    // Edge must be a simple directional method
    var edgeLabel = getEdgeClassName(edge);
    if (edgeLabel == null) {
      return null;
    }
    var direction = getEdgeDirection(edge);
    if (direction == null || (!"out".equals(direction) && !"in".equals(direction))) {
      return null;
    }

    var probeAlias = edge.out ? edge.edge.out.alias : edge.edge.in.alias;
    var targetAlias = edge.out ? edge.edge.in.alias : edge.edge.out.alias;

    return new CorrelatedOptionalDesc(
        correlatedAlias, probeAlias, targetAlias, edgeLabel, "out".equals(direction));
  }

  /**
   * Detects whether a WHILE edge can be replaced with an inverted reachability
   * hash filter. The pattern is: unconditional WHILE ({@code while: (true)})
   * with a simple WHERE filter, no $matched dependency, no depth/path alias,
   * and a simple directional edge method (out/in with single label).
   *
   * @return {@code true} if the edge qualifies for inverted-WHILE optimization
   */
  private boolean canUseInvertedWhileJoin(EdgeTraversal edge) {
    var filter = edge.edge.item.getFilter();
    if (filter == null) {
      return false;
    }

    // Must have a WHILE condition
    var whileCondition = filter.getWhileCondition();
    if (whileCondition == null) {
      return false;
    }

    // WHILE must be unconditional: (true). Check via the base expression.
    var baseExpr = whileCondition.getBaseExpression();
    if (baseExpr == null) {
      return false;
    }
    // The WHILE(true) condition parses as a SQLBooleanExpression wrapping "true"
    var baseStr = baseExpr.toString().strip().toLowerCase(Locale.ROOT);
    if (!baseStr.equals("true") && !baseStr.equals("(true)")) {
      return false;
    }

    // Must have a WHERE filter on the target node
    var targetFilter = filter.getFilter();
    if (targetFilter == null) {
      return false;
    }

    // No $matched/$parent dependency
    if (filterDependsOnContext(targetFilter)) {
      return false;
    }

    // No depth or path alias (user doesn't need traversal metadata)
    if (filter.getDepthAlias() != null || filter.getPathAlias() != null) {
      return false;
    }

    // The source alias (probe side) must be known — it's the alias whose RID
    // we probe against the reachable set. The target alias doesn't need a class
    // constraint; we find the anchor vertex via the WHERE filter by scanning the
    // source alias's connected class or the entire edge's target class.
    // For now, we need at least the edge label to determine the traversal.

    // Edge must be a simple directional method (out/in with single label)
    var edgeLabel = getEdgeClassName(edge);
    if (edgeLabel == null) {
      return false;
    }

    var direction = getEdgeDirection(edge);
    return "out".equals(direction) || "in".equals(direction);
  }

  /**
   * Returns the edge class name from an {@link EdgeTraversal}'s path item
   * method, or {@code null} if none is specified.
   */
  @Nullable static String getEdgeClassName(EdgeTraversal et) {
    var method = et.edge.item.getMethod();
    if (method == null) {
      return null;
    }
    var params = method.getParams();
    if (params == null || params.isEmpty()) {
      return null;
    }
    var expr = params.getFirst();
    if (!(expr.getMathExpression() instanceof SQLBaseExpression base)) {
      return null;
    }
    if (base.getModifier() != null) {
      return null;
    }
    var value = base.execute((Result) null, new BasicCommandContext());
    if (value instanceof String s && !s.isEmpty()) {
      // Strip surrounding quotes if present (defensive — execute() typically
      // returns the unquoted string, but some AST paths may retain quotes)
      var label = s;
      if (label.length() >= 2
          && ((label.charAt(0) == '\'' && label.charAt(label.length() - 1) == '\'')
              || (label.charAt(0) == '"' && label.charAt(label.length() - 1) == '"'))) {
        label = label.substring(1, label.length() - 1);
      }
      // Validate: edge class names must be valid identifiers. Reject anything
      // containing characters that could break SQL string interpolation (e.g.,
      // single quotes from escaped literals in the MATCH parser).
      if (!VALID_EDGE_LABEL.matcher(label).matches()) {
        return null;
      }
      return label;
    }
    return null;
  }

  /**
   * Returns the traversal direction ({@code "out"} or {@code "in"}) for the
   * given edge traversal, considering the scheduled direction.
   */
  @Nullable static String getEdgeDirection(EdgeTraversal et) {
    var method = et.edge.item.getMethod();
    if (method == null) {
      return null;
    }
    var nameStr = method.getMethodNameString();
    if (nameStr == null) {
      return null;
    }
    var syntacticDirection = nameStr.toLowerCase(Locale.ROOT);
    if (et.out) {
      return syntacticDirection;
    }
    // Reverse: flip out↔in
    return switch (syntacticDirection) {
      case "out" -> "in";
      case "in" -> "out";
      default -> syntacticDirection;
    };
  }

  /**
   * Returns the lowercased method name for the given edge traversal
   * (e.g. {@code "out"}, {@code "oute"}, {@code "inv"}).
   */
  @Nullable static String getMethodName(EdgeTraversal et) {
    var method = et.edge.item.getMethod();
    if (method == null) {
      return null;
    }
    var name = method.getMethodNameString();
    return name != null ? name.toLowerCase(Locale.ROOT) : null;
  }

  /**
   * Computes a dependency map for the topological scheduler. An alias `A` depends on
   * alias `B` if `A`'s `WHERE` clause contains a reference to `$matched.B` — meaning
   * that `B` must be resolved (visited) before `A` can be evaluated.
   *
   * @param pattern the pattern whose nodes' filters are analyzed
   * @return a map from alias name to the set of alias names it depends on
   */
  private Map<String, Set<String>> getDependencies(Pattern pattern) {
    Map<String, Set<String>> result = new HashMap<>();

    for (var node : pattern.aliasToNode.values()) {
      Set<String> currentDependencies = new HashSet<>();

      var filter = aliasFilters.get(node.alias);
      if (filter != null && filter.getBaseExpression() != null) {
        var involvedAliases = filter.getBaseExpression().getMatchPatternInvolvedAliases();
        if (involvedAliases != null) {
          currentDependencies.addAll(involvedAliases);
        }
      }

      result.put(node.alias, currentDependencies);
    }

    return result;
  }

  /**
   * Splits the global pattern graph into its connected components (disjoint sub-patterns).
   * Each connected component will be planned independently; the results are later joined
   * via a {@link CartesianProductStep}. This method is idempotent.
   */
  private void splitDisjointPatterns() {
    if (this.subPatterns != null) {
      return;
    }

    this.subPatterns = pattern.getDisjointPatterns();
  }

  /**
   * Emits execution steps for a single edge traversal.
   *
   * <p>When {@code first} is {@code true}, two steps are chained: a {@link MatchFirstStep}
   * (initial record scan for the source node) followed by a {@link MatchStep} or
   * {@link OptionalMatchStep}. When {@code first} is {@code false}, only the traversal
   * step is appended — the source node was already produced by a preceding step.
   *
   * <pre>
   *   first=true:   plan ← MatchFirstStep(source) ← MatchStep(edge)
   *   first=false:  plan ← MatchStep(edge)
   *
   *   Note: MatchStep is ALWAYS appended, even when first=true. The MatchFirstStep
   *   only provides the initial record set — the MatchStep still performs the actual
   *   edge traversal from that starting point.
   * </pre>
   *
   * If the target node of the edge is optional, an {@link OptionalMatchStep} is used
   * instead of a regular {@link MatchStep}.
   *
   * @param prefetchedAliases aliases a {@link MatchPrefetchStep} already loads into the context;
   *                          a root node whose alias is in this set gets the sub-plan-free
   *                          {@link MatchFirstStep} constructor
   */
  private void addStepsFor(
      SelectExecutionPlan plan,
      EdgeTraversal edge,
      CommandContext context,
      Set<String> prefetchedAliases,
      boolean first,
      @Nullable IndexOrderedPlanner.IndexOrderedCandidate candidate,
      boolean profilingEnabled) {
    if (first) {
      var patternNode = edge.out ? edge.edge.out : edge.edge.in;
      if (prefetchedAliases.contains(patternNode.alias)) {
        // A MatchPrefetchStep earlier in the chain has already loaded this alias, and
        // MatchFirstStep.internalStart reads that cache instead of starting a sub-plan. Building
        // a sub-plan anyway hung a plan that never runs off the root step, so a caller tallying
        // fetches through getSubSteps() counted the alias twice for a query that fetches it once.
        // The edge-free branch of createPlanForPattern already skips the sub-plan this way.
        plan.chain(new MatchFirstStep(context, patternNode, profilingEnabled));
      } else {
        var clazz = aliasClasses.get(patternNode.alias);
        var pinnedRids = pinnedRidsForAlias(patternNode.alias);
        var where = fetchFilterFor(patternNode.alias, pinnedRids);
        // Shared builder rather than a hand-rolled copy of it. The copy tested the class before
        // the RIDs, so a pattern root carrying both scanned the class and demoted the pinned list
        // to a post-filter — and the prefetch filter only spares aliases estimated under
        // THRESHOLD, so the aliases that reached here were exactly the large RID lists that most
        // needed the fetch. createSelectStatement takes the RIDs first, which is safe because
        // promoteStaticRidsFromFilters only pins RIDs it has proved are inside the alias's class
        // (see pinnedRidsProvablyInClass); a parser `rid:` slot pins one RID and estimates at 1,
        // so it is prefetched through the same builder and never arrives here.
        var select =
            createSelectStatement(clazz, pinnedRids, where == null ? null : where.copy());
        var subContext = new BasicCommandContext();
        subContext.setParentWithoutOverridingChild(context);
        plan.chain(
            new MatchFirstStep(
                context,
                patternNode,
                select.createExecutionPlan(subContext, profilingEnabled),
                profilingEnabled));
      }
    }
    // Skip edges consumed by a ChainSemiJoin on the next edge — the
    // BackRefHashJoinStep on the next edge covers both.
    if (edge.isConsumed()) {
      return;
    }
    if (edge.edge.in.isOptionalNode()) {
      // Check if this optional edge can be replaced with a correlated hash lookup
      var correlatedDesc = detectCorrelatedOptionalJoin(edge);
      if (correlatedDesc != null) {
        // Correlated optional hash join: pre-materialize neighbor set, probe per row
        plan.chain(new CorrelatedOptionalHashJoinStep(
            context,
            correlatedDesc.correlatedAlias(),
            correlatedDesc.probeAlias(),
            correlatedDesc.targetAlias(),
            correlatedDesc.edgeLabel(),
            correlatedDesc.edgeOut(),
            profilingEnabled));
        // Still set foundOptional so RemoveEmptyOptionalsStep is present
        // (no-op for our null values, but needed for other optional nodes)
        foundOptional = true;
      } else {
        foundOptional = true;
        plan.chain(new OptionalMatchStep(context, edge, profilingEnabled));
      }
    } else if (canUseInvertedWhileJoin(edge)) {
      // Replace WHILE recursion with pre-materialized reachability hash filter
      var targetAlias = edge.out ? edge.edge.in.alias : edge.edge.out.alias;
      var probeAlias = edge.out ? edge.edge.out.alias : edge.edge.in.alias;
      var rawFilter = edge.edge.item.getFilter().getFilter();
      var targetFilter = rawFilter != null ? rawFilter.copy() : null;
      var edgeLabel = getEdgeClassName(edge);
      var edgeDirection = "out".equals(getEdgeDirection(edge));
      // Anchor class from target alias — the WHERE filter applies to the target,
      // not the probe. If the alias has no explicit class (common for WHILE
      // targets where class inference is skipped), infer from edge LINK schema.
      // For out('IS_SUBCLASS_OF') this infers TagClass from the edge's "in" LINK.
      var anchorClass = aliasClasses.get(targetAlias);
      if (anchorClass == null) {
        anchorClass = inferClassFromEdgeSchema(
            edge.edge.item.getMethod(), null, context);
      }
      if (anchorClass != null) {
        plan.chain(new InvertedWhileHashJoinStep(
            context, anchorClass, targetFilter, edgeLabel, edgeDirection,
            probeAlias, targetAlias, edge, profilingEnabled));
      } else {
        // Cannot determine anchor class — fall back to standard WHILE traversal
        // to avoid catastrophic full V scan
        plan.chain(new MatchStep(context, edge, profilingEnabled));
      }
    } else if (edge.getSemiJoinDescriptor() instanceof AntiSemiJoin) {
      // Pattern D: normal traversal + anti-join filter. The NOT IN condition
      // was stripped from the WHERE clause at plan time so the MatchStep
      // traverses without it. The BackRefHashJoinStep filters results against
      // the exclusion set; on build failure it evaluates the stored NOT IN
      // condition per row as a correctness fallback.
      plan.chain(new MatchStep(context, edge, profilingEnabled));
      plan.chain(new BackRefHashJoinStep(
          context, edge.getSemiJoinDescriptor(), null, null, profilingEnabled));
    } else if (edge.getSemiJoinDescriptor() != null) {
      // Back-reference semi-join (Pattern A/B): replace per-row link bag
      // traversal with a one-time hash table build + O(1) probe. The
      // EdgeTraversal is passed for runtime fallback if the build fails.
      // For Pattern B (ChainSemiJoin), the consumed predecessor edge is
      // also passed so the fallback can traverse both edges sequentially.
      plan.chain(new BackRefHashJoinStep(
          context, edge.getSemiJoinDescriptor(), edge,
          edge.getConsumedPredecessor(), profilingEnabled));
    } else if (candidate != null
        && IndexOrderedPlanner.isIndexOrderedEdge(candidate, edge)) {
      // Index-ordered traversal: replace MatchStep with IndexOrderedEdgeStep
      plan.chain(new IndexOrderedEdgeStep(
          context,
          candidate.sourceAlias(),
          candidate.targetAlias(),
          candidate.edgeClassName(),
          candidate.linkBagFieldName(),
          candidate.index(),
          candidate.orderAsc(),
          candidate.comparisonItem(),
          candidate.nullsPlacement(),
          edge,
          candidate.limit(),
          candidate.multiSourceMode(),
          candidate.reverseFieldName(),
          candidate.sourceClassName(),
          candidate.targetFilter(),
          candidate.targetClassName(),
          candidate.isEdgeTraversal(),
          candidate.downstreamEdgeCount(),
          candidate.ridTieBreakAccepted(),
          profilingEnabled));
    } else {
      plan.chain(new MatchStep(context, edge, profilingEnabled));
    }
  }

  /**
   * For each alias in `aliasesToPrefetch`, creates a {@link MatchPrefetchStep} that
   * eagerly loads all matching records into the execution context under a well-known key.
   * Subsequent {@link MatchFirstStep}s will read from this cache instead of re-scanning.
   */
  private void addPrefetchSteps(
      SelectExecutionPlan result,
      Set<String> aliasesToPrefetch,
      CommandContext context,
      boolean profilingEnabled) {
    for (var alias : aliasesToPrefetch) {
      var targetClass = aliasClasses.get(alias);
      var pinnedRids = pinnedRidsForAlias(alias);
      var filter = fetchFilterFor(alias, pinnedRids);
      var prefetchStm =
          createSelectStatement(targetClass, pinnedRids, filter);

      var step =
          new MatchPrefetchStep(
              context,
              prefetchStm.createExecutionPlan(context, profilingEnabled),
              alias,
              profilingEnabled);
      result.chain(step);
    }
  }

  /**
   * Builds a synthetic {@code SELECT} that fetches from a RID list, scans a class,
   * or both (RID list plus {@code WHERE}). Used as the initial record source for
   * {@link MatchFirstStep} and {@link MatchPrefetchStep}.
   */
  private static SQLSelectStatement createSelectStatement(
      String targetClass, @Nullable List<SQLRid> targetRids, SQLWhereClause filter) {
    var prefetchStm = new SQLSelectStatement(-1);
    prefetchStm.setWhereClause(filter);
    var from = new SQLFromClause(-1);
    var fromItem = new SQLFromItem(-1);
    if (targetRids != null && !targetRids.isEmpty()) {
      fromItem.setRids(targetRids);
    } else if (targetClass != null) {
      fromItem.setIdentifier(new SQLIdentifier(targetClass));
    }
    from.setItem(fromItem);
    prefetchStm.setTarget(from);
    return prefetchStm;
  }

  /**
   * Lazily builds the internal pattern graph and extracts per-alias metadata.
   *
   * <p>Steps performed:
   * 1. Assign auto-generated aliases to any unnamed pattern nodes.
   * 2. Convert each {@link SQLMatchExpression} into {@link PatternNode}/{@link PatternEdge}
   *    pairs inside the {@link Pattern} graph.
   * 3. Collect per-alias `WHERE` filters, class constraints, and RID constraints,
   *    merging constraints when the same alias appears in multiple expressions.
   * 4. Rebind the merged filters back onto the original expressions so that subsequent
   *    traversal steps see the consolidated predicates.
   *
   * <p>This method is idempotent — it returns immediately if already called.
   */
  private void buildPatterns(CommandContext ctx) {
    if (this.pattern != null) {
      // Additive path (pre-built pattern + aliasFilters, e.g. the Gremlin-to-MATCH translator):
      // the pattern and per-alias filters are already populated, so skip the matchExpressions
      // rebuild below. Static @rid = / @rid IN filters still need promoting to aliasPinnedRids so
      // the RID fast path and the collapsed root estimate apply, exactly as the rebuild branch
      // does for the SQL path. Scoped by promoteFilterRidsOnBuild so only the MatchPlanInputs path
      // (which sets a mutable aliasPinnedRids) promotes; the GQL 3-arg ctor keeps its plans.
      if (promoteFilterRidsOnBuild) {
        this.promotedRidAliases = promoteStaticRidsFromFilters(
            this.aliasFilters, this.aliasClasses, this.aliasPinnedRids, ctx);
      }
      return;
    }
    List<SQLMatchExpression> allPatterns = new ArrayList<>();
    allPatterns.addAll(this.matchExpressions);
    allPatterns.addAll(this.notMatchExpressions);

    assignDefaultAliases(allPatterns);

    pattern = new Pattern();
    for (var expr : this.matchExpressions) {
      pattern.addExpression(expr);
    }

    Map<String, SQLWhereClause> aliasFilters = new LinkedHashMap<>();
    Map<String, String> aliasClasses = new LinkedHashMap<>();
    Map<String, String> aliasCollections = new LinkedHashMap<>();
    Map<String, List<SQLRid>> aliasPinnedRids = new LinkedHashMap<>();
    // Collect aliases that are directly part of a while-condition's recursive
    // zone (origin + while-item).  Class inference on these specific aliases
    // must be skipped — inferred classes change cost estimates which can
    // reorder the schedule, causing while/where recursive steps to be
    // traversed in the wrong direction.  Non-recursive aliases in the same
    // expression (downstream of the while) are safe to infer.
    var whileAliases = collectAliasesFromWhilePatterns(this.matchExpressions);
    var inferredAliases = new HashSet<String>();
    for (var expr : this.matchExpressions) {
      addAliases(expr, aliasFilters, aliasClasses, aliasCollections, aliasPinnedRids,
          ctx, whileAliases, inferredAliases);
    }

    this.aliasFilters = aliasFilters;
    this.aliasClasses = aliasClasses;
    this.aliasPinnedRids = aliasPinnedRids;
    this.inferredWhileExprAliases = inferredAliases;

    // Promote static `@rid = <literal|param>` and `@rid IN [...]` filters into
    // aliasPinnedRids before estimateRootEntries() and the topological
    // schedule run. With a RID slot populated, the estimate collapses to the list
    // size, so a 2M-row class restricted to a few RIDs stops losing root selection
    // to a 10K-row unfiltered class.
    // The promotion also lets createSelectStatement() take the FetchFromRids
    // fast path instead of a class scan with post-filter. aliasClasses is passed in because that
    // swap discards the alias's class — createSelectStatement treats RIDs and class as mutually
    // exclusive — so the promoter declines whenever the pinned RIDs are not provably inside the
    // class (see pinnedRidsProvablyInClass).
    // Back-refs (`@rid = $matched.X.@rid`) are skipped: those are bound at
    // runtime and are handled by EdgeRidLookup or Pattern A back-ref hash
    // join in the pre-filter pass.
    // The @rid term stays in aliasFilters on purpose. For {@code @rid = <expr>}
    // on a non-root alias, the DirectRid pre-filter pass on the producing edge
    // still relies on findRidEquality(). {@code @rid IN <list>} has no
    // RidFilterDescriptor analogue; non-root IN lists are enforced via prefetch
    // or post-fetch WHERE evaluation instead.
    this.promotedRidAliases =
        promoteStaticRidsFromFilters(aliasFilters, aliasClasses, aliasPinnedRids, ctx);

    rebindFilters(aliasFilters);
  }

  /**
   * Promotes static {@code @rid = <literal|param>} equalities and
   * {@code @rid IN <static-list>} into {@code aliasPinnedRids} so that
   * {@link #estimateRootEntries} returns the pinned cardinality and
   * {@link #createSelectStatement} uses a direct RID fetch.
   *
   * <p>The original {@code @rid} term is intentionally left in the filter. When
   * the alias does not end up as a root, the pre-filter pass on the producing
   * edge uses {@link SQLWhereClause#findRidEquality()} to attach a
   * {@code DirectRid} intersection descriptor for singleton equalities only.
   * {@code @rid IN <list>} has no matching pre-filter descriptor; those aliases
   * rely on prefetch / {@link #createSelectStatement} or post-fetch WHERE.
   *
   * <p>Skipped:
   * <ul>
   *   <li>aliases that already have a RID slot (e.g.
   *       {@code {as: a, rid: #1:2}})
   *   <li>{@code @rid = $matched.X.@rid} back-refs (handled by
   *       {@code EdgeRidLookup} or Pattern A back-ref hash join)
   *   <li>expressions that are not early-calculable (depend on per-row context)
   *   <li>{@code @rid IN <subquery>} or non-static right-hand sides
   *   <li>aliases whose class constraint the pinned RIDs are not provably inside
   *       (see {@link #pinnedRidsProvablyInClass})
   * </ul>
   *
   * @param aliasClasses per-alias class constraints, consulted so a promotion never
   *                     silently discards one
   * @return the aliases this call promoted, which is what tells {@link #fetchFilterFor} that an
   *         alias's {@code @rid} term is the same list its fetch target enumerates
   */
  // Visible for testing
  static Set<String> promoteStaticRidsFromFilters(
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, String> aliasClasses,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext ctx) {
    Set<String> promoted = new HashSet<>();
    for (var entry : aliasFilters.entrySet()) {
      var alias = entry.getKey();
      var filter = entry.getValue();
      if (filter == null) {
        continue;
      }
      // Don't overwrite an explicit RID slot from the parser.
      if (aliasPinnedRids.containsKey(alias)) {
        continue;
      }
      var aliasClass = aliasClasses.get(alias);
      var ridExpr = filter.findRidEquality();
      if (ridExpr != null) {
        var involved = ridExpr.getMatchPatternInvolvedAliases();
        if (involved != null && !involved.isEmpty()) {
          continue;
        }
        if (!ridExpr.isEarlyCalculated(ctx)) {
          continue;
        }
        var rid = new SQLRid(-1);
        SQLRid.internalPromoteExpression(rid, ridExpr);
        var promotedEquality = List.of(rid);
        if (!pinnedRidsProvablyInClass(promotedEquality, aliasClass, ctx)) {
          continue;
        }
        aliasPinnedRids.put(alias, promotedEquality);
        promoted.add(alias);
        logger.debug(
            "MATCH planner: promoted @rid = filter to aliasPinnedRids for alias '{}'",
            alias);
        continue;
      }

      var ridIn = filter.findRidInList();
      if (ridIn == null) {
        continue;
      }
      var involved = ridIn.getMatchPatternInvolvedAliases();
      if (involved != null && !involved.isEmpty()) {
        continue;
      }
      var promotedList = toPromotedSqlRidList(ridIn, ctx);
      if (promotedList == null || promotedList.isEmpty()) {
        continue;
      }
      if (!pinnedRidsProvablyInClass(promotedList, aliasClass, ctx)) {
        continue;
      }
      aliasPinnedRids.put(alias, promotedList);
      promoted.add(alias);
      logger.debug(
          "MATCH planner: promoted @rid IN filter ({} RIDs) for alias '{}'",
          promotedList.size(),
          alias);
    }
    return promoted;
  }

  /**
   * Returns the WHERE clause to attach to an alias's direct-RID fetch: the alias filter without
   * the {@code @rid} term the fetch target already enforces, or the filter unchanged when the term
   * cannot be removed safely.
   *
   * <p>A promoted alias otherwise plans as {@code SELECT FROM [#a, #b, …] WHERE @rid IN [#a, #b,
   * …]}. The RID target is the enumeration, so the predicate can only ever be true, and it is not
   * free: a RID target carries no class, so no index absorbs the term and it runs as a filter step
   * per fetched record. {@code SQLInCondition.evaluate} re-executes the literal collection and
   * materialises a fresh list of evaluated RIDs on each of those rows, then walks it, which makes
   * an N-RID lookup cost O(N^2) comparisons and allocations for N record loads.
   *
   * <p>Removal is refused unless the term is provably the promoted one. Three conditions have to
   * hold together: the alias is one this planner's own promotion pinned (a parser {@code rid:}
   * slot pins a RID the filter never mentions, and stripping an unrelated {@code @rid} term there
   * would drop a live constraint); the extractor reaches the same term the promoter did, checked
   * by taking the equality branch exactly when the promoter would have; and what remains carries
   * no {@code @rid} term at all, which is what rules out a clause holding two of them where the
   * extractor removed the other one. Any failure returns the original filter and costs only the
   * optimisation.
   */
  @Nullable private SQLWhereClause fetchFilterFor(
      String alias, @Nullable List<SQLRid> pinnedRids) {
    var filter = aliasFilters.get(alias);
    if (filter == null
        || pinnedRids == null
        || pinnedRids.isEmpty()
        || !promotedRidAliases.contains(alias)) {
      return filter;
    }
    // The promoter tries the equality first and only looks for an IN list when there is no rid
    // equality at all, so branching on the same test reaches the same term it pinned from.
    var extracted =
        filter.findRidEquality() != null ? filter.extractRidEquality() : filter.extractRidInList();
    if (extracted == null) {
      return filter;
    }
    var remaining = extracted.remainingWhere();
    if (remaining != null
        && (remaining.findRidEquality() != null || remaining.findRidInList() != null)) {
      return filter;
    }
    return remaining;
  }

  /**
   * Whether every pinned RID provably belongs to {@code className} or one of its subclasses.
   *
   * <p>The promotion is only safe when it loses nothing. {@link #createSelectStatement} treats a
   * RID list and a class as mutually exclusive — a non-empty RID list sets the FROM target and the
   * class arm never runs — so promoting an alias that also carries a class silently drops that
   * class from the plan. Where the class is the only type constraint the alias has, the fetch then
   * resolves whatever record the RID names. A translated {@code g.V(rid).hasLabel(L)} under
   * polymorphic mode is exactly that shape: {@code HasStepRecogniser} re-types the boundary node to
   * {@code L} and deliberately adds no {@code @class} conjunct, so with the class gone the label
   * stops being checked at all and a record from an unrelated class is emitted.
   *
   * <p>Proof is by collection ownership, the same zero-I/O route
   * {@code MatchEdgeTraverser.matchesClass} takes: a RID's collection is owned by at most one
   * schema class, so the immutable schema snapshot answers membership without loading the record.
   * An unresolvable RID (a parameter expression that needs per-row bindings) or an unowned
   * collection returns {@code false} — the promotion is skipped and the plan falls back to the
   * class scan with the {@code @rid} post-filter, which is correct at any arity and is what the
   * planner emitted before the promotion learned to see code-assembled clauses.
   *
   * @param className the alias's class constraint, or {@code null} when it has none — with no
   *                  class to lose, every promotion is safe
   */
  private static boolean pinnedRidsProvablyInClass(
      List<SQLRid> rids, @Nullable String className, CommandContext ctx) {
    if (className == null) {
      return true;
    }
    // Collect the collection ids before reaching for the schema: a RID whose collection needs
    // per-row bindings to resolve is unprovable whatever the schema says, and answering that
    // first keeps the schema lookup off the path entirely.
    var collectionIds = new ArrayList<Integer>(rids.size());
    for (var rid : rids) {
      var collection = rid.getCollection();
      if (collection == null || collection.getValue() == null) {
        return false;
      }
      collectionIds.add(collection.getValue().intValue());
    }
    var session = ctx == null ? null : ctx.getDatabaseSession();
    if (session == null) {
      return false;
    }
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    if (schema == null) {
      return false;
    }
    for (var collectionId : collectionIds) {
      var owner = schema.getClassByCollectionId(collectionId);
      if (owner == null || !owner.isSubClassOf(className)) {
        return false;
      }
    }
    return true;
  }

  /** Returns the pinned RID list for an alias, or {@code null} if none. */
  @Nullable private List<SQLRid> pinnedRidsForAlias(String alias) {
    return aliasPinnedRids.get(alias);
  }

  @Nullable private static List<SQLRid> pinnedRidsForAlias(
      String alias,
      Map<String, List<SQLRid>> aliasPinnedRids) {
    return aliasPinnedRids.get(alias);
  }

  /** Returns the sole pinned RID when the list has exactly one entry (for {@code setLeftRid}). */
  // Visible for testing
  @Nullable static SQLRid singletonPinnedRid(@Nullable List<SQLRid> pinnedRids) {
    return pinnedRids != null && pinnedRids.size() == 1 ? pinnedRids.get(0) : null;
  }

  /**
   * Converts a static {@code @rid IN <expr>} condition into concrete {@link SQLRid}
   * nodes suitable for {@code SELECT FROM [#a, #b, ...]}.
   */
  @Nullable static List<SQLRid> toPromotedSqlRidList(SQLInCondition inCond, CommandContext ctx) {
    if (inCond.getRightStatement() != null) {
      return null;
    }
    if (!isEarlyCalculableInRight(inCond, ctx)) {
      return null;
    }
    Object rightVal;
    if (inCond.getRightParam() != null) {
      rightVal = inCond.getRightParam().getValue(ctx.getInputParameters());
    } else if (inCond.getRightMathExpression() != null) {
      rightVal = inCond.getRightMathExpression().execute((Result) null, ctx);
    } else {
      return null;
    }
    if (!(rightVal instanceof Iterable<?> iterable)) {
      return null;
    }
    // Duplicates are dropped, first occurrence wins. IN is set membership: a class scan with an
    // `@rid IN [#5:0, #5:0]` post-filter tests each record once and emits it once. The promoted
    // form enumerates the pinned list instead, so a duplicate left in place would fetch the same
    // record twice and change the result multiset — g.V().hasId(a, a) is the shape that exposes it.
    // Keyed on the (collection, position) pair rather than on SQLRid, which inherits identity
    // equality. The pair is carried by a value record and never folded into a single long: a
    // 32-bit collection id and a 64-bit position do not fit in 64 bits, and any fold collides —
    // #0:4294967296 and #1:0 share the packed key (collection << 32) ^ position, and a collision
    // silently drops the second RID from the list the promoted plan fetches.
    List<SQLRid> rids = new ArrayList<>();
    Set<RidKey> seen = new HashSet<>();
    for (var item : iterable) {
      var sqlRid = sqlRidFromRuntimeValue(item);
      if (sqlRid == null) {
        return null;
      }
      // sqlRidFromRuntimeValue is the only producer and always sets both fields, so the pair is
      // always available here; the assert states that rather than adding a dead runtime branch.
      var collection = sqlRid.getCollection();
      var position = sqlRid.getPosition();
      assert collection != null && position != null
          : "sqlRidFromRuntimeValue must populate both collection and position";
      if (seen.add(new RidKey(collection.getValue().intValue(), position.getValue().longValue()))) {
        rids.add(sqlRid);
      }
    }
    return rids.isEmpty() ? null : rids;
  }

  /**
   * Dedup key for a promoted RID: the (collection id, position) pair by value.
   *
   * <p>{@link SQLRid} inherits identity equality, so a set of RID nodes never dedups. The pair is
   * kept as a record rather than packed into a {@code long} because the two fields are 32 and 64
   * bits wide and any packing loses information — the same reason
   * {@code StartStepRecogniser.RidKey} exists on the translator side of the same feature.
   */
  private record RidKey(int collectionId, long position) {

  }

  private static boolean isEarlyCalculableInRight(SQLInCondition inCond, CommandContext ctx) {
    if (inCond.getRightParam() != null) {
      return true;
    }
    var math = inCond.getRightMathExpression();
    return math != null && math.isEarlyCalculated(ctx);
  }

  @Nullable private static SQLRid sqlRidFromRuntimeValue(Object value) {
    if (value instanceof Identifiable identifiable) {
      var identity = identifiable.getIdentity();
      var rid = new SQLRid(-1);
      var collection = new SQLInteger(-1);
      collection.setValue(identity.getCollectionId());
      var position = new SQLInteger(-1);
      position.setValue(identity.getCollectionPosition());
      rid.setLegacy(true);
      rid.setCollection(collection);
      rid.setPosition(position);
      return rid;
    }
    if (value instanceof String s) {
      RecordIdInternal parsed;
      try {
        parsed = RecordIdInternal.fromString(s, false);
      } catch (RuntimeException ignored) {
        // Drop any unparseable or out-of-range RID literal (a malformed string, or a
        // collection id past the limit — which throws DatabaseException, not
        // IllegalArgumentException) and abort promotion, so the retained WHERE filter
        // handles it. Matches QueryOperatorEquals's catch-all on the scan path and
        // SelectExecutionPlanner.toRecordIdCandidate, so a bad @rid literal yields an
        // empty result in either planner rather than a plan-time throw.
        return null;
      }
      if (parsed == null) {
        return null;
      }
      var rid = new SQLRid(-1);
      var collection = new SQLInteger(-1);
      collection.setValue(parsed.getCollectionId());
      var position = new SQLInteger(-1);
      position.setValue(parsed.getCollectionPosition());
      rid.setLegacy(true);
      rid.setCollection(collection);
      rid.setPosition(position);
      return rid;
    }
    return null;
  }

  /**
   * After per-alias filters have been merged in {@link #buildPatterns}, this method
   * pushes the consolidated `WHERE` clause back into each {@link SQLMatchFilter} inside
   * the original match expressions, so the traversal steps see the unified predicate.
   */
  private void rebindFilters(Map<String, SQLWhereClause> aliasFilters) {
    for (var expression : matchExpressions) {
      var newFilter = aliasFilters.get(expression.getOrigin().getAlias());
      expression.getOrigin().setFilter(newFilter);

      for (var item : expression.getItems()) {
        newFilter = aliasFilters.get(item.getFilter().getAlias());
        item.getFilter().setFilter(newFilter);
      }
    }
  }

  /**
   * Collects all aliases that appear in patterns containing a {@code while}
   * condition. Used to determine which patterns must skip class inference.
   */
  private static Set<String> collectAliasesFromWhilePatterns(
      List<SQLMatchExpression> expressions) {
    var result = new HashSet<String>();
    for (var expr : expressions) {
      for (var item : expr.getItems()) {
        if (item.getFilter() != null
            && item.getFilter().getWhileCondition() != null) {
          // Only the origin alias and the while-item's own alias are in the
          // recursive zone.  Downstream items (after the while in the pattern
          // chain) are not recursive and can safely have class inference —
          // their inferred classes do not affect while-traversal direction.
          if (expr.getOrigin() != null && expr.getOrigin().getAlias() != null) {
            result.add(expr.getOrigin().getAlias());
          }
          if (item.getFilter().getAlias() != null) {
            result.add(item.getFilter().getAlias());
          }
        }
      }
    }
    return result;
  }

  /**
   * Extracts alias metadata (filters, class, collection, RID) from a single match
   * expression and merges them into the accumulation maps.
   *
   * <p>Tracks a {@code currentEdgeClass} state across items: {@code outE('X')}/{@code inE('X')}
   * set it to {@code X}; {@code inV()}/{@code outV()} consume it for vertex class inference
   * then reset it; all other methods reset it to {@code null}.
   */
  // Visible for testing
  static void addAliases(
      SQLMatchExpression expr,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, String> aliasClasses,
      Map<String, String> aliasCollections,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context,
      Set<String> whileAliases,
      Set<String> inferredWhileExprAliases) {
    addAliases(expr.getOrigin(), aliasFilters, aliasClasses, aliasCollections, aliasPinnedRids,
        context);

    // Track the edge class set by the most recent outE/inE item, so that a
    // following inV/outV can look up the linked vertex class from the edge schema.
    // Reset after inV/outV consumes it, or when a non-edge-method item is seen.
    @Nullable String currentEdgeClass = null;

    for (var item : expr.getItems()) {
      if (item.getFilter() != null) {
        addAliases(item.getFilter(), aliasFilters, aliasClasses, aliasCollections, aliasPinnedRids,
            context);

        // Skip class inference only for aliases in the recursive zone
        // (origin + while-item).  Downstream aliases are safe to infer.
        var alias = item.getFilter().getAlias();
        boolean skipThisItem = alias != null && whileAliases.contains(alias);

        if (!skipThisItem) {
          // Determine the method name for edge-class state tracking.
          var method = item.getMethod();
          var methodName = method != null ? method.getMethodNameString() : null;
          var methodLower = methodName != null
              ? methodName.toLowerCase(Locale.ROOT) : null;

          // Update currentEdgeClass state based on the method type.
          // inV/outV is deferred: it consumes currentEdgeClass after inference below.
          var isInVOrOutV = "inv".equals(methodLower) || "outv".equals(methodLower);
          if ("oute".equals(methodLower) || "ine".equals(methodLower)
              || "bothe".equals(methodLower)) {
            // bothE('X') identifies the edge class just like outE/inE — store it so that
            // a subsequent inV/outV step can infer its linked vertex class.
            currentEdgeClass = extractEdgeClassName(method);
          } else if (!isInVOrOutV) {
            // Any other method (out, in, both, etc.) resets the state.
            currentEdgeClass = null;
          }

          // Infer target class from edge LINK schema when no explicit class
          // is set. Handles both vertex-to-vertex traversals (out/in) and
          // edge-method traversals (outE/inE/inV/outV).
          if (alias != null && !aliasClasses.containsKey(alias)) {
            var inferred = inferClassFromEdgeSchema(method, currentEdgeClass, context);
            if (inferred != null) {
              aliasClasses.put(alias, inferred);
              inferredWhileExprAliases.add(alias);
              logger.debug(
                  "MATCH class inference: alias '{}' -> class '{}' "
                      + "(from edge LINK schema)",
                  alias, inferred);
            }
          }

          // Reset currentEdgeClass after inV/outV consumes it.
          if (isInVOrOutV) {
            currentEdgeClass = null;
          }
        } else {
          // While-alias: reset edge class state since we skip inference.
          currentEdgeClass = null;
        }
      }
    }
  }

  /**
   * Infers the alias class from the edge schema LINK declarations.
   *
   * <p>Handles eight method types:
   * <ul>
   *   <li>{@code out('X')} / {@code in('X')}: target is the opposite endpoint
   *       of edge class X (vertex class)</li>
   *   <li>{@code both('X')}: target class is inferred only when both endpoints
   *       of edge class X resolve to the same vertex class (symmetric edges
   *       such as {@code KNOWS}). For heterogeneous edges (e.g. in=Post,
   *       out=Person) inference returns {@code null}, because a single alias
   *       cannot represent both endpoint classes.</li>
   *   <li>{@code outE('X')} / {@code inE('X')} / {@code bothE('X')}: alias class
   *       is X itself (the edge class)</li>
   *   <li>{@code inV()} / {@code outV()}: alias class is the linked vertex
   *       class from the preceding edge's LINK schema ({@code currentEdgeClass})</li>
   * </ul>
   *
   * @param currentEdgeClass the edge class set by a preceding {@code outE}/{@code inE},
   *     or {@code null} if none
   * @return the inferred class name, or {@code null} if it cannot be inferred
   */
  @Nullable static String inferClassFromEdgeSchema(
      @Nullable SQLMethodCall method, @Nullable String currentEdgeClass,
      CommandContext context) {
    if (method == null) {
      return null;
    }
    var dirName = method.getMethodNameString();
    if (dirName == null) {
      return null;
    }
    dirName = dirName.toLowerCase(Locale.ROOT);

    // outE('X') / inE('X') / bothE('X'): the edge class itself is the alias class
    if ("oute".equals(dirName) || "ine".equals(dirName) || "bothe".equals(dirName)) {
      return extractEdgeClassName(method);
    }

    // inV() / outV(): look up the linked vertex class from the preceding edge.
    // Delegates to the shared helper so this branch stays in lock-step with
    // resolveChainedTarget's precedence-2 fallback (both resolve a vertex
    // step against an edge schema; centralising avoids divergent logic).
    if ("inv".equals(dirName) || "outv".equals(dirName)) {
      return linkedVertexClassForVertexStep(
          dirName, currentEdgeClass, context.getDatabaseSession());
    }

    // out('X') / in('X') / both('X'): infer the target vertex class from the
    // edge LINK schema
    if (!"in".equals(dirName) && !"out".equals(dirName)
        && !"both".equals(dirName)) {
      return null;
    }

    var edgeClassName = extractEdgeClassName(method);
    if (edgeClassName == null) {
      return null;
    }

    // both('X'): infer only when both endpoints resolve to the same vertex
    // class. For symmetric edges (e.g. KNOWS: in=Person, out=Person) this
    // returns Person; for heterogeneous edges (e.g. HAS_CREATOR: in=Post,
    // out=Person) it returns null, because a single alias cannot safely
    // represent both endpoint classes and an index lookup on the alias
    // could miss records of the other class.
    if ("both".equals(dirName)) {
      var inClass = lookupLinkedVertexClass(edgeClassName, "in", context.getDatabaseSession());
      var outClass = lookupLinkedVertexClass(edgeClassName, "out", context.getDatabaseSession());
      if (inClass != null && inClass.equals(outClass)) {
        return inClass;
      }
      return null;
    }

    // out('X') targets the "in" side; in('X') targets the "out" side
    var targetPropName = "out".equals(dirName) ? "in" : "out";
    return lookupLinkedVertexClass(edgeClassName, targetPropName, context.getDatabaseSession());
  }

  /**
   * Looks up the linked vertex class from an edge class's LINK property.
   * Defensive: returns {@code null} if the session or any link in the
   * schema-lookup chain is missing, so this is the single source of truth
   * for the "edge-class → linked-vertex-class" resolution (also used by
   * {@link #linkedVertexClassForVertexStep}).
   *
   * @param edgeClassName the edge class to look up
   * @param propName the property name to read — must be {@code "in"} or {@code "out"}
   * @param session the database session for schema access; {@code null} yields {@code null}
   * @return the linked class name, or {@code null} if not found
   */
  @Nullable private static String lookupLinkedVertexClass(
      String edgeClassName, String propName, @Nullable DatabaseSessionEmbedded session) {
    if (session == null) {
      return null;
    }
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    if (schema == null) {
      return null;
    }
    var edgeClass = schema.getClassInternal(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    var prop = edgeClass.getPropertyInternal(propName);
    if (prop == null || prop.getLinkedClass() == null) {
      return null;
    }
    assert prop.getLinkedClass().getName() != null
        && !prop.getLinkedClass().getName().isEmpty()
        : "lookupLinkedVertexClass: linked class has null/empty name for edge "
            + edgeClassName;
    return prop.getLinkedClass().getName();
  }

  /**
   * Merges a single {@link SQLMatchFilter}'s metadata into the accumulation maps.
   *
   * <p>- **Filters**: all `WHERE` predicates for the same alias are AND-ed together into a
   *   single {@link SQLAndBlock}.
   * - **Classes**: if two expressions constrain the same alias to different classes, the
   *   method keeps the more specific sub-class, or throws if the classes are unrelated.
   * - **Collections / RIDs**: duplicates are validated for equality; mismatches throw.
   *
   * @throws CommandExecutionException if the same alias is constrained to unrelated
   *         classes, conflicting collections, or conflicting RIDs
   */
  private static void addAliases(
      SQLMatchFilter matchFilter,
      Map<String, SQLWhereClause> aliasFilters,
      Map<String, String> aliasClasses,
      Map<String, String> aliasCollections,
      Map<String, List<SQLRid>> aliasPinnedRids,
      CommandContext context) {
    var alias = matchFilter.getAlias();
    var filter = matchFilter.getFilter();
    if (alias != null) {
      if (filter != null && filter.getBaseExpression() != null) {
        var previousFilter = aliasFilters.get(alias);
        if (previousFilter == null) {
          previousFilter = new SQLWhereClause(-1);
          previousFilter.setBaseExpression(new SQLAndBlock(-1));
          aliasFilters.put(alias, previousFilter);
        }
        var filterBlock = (SQLAndBlock) previousFilter.getBaseExpression();
        if (filter.getBaseExpression() != null) {
          filterBlock.getSubBlocks().add(filter.getBaseExpression());
        }
      }

      var clazz = matchFilter.getClassName(context);
      if (clazz != null) {
        var previousClass = aliasClasses.get(alias);
        if (previousClass == null) {
          aliasClasses.put(alias, clazz);
        } else {
          var lower = getLowerSubclass(context.getDatabaseSession(), clazz, previousClass);
          if (lower == null) {
            throw new CommandExecutionException(context.getDatabaseSession(),
                "classes defined for alias "
                    + alias
                    + " ("
                    + clazz
                    + ", "
                    + previousClass
                    + ") are not in the same hierarchy");
          }
          aliasClasses.put(alias, lower);
        }
      }

      var collectionName = matchFilter.getCollectionName(context);
      if (collectionName != null) {
        var previousCollection = aliasCollections.get(alias);
        if (previousCollection == null) {
          aliasCollections.put(alias, collectionName);
        } else if (!previousCollection.equalsIgnoreCase(collectionName)) {
          throw new CommandExecutionException(context.getDatabaseSession(),
              "Invalid expression for alias "
                  + alias
                  + " cannot be of both collections "
                  + previousCollection
                  + " and "
                  + collectionName);
        }
      }

      var rid = matchFilter.getRid(context);
      if (rid != null) {
        var previousRids = aliasPinnedRids.get(alias);
        if (previousRids == null) {
          aliasPinnedRids.put(alias, List.of(rid));
        } else if (previousRids.size() != 1 || !previousRids.get(0).equals(rid)) {
          throw new CommandExecutionException(context.getDatabaseSession(),
              "Invalid expression for alias "
                  + alias
                  + " cannot be of both RIDs "
                  + previousRids.get(0)
                  + " and "
                  + rid);
        }
      }
    }
  }

  /**
   * Returns the more specific of two class names if one is a subclass of the other,
   * or `null` if they are unrelated in the class hierarchy.
   */
  @Nullable private static String getLowerSubclass(
      DatabaseSessionEmbedded db, String className1, String className2) {
    Schema schema = db.getMetadata().getSchema();
    var class1 = schema.getClass(className1);
    var class2 = schema.getClass(className2);
    if (class1.isSubClassOf(class2)) {
      return class1.getName();
    }
    if (class2.isSubClassOf(class1)) {
      return class2.getName();
    }
    return null;
  }

  /**
   * Assigns auto-generated aliases (prefixed with {@link #DEFAULT_ALIAS_PREFIX}) to
   * pattern nodes that the user did not name explicitly. This ensures every node in the
   * pattern graph has a unique alias, which simplifies downstream processing (edge
   * creation, filter merging, result projection).
   */
  private static void assignDefaultAliases(List<SQLMatchExpression> matchExpressions) {
    var counter = 0;
    for (var expression : matchExpressions) {
      if (expression.getOrigin().getAlias() == null) {
        expression.getOrigin().setAlias(DEFAULT_ALIAS_PREFIX + counter++);
      }

      for (var item : expression.getItems()) {
        if (item.getFilter() == null) {
          item.setFilter(new SQLMatchFilter(-1));
        }
        if (item.getFilter().getAlias() == null) {
          item.getFilter().setAlias(DEFAULT_ALIAS_PREFIX + counter++);
        }
      }
    }
  }

  /**
   * Thin adapter that constructs {@link IndexOrderedPlanner} from this planner's
   * state and runs detection. Kept as a separate method so the planner
   * construction's bytecode lives here (cold path) and does not bloat
   * {@link #createExecutionPlan}, which HotSpot benefits from keeping small.
   */
  @Nullable private IndexOrderedPlanner.IndexOrderedCandidate detectIndexOrderedCandidate(
      List<EdgeTraversal> sortedEdges,
      CommandContext context,
      Map<String, Long> estimatedRootEntries) {
    return new IndexOrderedPlanner(
        pattern, aliasClasses, aliasFilters, aliasPinnedRids,
        orderBy, skip, limit, returnItems, returnAliases, returnDistinct,
        returnElements, returnPaths, returnPatterns, returnPathElements)
        .detect(sortedEdges, context, estimatedRootEntries);
  }

  /**
   * Estimates the number of records each aliased root node will produce. These estimates
   * drive two optimizations:
   *
   * <p>- **Prefetching**: aliases below {@link #THRESHOLD} records are loaded eagerly.
   * - **Root selection**: the topological scheduler starts from the cheapest root.
   *
   * <p>Estimation strategy per alias:
   * - Single RID constraint → exactly 1 record.
   * - Static RID list constraint → list size records.
   * - Class constraint with `WHERE` → uses the filter's own
   *   {@link SQLWhereClause#estimate} method (which may use index statistics).
   * - Class constraint without filter → uses the class's record count.
   * - No constraint → omitted from the map (the alias is not a root candidate).
   *
   * @return a map from alias name to estimated record count
   * @throws CommandExecutionException if a referenced class does not exist in the schema
   */
  static Map<String, Long> estimateRootEntries(
      Map<String, String> aliasClasses,
      Map<String, List<SQLRid>> aliasPinnedRids,
      Map<String, SQLWhereClause> aliasFilters,
      CommandContext ctx) {
    Set<String> allAliases = new LinkedHashSet<>();
    allAliases.addAll(aliasClasses.keySet());
    allAliases.addAll(aliasFilters.keySet());
    allAliases.addAll(aliasPinnedRids.keySet());

    var db = ctx.getDatabaseSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    Map<String, Long> result = new LinkedHashMap<>();
    for (var alias : allAliases) {
      var ridList = aliasPinnedRids.get(alias);
      if (ridList != null) {
        result.put(alias, (long) ridList.size());
        continue;
      }

      var className = aliasClasses.get(alias);

      if (className == null) {
        continue;
      }

      if (!schema.existsClass(className)) {
        throw new CommandExecutionException(ctx.getDatabaseSession(),
            "class not defined: " + className);
      }
      var oClass = schema.getClassInternal(className);
      var classCount = oClass.approximateCount(ctx.getDatabaseSession());
      long upperBound;
      var filter = aliasFilters.get(alias);
      if (filter != null) {
        // A WHERE filter always reduces or equals the full class scan.
        // The estimate() heuristic may return a higher value (e.g.
        // count/2 > count for another class), so we cap it to ensure
        // filtered nodes are always preferred over unfiltered ones.
        upperBound = Math.min(filter.estimate(oClass, THRESHOLD, ctx), classCount);
      } else {
        // No WHERE filter — full class scan. Add +1 bias so that a
        // filtered node with the same class count is preferred.
        upperBound = classCount + 1;
      }
      result.put(alias, upperBound);
    }

    return result;
  }

}
