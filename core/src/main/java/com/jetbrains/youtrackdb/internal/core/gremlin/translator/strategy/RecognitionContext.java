package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.BoundaryOutputType;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.ListShapingOp;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.PostConcatOp;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.ResultShaping;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchPatternBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGroupBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLimit;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSkip;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * The recogniser-facing view of the walk. A {@link StepRecogniser} reads resolved flags and the
 * current boundary, mints aliases, and contributes to the pattern through the named methods here — it
 * cannot reach the traversal, the strategy list, the step cursor's position, or the pattern builder.
 * The concrete {@link WalkerContext} implements this interface and owns that full state.
 *
 * <p>Narrowing the surface this way is what keeps a new recogniser from perturbing the others: it
 * cannot add order-dependence on the raw traversal or scan strategies on a hot path, and every
 * contribution goes through a method whose effect is fixed here rather than through direct field
 * writes.
 *
 * <h2>No mutation discipline</h2>
 *
 * A recogniser may read and contribute in any order. A {@link Outcome#DECLINE} — its own or a later
 * recogniser's — makes {@link GremlinStepWalker} discard the whole walk, so a partial contribution
 * can never leak into a translated plan. "Validate before you mutate" is unnecessary here.
 */
interface RecognitionContext extends ParamSink {

  // --- Resolved flags, each resolved once by the walker -----------------------------------------

  /**
   * Whether the traversal runs as a polymorphic query ({@code YTDBStrategyUtil.isPolymorphic}).
   * Resolved once by {@link GremlinStepWalker}; a {@code null} resolution declines the whole walk
   * before any recogniser runs, so this is always a resolved value. The {@code hasLabel(L)}
   * recogniser reads it to pick the boundary-node re-typing: polymorphic re-types to {@code {class:
   * L}} (MATCH matches subclasses, mirroring native hierarchy-aware {@code hasLabel}), while
   * non-polymorphic re-types to {@code L} and adds an exact {@code @class = 'L'} filter (leaf-exact,
   * mirroring native non-polymorphic {@code hasLabel}). The vertex-source and bare-hop recognisers
   * root every node at the generic {@code V} class regardless of it.
   */
  boolean polymorphic();

  /**
   * Whether the traversal opts into {@code EdgeLabelVerificationStrategy}. Resolved once by
   * {@link GremlinStepWalker} so a recogniser reads a boolean instead of scanning the strategy list.
   * A label-less hop declines when this is {@code true}: translating the hop away would suppress the
   * label-less error that strategy exists to raise (see
   * {@link GremlinPatternAssembler#resolveEdgeLabel}).
   */
  boolean edgeLabelVerificationEnabled();

  /**
   * Whether {@code ProductiveByStrategy} makes a {@code by(propertyKey)} modulator productive on
   * this traversal.
   *
   * <p>The strategy inverts the rule {@link ByModulatorPresence} encodes: a productive
   * {@code by(key)} yields {@code null} for an element without the property instead of dropping
   * the traverser, so {@code g.withStrategies(ProductiveByStrategy).V().groupCount().by("age")}
   * keeps the {@code null} bucket YQL produces anyway and the presence conjunct must not be added.
   * The translator has to answer this itself rather than let the strategy rewrite the modulator:
   * it runs first and folds the {@code by(...)} into the MATCH plan, so by the time
   * {@code ProductiveByStrategy} looks for {@link
   * org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating} steps to wrap, there are
   * none left.
   */
  boolean byModulatorIsProductive(String propertyKey);

  /**
   * Whether a GLOBAL-scope {@code order().by(key)} keeps a record that does not carry {@code key}.
   *
   * <p>{@link GremlinStepWalker} resolves the option, user strategy, and database setting once.
   * Native and translated execution use the same resolver. {@code true}, the shipped default,
   * makes the translated plan omit the order-key
   * {@code IS DEFINED} conjunct so the record survives the pattern and sorts as a null key.
   * {@code false} restores standard order semantics filtering and the conjunct is emitted.
   *
   * <p>Read through {@link OrderKeyPresencePolicy} rather than directly: the policy is the one
   * place that turns this flag into an emission decision.
   */
  boolean orderIncludesMissingKey();

  /** The direction-specific null placements resolved once for this translation. */
  ResolvedOrderByNullsPlacement orderByNullsPlacements();

  // --- Boundary read ----------------------------------------------------------------------------

  /**
   * The alias of the traversal's current terminator, or {@code null} before any step has pinned a
   * boundary. A hop reads this as its "from" endpoint; the start-step recogniser uses a {@code null}
   * boundary as its "I am the start" guard.
   */
  @Nullable String boundaryAlias();

  /**
   * The schema class registered for {@link #boundaryAlias()} in the positive pattern, or {@code null}
   * when the boundary is still the generic {@code V} root. {@code WherePredicateStep} uses this for
   * {@link GremlinPredicateAdapter.PropertyTypeGate} routing when a label comparison also names a
   * property key.
   */
  @Nullable String boundaryClassName();

  /**
   * The {@link BoundaryOutputType} pinned on the current boundary, or {@code null} before any step
   * pins one. A terminator recogniser reads this to refuse a shape a prior terminator already fixed
   * — e.g. {@code dedup()} after a value / map / scalar projection, where {@code RETURN DISTINCT}
   * over the boundary presence column would dedup on the unique entity and remove nothing.
   */
  @Nullable BoundaryOutputType boundaryOutputType();

  /**
   * Whether a projection configured on this context can reach the caller as a returned row payload.
   * The top-level walk answers {@code true}; a sub-walk capture answers {@code false}, because a
   * combinator child's commit keeps only the filters and pattern fragments it captured and discards
   * the child's own projection.
   *
   * <p>A recogniser reads this when a shape is exact enough to translate as a filter signal but not as
   * a payload. {@link PropertiesStepRecogniser} is the case in hand: {@code properties(key)} yields a
   * property element rather than its value, which matters only if something downstream reads the row.
   */
  boolean projectsReturnedPayload();

  // --- The fold latch ---------------------------------------------------------------------------

  /**
   * Whether the step now being dispatched sits in the run of steps that {@code YTDBGraphStepStrategy}
   * folds into its {@code YTDBGraphStep} — true exactly when the step is dispatched from the
   * top-level walk and every step consumed at top level since the most recent {@code GraphStep} was
   * itself a {@code HasStep}.
   *
   * <p>The distinction is load-bearing because a folded {@code has(key, range)} and an unfolded one
   * are evaluated by different comparators once the translator declines. Folded, the native arm runs
   * {@code YTDBGraphStep}'s YQL-backed comparison, which orders values of different runtime types
   * (it ranks a String above an Integer) — the same answer the translated YQL gives. Unfolded, the
   * native arm runs TinkerPop's {@code GremlinValueComparator}, whose rule is that operands of
   * different comparability blocks never compare, so a range comparison across types matches
   * nothing. A translation that reproduces one of those two answers contradicts the other, so the
   * per-record type guard {@link GremlinPredicateAdapter} emits is scoped to unfolded positions
   * only. Applying it in folded positions was measured to break eight shapes that agree today.
   *
   * <p>The latch mirrors {@code YTDBGraphStepStrategy.rebuildTraversal}'s {@code isTraversalStart}
   * variable, including its restart on <em>any</em> {@code GraphStep} rather than only the first —
   * a mid-traversal {@code V()} restarts the fold there, and a latch that special-cased the first
   * step would drift from it. {@link GremlinStepWalker} owns the update rule; see
   * {@link #setAtTraversalStart(boolean)}.
   *
   * <p>A sub-walk answers {@code false} unconditionally: a child traversal's steps are never visited
   * by {@code rebuildTraversal}'s top-level scan, so nothing inside a {@code where} / {@code and} /
   * {@code not} child is ever folded. (The {@code where} / {@code filter} / all-filter-{@code and}
   * spellings that <em>do</em> reach the fold get there by {@code InlineFilterStrategy} hoisting the
   * child's steps to top level before the translator runs, so the walker sees them as top-level
   * steps and the latch classifies them correctly without special-casing.)
   *
   * <p>The union fork is the second child path and it does not go through a sub-walk.
   * {@code UnionForkHostImpl.walkFork} re-enters the top-level {@link GremlinStepWalker#walk} with a
   * synthesised list of the recognised prefix followed by one arm's steps, so an arm's leading
   * {@code has} would sit right after the prefix's {@code GraphStep} and read as folded. It is not
   * folded natively — {@code rebuildTraversal} does not descend into a union child — so the fork
   * hands the prefix length in as a child-scope boundary and the latch closes across the seam. See
   * {@link GremlinStepWalker#walk(org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin,
   * int)}.
   */
  boolean atTraversalStart();

  /**
   * Sets the fold latch read by {@link #atTraversalStart()}. Called only by
   * {@link GremlinStepWalker}'s dispatch loop, which owns the update rule; a recogniser reads the
   * latch and never writes it. A sub-walk swallows the write, so a child's steps cannot arm the
   * parent's latch.
   */
  void setAtTraversalStart(boolean atStart);

  // --- Schema-aware type gating -----------------------------------------------------------------

  /**
   * Whether {@code propertyKey} is declared with the {@code STRING} schema type on {@code className}
   * (or a supertype it inherits from). This selects the {@code startingWith} translation form: a
   * declared-String property can only ever hold String values, so a {@code startingWith} on it uses
   * the index-aware half-open prefix range (a B-tree prefix scan); every other case (unknown /
   * undeclared type, a declared non-String type, or no schema) uses the strict full-scan {@code
   * STARTSWITH} node, which throws on a present non-String value exactly as native {@code
   * Text.startingWith} does. No subclass sweep: a subclass cannot override an inherited property's
   * type (type overrides are forbidden), and a subclass-only property is not declared on {@code
   * className}, so the check is exactly the class's own (superclass-walking) property lookup.
   * Returns {@code false} when {@code className} is {@code null} (a generic {@code V} boundary whose
   * leaf class is unknown), the class or property is not declared (schema-less / mixed), or the
   * schema is unavailable. Resolved against the schema snapshot {@link GremlinStepWalker} pins once
   * per walk.
   */
  boolean isDeclaredStringProperty(@Nullable String className, String propertyKey);

  /**
   * Whether {@code className} is a declared vertex class in the resolved schema. The {@code
   * hasLabel(L)} recogniser re-types the boundary node to {@code L}, which builds a {@code SELECT
   * FROM L} scan; a non-existent class (a typo'd or never-used label) or an edge class would make
   * that scan error or return the wrong element type, while native {@code hasLabel} simply matches no
   * vertex and returns empty. The recogniser declines to native when this is {@code false} so the two
   * pipelines agree. Returns {@code false} when the schema is unavailable, so a walk with no schema
   * never re-types.
   */
  boolean isVertexClass(String className);

  // --- Alias minting ----------------------------------------------------------------------------

  /** Mints the next anonymous vertex alias ({@code $g2m_anon_0}, {@code $g2m_anon_1}, …). */
  String nextAnonVertexAlias();

  /** Mints the next anonymous edge alias ({@code $g2m_edge_0}, {@code $g2m_edge_1}, …). */
  String nextEdgeAlias();

  // --- Contributions ----------------------------------------------------------------------------

  /** Registers a pattern node under {@code alias} rooted at {@code className}, non-optional. */
  void addNode(String alias, String className);

  /** Registers an unfiltered edge {@code fromAlias --dir(edgeLabel)--> toAlias} on the pattern. */
  void addEdge(
      String fromAlias, String toAlias, MatchPatternBuilder.Direction dir,
      @Nullable String edgeLabel);

  /**
   * Registers the edge-as-node form {@code fromAlias --edgeDir E(edgeLabel){edgeFilter}--> edgeAlias
   * --closingVertexDir V()--> toAlias}, the only IR shape that can filter an edge rather than the
   * target vertex.
   */
  void addEdgeAsNode(
      String fromAlias,
      String edgeAlias,
      String toAlias,
      MatchPatternBuilder.Direction edgeDir,
      @Nullable String edgeLabel,
      MatchPatternBuilder.Direction closingVertexDir,
      @Nullable SQLWhereClause edgeFilter);

  /**
   * Records a per-alias {@code WHERE} contributed outside the pattern builder (e.g. {@code @rid IN
   * [...]}). Merged into the built pattern's alias filters at result-build time, overriding a builder
   * entry on the same alias.
   */
  void putAliasFilter(String alias, SQLWhereClause where);

  /** Records the accumulated edge {@code WHERE} under an edge alias, so the edge filter is
   *  observable on the walk state. The filter also travels on the edge path item via
   *  {@link #addEdgeAsNode}. */
  void putEdgeFilter(String edgeAlias, SQLWhereClause where);

  /**
   * Whether {@code alias} is already registered in the positive pattern under construction. Edge-bearing
   * {@code NotStep} recognisers use this to pre-validate the planner's NOT-origin constraint before
   * emitting a detached {@link SQLMatchExpression}.
   */
  boolean positivePatternHasAlias(String alias);

  /**
   * Appends a detached NOT {@link SQLMatchExpression} to the walk's {@code notMatchExpressions} sink.
   * Edge-bearing {@code NotStep} recognisers reach this after a successful sub-walk; pure-filter NOT
   * shapes merge into {@link #putAliasFilter} instead.
   */
  void addNotMatchExpression(SQLMatchExpression expression);

  /**
   * Marks this walk as RID-bearing ({@code g.V(ids)} start ids or a {@code hasId(...)} filter).
   * RID-bearing shapes bypass the plan cache because their fingerprint would vary per id set.
   */
  void markRidBearing();

  /**
   * Pins the boundary metadata: the alias the matched element appears under in each row, how the row
   * projects onto a traverser, and the TinkerPop element class the boundary emits.
   */
  void pinBoundary(String alias, BoundaryOutputType type, Class<? extends Element> returnClass);

  /**
   * Replaces the RETURN projection with a single column {@code alias AS alias}. A chain hop calls this
   * to make its new target the traversal's one result column; the start step calls it to key the row
   * on the source vertex.
   */
  void setSingleReturnColumn(String alias);

  // --- User label propagation (Track 6) ---------------------------------------------------------

  /**
   * Binds every non-null {@code as(label)} on {@code step} to {@code internalAlias}. Returns {@code
   * false} when a user label is already bound to a different internal alias — the recogniser should
   * decline the whole walk. A step with no labels is a no-op success.
   */
  boolean bindStepLabels(Step<?, ?> step, String internalAlias);

  /**
   * Resolves a Gremlin user label to the internal pattern alias it was bound to, or {@code null}
   * when the label was never surfaced by an accepted {@code as(...)} step.
   */
  @Nullable String resolveUserLabel(String userLabel);

  /** Clears the three parallel RETURN lists before a terminator replaces the projection. */
  void clearReturnProjection();

  /**
   * Appends one RETURN column ({@code expression AS alias} when {@code returnAlias} is non-null).
   */
  void appendReturnColumn(SQLExpression expression, @Nullable String returnAlias);

  /** Read-only view of RETURN items accumulated so far (for recogniser post-checks). */
  List<SQLExpression> returnItems();

  // --- Result shaping (Track 6) -----------------------------------------------------------------

  /** Sets {@code RETURN DISTINCT} on the assembled MATCH plan. */
  void setReturnDistinct(boolean distinct);

  /** Sets the {@code GROUP BY} clause for aggregate terminators. */
  void setGroupBy(@Nullable SQLGroupBy groupBy);

  /** Sets the {@code ORDER BY} clause for {@code order()} terminators. */
  void setOrderBy(@Nullable SQLOrderBy orderBy);

  /** Sets the {@code LIMIT} clause for {@code limit()} / {@code range()} terminators. */
  void setLimit(@Nullable SQLLimit limit);

  /** Sets the {@code SKIP} clause for {@code skip()} / {@code range()} terminators. */
  void setSkip(@Nullable SQLSkip skip);

  /** The {@code ORDER BY} set so far, or {@code null} — lets a recogniser refuse a second order(). */
  @Nullable SQLOrderBy orderBy();

  /**
   * The {@code GROUP BY} set so far, or {@code null} — lets a following terminator refuse a shape
   * that would silently clobber it ({@code group().by(label).count()} must not become a bare
   * {@code count(*)} over the ungrouped rows).
   */
  @Nullable SQLGroupBy groupBy();

  /** The {@code LIMIT} set so far, or {@code null} — lets a recogniser refuse a second limit/range. */
  @Nullable SQLLimit limit();

  /** The {@code SKIP} set so far, or {@code null} — lets a recogniser refuse a second skip/range. */
  @Nullable SQLSkip skip();

  /**
   * Whether {@code RETURN DISTINCT} has been set so far ({@code dedup()}). A reducing/grouping
   * terminator reads it to refuse a shape that would apply the aggregate <em>after</em> the distinct
   * — {@code out().dedup().count()} would emit {@code RETURN DISTINCT count(*)}, which counts
   * duplicates. See {@link #limit()} / {@link #skip()} for the same pre-aggregate hazard.
   */
  boolean returnDistinct();

  /**
   * Pins the boundary row-projection shaping — the seven flags plus the ordered list-shaping ops
   * that control how the boundary base ({@link
   * com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.AbstractMatchPlanStep}) projects
   * each MATCH row: row dropping, presence checks, valueMap list wrapping, group-map accumulation,
   * singleton-map unwrapping, elementMap token keys, and the {@code fold} / {@code unfold} /
   * {@code reverse} / {@code tail} stream stages. A terminator builds the exact combination from
   * {@link ResultShaping#NONE} plus its overrides and calls this; the element-path default is
   * {@link ResultShaping#NONE}.
   *
   * <p>This replaces the whole record, {@link ResultShaping#listShapingOps()} included. A
   * list-shaping terminator therefore contributes through {@link #appendListShapingOp} instead: an
   * append composes with the ops and flags already pinned, where a rebuild from {@code NONE} would
   * drop a sibling recogniser's contribution. The append's no-clobber guarantee covers only the
   * recognisers that use that method — a later {@code setResultShaping} still overwrites every op
   * appended before it.
   *
   * <p>Two rules keep the write paths from colliding, and both are enforced. Nothing may claim a step
   * behind a captured op unless {@link GremlinStepWalker}'s may-follow allow-lists admit the claiming
   * recogniser, and membership there requires that the recogniser contribute through {@link
   * #appendListShapingOp} alone — so no caller of this method can sit behind a captured op, and the
   * walker compares {@link #listShapingOps()} before and after every accept rather than trusting the
   * membership, failing under {@code -ea} and declining the walk under {@code -da}. The rule
   * is stated as the gate applies it because a list-shaping terminator may follow another one
   * ({@code reverse().unfold()}, {@code reverse().fold()}): the gate forbids a step behind a captured
   * op and says nothing about where a terminator sits. The gate is armed rather than exercised for
   * now — no recogniser appends an op yet — so the rule holds by the gate rather than by the absence
   * of a caller. The second rule is {@link UnionStepRecogniser} calling this with the agreed child
   * shaping before any post-union suffix op appends, so on that path the replace always precedes
   * the append.
   */
  void setResultShaping(@Nonnull ResultShaping shaping);

  /**
   * Whether the shaping pinned so far drops rows whose entity lacks a projected property — the
   * {@code dropOnAbsent} half of {@code values(key)}. {@link RangeGlobalStepRecogniser} reads it
   * when a slice arrives: a statement-level {@code SKIP} / {@code LIMIT} would count rows the
   * post-plan drop has not removed yet, so the recogniser promotes the drop into a pattern conjunct
   * through {@link #promotePresenceDropToPatternFilter()} rather than declining. See that
   * recogniser for the measured divergence that made the promotion necessary.
   */
  boolean dropsRowsOnAbsentProperty();

  /**
   * Records the alias whose entity {@link #dropsRowsOnAbsentProperty()} checks. The projection that
   * turns {@code dropOnAbsent} on writes it alongside the shaping; a later {@link #setResultShaping}
   * clears it. {@link #promotePresenceDropToPatternFilter()} reads it.
   */
  void setPresenceDropAlias(@Nullable String alias);

  /**
   * Promotes a pinned {@code dropOnAbsent} into {@code key IS DEFINED} alias conjuncts so a
   * following slice counts survivors rather than pre-drop rows. Returns {@code true} when there is
   * nothing to promote, or when the conjuncts were written. Returns {@code false} when this context
   * cannot express the promotion — a combinator child whose shaping is swallowed, or a drop whose
   * alias was never recorded — in which case the caller declines. See {@link
   * #dropsRowsOnAbsentProperty()} for why the slice needs the conjunct at all.
   */
  boolean promotePresenceDropToPatternFilter();

  /**
   * Appends one ordered list-shaping stage to the shaping pinned so far, keeping the flags and the
   * ops already there. The four list-shaping terminators ({@code fold} / {@code unfold} /
   * {@code reverse} / {@code tail}) contribute through this rather than through {@link
   * #setResultShaping}, because two of them in one traversal ({@code reverse().unfold()}) have to
   * compose and a sibling recogniser's flags have to survive. Declared order is the order the
   * boundary base applies the stages in.
   *
   * <p>Call this only when {@link #supportsListShaping()} answers {@code true}. A context that
   * answers {@code false} cannot carry an op, and the recogniser declines the whole walk instead of
   * appending; see that method for why the pairing is a query the recogniser reads rather than a
   * silent swallow behind this one.
   */
  void appendListShapingOp(@Nonnull ListShapingOp op);

  /**
   * Whether a {@link ListShapingOp} appended on this context can reach the boundary. Declared
   * non-default so both implementations state an answer, mirroring {@link
   * #dropsRowsOnAbsentProperty()} — the same query-then-decline pairing, read by a recogniser
   * before it contributes rather than reported back after.
   *
   * <p>This javadoc is the canonical statement of why the pairing is a boolean. The other sites that
   * touch the decline channel — {@link SubTraversalPredicateAdapter#supportsListShaping()}, {@link
   * SubTraversalPredicateAdapter#appendListShapingOp}, and the tests that pin them — carry a one-line
   * summary and link here, so a change to the decline design has one place to edit.
   *
   * <p>A context answers {@code true} when the shaping it holds is the one its own boundary base
   * reads. Two contexts qualify: the top-level walk, and a union arm, which {@link
   * UnionForkHostImpl#walkFork} runs as a fresh top-level walk over the recognised prefix plus the
   * arm's own steps, with its own {@link WalkerContext}. A combinator child sub-walk answers
   * {@code false}, and that answer is the decline channel for all four list-shaping terminators.
   * {@link #walkChild} drives {@code and} / {@code or} / {@code not} / {@code where} /
   * {@code filter} children through the same recogniser registry the top-level walk uses, so a
   * child's trailing {@code fold()} reaches the recogniser that would claim it at top level; with no
   * boolean to read, that recogniser has no way to back out. A union arm's {@code true} speaks only
   * for that arm's own boundary: one list per arm and one list over the concatenated arms are
   * different answers, so declining an arm that carries an op belongs in {@link
   * UnionStepRecogniser}'s child loop rather than here.
   *
   * <p>Both simpler alternatives answer wrongly. Swallowing the append the way {@link
   * SubTraversalPredicateAdapter} swallows {@link #setResultShaping} lets the child's terminator
   * accept while its stage is dropped, which changes the child's truth value. Take
   * {@code g.V().not(__.out().fold())}: natively the {@code fold()} makes the child emit one result
   * for every vertex — a dry upstream still emits one empty list — so the {@code not} rejects every
   * vertex and the query returns nothing. With the append swallowed the child is a plain
   * edge-bearing hop, {@link NotStepRecogniser} translates it as a detached anti-join, and every
   * vertex with no outgoing edge comes back. Which way the row set moves depends on the combinator —
   * the same swallow under {@code and} / {@code where} loses rows where this {@code not} gains
   * them — so the terminator has to learn it cannot contribute and decline the whole walk. Throwing
   * out of the append (the shape {@link #appendPostConcatOp} uses) lands in
   * {@link GremlinToMatchStrategy}'s {@code RuntimeException} net, which degrades it to a silent
   * decline minus the diagnostic.
   */
  boolean supportsListShaping();

  /**
   * The {@link ListShapingOp}s appended to the shaping pinned so far, in declared order, empty when
   * none. {@link GremlinStepWalker}'s dispatch loop reads this twice per step, and the two reads are
   * why the answer is the list rather than a boolean. Whether it is empty is the last-step gate for
   * the four list-shaping terminators: a step dispatched behind a captured op is refused unless the
   * walker's may-follow allow-lists admit the recogniser claiming it. The list itself is what the
   * loop compares before and after each accept, so a recogniser that replaces the shaping and drops
   * a stage it was admitted behind is caught rather than taken on the membership's word — a boolean
   * cannot tell "an op is captured" from "the captured op is the one that was there". Declared
   * non-default for the same reason {@link #supportsListShaping()} is — both implementations state
   * an answer rather than inherit one.
   *
   * <p>The returned list is immutable ({@link ResultShaping} copies it on construction and every
   * write builds a new record), so the loop may hold one answer across a dispatch and compare it
   * against the next.
   *
   * <p>{@code GremlinStepWalker}'s own {@code capturedListShapingOp} carries why the gate is the
   * loop's job and what a step lands on the wrong side of without it; its {@code
   * listShapingOpsSurvived} carries the second read. A sub-walk answers empty: it can never carry an
   * op, because {@link #appendListShapingOp} throws there and {@link #supportsListShaping()} tells a
   * recogniser so first.
   */
  @Nonnull
  List<ListShapingOp> listShapingOps();

  /**
   * Whether {@link UnionStepRecogniser} has stashed a multi-plan carrier on this walk. Post-union
   * barriers ({@code count}/{@code limit}/{@code dedup}) branch on this instead of mutating a
   * single-plan {@code MatchPlanInputs}.
   */
  default boolean hasUnionCarrier() {
    return false;
  }

  /**
   * Appends a post-concatenation reduction for {@link
   * com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.MultiPlanMatchStep}. Top-level
   * walks only; sub-walks throw.
   */
  default void appendPostConcatOp(@Nonnull PostConcatOp op) {
    throw new UnsupportedOperationException("post-concat ops are top-level only");
  }

  /** Ordered post-concat ops stashed so far (empty when none). */
  default @Nonnull List<PostConcatOp> postConcatOps() {
    return List.of();
  }

  /**
   * Whether any stashed union child carries its own {@code LIMIT} / {@code SKIP} / {@code RETURN
   * DISTINCT}. A lone post-union {@code count()} is served by rewriting every child to a bare
   * {@code RETURN count(*)}, which drops exactly those three clauses, so a child that has one would
   * contribute rows it would never have emitted. This is the multi-plan counterpart of the
   * single-plan pre-aggregate cardinality check in {@code GremlinAggregateAssembler}. The default
   * answers {@code false}: a context with no union carrier has no children to inspect, and only the
   * post-union count path asks.
   */
  default boolean anyUnionChildHasCardinalityClause() {
    return false;
  }

  /**
   * A resolved single-key {@code values(key)} / {@code properties(key)} projection. Carries the
   * alias and the property key alongside the field-access expression because the terminators that
   * re-point at it need different halves: an aggregate needs the expression
   * ({@code values("age").sum()} → {@code sum(alias.age)}), while a bare {@code order()} needs the
   * alias and key to build an {@code ORDER BY} item, which {@code SQLOrderByItem} models as an
   * alias plus a modifier rather than as an arbitrary expression.
   */
  record PropertyProjection(String alias, String propertyKey, SQLExpression expression) {
  }

  /**
   * Records the projection from the most recent single-key {@code values(key)} /
   * {@code properties(key)} step so a following terminator can re-point at the projected value
   * instead of at the element ({@code values("age").sum()}, {@code values("name").order()},
   * {@code values("name").groupCount()}).
   */
  void setLastPropertyProjection(@Nullable PropertyProjection projection);

  /** The projection recorded by {@link #setLastPropertyProjection}, or {@code null} when unset. */
  @Nullable PropertyProjection lastPropertyProjection();

  // --- Sub-walk seam ----------------------------------------------------------------------------

  /**
   * Drives a sub-walk of {@code child} against the same recogniser registry the top-level walk uses,
   * returning the {@link SubTraversalPredicateAdapter} that captured the child's contributions. This
   * is the one seam through which a logical-combinator recogniser (a later track) translates a child
   * sub-traversal without reaching the walker's private registry or dispatch loop: it sees only this
   * interface, and this method hands it a driven sub-context to read back.
   *
   * <p>The returned adapter carries the sub-walk {@link SubTraversalPredicateAdapter#outcome()} —
   * {@link Outcome#DECLINE} when any child step is unrecognised (or the child is empty) — and, on an
   * {@link Outcome#ACCEPTED}, the captured classification the combinator composes: {@link
   * SubTraversalPredicateAdapter#hasEdges()} plus the captured filters and pattern fragments. The
   * child's contributions are captured, not committed, so a declined child leaves this context
   * untouched — the caller commits the captured state itself only on success.
   */
  SubTraversalPredicateAdapter walkChild(Traversal.Admin<?, ?> child);

  /**
   * Union-only fork seam installed by the top-level walker. Returns {@code null} on sub-walks and
   * test fixtures that never drive {@code union(...)} — {@link UnionStepRecogniser} declines when
   * absent. Does not expose the parent traversal.
   */
  @Nullable default UnionForkHost unionForkHost() {
    return null;
  }
}
