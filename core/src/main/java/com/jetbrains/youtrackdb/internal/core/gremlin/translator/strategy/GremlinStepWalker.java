package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.ListShapingOp;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Schema;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.MatchPlanInputs;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchWhereBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.parser.Pattern;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReverseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;

/**
 * Walks a {@link Traversal.Admin}'s step list through a {@link StepCursor} and dispatches each head
 * to the {@link StepRecogniser} registered for its exact runtime class in a class-keyed registry
 * ({@code Map<Class<?>, StepRecogniser>}). The walker is the entry point the
 * {@link GremlinToMatchTranslator} delegates to; recognisers carry the per-step recognition logic so
 * the walker itself stays a thin dispatch loop.
 *
 * <h2>All-or-nothing translation</h2>
 *
 * Any step the registered recognisers cannot claim declines the entire walk (returning {@code null}).
 * There is no "partial prefix" mechanism — either every step is recognised or the traversal is
 * declined whole and stays on the native TinkerPop pipeline.
 *
 * <h2>Cursor-driven dispatch</h2>
 *
 * Each iteration peeks the head through the cursor (transparent barrier steps skipped), looks up the
 * recogniser for the head's exact class, and runs it. A missing recogniser declines the whole walk;
 * so does a recogniser's {@link Outcome#DECLINE}. The recogniser advances the cursor by consuming its
 * head and any trailing steps of its shape, so the recognised set is bounded by which step
 * <em>classes</em> have a recogniser, not by a step count: a long but fully-recognised chain
 * translates, and the first unrecognised step class declines. An empty traversal is declined up
 * front. The loop ends when {@link StepCursor#peek()} returns {@code null} — the cursor has skipped
 * every trailing barrier and reached the end of the list, so a walk that reaches the terminator
 * invariant has recognised every step.
 *
 * <h2>Reserved-prefix pre-flight scan</h2>
 *
 * Before dispatching any step, the walker rejects the whole traversal — throwing a {@link
 * ReservedAliasException} — if any user label starts with {@code $}. The {@code $} space is reserved
 * for the translator's minted {@code $g2m_} aliases (see {@link WalkerContext#ANON_VERTEX_ALIAS_PREFIX})
 * and for YouTrackDB's query-context variables, so a user label there is prohibited rather than
 * declined to native. {@link GremlinToMatchStrategy}'s throw-safety net re-throws this one exception
 * type so the query fails loudly, while every other failure still degrades to a native decline. The
 * scan is purely lexical, so it runs before the session-dependent flag resolution.
 *
 * <h2>Resolved flags in the walker; shape gates in recognisers</h2>
 *
 * Every per-step shape gate (start-step shape, vertex-vs-edge, ID convertibility, hasContainer
 * presence, predicate well-formedness, …) lives inside the responsible recogniser. The walker
 * resolves the two traversal-level flags once up front and stores them on the {@link WalkerContext}:
 * the polymorphism flag ({@code YTDBStrategyUtil.isPolymorphic}) and whether {@code
 * EdgeLabelVerificationStrategy} is present. Resolving polymorphism here is safe: {@code
 * isPolymorphic} is null-safe (it gates on an attached YTDB graph and transaction before touching
 * {@code tx()}), and a {@code null} result declines the whole walk.
 *
 * <h2>Result assembly</h2>
 *
 * On a successful walk, the walker calls {@link
 * com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchPatternBuilder#build}
 * exactly once to lock the pattern, merges the builder's alias filters with any filters recognisers
 * contributed outside the builder, and packages the {@link MatchPlanInputs} into a {@link
 * GremlinToMatchTranslator.TranslationResult}.
 */
final class GremlinStepWalker {

  /**
   * Step classes the cursor treats as transparent: skipped on every read and counted as consumed.
   * Adding a transparent type is a one-line change here that touches no recogniser.
   *
   * <p>{@link NoOpBarrierStep} is transparent because {@code LazyBarrierStrategy} wedges one
   * between chained hops, and {@code RepeatUnrollStrategy} wedges one between the hops it unrolls a
   * {@code repeat(...)} into. Skipping it preserves the <em>answer set</em>: the barrier merges
   * identical traversers into bulks, and a MATCH plan reaches the same answers by other means.
   *
   * <p>The transparency rule carries no <em>cost</em> bound, for either barrier. Bulking is what
   * keeps a chain of n hops at n passes over the edge set; a MATCH plan enumerates one row per
   * distinct path, which grows as the n-th power of the average degree. That is a property of the
   * chain and not of the syntax it came from, so a hand-written n-hop chain reaches the same
   * enumeration as an unrolled {@code repeat(__.out()).times(n)}. {@link RepeatDeclineStrategy}
   * bounds the {@code repeat(...)} spelling, which is the one the TinkerPop feature suite drove to
   * a stall; a deep hand-written chain is still translated and still pays the enumeration. Bounding
   * that shape needs a depth or fan-out gate the translator does not have yet.
   *
   * <h2>Why the {@code where(...)} scope steps are not here</h2>
   *
   * {@code WhereStartStep} and {@code WhereEndStep} were transparent, and that was wrong: they are
   * not barriers, they are the child's scope binding. {@code WhereStartStep(a)} says the child
   * starts from the traverser labelled {@code a} rather than from the current element, and {@code
   * WhereEndStep(b)} says the child's result must <em>equal</em> the traverser labelled {@code b}.
   * Skipping them dropped both conditions and translated a weaker filter than the user wrote, so a
   * labelled {@code where} returned a silently different row set. Measured on one {@code knows}
   * edge plus an isolated vertex, no self-loops:
   *
   * <ul>
   *   <li>{@code g.V().as(a).out().as(b).where(__.as(a).out().as(b))} — empty translated, one row
   *       native. The end comparison is dropped.
   *   <li>{@code g.V().as(a).where(__.as(a).out().as(a))} — one row translated, empty native. The
   *       child asks for a self-loop; without the end comparison it asks only for an out-edge.
   *   <li>{@code g.V().as(a).out().as(b).where(__.as(a).out())} — empty translated, one row native.
   *       No end step at all: the start binding alone is load-bearing, because the child runs from
   *       {@code b} instead of from {@code a}. This is why both classes go rather than only the end
   *       one.
   * </ul>
   *
   * <p>Removing them from this set is the whole fix. Neither class has a recogniser, so the
   * all-or-nothing rule declines any traversal whose {@code where} child carries one, and the child
   * runs on the native pipeline where the bindings mean what they say.
   *
   * <p>The price is every {@code WhereTraversalStep}, which is every {@code where} whose child
   * carries a scope label at either end — not only the {@code where(__.as(a)…)} spelling the three
   * measured bullets above use. {@code WhereTraversalStep.configureStartAndEndSteps} inserts a
   * {@code WhereStartStep} both for a labelled start and, with a null label, for a child whose
   * <em>end</em> step is labelled, so {@code where(__.out().as(b))} carries one too. What stays
   * unaffected is the rest of the filter surface: a plain {@code where(__.out(k))} is a {@code
   * TraversalFilterStep} whose child carries neither class, as are the {@code filter} / {@code and}
   * / {@code not} children. {@link WhereTraversalStepRecogniser}'s own Javadoc records what that
   * leaves the class reachable for.
   *
   * <p>Two spellings that agree today go with it — {@code where(__.as(a).out(k))} where {@code a} is
   * the current element, and a child whose end label matches what the hop already reaches — because
   * the walker cannot tell those from the divergent ones without resolving the label to an alias.
   * Teaching it to do so would recover them, and is the obvious next move if the surface turns out
   * to matter.
   */
  static final Set<Class<?>> TRANSPARENT_STEPS =
      Set.of(NoOpBarrierStep.class);

  /**
   * Child-scope boundary meaning "every step in this list is top level". No step index can reach
   * {@code Integer.MAX_VALUE}, so the fold latch's boundary term is unconditionally true under it.
   */
  static final int NO_CHILD_SCOPE = Integer.MAX_VALUE;

  /**
   * Production recogniser registry, keyed on the exact step class. Dispatch is O(1) on the step's
   * runtime class, and a step whose class is not a key declines the whole traversal (all-or-nothing),
   * so an unregistered or unexpected subclass fails safe to the native pipeline rather than being
   * misrouted. The registered families:
   *
   * <ul>
   *   <li><b>Source</b> — {@link StartStepRecogniser} claims the vertex source under {@link
   *       GraphStep}.
   *   <li><b>Traversal</b> — {@link VertexStepRecogniser} owns {@link VertexStep} and {@link
   *       VertexStepPlaceholder} (the latter appears on combinator child sub-traversals after {@code
   *       AdjacentToIncidentStrategy} runs recursively during {@code applyStrategies()}) and routes on
   *       {@code returnsEdge()}: a folded bare hop to {@link VertexHopRecogniser}, an edge-returning
   *       {@code outE(L).has(...).inV()} chain to {@link EdgeHopRecogniser}, and a combinator sub-walk
   *       singleton edge-returning hop to {@link CombinatorFoldedHopRecogniser}.
   *   <li><b>Filter</b> — {@link HasStepRecogniser} ({@link HasStep}), {@link
   *       TraversalFilterStepRecogniser} ({@link TraversalFilterStep}), the connectives {@link
   *       AndStepRecogniser} / {@link OrStepRecogniser} / {@link NotStepRecogniser}, and {@link
   *       WhereTraversalStepRecogniser} / {@link WherePredicateStepRecogniser}.
   *   <li><b>Result shaping</b> — {@link DedupGlobalStepRecogniser}; the projections {@link
   *       PropertiesStepRecogniser} / {@link PropertyMapStepRecogniser} / {@link
   *       ElementMapStepRecogniser} / {@link SelectOneStepRecogniser} / {@link SelectStepRecogniser} /
   *       {@link ProjectStepRecogniser}; and pagination {@link OrderGlobalStepRecogniser} / {@link
   *       RangeGlobalStepRecogniser} ({@link RangeGlobalStep} and {@link RangeGlobalStepPlaceholder}).
   *   <li><b>Aggregate</b> — {@link CountGlobalStepRecogniser}; {@link PropertyAggregateStepRecogniser}
   *       for {@code sum} / {@code min} / {@code max} / {@code mean}; and {@link GroupStepRecogniser} /
   *       {@link GroupCountStepRecogniser}.
   *   <li><b>Branch</b> — {@link UnionStepRecogniser} for mid-traversal {@code union(c1, …, cN)},
   *       emitting a multi-plan translation when every child agrees on the projection contract.
   *   <li><b>List shaping</b> — the four terminators register an ordered stage on the shaping rather
   *       than a clause on the statement: {@link FoldStepRecogniser} claims the list form of {@link
   *       FoldStep} (the seeded-reduce form of the same class declines), {@link UnfoldStepRecogniser}
   *       and {@link ReverseStepRecogniser} claim their per-payload steps, and {@link
   *       TailGlobalStepRecogniser} claims both forms {@link TailGlobalStepContract#CONCRETE_STEPS}
   *       enumerates.
   * </ul>
   */
  private static final Map<Class<?>, StepRecogniser> PRODUCTION_RECOGNISERS =
      productionRecognisers();

  /**
   * Builds the production registry: the literal entries below plus one per {@code tail} form.
   *
   * <p>The {@code tail} entries come from {@link TailGlobalStepContract#CONCRETE_STEPS} rather than from
   * two hand-written literals, because the fork owns that enumeration — it is the same list TinkerPop's
   * own placeholder-aware machinery reads, so a third form added upstream reaches this registry instead
   * of silently declining. {@code RangeGlobalStepContract} and {@code VertexStepContract} carry the same
   * constant and their entries below are still literal; converting them is a mechanical change with no
   * behavioural effect while each contract enumerates exactly the two classes already listed.
   */
  private static Map<Class<?>, StepRecogniser> productionRecognisers() {
    var byStepClass = new LinkedHashMap<Class<?>, StepRecogniser>(literalRecogniserEntries());
    for (Class<?> tailForm : TailGlobalStepContract.CONCRETE_STEPS) {
      var previous = byStepClass.put(tailForm, TailGlobalStepRecogniser.INSTANCE);
      assert previous == null
          : "the registry keys one recogniser per step class, but a tail form was already claimed by "
              + previous;
    }
    return Map.copyOf(byStepClass);
  }

  /** The registry entries written as literals, keyed one recogniser per exact step class. */
  private static Map<Class<?>, StepRecogniser> literalRecogniserEntries() {
    return Map.ofEntries(
        Map.entry(GraphStep.class, StartStepRecogniser.INSTANCE),
        Map.entry(VertexStep.class, VertexStepRecogniser.INSTANCE),
        Map.entry(VertexStepPlaceholder.class, VertexStepRecogniser.INSTANCE),
        Map.entry(EdgeVertexStep.class, RedundantEdgeVertexStepRecogniser.INSTANCE),
        Map.entry(HasStep.class, HasStepRecogniser.INSTANCE),
        Map.entry(TraversalFilterStep.class, TraversalFilterStepRecogniser.INSTANCE),
        Map.entry(AndStep.class, AndStepRecogniser.INSTANCE),
        Map.entry(OrStep.class, OrStepRecogniser.INSTANCE),
        Map.entry(NotStep.class, NotStepRecogniser.INSTANCE),
        Map.entry(WhereTraversalStep.class, WhereTraversalStepRecogniser.INSTANCE),
        Map.entry(WherePredicateStep.class, WherePredicateStepRecogniser.INSTANCE),
        Map.entry(DedupGlobalStep.class, DedupGlobalStepRecogniser.INSTANCE),
        Map.entry(PropertiesStep.class, PropertiesStepRecogniser.INSTANCE),
        Map.entry(PropertyMapStep.class, PropertyMapStepRecogniser.INSTANCE),
        Map.entry(ElementMapStep.class, ElementMapStepRecogniser.INSTANCE),
        Map.entry(SelectOneStep.class, SelectOneStepRecogniser.INSTANCE),
        Map.entry(SelectStep.class, SelectStepRecogniser.INSTANCE),
        Map.entry(ProjectStep.class, ProjectStepRecogniser.INSTANCE),
        Map.entry(OrderGlobalStep.class, OrderGlobalStepRecogniser.INSTANCE),
        Map.entry(RangeGlobalStep.class, RangeGlobalStepRecogniser.INSTANCE),
        Map.entry(RangeGlobalStepPlaceholder.class, RangeGlobalStepRecogniser.INSTANCE),
        Map.entry(CountGlobalStep.class, CountGlobalStepRecogniser.INSTANCE),
        Map.entry(SumGlobalStep.class, PropertyAggregateStepRecogniser.INSTANCE),
        Map.entry(MinGlobalStep.class, PropertyAggregateStepRecogniser.INSTANCE),
        Map.entry(MaxGlobalStep.class, PropertyAggregateStepRecogniser.INSTANCE),
        Map.entry(MeanGlobalStep.class, PropertyAggregateStepRecogniser.INSTANCE),
        Map.entry(GroupStep.class, GroupStepRecogniser.INSTANCE),
        Map.entry(GroupCountStep.class, GroupCountStepRecogniser.INSTANCE),
        Map.entry(UnionStep.class, UnionStepRecogniser.INSTANCE),
        Map.entry(FoldStep.class, FoldStepRecogniser.INSTANCE),
        Map.entry(UnfoldStep.class, UnfoldStepRecogniser.INSTANCE),
        Map.entry(ReverseStep.class, ReverseStepRecogniser.INSTANCE));
  }

  /**
   * The only recognisers allowed to claim a step <em>after</em> {@link UnionStepRecogniser} has
   * stashed a multi-plan carrier. Two kinds sit here. Three map their step onto a {@link
   * com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.PostConcatOp} the concatenation
   * can absorb ({@code count}, {@code limit}/{@code range}/{@code skip}, {@code dedup}); two more
   * append a per-payload {@link ListShapingOp} the boundary base applies once over the whole
   * concatenation ({@code unfold}, {@code reverse}), and one appends a window that the positional
   * rule below then refuses in every spelling but one ({@code tail}).
   *
   * <p>The gate has to be here rather than left to each recogniser because {@link #buildResult}'s
   * multi-plan branch reads only the boundary metadata, the shaping, and the post-concat ops: a
   * recogniser that writes into the pattern builder, the alias filters, the RETURN projection, the
   * DISTINCT / GROUP BY / ORDER BY / LIMIT / SKIP fields, or the positional-parameter map after a
   * union has its contribution silently discarded, and the query returns rows that ignore the step.
   * Gating on an allow-list keeps the property true by construction: a recogniser added later is
   * declined post-union until it is deliberately taught to branch on the carrier and added here.
   *
   * <p>The set is read from two places, and both must read this one field. {@link #dispatchAll}
   * consults it per step as the fail-closed gate, and {@link #postUnionSuffixTranslatable} consults
   * it as a look-ahead so {@link UnionStepRecogniser} can decline <em>before</em> forking and
   * walking every child — see that method for why the look-ahead exists.
   *
   * <p>Membership is necessary, not sufficient. {@link RangeGlobalStepRecogniser} sits here because
   * a slice <em>can</em> ride the concatenation, but it accepts only when a {@code count()} follows
   * immediately: the concatenation emits child one's rows then child two's while native {@code
   * union(...)} interleaves the arms, so any surviving positional selection would return a different
   * multiset with the translator on than off. That recogniser's class Javadoc carries the full
   * argument.
   *
   * <p>That second condition rides on {@link StepRecogniser#selectsPositionally}, so adding a member
   * here is two decisions rather than one: whether the recogniser's contribution survives {@link
   * #buildResult}'s multi-plan branch, and whether its step selects rows by position. Every member
   * must override {@code selectsPositionally} rather than inherit the interface default — a unit
   * test over this field pins that, so a member added without an answer fails the build instead of
   * silently inheriting {@code false}. The field is package-private for exactly that test.
   *
   * <p>The three list-shaping members answer that second question differently from each other, and
   * the answers are what decide which post-union spellings survive. {@code unfold} and {@code
   * reverse} are per-payload: each payload is expanded or transformed on its own, so applying the
   * stage once over the concatenation and applying it once per arm produce the same multiset
   * whichever order the arms arrived in. Both answer {@code false} and both translate.
   * {@code tail(n)} keeps the last {@code n} payloads of whatever stream it is handed, which is the
   * position the branch-major concatenation and native's per-traverser interleaving disagree about
   * hardest, so it answers {@code true} and reaches the fork only ahead of an immediate {@code
   * count()} — a spelling {@link #LIST_SHAPING_DRAIN_RECOGNISERS} then declines for its own reason.
   *
   * <p><strong>{@code fold} is deliberately not a member, and that is this list's one recorded
   * exclusion.</strong> A fold over the concatenation is one payload whose value is a {@code List},
   * and a list compares by order, so the two arrival orders would hand back two different answers
   * for {@code union(...).fold()} — the divergence the positional rule exists to stop, arriving
   * through a value rather than through a row position. The alternative was to admit {@code
   * FoldStepRecogniser} with {@code selectsPositionally} answering {@code true}, which reaches the
   * same decline for {@code union(...).fold()} and additionally lets {@code union(...).fold().count()}
   * through the look-ahead. That spelling declines anyway at the list-shaping gate (see {@link
   * #LIST_SHAPING_DRAIN_RECOGNISERS}), so the two designs differ in nothing observable and the
   * simpler one is written down here. Leaving the recogniser off the list also keeps the build gate
   * useful: whoever adds it later has to state a positional answer, because the reflective test
   * fails until they do.
   */
  static final Set<StepRecogniser> POST_UNION_RECOGNISERS =
      Set.of(
          CountGlobalStepRecogniser.INSTANCE,
          RangeGlobalStepRecogniser.INSTANCE,
          DedupGlobalStepRecogniser.INSTANCE,
          UnfoldStepRecogniser.INSTANCE,
          ReverseStepRecogniser.INSTANCE,
          TailGlobalStepRecogniser.INSTANCE);

  /**
   * Pre-built production walker. The walker is stateless — only the immutable {@code recognisers}
   * field — so a single shared instance avoids one allocation per Gremlin traversal that reaches the
   * strategy. Mirrors the singleton pattern the strategy itself uses.
   */
  private static final GremlinStepWalker PRODUCTION_INSTANCE =
      new GremlinStepWalker(PRODUCTION_RECOGNISERS);

  /** Stateless builder used to AND-compose same-alias filters at result-build time; construction is
   *  trivial so a shared instance is fine. */
  private static final MatchWhereBuilder WHERE = new MatchWhereBuilder();

  private final Map<Class<?>, StepRecogniser> recognisers;

  /**
   * Package-private constructor accepting a curated recogniser registry keyed on step class. Both the
   * production singleton and unit tests use this constructor; production code reaches it via {@link
   * #production()}, tests pass fixture registries directly.
   */
  GremlinStepWalker(Map<Class<?>, StepRecogniser> recognisers) {
    this.recognisers = Map.copyOf(recognisers);
  }

  /** Returns the shared production walker wired with the production recogniser registry. */
  static GremlinStepWalker production() {
    return PRODUCTION_INSTANCE;
  }

  /**
   * Unique production-registry recogniser instances. The translation-cache extractor and the
   * {@code contributeShape} override gate both read this set so a recogniser added to the registry
   * without a shape encoder fails the build instead of silently inheriting {@code false}.
   */
  static Set<StepRecogniser> productionRecogniserInstances() {
    return Set.copyOf(PRODUCTION_RECOGNISERS.values());
  }

  /**
   * Pre-walk shape key and {@code ?} bindings for the translation cache, using this walker's
   * production registry so the recogniser that would translate a step is the one that lists the
   * tokens it reads.
   */
  static GremlinShapeExtractor.Extraction extractShape(
      Traversal.Admin<?, ?> traversal, DatabaseSessionEmbedded session) {
    return extractShape(
        traversal,
        session,
        YTDBStrategyUtil.orderIncludesMissingKey(traversal),
        YTDBStrategyUtil.orderByNullsPlacements(traversal));
  }

  /**
   * The caller can provide the order mode to {@link
   * #extractShape(Traversal.Admin, DatabaseSessionEmbedded)}. The strategy resolves the mode once.
   * The same value reaches the shape key and the traversal walk.
   */
  static GremlinShapeExtractor.Extraction extractShape(
      Traversal.Admin<?, ?> traversal,
      DatabaseSessionEmbedded session,
      @Nullable Boolean orderIncludesMissingKey) {
    return extractShape(
        traversal,
        session,
        orderIncludesMissingKey,
        YTDBStrategyUtil.orderByNullsPlacements(traversal));
  }

  static GremlinShapeExtractor.Extraction extractShape(
      Traversal.Admin<?, ?> traversal,
      DatabaseSessionEmbedded session,
      @Nullable Boolean orderIncludesMissingKey,
      ResolvedOrderByNullsPlacement orderByNullsPlacements) {
    return GremlinShapeExtractor.extract(
        PRODUCTION_RECOGNISERS,
        TRANSPARENT_STEPS,
        traversal,
        session,
        orderIncludesMissingKey,
        orderByNullsPlacements);
  }

  /**
   * Attempts to translate {@code traversal} by walking its steps in order. Returns the {@link
   * GremlinToMatchTranslator.TranslationResult} when every step was recognised, otherwise {@code
   * null}.
   */
  @Nullable GremlinToMatchTranslator.TranslationResult walk(Traversal.Admin<?, ?> traversal) {
    return walk(traversal, NO_CHILD_SCOPE, null, null);
  }

  /**
   * The caller can provide the order mode to {@link #walk(Traversal.Admin)}.
   * A {@code null} value asks the walk to resolve the mode.
   */
  @Nullable GremlinToMatchTranslator.TranslationResult walk(
      Traversal.Admin<?, ?> traversal, @Nullable Boolean orderIncludesMissingKey) {
    return walk(traversal, NO_CHILD_SCOPE, orderIncludesMissingKey, null);
  }

  @Nullable GremlinToMatchTranslator.TranslationResult walk(
      Traversal.Admin<?, ?> traversal,
      @Nullable Boolean orderIncludesMissingKey,
      @Nonnull ResolvedOrderByNullsPlacement orderByNullsPlacements) {
    return walk(traversal, NO_CHILD_SCOPE, orderIncludesMissingKey, orderByNullsPlacements);
  }

  /**
   * As {@link #walk(Traversal.Admin)}, but treats every step from index {@code childScopeBoundary}
   * onwards as child-scoped for the fold latch: no container at or past that index is ever
   * classified as folded, however the steps before it are classified.
   *
   * <p>Only the union fork passes a real boundary. {@link UnionForkHostImpl#walkFork} synthesises a
   * traversal out of the recognised prefix plus one arm's steps, so the arm's leading {@code has}
   * follows the prefix's {@code GraphStep} in a flat list and would otherwise read as folded.
   * Natively it is not: {@code rebuildTraversal} scans only the parent's top level and never
   * descends into a union child, so the arm's {@code HasStep} survives and TinkerPop's comparator
   * answers it. The boundary is what keeps the synthesised list from telling the latch otherwise.
   * See {@link RecognitionContext#atTraversalStart()} for the two positions and why they translate
   * differently.
   */
  @Nullable GremlinToMatchTranslator.TranslationResult walk(
      Traversal.Admin<?, ?> traversal, int childScopeBoundary) {
    return walk(traversal, childScopeBoundary, null, null);
  }

  /**
   * The full walk entry point receives resolved order settings. Null values ask this method to
   * resolve the corresponding settings.
   */
  @Nullable GremlinToMatchTranslator.TranslationResult walk(
      Traversal.Admin<?, ?> traversal,
      int childScopeBoundary,
      @Nullable Boolean orderIncludesMissingKey,
      @Nullable ResolvedOrderByNullsPlacement orderByNullsPlacements) {
    // Empty-traversal gate, before any per-step work. A step-less traversal has nothing to translate
    // and could never pin a boundary, so decline it here rather than let it fall through to the
    // terminator invariant below — an empty traversal is a normal shape, not a recogniser bug.
    var steps = traversal.getSteps();
    if (steps.isEmpty()) {
      return null;
    }

    // Reserved-prefix pre-flight: a user label starting with '$' is prohibited — the namespace is
    // reserved for the minted $g2m_ aliases and YouTrackDB query variables — so reject the whole
    // traversal with a ReservedAliasException. GremlinToMatchStrategy's throw-safety net propagates
    // this one type rather than swallowing it, so the query fails loudly instead of running on native
    // (which accepts the '$' label). Purely lexical (no graph access), so it runs before flag
    // resolution below.
    rejectReservedPrefixLabels(steps);

    // Resolve the polymorphism flag once. isPolymorphic is null-safe: it gates on an attached YTDB
    // graph + transaction before touching tx(), so a detached EmptyGraph or non-YTDB graph yields
    // null rather than throwing. A null result means the traversal has no resolvable polymorphism
    // setting and cannot be translated faithfully — decline the whole walk before building the
    // context. Owning the resolution here keeps every recogniser free of the flag's initialisation.
    Boolean resolved = YTDBStrategyUtil.isPolymorphic(traversal);
    if (resolved == null) {
      return null;
    }
    boolean polymorphic = resolved;

    // Resolve the EdgeLabelVerificationStrategy presence once too, so resolveEdgeLabel reads a
    // boolean instead of scanning the strategy list per hop.
    boolean edgeLabelVerification =
        traversal.getStrategies().getStrategy(EdgeLabelVerificationStrategy.class).isPresent();

    // Resolve the schema snapshot once for the has(...) recogniser's non-String Text type gate. The
    // isPolymorphic resolution above already proved an attached YTDB session (it returns null
    // otherwise, declining the walk), so the session resolves here too; a null schema is a defensive
    // fallback that disables the type gate, translating string predicates best-effort.
    var session = YTDBStrategyUtil.resolveYtdbSession(traversal);
    Schema schema = session != null ? session.getSchema() : null;

    var ctx = new WalkerContext(polymorphic, edgeLabelVerification, schema, recognisers);
    // Resolve ProductiveByStrategy's productive-key set once, for the same reason the two flags
    // above are resolved once: every by(...) modulator would otherwise re-scan the strategy list.
    ctx.setProductiveByKeys(
        traversal
            .getStrategies()
            .getStrategy(ProductiveByStrategy.class)
            .map(ProductiveByStrategy::getProductiveKeys)
            .orElse(null));
    // Resolve order behavior through the same entry points used by native execution.
    ctx.setOrderIncludesMissingKey(
        Boolean.TRUE.equals(
            orderIncludesMissingKey != null
                ? orderIncludesMissingKey
                : YTDBStrategyUtil.orderIncludesMissingKey(traversal)));
    var resolvedNullsPlacements =
        orderByNullsPlacements != null
            ? orderByNullsPlacements
            : YTDBStrategyUtil.orderByNullsPlacements(traversal);
    if (resolvedNullsPlacements == null) {
      return null;
    }
    ctx.setOrderByNullsPlacements(resolvedNullsPlacements);
    var cursor = new StepStreamCursor(steps, TRANSPARENT_STEPS);
    // Install the union fork host after the cursor exists: the host reads prefix length from the
    // cursor position after UnionStepRecogniser.take(), and keeps the parent Admin private.
    ctx.setUnionForkHost(
        new UnionForkHostImpl(
            traversal, cursor, ctx, recognisers, resolvedNullsPlacements));

    // Cursor-driven dispatch. A missing recogniser or a DECLINE declines the whole traversal
    // (all-or-nothing), returning false; the shared driver is reused by the sub-walk below.
    if (!dispatchAll(cursor, ctx, recognisers, childScopeBoundary)) {
      return null;
    }

    // Invariant: a fully-recognised non-empty traversal has its terminator metadata pinned — boundary
    // alias, output type, and return class — by the recogniser that owns its terminator. Empty
    // traversals are gated out above, so reaching here with a null field is not a normal decline: it
    // means a recogniser returned ACCEPTED without pinning the boundary, a recogniser-logic bug.
    //
    // The assert surfaces that bug loudly under -ea (the test/CI default); under -da the decline
    // below is the safety net, so rather than build a null-bearing TranslationResult the walk declines
    // and the traversal stays on the native pipeline unchanged.
    assert ctx.boundaryAlias != null && ctx.outputType != null && ctx.returnClass != null
        : "walk recognised all " + steps.size() + " step(s) but left the boundary unpinned";
    if (ctx.boundaryAlias == null || ctx.outputType == null || ctx.returnClass == null) {
      return null;
    }

    return buildResult(ctx);
  }

  /**
   * Runs the cursor-driven dispatch loop over {@code cursor} against {@code recognisers}, contributing
   * each recognised step to {@code ctx}. Returns {@code true} when every step was recognised, {@code
   * false} on the first step whose exact class has no recogniser or whose recogniser declines
   * (all-or-nothing). This is the loop shared by the top-level {@link #walk} and the {@link #subWalk}
   * sub-walk: the sub-walk needs exactly this loop and none of {@code walk}'s surrounding
   * machinery — the reserved-prefix scan, the once-per-walk flag resolution, the terminator invariant,
   * and {@code buildResult} are top-level-only.
   *
   * <p>{@code childScopeBoundary} is the index from which the step list stops being top level. Pass
   * {@link #NO_CHILD_SCOPE} for a genuine top-level walk; the union fork passes its prefix length
   * (see {@link #walk(Traversal.Admin, int)}).
   */
  private static boolean dispatchAll(
      StepStreamCursor cursor,
      RecognitionContext ctx,
      Map<Class<?>, StepRecogniser> recognisers,
      int childScopeBoundary) {
    Step<?, ?> head;
    // The drain half of the list-shaping rule (see mayFollowListShaping): set once a recogniser that
    // is not a per-payload shaper has claimed a step while a stage was captured, after which nothing
    // may follow it. Walker-local rather than read off the context because ListShapingOp carries no
    // op-type discriminator, so the claiming recogniser's membership is the only classification the
    // loop has.
    boolean afterListShapingDrain = false;
    while (true) {
      // Read the position before peek(), because peek() advances past any transparent steps at the
      // head. rebuildTraversal has no transparency rule: a NoOpBarrierStep is an ordinary "else"
      // step there and clears isTraversalStart, so a barrier the cursor swallowed still breaks the
      // fold. Comparing the two positions is how the loop observes a swallowed one.
      int positionBeforePeek = cursor.position();
      head = cursor.peek();
      if (cursor.position() > positionBeforePeek) {
        ctx.setAtTraversalStart(false);
      }
      if (head == null) {
        return true;
      }
      var recogniser = recognisers.get(head.getClass());
      if (recogniser == null) {
        return false;
      }
      // Post-union suffix gate (see POST_UNION_RECOGNISERS). Only the post-concat-aware recognisers
      // may claim a step once a union carrier is on the context; every other one would write into
      // state buildResult discards for a multi-plan translation, so decline the whole walk and let
      // the traversal run on the native pipeline. Membership is necessary and not sufficient: a
      // member that selects rows by position may only stand ahead of an immediate count(), and that
      // second condition is applied here through the same body the pre-fork look-ahead uses, so
      // neither reader can admit a step the other refuses. Without it the look-ahead would be the
      // only guard against a translated union(...).tail(n) — a property of call order rather than
      // of the gates, and one a later change to the fork path could take away silently.
      if (ctx.hasUnionCarrier()
          && (!POST_UNION_RECOGNISERS.contains(recogniser)
              || !postUnionPositionalGateSatisfied(cursor, recognisers, recogniser, head, 0))) {
        return false;
      }
      // Single-plan cardinality gate (see capturedCardinalityClause and the allow-list below).
      // Once a SKIP / LIMIT / DISTINCT is captured, only the pure projections may claim a further
      // step; anything else would run before the clause in the compiled statement and so return a
      // different row set.
      if (capturedCardinalityClause(ctx) && !POST_CARDINALITY_RECOGNISERS.contains(recogniser)) {
        return false;
      }
      // Single-plan list-shaping gate (see capturedListShapingOp and mayFollowListShaping). Once a
      // terminator has appended a stream stage, only a per-payload shaper or a drain may claim a
      // further step, and nothing at all may claim one behind a drain or a window — every other
      // contribution rides the statement, which MATCH applies before the stage runs.
      if (capturedListShapingOp(ctx)
          && !mayFollowListShaping(
              recogniser,
              afterListShapingDrain,
              LIST_SHAPING_PER_PAYLOAD_RECOGNISERS,
              LIST_SHAPING_DRAIN_RECOGNISERS)) {
        return false;
      }
      int positionBefore = cursor.position();
      // Read beside positionBefore for the invariant checked after the call: a recogniser the gate
      // admitted behind a captured stage contributes through appendListShapingOp and never replaces
      // the shaping wholesale, which would erase the stage it was admitted behind (see
      // LIST_SHAPING_PER_PAYLOAD_RECOGNISERS for why that is a membership condition). The whole list
      // rather than a boolean, because the check has to show the captured stages survived and not
      // merely that some stage is captured afterwards — see listShapingOpsSurvived. The list is
      // immutable, so holding this reference across the call is safe.
      List<ListShapingOp> opsBefore = ctx.listShapingOps();
      Outcome outcome = recogniser.recognize(cursor, ctx);
      if (outcome == Outcome.DECLINE) {
        return false;
      }
      // An ACCEPTED must have advanced the cursor. An accept that consumed nothing would re-dispatch
      // the same head forever, so it is a recogniser bug: the assert surfaces it loudly under -ea (an
      // AssertionError, which GremlinToMatchStrategy's RuntimeException-only throw-safety net does not
      // swallow); under -da the defensive decline keeps such a bug from spinning a live query.
      assert cursor.position() > positionBefore
          : "recogniser for "
              + head.getClass().getSimpleName()
              + " returned ACCEPTED without consuming any step";
      if (cursor.position() <= positionBefore) {
        return false;
      }
      // A recogniser admitted behind a captured stage must leave that stage in place. Dropping it —
      // which any setResultShaping call does, since the replace covers listShapingOps — disarms the
      // gate in the same instant, so the walk would keep claiming steps and buildResult would ship a
      // shaping with the stage gone and no decline anywhere. Unreachable while every member of the two
      // allow-lists honours its append-only membership condition, which none breaks today — so the
      // assert and the decline below are the net for whoever adds the member that does, paired the way
      // this file's other recogniser-contract invariants are: the assert names the bug under -ea, the
      // decline keeps a -da build on the native pipeline instead of shipping a clobbered shaping.
      boolean listShapingStagesSurvived = listShapingOpsSurvived(opsBefore, ctx.listShapingOps());
      assert listShapingStagesSurvived
          : "recogniser for "
              + head.getClass().getSimpleName()
              + " was admitted behind a captured list-shaping stage and dropped it";
      if (!listShapingStagesSurvived) {
        return false;
      }
      // Latch the drain half of the list-shaping rule: arm unless the recogniser that just ran is a
      // per-payload shaper, because everything else that can hold a stage drains or windows the
      // stream and nothing may follow it. Gate the next iteration on that rather than on the op,
      // which carries no discriminator. The classification is exact because the gate above runs
      // before the recogniser: reaching here with a stage captured means either this recogniser
      // appended it, or the gate admitted the recogniser as one of the two list-shaping kinds. A
      // recogniser that appends while on neither list is latched as a drain, which is the fail-closed
      // direction, and so is a union recogniser whose agreed child shaping carried ops.
      if (capturedListShapingOp(ctx)
          && !LIST_SHAPING_PER_PAYLOAD_RECOGNISERS.contains(recogniser)) {
        afterListShapingDrain = true;
      }
      // Advance the fold latch, mirroring YTDBGraphStepStrategy.rebuildTraversal's isTraversalStart:
      // a GraphStep opens a fold (any GraphStep, not only the first — a mid-traversal V() restarts
      // one there too), a HasStep leaves an open fold open, and every other step closes it. Only the
      // head is classified: a recogniser that consumes several steps at once starts with a
      // non-HasStep head (an edge hop's outE, a union's UnionStep), which closes the fold, and no
      // registered recogniser other than StartStepRecogniser can consume a GraphStep, so a fold can
      // never open from a step the loop did not classify. Read before the update by the recogniser
      // that just ran, so a HasStep sees the state of the run it closes rather than joins.
      //
      // The cursor's new position is the index of the next head, so comparing it against
      // childScopeBoundary closes the latch across the prefix/child seam of a synthesised step list
      // — see walk(Traversal.Admin, int). On a genuine top-level walk the boundary is
      // NO_CHILD_SCOPE and the term is always true.
      ctx.setAtTraversalStart(
          (head instanceof GraphStep<?, ?>
              || (head instanceof HasStep<?> && ctx.atTraversalStart()))
              && cursor.position() < childScopeBoundary);
    }
  }

  /**
   * Whether the walk has already captured a statement-level cardinality clause — {@code SKIP},
   * {@code LIMIT}, or {@code RETURN DISTINCT} — which makes every step after it untranslatable on
   * the single-plan path.
   *
   * <p>The rule this gate is one half of: every clause the translator captures at statement level
   * is applied by MATCH at a fixed point in the statement, while Gremlin applies the corresponding
   * step where the user wrote it. Two spellings differing only in that step's position compile to
   * one statement, and the statement can mean only one of them.
   *
   * <p>Here that reads: the clauses ride the assembled statement and MATCH applies them after the
   * pattern and every {@code WHERE}, where Gremlin applies them to the stream the next step
   * consumes. A step recognised afterwards lands on the wrong side. {@code g.V().limit(2).out()}
   * compiles to the same statement as {@code g.V().out().limit(2)} and returns the first two
   * out-neighbours of the whole graph, where native returns the out-neighbours of the first two
   * vertices. Measured on a five-vertex fixture whose only edge-bearing vertex has three out-edges
   * — two rows translated against zero native for {@code limit(2).out()}, one against three for
   * {@code skip(2).out()}, one against zero for {@code limit(2).has(name, x)}, and a different
   * pair of vertices for {@code limit(2).order().by(name)}. {@code RETURN DISTINCT} behaves the
   * same way: {@code g.V().in(k).dedup().out(k)} returns two rows translated against three native,
   * the duplicate being a target reachable from two of its in-neighbours.
   *
   * <p>Almost no suffix is exempt: hops and filters change the row set, {@code order()} changes
   * which rows the clause selects, and an aggregate consumes a stream the clause was meant to
   * bound. {@link #POST_CARDINALITY_RECOGNISERS} holds the exceptions and says why they are ones.
   * A slice that normalises away to nothing ({@code skip(0)}, {@code range(0, -1)}) never arms the
   * gate — {@link RangeGlobalStepRecogniser} accepts it without setting a clause.
   *
   * <p>This gate says nothing about the complement ordering, where the slice comes last: there the
   * statement applies the clause where the user wrote it, so the two spellings do not collide. That
   * ordering carries a hazard of its own. A slice last behind a captured {@code ORDER BY} cuts into
   * a tie group the sort leaves unordered, and the two pipelines resolve the tie differently — the
   * measured case is in {@link RangeGlobalStepRecogniser}'s "A slice behind a captured {@code ORDER
   * BY}", which declines the shape from the recogniser's own side rather than here.
   *
   * <p>This is the single-plan twin of the post-union gate above, and it lives in the loop for the
   * same reason: a recogniser added later inherits it without being told.
   *
   * <p>{@link GremlinAggregateAssembler} tests the same three clauses before an aggregate, and the
   * relationship is not the one it looks like. That test is not a second route to this property —
   * it holds out a call site that would otherwise be <em>safe</em>. The aggregate path writes its
   * presence conjunct into the pattern, so its row-dropping happens before the clause exactly as
   * native does it. The unguarded projection path is the wrong one, because its drop rides
   * post-plan shaping instead; {@link RangeGlobalStepRecogniser} carries the other half of that
   * story and promotes the drop into a pattern conjunct when a slice arrives.
   */
  private static boolean capturedCardinalityClause(RecognitionContext ctx) {
    return ctx.skip() != null || ctx.limit() != null || ctx.returnDistinct();
  }

  /**
   * The only recognisers allowed to claim a step once {@link #capturedCardinalityClause} holds — the
   * three pure projections, whose entire contribution is RETURN columns, result shaping, and the
   * boundary pin.
   *
   * <p>Membership is a claim about <em>when</em> a contribution takes effect, and the test is
   * whether the recogniser can change the row set, its order, or its multiplicity. These three
   * cannot. {@code GremlinProjectionAssembler.configureSingleKeyValues} and {@code
   * configurePropertyMap} write only the RETURN columns and a {@link
   * com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.ResultShaping}; the row-dropping
   * half of {@code values(k)} rides {@code dropOnAbsent} on that shaping, which the boundary step
   * applies to the plan's output — after {@code SKIP} / {@code LIMIT}, which is where Gremlin
   * applies it too. Changing which columns a row carries does not move the row.
   *
   * <p>Across {@code RETURN DISTINCT} the same members hold, for a reason worth naming because it
   * rests on a detail: {@code values(k)} projects the boundary entity <em>alongside</em> the value,
   * so the {@code DISTINCT} ranges over {@code (entity, value)} and cannot collapse two distinct
   * elements that happen to share a value — which is what native {@code dedup()} on elements also
   * refuses to do. Were the entity column ever dropped from that projection, this membership would
   * stop being sound.
   *
   * <p>The projection family is not admitted wholesale. {@code SelectOneStepRecogniser} and {@code
   * SelectStepRecogniser} call {@code ByModulatorPresence.requireModulatedProperty} for a {@code
   * by(key)} modulator, which contributes {@code key IS DEFINED} into the pattern's alias filters —
   * a filter that runs before the slice counts rows, which is the whole defect this gate exists to
   * close. They stay out, so {@code g.V().as("a").limit(2).select("a").by(k)} declines.
   *
   * <p>Fail-closed by construction, like the post-union allow-list above: a recogniser added later
   * is refused after a slice until someone establishes that its contribution lands on the far side
   * of the clause and adds it here.
   */
  private static final Set<StepRecogniser> POST_CARDINALITY_RECOGNISERS =
      Set.of(
          PropertiesStepRecogniser.INSTANCE,
          PropertyMapStepRecogniser.INSTANCE,
          ElementMapStepRecogniser.INSTANCE);

  /**
   * Whether the walk has already captured a list-shaping stream stage — a {@code fold} / {@code
   * unfold} / {@code reverse} / {@code tail} op appended through {@link
   * RecognitionContext#appendListShapingOp} — which makes almost every step after it untranslatable
   * on the single-plan path.
   *
   * <p>The rule is the one {@link #capturedCardinalityClause} states, read at a different seam: a
   * translated traversal has two halves that run at different times, and a step recognised on the
   * wrong side of the boundary between them lands after what Gremlin puts before it. Here the two
   * halves are the statement and the boundary base's post-projection stream. Everything else a
   * recogniser contributes — the pattern, every {@code WHERE}, {@code GROUP BY}, {@code ORDER BY},
   * {@code SKIP} / {@code LIMIT}, {@code RETURN DISTINCT}, an aggregate's {@code count(*)} — is
   * applied by MATCH as the plan runs. A list-shaping op is applied by {@code
   * AbstractMatchPlanStep.applyListShaping} strictly afterwards, over the payload stream the
   * projection already built.
   *
   * <p>So the wrong answers are ordinary shapes. {@code g.V().values("name").fold().limit(2)}
   * compiles {@code LIMIT 2} into the statement and then folds two rows into a list of two, where
   * native folds every row into one list and keeps the one list it made. {@code .fold().order()},
   * {@code .fold().count()} and {@code .unfold().dedup()} are the same defect with a different
   * clause: the clause bounds, sorts or counts the rows the stage was meant to consume.
   *
   * <p>{@link #LIST_SHAPING_PER_PAYLOAD_RECOGNISERS} and {@link #LIST_SHAPING_DRAIN_RECOGNISERS} hold
   * the exceptions and argue each one, and {@link #mayFollowListShaping} is the rule the loop applies
   * to them.
   *
   * <p>Like its cardinality twin, this gate lives in the loop rather than in the terminator
   * recognisers: a recogniser added later inherits it without being told, where a per-recogniser
   * check has to be remembered by every author after this one. The wrapper reduces the context's op
   * list to the one bit the gate needs, where the cardinality clauses live in three fields its twin
   * reads in turn — the two call sites stay symmetric and this rule gets a home beside that one. The
   * loop's other read of the same list is {@link #listShapingOpsSurvived}, which needs the ops
   * themselves rather than their presence.
   */
  private static boolean capturedListShapingOp(RecognitionContext ctx) {
    return !ctx.listShapingOps().isEmpty();
  }

  /**
   * The per-payload list shapers — {@code unfold} and {@code reverse} — which may claim a step once
   * {@link #capturedListShapingOp} holds and whose own stage another one may in turn follow.
   *
   * <p>The first condition is what {@link #POST_CARDINALITY_RECOGNISERS} asks — can this recogniser
   * change the row set, its order, or its multiplicity as the statement sees it. These two cannot:
   * each is one more stage on the payload stream the captured stage reshaped, {@code
   * applyListShaping} runs it in declared order behind that stage, and that is where Gremlin runs it
   * too. {@code reverse().unfold()} and {@code unfold().reverse()} are both accepted for that reason,
   * and stay observably different shapes because the carrier is ordered. Every recogniser outside
   * this field and {@link #LIST_SHAPING_DRAIN_RECOGNISERS} writes into the statement, which MATCH
   * applies before the boundary base builds the stream a stage reshapes — {@link
   * #capturedListShapingOp} carries the measured shapes.
   *
   * <p>The second condition is specific to this gate: a member contributes through {@link
   * RecognitionContext#appendListShapingOp} only, and never calls {@link
   * RecognitionContext#setResultShaping}. That call replaces the whole record, {@code
   * listShapingOps} included, so it erases the very stage the gate admitted the member behind — and
   * because the gate reads whether ops exist, it disarms in the same instant and the loss produces no
   * decline anywhere. The three pure projections on the cardinality allow-list each rebuild from
   * {@code ResultShaping.NONE}, so they fail this second condition even though they pass the first:
   * the two allow-lists are not interchangeable. {@link #dispatchAll} does not take the condition on
   * the field's word — it compares the captured ops before and after every accept through {@link
   * #listShapingOpsSurvived}, so a member that breaks it fails loudly under {@code -ea} and declines
   * the walk under {@code -da} rather than shipping a shaping with the stage silently gone.
   *
   * <p>Both members satisfy the two conditions. {@code UnfoldStepRecogniser} appends a flat-map stage
   * and {@code ReverseStepRecogniser} appends a per-payload value transform; neither writes a clause,
   * a RETURN column or a boundary pin, and neither calls {@code setResultShaping}.
   */
  static final Set<StepRecogniser> LIST_SHAPING_PER_PAYLOAD_RECOGNISERS =
      Set.of(UnfoldStepRecogniser.INSTANCE, ReverseStepRecogniser.INSTANCE);

  /**
   * The list-shaping drains and windows — {@code fold} and {@code tail} — which may claim a step once
   * {@link #capturedListShapingOp} holds but which nothing may follow. Both membership
   * conditions on {@link #LIST_SHAPING_PER_PAYLOAD_RECOGNISERS} hold for both members: the whole
   * contribution is one more stage on the payload stream rather than a clause on the statement, and it
   * is made through {@link RecognitionContext#appendListShapingOp} alone.
   *
   * <p>A drain takes N payloads to one and a window takes them to a bounded few, so a stage behind
   * either reshapes an output the user never wrote a stage for: {@code fold().unfold()} and {@code
   * fold().tail(3)} decline. The rule bites only on what comes after a drain. {@code
   * reverse().fold()} folds the reversed payloads, which is what Gremlin does with that spelling too,
   * so the drain is admitted here and {@link #dispatchAll}'s latch refuses whatever comes after it.
   * Two fields rather than one is what lets the gate say both things: a
   * single allow-list read by the gate and the latch alike would either decline {@code
   * reverse().fold()} or admit {@code fold().unfold()}, since one membership answer cannot mean
   * "may follow" at the gate and "is a drain" at the latch.
   *
   * <p>A drain never reaches a post-union suffix, which is worth writing down because {@link
   * #POST_UNION_RECOGNISERS} looks like a place to add one. {@code union(...).fold().count()} and
   * {@code union(...).tail(1).count()} both need {@code count} to claim a step behind the captured
   * stage, and {@code count} writes {@code count(*)} into the statement, so this gate declines them
   * however that allow-list is populated. The decline is the right answer — {@code count(*)} rides
   * the statement and would count the concatenation's rows rather than the one drained payload — so
   * widening either allow-list to make the shape translate ships a wrong scalar instead of fixing
   * anything.
   */
  static final Set<StepRecogniser> LIST_SHAPING_DRAIN_RECOGNISERS =
      Set.of(FoldStepRecogniser.INSTANCE, TailGlobalStepRecogniser.INSTANCE);

  /**
   * The rule {@link #dispatchAll} applies once a list-shaping stage is captured: {@code recogniser}
   * may claim the step at the head only when it is one of the two list-shaping kinds <em>and</em> no
   * drain or window has claimed a step already.
   *
   * <p>Three inputs, because a membership answer alone cannot express both halves of the composition
   * rule. {@code perPayloadShapers} and {@code drains} together say which recognisers contribute
   * another stage rather than a statement clause, and the two fields argue their own memberships.
   * {@code afterDrain} says whether a stage already captured drains or windows the stream, in which
   * case nothing may follow it at all: without that term {@code fold().unfold()} would translate,
   * because {@code unfold} is per-payload and passes the membership test behind the drain.
   *
   * <p>The gate reads both sets and the latch reads only {@code perPayloadShapers}, which is the
   * whole reason they are two sets. The latch arms unless the recogniser that just ran is a
   * per-payload shaper, so a drain is admitted behind a stage ({@code reverse().fold()}) and still
   * stops the walk from claiming anything after it ({@code fold().unfold()}). {@link ListShapingOp}
   * carries no op-type discriminator, so the claiming recogniser's own membership is the
   * classification the loop latches
   * on — which also makes the latch fail closed for a shaper added later: a recogniser that appends
   * while on neither list is treated as a drain.
   *
   * <p>Package-private and set-parameterised rather than reading the fields so every row of the rule
   * can be asserted over synthetic sets — the reason {@link #bindPathItemConstraints} is
   * package-private as well. Production traversals reach the admit branch ({@code reverse().fold()} is
   * one) and the refusals ({@code fold().unfold()}), so the rule is driven end to end too; the
   * synthetic-set tests are what keep each row attributable to this method rather than to a membership.
   *
   * @param recogniser the recogniser dispatch selected for the head's exact runtime class
   * @param afterDrain whether a drain or a window has already claimed a step
   * @param perPayloadShapers see {@link #LIST_SHAPING_PER_PAYLOAD_RECOGNISERS}
   * @param drains see {@link #LIST_SHAPING_DRAIN_RECOGNISERS}
   * @return {@code true} when the recogniser may claim the step behind the captured stages
   */
  static boolean mayFollowListShaping(
      StepRecogniser recogniser,
      boolean afterDrain,
      Set<StepRecogniser> perPayloadShapers,
      Set<StepRecogniser> drains) {
    return !afterDrain
        && (perPayloadShapers.contains(recogniser) || drains.contains(recogniser));
  }

  /**
   * The second half of what {@link #dispatchAll} checks around an accept behind a captured stage:
   * every op captured before the recogniser ran is still captured after it, in the same order and at
   * the same place. {@code after} must start with {@code before}.
   *
   * <p>A prefix comparison rather than a presence or a count one, because presence and count are what
   * the violating shape defeats. The gate reads whether any op is captured ({@link
   * #capturedListShapingOp}), so an admitted member that calls {@link
   * RecognitionContext#setResultShaping} — dropping the stage it was admitted behind, since the
   * replace covers the ops — and then appends its own leaves exactly one op captured before and
   * after. {@code values("name").reverse().unfold()} would ship {@code [unfold]} where the reader
   * expects {@code [reverse, unfold]} and return the values unreversed, with the gate still armed and
   * nothing declined anywhere. Since every member of both allow-lists appends an op by definition,
   * that ordering is the shape a broken membership actually takes.
   *
   * <p>Ops a member appends of its own land after the prefix, so an append-only contribution — the
   * second membership condition on {@link #LIST_SHAPING_PER_PAYLOAD_RECOGNISERS} — always passes, and
   * a legal composition like {@code reverse().unfold()} is not refused. An empty {@code before} is a
   * prefix of everything, which is why the loop can run the check on every accept rather than only
   * behind a captured stage.
   *
   * <p>{@link ListShapingOp} has no value equality, so the comparison is by reference: the ops that
   * were there must be the same instances, not merely equal-looking ones.
   *
   * <p>Package-private rather than private for the reason {@link #mayFollowListShaping} is: the
   * violating shape needs a member of one of the two allow-lists that breaks its own append-only
   * condition, which no member does, so the in-loop check's {@code false} arm cannot be driven from a
   * traversal — a unit test against this method is the only net it has.
   *
   * @param before the ops the context carried when the recogniser was dispatched
   * @param after the ops it carries now
   * @return {@code true} when {@code after} starts with {@code before}
   */
  static boolean listShapingOpsSurvived(
      List<ListShapingOp> before, List<ListShapingOp> after) {
    return after.size() >= before.size() && after.subList(0, before.size()).equals(before);
  }

  /**
   * Look-ahead form of {@link #dispatchAll}'s post-union gate: {@code true} when every step still
   * ahead of {@code cursor} would be claimed by a {@link #POST_UNION_RECOGNISERS} member, including
   * the vacuous case of no steps left. Reads the same allow-list the in-loop gate reads, so the two
   * agree on which recognisers may claim a post-union step. The positional rule below is a second
   * gate, deliberately applied here rather than left to its recogniser; both readers reach it
   * through one shared body ({@link #postUnionPositionalGateSatisfied}), so the two cannot drift
   * the way two hand-written copies of the condition would.
   *
   * <p>Why look ahead at all: {@link UnionStepRecogniser#recognize} forks the recognised prefix into
   * every child and runs a complete sub-walk per child before returning, and the in-loop gate only
   * fires on the step <em>after</em> the union. A suffix the gate will refuse therefore throws away
   * N full sub-walks on every traversal compilation — {@code applyStrategies()} runs per execution
   * and no walk-level cache exists. Calling this before the fork turns that into an O(suffix) scan
   * over steps already in memory.
   *
   * <p>The scan reads through {@link StepCursor#peek(int)}, which leaves the cursor position
   * untouched and skips transparent steps, so a barrier in the suffix is not mistaken for an
   * unclaimable step. Recogniser lookup is by exact class against the same registry dispatch uses,
   * so a step class that maps onto an allow-listed recogniser through a second key (the {@code
   * RangeGlobalStepPlaceholder} → {@link RangeGlobalStepRecogniser} entry) is accepted here exactly
   * as dispatch would accept it.
   *
   * <p>This is a necessary condition, not a simulation: an allow-listed recogniser may still decline
   * its own step (a second {@code count()}, a {@code dedup(labels)}), in which case the fork is
   * still paid and the in-loop gate plus the recogniser decline the walk. Fail-closed is preserved
   * in both directions — the two readers apply the same two conditions over the same allow-list and
   * the same shared positional body, so neither admits a shape the other refuses.
   *
   * <p>The one gate applied here as well as in the dispatch loop, rather than left to its
   * recogniser, is the positional one: a step that selects rows by position needs a {@code count()}
   * immediately after it (see {@link RangeGlobalStepRecogniser}), and {@code union(...).limit(n)} is
   * common enough that paying N discarded sub-walks per compilation for it would give back most of
   * what this look-ahead exists to save. The shared body asks the recogniser through {@link
   * StepRecogniser#selectsPositionally} rather than testing one recogniser's identity or re-deriving
   * the normalisation, so a slice that normalises away to nothing still reaches the fork exactly as
   * it did before, the look-ahead stays no stricter than the recogniser, and a member added to
   * {@link #POST_UNION_RECOGNISERS} later gets the positional gate along with the membership one.
   *
   * <p>The two halves spell "a count follows" differently on purpose. Here the next step is resolved
   * through {@code recognisers}, the same registry dispatch would use, so a curated test registry is
   * simulated faithfully; the recogniser matches {@code CountGlobalStep} by exact class, which is
   * what it can see from a bare cursor. {@code CountGlobalStep} is the sole production key mapping
   * to {@link CountGlobalStepRecogniser}, so the two conditions coincide under the production
   * registry, and both drift directions decline rather than translate.
   */
  static boolean postUnionSuffixTranslatable(
      StepCursor cursor, Map<Class<?>, StepRecogniser> recognisers) {
    for (var ahead = 0;; ahead++) {
      var step = cursor.peek(ahead);
      if (step == null) {
        return true;
      }
      // An unregistered class has no recogniser at all, which dispatch declines before it reaches
      // the gate. Check for it separately — Set.of(...).contains(null) throws.
      var recogniser = recognisers.get(step.getClass());
      if (recogniser == null || !POST_UNION_RECOGNISERS.contains(recogniser)) {
        return false;
      }
      if (!postUnionPositionalGateSatisfied(cursor, recognisers, recogniser, step, ahead)) {
        return false;
      }
    }
  }

  /**
   * The second of the two post-union conditions, asked once membership has been established: a step
   * that selects rows by position may stand after a union only when the very next significant step
   * is an immediate {@code count()}, which collapses the concatenation to a cardinality before the
   * arrival-order difference can be observed. A step that selects no position passes unconditionally.
   *
   * <p>Both readers of the post-union rule call this — {@link #postUnionSuffixTranslatable} while
   * scanning the suffix before the fork, {@link #dispatchAll} on its own head once the union carrier
   * is on the context — which is what makes the in-loop gate no weaker than the look-ahead by
   * construction rather than by call order. {@code ahead} is the caller's scan offset, so the
   * look-ahead asks about the step it is inspecting and the dispatch loop asks at {@code 0} about
   * the head it already peeked; {@link StepCursor#peek(int)} leaves the position untouched in both
   * cases and skips transparent steps, so a barrier between the two is not mistaken for the
   * successor.
   *
   * <p>The successor is resolved through {@code recognisers} rather than by step class, matching how
   * dispatch would resolve it, so a curated test registry is simulated faithfully and an
   * unregistered successor declines.
   */
  private static boolean postUnionPositionalGateSatisfied(
      StepCursor cursor,
      Map<Class<?>, StepRecogniser> recognisers,
      StepRecogniser recogniser,
      Step<?, ?> step,
      int ahead) {
    if (!recogniser.selectsPositionally(step)) {
      return true;
    }
    var next = cursor.peek(ahead + 1);
    return next != null && recognisers.get(next.getClass()) == CountGlobalStepRecogniser.INSTANCE;
  }

  /**
   * Drives a sub-walk of {@code child} against {@code recognisers}, capturing the child's
   * contributions into a fresh {@link SubTraversalPredicateAdapter} that wraps {@code parent}. The
   * seam a logical-combinator recogniser reaches through {@link RecognitionContext#walkChild}: it runs
   * the same dispatch loop the top-level walk uses, but over the child's step list and against the
   * delegating capture context, so alias minting bottoms out at the top-level context while every
   * contribution stays buffered in the returned adapter until the combinator commits it.
   *
   * <p>An empty child declines up front, mirroring {@link #walk}'s empty-traversal gate — a combinator
   * child with no steps expresses no filter. Otherwise the adapter's {@link
   * SubTraversalPredicateAdapter#outcome()} is {@link Outcome#ACCEPTED} when every child step was
   * recognised and {@link Outcome#DECLINE} on the first unrecognised one.
   */
  static SubTraversalPredicateAdapter subWalk(
      Traversal.Admin<?, ?> child,
      RecognitionContext parent,
      Map<Class<?>, StepRecogniser> recognisers) {
    var adapter = new SubTraversalPredicateAdapter(parent, recognisers);
    var steps = new ArrayList<>(child.getSteps());
    // Anonymous child traversals inside where/and/or/not sometimes carry a leading GraphStep
    // placeholder; the sub-walk's meaningful steps start after it.
    while (!steps.isEmpty() && steps.getFirst() instanceof GraphStep) {
      steps.removeFirst();
    }
    if (steps.isEmpty()) {
      adapter.markOutcome(Outcome.DECLINE);
      return adapter;
    }
    var cursor = new StepStreamCursor(steps, TRANSPARENT_STEPS);
    // NO_CHILD_SCOPE, not 0: the sub-walk's adapter answers atTraversalStart() with a hard false
    // and swallows every write, so the boundary term has nothing to add here.
    adapter.markOutcome(
        dispatchAll(cursor, adapter, recognisers, NO_CHILD_SCOPE)
            ? Outcome.ACCEPTED
            : Outcome.DECLINE);
    return adapter;
  }

  /**
   * Rejects the whole traversal with a {@link ReservedAliasException} if any step carries a user label
   * starting with the reserved {@code $} prefix ({@link WalkerContext#RESERVED_ALIAS_PREFIX}). That
   * namespace is reserved for the translator's minted {@code $g2m_} aliases and YouTrackDB's
   * query-context variables, so a user label there is prohibited rather than translated. Scans every
   * step's {@code getLabels()} once; the scan is purely lexical (no graph access), so the walker runs
   * it before resolving any session-dependent state. The exception is the one failure {@link
   * GremlinToMatchStrategy}'s throw-safety net re-throws rather than degrading to a native decline.
   */
  private static void rejectReservedPrefixLabels(List<?> steps) {
    for (Object raw : steps) {
      // getSteps() is a raw List<Step>; each element is a Step whose labels are user-supplied.
      var step = (Step<?, ?>) raw;
      for (String label : step.getLabels()) {
        // A step's label set can contain a null: as((String) null) reaches AbstractStep.addLabel,
        // which adds the label with no null guard. Skip nulls — a null label is lexical noise that
        // cannot collide with the reserved '$' namespace, so it is never a rejection.
        if (label != null && label.startsWith(WalkerContext.RESERVED_ALIAS_PREFIX)) {
          throw new ReservedAliasException(
              "Gremlin alias '"
                  + label
                  + "' uses the reserved '"
                  + WalkerContext.RESERVED_ALIAS_PREFIX
                  + "' prefix: this namespace is reserved for YouTrackDB internal aliases and query"
                  + " variables. Rename the as(...) label.");
        }
      }
    }
  }

  /**
   * Snapshots the walker context into a {@link GremlinToMatchTranslator.TranslationResult}. When a
   * {@link UnionStepRecogniser} accepted, emits a multi-plan result from the stashed child inputs
   * (the prefix-only pattern on the context is discarded — each child re-walked the prefix).
   * Otherwise locks the pattern, merges alias filters, and packages a single-plan
   * {@link MatchPlanInputs}.
   *
   * <p>Returns {@code null} to decline a bare RID point-lookup (single node, zero edges) — see the
   * inline note; declining leaves the traversal on the native pipeline unchanged.
   */
  @Nullable private static GremlinToMatchTranslator.TranslationResult buildResult(WalkerContext ctx) {
    if (ctx.hasUnionCarrier()) {
      assert ctx.boundaryAlias != null && ctx.outputType != null && ctx.returnClass != null;
      return GremlinToMatchTranslator.TranslationResult.multiPlan(
          zipChildPlans(ctx),
          ctx.postConcatOps(),
          ctx.boundaryAlias,
          ctx.outputType,
          ctx.returnClass,
          ctx.shaping());
    }

    var ir = ctx.patternBuilder.build();

    // Bare RID point-lookup (g.V(id) / g.V(ids) with no subsequent hop): a single pinned node and
    // zero edges. Native resolves the RIDs directly with no query at all, whereas the translator
    // would compile an UNCACHED MATCH plan every call (a RID-bearing walk sets cacheEligible=false,
    // so it bypasses GremlinPlanCache) — a net regression with no join to optimise. Decline so native
    // handles it; a decline is trivially on==off (both run the native pipeline). A RID start FOLLOWED
    // by hops has at least one edge and still translates, since the join is where MATCH can win.
    if (ctx.ridBearing()
        && ir.pattern().getNumOfEdges() == 0
        && ir.pattern().aliasToNode.size() == 1) {
      return null;
    }

    Map<String, SQLWhereClause> finalAliasFilters = new LinkedHashMap<>(ir.aliasFilters());
    // AND-compose recogniser-contributed filters with any builder-supplied filter on the same alias
    // rather than overwriting: a hasLabel(L) @class narrowing and a has(...) predicate can both land
    // on the boundary alias, and dropping either would return a wrong (over-large) multiset.
    for (var entry : ctx.aliasFilters.entrySet()) {
      finalAliasFilters.merge(entry.getKey(), entry.getValue(), GremlinStepWalker::andWhere);
    }

    // The merged map above is what the planner reads for the alias it roots the plan at; every other
    // alias's constraint has to be pushed onto the path item that produced it. This is the first
    // point where both halves exist — the pattern is assembled and the recogniser-contributed
    // filters are merged — so the pass runs here.
    bindPathItemConstraints(ir.pattern(), finalAliasFilters, ir.aliasClasses());

    // Only the fields a single-node g.V() translation actually carries are set; the rest keep their
    // null/false defaults (matchExpressions/notMatchExpressions normalise to empty lists in the
    // compact constructor). The builder names each field so a future track adding one cannot silently
    // transpose a positional argument.
    var inputs =
        MatchPlanInputs.builder(ir.pattern())
            .aliasClasses(ir.aliasClasses())
            .aliasFilters(finalAliasFilters)
            .notMatchExpressions(
                ctx.notMatchExpressions.isEmpty() ? null : List.copyOf(ctx.notMatchExpressions))
            .returnItems(ctx.returnItems)
            .returnAliases(ctx.returnAliases)
            .returnNestedProjections(ctx.returnNestedProjections)
            .returnDistinct(ctx.returnDistinct)
            .groupBy(ctx.groupBy)
            .orderBy(ctx.orderBy)
            .limit(ctx.limit)
            .skip(ctx.skip)
            .build();

    Map<Object, Object> inputParameters = new LinkedHashMap<>(ctx.inputParameters.size());
    ctx.inputParameters.forEach(inputParameters::put);
    return GremlinToMatchTranslator.TranslationResult.singlePlan(
        inputs,
        ctx.boundaryAlias,
        ctx.outputType,
        ctx.returnClass,
        Map.copyOf(inputParameters),
        !ctx.ridBearing(),
        ctx.shaping());
  }

  /**
   * Zips the walker context's three parallel union-child lists into the one {@code ChildPlan} list
   * the translation carrier takes. The context keeps them separate because the fork host stashes
   * them as three accumulators; the carrier bundles them so nothing downstream can index them out of
   * step. The parity the zip relies on is asserted where they are stashed.
   */
  private static List<GremlinToMatchTranslator.TranslationResult.ChildPlan> zipChildPlans(
      WalkerContext ctx) {
    var childInputs = ctx.unionChildInputs();
    var childParameters = ctx.unionChildInputParameters();
    var childCacheEligible = ctx.unionChildCacheEligible();
    var childPlans =
        new ArrayList<GremlinToMatchTranslator.TranslationResult.ChildPlan>(childInputs.size());
    for (int i = 0; i < childInputs.size(); i++) {
      childPlans.add(
          new GremlinToMatchTranslator.TranslationResult.ChildPlan(
              childInputs.get(i), childParameters.get(i), childCacheEligible.get(i)));
    }
    return List.copyOf(childPlans);
  }

  /** AND-composes two same-alias {@code WHERE} clauses into one — the merge function used when both
   *  the pattern builder and a recogniser contribute a filter to the same alias. */
  private static SQLWhereClause andWhere(SQLWhereClause a, SQLWhereClause b) {
    return WHERE.wrap(WHERE.and(a.getBaseExpression(), b.getBaseExpression()));
  }

  /**
   * Pushes each alias's class and {@code WHERE} onto the path item that targets it, so a constraint
   * on an alias the planner does not root still reaches the executor.
   *
   * <p>A forward hop's target constraint is read off the path item itself — {@code
   * MatchEdgeTraverser} calls {@code item.getFilter().getFilter()} and {@code .getClassName(…)} — and
   * not off the plan's alias maps, which the planner consults for the root alias and, when it
   * traverses an edge backwards, for the syntactic source. Every positive path item is constructed
   * alias-only, so before this pass a predicate on any other alias had no consumer at all:
   * {@code g.V(a).out().has(k, v)} returned every out-neighbour of {@code a} rather than the matching
   * one, and returned it with no error.
   *
   * <p>The binding merges and never rebinds:
   *
   * <ul>
   *   <li>It AND-composes with the {@code WHERE} the item already carries. An edge path item's own
   *       predicate ({@code outE(L).has(p, v).inV()}) lives nowhere else — the walker's edge-filter
   *       map is observability-only and never reaches the plan inputs — so an overwriting rebind
   *       would drop it silently.
   *   <li>It leaves an item whose alias is in neither map untouched, for the same reason.
   *   <li>It does not overwrite a class the item already carries.
   * </ul>
   *
   * <p>The class is bound as well as the {@code WHERE} because under polymorphic mode a {@code
   * hasLabel(L)} contributes no {@code @class} term to the alias filter (see {@link
   * HasStepRecogniser}), which leaves the item's class slot as the constraint's only carrier — a
   * {@code WHERE}-only binding would lose {@code g.V(a).out().hasLabel(L)} entirely. The generic
   * {@code V} root class the hop recognisers register is skipped: it excludes nothing a vertex hop
   * can reach, and binding it would add a per-candidate class check to every translated hop.
   *
   * <p>The items are mutated in place. {@code Pattern.copy()} shares its path items with the
   * builder's pattern and the planner takes the pattern by reference, so these are the objects the
   * executor reads; the builder is locked by {@code build()} before this runs, so no later
   * construction call can observe a half-bound item. Sharing is at the item level, not the filter
   * level: every construction path attaches a fresh {@code SQLMatchFilter} per item and {@code
   * SQLMatchPathItem.copy()} deep-copies it, so no two items hold one filter. What the {@code
   * existing.copy()} below buys is therefore narrow — the bound filter is a new object, so anything
   * still holding the pre-bind filter does not observe the binding. It buys no isolation between
   * patterns: {@code setFilter} on a shared item is visible through every pattern holding that item,
   * copy or no copy. The bound {@code WHERE} is copied for the same reason the planner copies its
   * own sub-clauses — one {@code SQLWhereClause} instance would otherwise serve both the path item
   * and {@code MatchPlanInputs.aliasFilters}, and an AST rewrite on either side would reach both.
   *
   * <p>Package-private rather than private so the merge rules above can be asserted directly. Only
   * the leave-an-unlisted-alias-alone rule is reachable end-to-end today — {@code
   * outE(L).has(p, v).inV().has(q, w)} binds the target while the edge item stays untouched, and an
   * equivalence case covers it. The other two (an item that already carries a {@code WHERE}, an item
   * that already carries a class) have no traversal shape that reaches them, so a regression would
   * break them silently; unit tests against this method are the only net they have.
   */
  static void bindPathItemConstraints(
      Pattern pattern, Map<String, SQLWhereClause> aliasFilters, Map<String, String> aliasClasses) {
    for (var node : pattern.aliasToNode.values()) {
      for (var edge : node.out) {
        // Pattern.addExpression names each node after its item's own filter alias, so the alias at
        // the head of the edge is the one whose constraints belong on this item.
        var alias = edge.in.alias;
        var where = aliasFilters.get(alias);
        var className = aliasClasses.get(alias);
        if (WalkerContext.VERTEX_ROOT_CLASS.equals(className)) {
          // The generic vertex root the hop recognisers register excludes nothing a vertex hop can
          // reach; binding it would cost a class check per candidate row for no narrowing.
          className = null;
        }
        if (where == null && className == null) {
          continue;
        }
        var item = edge.item;
        var existing = item.getFilter();
        assert existing != null
            : "path item for alias " + alias + " has no filter block to bind onto";
        var bound = existing.copy();
        if (className != null && bound.getClassName(null) == null) {
          bound.setClassName(className);
        }
        if (where != null) {
          var existingWhere = bound.getFilter();
          // A copy rather than the map's own instance: the same map goes to MatchPlanInputs, so
          // binding the clause itself would leave one mutable AST shared by the path item and the
          // planner. andWhere reuses its operands' expression nodes, so the copy is taken on both
          // branches, matching the planner's own copy-before-you-share policy.
          var isolated = where.copy();
          bound.setFilter(existingWhere == null ? isolated : andWhere(existingWhere, isolated));
        }
        item.setFilter(bound);
      }
    }
  }
}
