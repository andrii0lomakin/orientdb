package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBGraph;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.AbstractMatchPlanStep;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.MultiPlanMatchStep;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.YTDBMatchPlanStep;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphCountStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphMatchStepStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphStepStrategy;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.MatchExecutionPlanner;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.MatchPlanInputs;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider-optimization strategy that rewrites a fully-recognized Gremlin traversal into a
 * YouTrackDB MATCH execution plan and replaces the traversal's entire step list with a single
 * {@link YTDBMatchPlanStep} boundary. Translation is all-or-nothing: if
 * <em>any</em> step is outside the recognized set the strategy declines the whole traversal
 * and the native TinkerPop pipeline — including the three pre-existing YTDB half-measure
 * strategies — keeps handling it verbatim. A translated traversal therefore contains exactly
 * one step (the boundary); a declined traversal is left byte-for-byte unchanged.
 *
 * <h2>Current state — recognizes the vertex source only</h2>
 *
 * <p>The whole-traversal walk is delegated to {@link GremlinToMatchTranslator#translate},
 * which drives the shared {@code GremlinStepWalker} + {@code StepRecogniser} registry. Phase 1
 * recognizes only the vertex source ({@code g.V()} / {@code g.V(ids)}); any traversal carrying
 * an unrecognized step declines whole and stays on the native pipeline. On a recognized shape
 * {@code apply} runs its gates, receives a non-empty translation, and splices the boundary
 * step in place of the entire step list ({@link #applyTranslation}). Landing the gating
 * cascade + throw-safety net + kill-switch before any recognizer ran under the strategy was
 * deliberate: it guarantees the "a translator bug in {@code apply} can only ever decline, never break
 * a query" invariant holds from the moment the strategy is first registered. Two deliberate throws
 * reach the caller. The first is a {@link ReservedAliasException} for a prohibited user alias in
 * the reserved {@code $} namespace. The second is a {@link ContradictoryOrderSemanticsException}
 * for incompatible order-mode instructions. The net re-throws both rather than degrading them
 * (see "Throw-safety net").
 *
 * <h2>Gating cascade</h2>
 *
 * The strategy declines (returns without mutating the traversal) when any of the following is
 * true, checked in order:
 *
 * <ol>
 *   <li><b>No YouTrackDB session / kill switch.</b> The traversal has no attached {@link
 *       YTDBGraph} (e.g. an anonymous {@code __.V()} detached traversal, or a non-YTDB graph),
 *       the session exposes no {@code ContextConfiguration} (its {@code getConfiguration()} is
 *       {@code @Nullable}), or {@link
 *       GlobalConfiguration#QUERY_GREMLIN_TO_MATCH_TRANSLATOR_ENABLED} is {@code false} for the
 *       traversal's session. Reading the flag per-session gives operators a runtime kill-switch
 *       without a redeploy; an unresolvable configuration declines rather than throwing.</li>
 *   <li><b>Non-graph / non-vertex start.</b> The first step is not a start-emitting {@link
 *       GraphStep}, or it is an edge start ({@code g.E()}). The current translator only models
 *       vertex-rooted patterns; edge starts are a later milestone. This O(1) start-shape gate
 *       runs before the O(steps) idempotency scan below, so a non-vertex traversal declines
 *       without walking its whole step list. Gating on the plain TinkerPop
 *       {@link GraphStep} (not {@code
 *       YTDBGraphStep}) is deliberate: this strategy runs <em>before</em> {@code
 *       YTDBGraphStepStrategy} — the sole producer of {@code YTDBGraphStep} — so at translator
 *       time the start step is still a plain {@code GraphStep}. Keying on {@code YTDBGraphStep}
 *       would decline every recognized shape. The check is also ordering-robust, since a
 *       {@code YTDBGraphStep} <em>is</em> a {@code GraphStep}.</li>
 *   <li><b>Per-traversal veto.</b> {@link RepeatDeclineStrategy#isVetoed} answers true, because
 *       {@link RepeatDeclineStrategy} marked the traversal at decoration time — it marks every
 *       traversal whose subtree was written with {@code repeat(...)}. The marker is how a decision
 *       taken before {@code RepeatUnrollStrategy} flattened the repeat survives into this pass; see
 *       that class for why the decline cannot be made here, and for why the marker is carried on
 *       the traversal's strategies <em>reference</em> without altering the list it points at.</li>
 *   <li><b>Idempotency.</b> The traversal already contains a boundary step ({@link
 *       AbstractMatchPlanStep}, either the single-plan {@link YTDBMatchPlanStep} or any other
 *       concrete boundary form) anywhere in its step list. A traversal's strategy chain can be
 *       applied more than once
 *       (clone-for-reuse, test-harness re-application, lazy first-iteration apply); leaving an
 *       already-translated traversal alone keeps rewriting deterministic and avoids discarding
 *       a built plan.</li>
 *   <li><b>No translation.</b> {@link GremlinToMatchTranslator#translate} returns {@code null}
 *       — no whole-traversal translation is available because the walker declined a step.
 *       Replacing zero steps would be a no-op, so the strategy returns.</li>
 * </ol>
 *
 * <h2>Throw-safety net</h2>
 *
 * {@code apply} runs inside {@code traversal.applyStrategies()}, which fires on <em>every</em>
 * Gremlin traversal compilation, and the strategy is registered globally for all YTDB graph
 * traversals. An uncaught exception in {@code apply} would abort compilation for that
 * traversal, and the blast radius of a translator / walker bug would be every Gremlin query
 * the server runs, not only the recognized shapes. The whole body therefore runs inside a
 * {@code try} that catches {@link RuntimeException} and turns any ordinary failure — a walker bug, a
 * recognizer NPE, a malformed {@code MatchPlanInputs}, a planner exception — into a clean
 * decline: the method returns and the original step list is preserved for the native pipeline
 * (which, under the all-or-nothing rule, is at least as well-served as before). The net
 * degrades a translator bug to native execution rather than a broken query, and it exists from
 * the skeleton so the invariant holds before any recognizer runs under the strategy.
 *
 * <p>The net makes two deliberate exceptions. A {@link ReservedAliasException} is thrown by the
 * walker's reserved-prefix pre-flight when a user {@code as(...)} label sits in the reserved {@code
 * $} namespace and rejects prohibited input rather than reporting a translator failure.
 *
 * <p>A {@link ContradictoryOrderSemanticsException} rejects a per-traversal request that combines
 * {@code orderIncludesMissingKey=true} with {@code StandardOrderSemanticsStrategy}. The {@code
 * catch} re-throws both exceptions before the {@link RuntimeException} clause.
 *
 * <p>Native execution
 * would otherwise accept the reserved alias. The native fallback invokes the same order-mode
 * resolver, so it does not avoid the contradictory-order exception. Degrading either exception to
 * a decline would therefore hide a user error.
 *
 * <p>The catch is narrowed to {@link RuntimeException}, so {@link Error} — including {@link
 * AssertionError} — is never swallowed: it is not a {@code RuntimeException} and propagates
 * untouched. Under {@code -ea} (the test/CI default) a genuine invariant violation in the walk
 * or plan build must surface loudly instead of degrading to a silent decline that masks a real
 * bug, and a fatal {@code OutOfMemoryError} / {@code StackOverflowError} must not be handed to
 * the native pipeline to re-attempt in an already-exhausted JVM. The body throws only unchecked
 * exceptions — its calls go through the {@code TraversalTranslator} / {@code MatchPlanBuilder}
 * seams and TinkerPop APIs — so {@code RuntimeException} covers every failure that can actually
 * occur.
 *
 * <p>The net catches during the walk and the plan build; the actual step-list mutation happens
 * only after both succeed (see {@link #applyTranslation}), so a caught exception always leaves
 * the step list untouched.
 *
 * <h2>Strategy ordering</h2>
 *
 * The strategy declares <b>empty</b> {@link #applyPrior()} and {@link #applyPost()}. Ordering
 * relative to the three half-measure strategies is established the canonical TinkerPop way:
 * each half-measure strategy lists {@code GremlinToMatchStrategy} in its <em>own</em> {@code
 * applyPrior()}, so TinkerPop's topological sort runs this strategy first and the
 * half-measures become the decline fallback. Those half-measure {@code applyPrior()} edits and
 * the registration into the optimization chain are in place — see {@code YTDBGraphImplAbstract}
 * and each half-measure strategy's {@code applyPrior()}; this strategy only declares its own
 * (empty) ordering constraints.
 *
 * <h2>Plan caching</h2>
 *
 * Cache-eligible walks build through {@link GremlinPlanCache}, keyed by {@link
 * GremlinPlanFingerprint} on the post-walk {@link MatchPlanInputs}. A second map, keyed by
 * {@link GremlinStepWalker#extractShape} (the production recognisers' {@code contributeShape}
 * tokens plus this invocation's {@code ?} bindings), stores {@link GremlinTranslationTemplate}:
 * a hit skips the walker entirely (splice from the stored template, or return immediately on a
 * cached decline). An extraction that cannot prove completeness never reads or writes that map.
 * RID-bearing shapes ({@code g.V(ids)}, {@code hasId(...)}) bypass both caches.
 * Per-walk predicate values bind as positional parameters and are installed on the boundary step
 * at execution time. A plan is stored only when no metadata invalidation landed after the
 * {@code planningStart} captured before the walk, so a concurrent schema change during translation
 * never leaves a stale plan in the shared per-database cache — the same guard
 * {@code MatchExecutionPlanner} applies for the YQL/GQL plan cache. Cache-backed single-plan
 * steps copy the stored template on first open rather than during {@code apply}.
 *
 * <h2>Translation telemetry</h2>
 *
 * Outcomes that reach a translation attempt feed {@link GremlinTranslationMetrics} on the
 * database {@code SharedContext}: successes, declines (unsupported shape, edge / non-vertex start,
 * repeat veto, or cached decline), and throw-safety-net errors. Each recording also updates the
 * matching {@link com.jetbrains.youtrackdb.internal.common.profiler.metrics.CoreMetrics} success /
 * decline / error {@code Ratio} metrics. Declined shapes are retained (bounded) so operators can
 * ask which unsupported step lists dominate. Kill-switch / missing session and idempotent
 * re-applies (boundary already present) are not attempts — those are not coverage failures.
 *
 * <h2>Testability</h2>
 *
 * The translator and plan builder are injected as the {@link TraversalTranslator} and {@link
 * MatchPlanBuilder} seams through package-private constructors, so unit tests can drive the
 * post-gate splice path with a fixture {@link GremlinToMatchTranslator.TranslationResult} (and
 * can supply a throwing fixture to exercise the throw-safety net) without wiring a real walker.
 * Production code uses {@link #instance()}, which wires the production facade.
 */
public final class GremlinToMatchStrategy
    extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(GremlinToMatchStrategy.class);

  /**
   * Empty prior/post set. Ordering is expressed by the half-measure strategies naming this
   * class in their own {@code applyPrior()}, not by this strategy naming them; see the
   * class Javadoc "Strategy ordering" section. The reference to the half-measure classes is
   * kept alive through the Javadoc {@code @link}s below so the ordering contract stays
   * discoverable from here even though the sets are empty.
   *
   * @see YTDBGraphStepStrategy
   * @see YTDBGraphCountStrategy
   * @see YTDBGraphMatchStepStrategy
   */
  private static final Set<Class<? extends ProviderOptimizationStrategy>> NO_ORDERING =
      Set.of();

  /**
   * The production translator. Written as a named implementation rather than a method reference so
   * it can carry the resolved-setting overload, which is what keeps the shape key and the plan on
   * one reading of the order mode.
   */
  private static final TraversalTranslator PRODUCTION_TRANSLATOR = new TraversalTranslator() {
    @Nullable @Override
    public GremlinToMatchTranslator.TranslationResult translate(Traversal.Admin<?, ?> traversal) {
      return GremlinToMatchTranslator.translate(traversal);
    }

    @Nullable @Override
    public GremlinToMatchTranslator.TranslationResult translate(
        Traversal.Admin<?, ?> traversal, @Nullable Boolean orderIncludesMissingKey) {
      return GremlinToMatchTranslator.translate(traversal, orderIncludesMissingKey);
    }

    @Nullable @Override
    public GremlinToMatchTranslator.TranslationResult translate(
        Traversal.Admin<?, ?> traversal,
        @Nullable Boolean orderIncludesMissingKey,
        ResolvedOrderByNullsPlacement orderByNullsPlacements) {
      return GremlinToMatchTranslator.translate(
          traversal, orderIncludesMissingKey, orderByNullsPlacements);
    }
  };

  private static final GremlinToMatchStrategy INSTANCE =
      new GremlinToMatchStrategy(
          PRODUCTION_TRANSLATOR, GremlinToMatchStrategy::buildPlan, true);

  private final TraversalTranslator translator;

  private final MatchPlanBuilder planBuilder;

  /**
   * When {@code true}, {@link #applyOrDecline} reads and writes {@link GremlinPlanCache}'s
   * translation map. Production {@link #instance()} enables it; test constructors leave it off so
   * a fixture translator is never skipped by a cached production walk.
   */
  private final boolean populateTranslationCache;

  /**
   * Package-private — tests construct a strategy with a fixture translator (and the production
   * plan builder). Production code goes through {@link #instance()}.
   */
  GremlinToMatchStrategy(TraversalTranslator translator) {
    this(translator, GremlinToMatchStrategy::buildPlan, false);
  }

  /**
   * Package-private — tests inject both a fixture translator and a fixture plan builder so the
   * splice path ({@link #applyTranslation}) can be exercised with a stub plan, without
   * standing up the real {@link MatchExecutionPlanner}. Production code goes through {@link
   * #instance()}, which wires the production translator and the production plan builder.
   */
  GremlinToMatchStrategy(TraversalTranslator translator, MatchPlanBuilder planBuilder) {
    this(translator, planBuilder, false);
  }

  private GremlinToMatchStrategy(
      TraversalTranslator translator,
      MatchPlanBuilder planBuilder,
      boolean populateTranslationCache) {
    this.translator = translator;
    this.planBuilder = planBuilder;
    this.populateTranslationCache = populateTranslationCache;
  }

  /** Singleton accessor — the strategy is stateless and cheap to share. */
  public static GremlinToMatchStrategy instance() {
    return INSTANCE;
  }

  @Override
  public void apply(Traversal.Admin<?, ?> traversal) {
    // Throw-safety net: the whole body runs on every Gremlin compilation and the strategy is
    // registered globally, so any recognizer/planner RuntimeException here must degrade to a
    // decline (leave the native step list untouched), never abort compilation. The catch is
    // narrowed to RuntimeException so Error and AssertionError are NOT caught — they propagate,
    // surfacing JVM errors and -ea invariant violations instead of masking them (see the class
    // Javadoc "Throw-safety net"). The mutation in applyTranslation runs only after the walk and
    // the plan build both succeed, so a caught exception always leaves the step list unmodified.
    try {
      applyOrDecline(traversal);
    } catch (ReservedAliasException | ContradictoryOrderSemanticsException e) {
      // Deliberate input rejections must reach the caller instead of becoming native declines.
      throw e;
    } catch (RuntimeException e) {
      // Swallow every other unchecked exception deliberately: translation is a best-effort
      // optimization. A recognizer/planner failure declines to the native pipeline, which handles the
      // traversal correctly. Rethrowing would break every Gremlin query, recognized or not. Error and
      // AssertionError are not RuntimeExceptions, so they are intentionally not caught here — a
      // JVM Error or an -ea invariant violation must surface loudly, never degrade to a silent
      // decline.
      declineOnThrow(traversal, e);
      recordTranslationError(traversal);
    }
  }

  /**
   * The gate-and-translate body, extracted so the throw-safety net in {@link #apply} wraps a
   * single call. Returns without mutating the traversal on any decline; on a non-empty
   * translation it splices the boundary step in place of the whole step list.
   */
  private void applyOrDecline(Traversal.Admin<?, ?> traversal) {
    var session = resolveSessionIfEnabled(traversal);
    if (session == null) {
      return;
    }
    // Start-shape gate is O(1). Already-translated traversals also fail hasVertexGraphStart
    // (start is a boundary, not a GraphStep), so check containsBoundaryStep before counting a
    // decline — re-applies must stay outside attempts. Fresh edge / non-vertex starts are product
    // gaps and count as declines; kill-switch / missing session already returned above.
    if (!hasVertexGraphStart(traversal)) {
      if (containsBoundaryStep(traversal)) {
        return;
      }
      GremlinTranslationMetrics.of(session).recordDecline(stepShape(traversal));
      return;
    }
    // Honour a per-traversal veto. RepeatDeclineStrategy marks a repeat-bearing traversal at
    // decoration time, but TinkerPop captures the strategy set before the first strategy runs, so
    // the mark cannot stop this invocation on its own — reading it here is what makes it effective.
    // The check keys on the marker's presence and not on this strategy's absence: a child
    // traversal's own strategy list never carries a provider strategy during the strategy pass, so
    // an absence test would decline every sub-traversal rather than the vetoed ones. See
    // RepeatDeclineStrategy for why the decision has to be taken that early, and for why the marker
    // lives on the strategies reference's type rather than in the list itself. Variable-depth
    // repeat is out of scope — count as a decline like any other unsupported shape.
    if (RepeatDeclineStrategy.isVetoed(traversal)) {
      GremlinTranslationMetrics.of(session).recordDecline(stepShape(traversal));
      return;
    }
    if (containsBoundaryStep(traversal)) {
      return;
    }
    // Resolve all order settings once for this compilation. The shape key below and the walk
    // further down both read these values. Two independent reads could straddle a runtime flip
    // and file a plan built under one setting under the other setting's key, in a cache that is
    // storage-wide and outlives the session.
    var orderIncludesMissingKey = YTDBStrategyUtil.orderIncludesMissingKey(traversal);
    var orderByNullsPlacements = YTDBStrategyUtil.orderByNullsPlacements(traversal);
    if (orderByNullsPlacements == null) {
      return;
    }
    var extraction =
        GremlinStepWalker.extractShape(
            traversal, session, orderIncludesMissingKey, orderByNullsPlacements);
    var metrics = GremlinTranslationMetrics.of(session);
    if (populateTranslationCache && extraction.complete()) {
      var cached = GremlinPlanCache.getTranslation(extraction.key(), session);
      if (cached instanceof GremlinTranslationTemplate.Decline) {
        metrics.recordDecline(stepShape(traversal));
        return;
      }
      if (cached instanceof GremlinTranslationTemplate.Translate translate
          && extraction.bindings().size() == translate.bindingCount()) {
        spliceFromTranslationCache(traversal, translate, extraction.bindings());
        metrics.recordSuccess();
        return;
      }
    }
    // Capture the planning start before the walk: the schema read that shapes the plan happens
    // inside translate(), so the concurrent-invalidation guard in buildPlan must time from here to
    // catch a DDL that races the walk (see the class Javadoc "Plan caching").
    var planningStart = System.nanoTime();
    var translation =
        translator.translate(traversal, orderIncludesMissingKey, orderByNullsPlacements);
    if (translation == null) {
      if (populateTranslationCache && extraction.complete()) {
        GremlinPlanCache.putTranslation(
            extraction.key(), GremlinTranslationTemplate.DECLINE, session);
      }
      metrics.recordDecline(stepShape(traversal));
      return;
    }
    applyTranslation(traversal, session, translation, planningStart, extraction);
    metrics.recordSuccess();
  }

  /**
   * Hook for the throw-safety net so a caught exception has a single, greppable landing point.
   * Only {@link RuntimeException}s reach here — {@link Error} (including {@link AssertionError})
   * and checked exceptions are not caught by {@link #apply} and propagate. The decline itself is
   * the absence of a mutation (the traversal is already unmodified because the step-list swap
   * runs last); this hook additionally records the swallowed exception at {@code DEBUG} so an
   * operator diagnosing "why is nothing being translated?" has a signal. {@code DEBUG} rather
   * than {@code WARN} keeps a deterministic translator bug — which would otherwise log on every
   * matching traversal — from flooding the log, while staying discoverable when the level is
   * raised.
   */
  private static void declineOnThrow(Traversal.Admin<?, ?> traversal, RuntimeException cause) {
    // Log the step-class SHAPE, not traversal.toString(): the latter renders inline literal
    // predicate values (e.g. has("ssn", "...")), so logging the shape keeps sensitive query values
    // — and any newline / control characters that could forge log lines — out of the log while
    // still identifying which traversal shape declined.
    LogManager.instance()
        .debug(
            GremlinToMatchStrategy.class,
            "Gremlin-to-MATCH translation declined after an unexpected exception;"
                + " falling back to native execution for traversal shape: %s",
            LOGGER,
            cause,
            stepShape(traversal));
  }

  /**
   * Feeds {@link GremlinTranslationMetrics#recordError} when a session is still attached. The
   * throw-safety net may fire after the session gate (walk/plan throw) or, rarely, during session
   * resolution itself — missing session means we cannot attribute the error to a database, so the
   * counter is skipped and the DEBUG log in {@link #declineOnThrow} remains the sole signal.
   */
  private static void recordTranslationError(Traversal.Admin<?, ?> traversal) {
    var session = YTDBStrategyUtil.resolveYtdbSession(traversal);
    if (session == null) {
      return;
    }
    GremlinTranslationMetrics.of(session).recordError(stepShape(traversal));
  }

  /**
   * Renders the traversal as its ordered list of step class simple names, e.g. {@code [GraphStep,
   * HasStep]}. A diagnostic shape that omits the inline literal values {@code Traversal.toString()}
   * would include; see {@link #declineOnThrow}.
   */
  private static String stepShape(Traversal.Admin<?, ?> traversal) {
    var shape = new StringBuilder("[");
    var steps = traversal.getSteps();
    for (int i = 0; i < steps.size(); i++) {
      if (i > 0) {
        shape.append(", ");
      }
      shape.append(steps.get(i).getClass().getSimpleName());
    }
    return shape.append("]").toString();
  }

  /**
   * Returns the traversal's session iff a YouTrackDB graph is attached and the kill-switch is
   * on, or {@code null} to signal "decline".
   *
   * <p>Session resolution is delegated to {@link YTDBStrategyUtil#resolveYtdbSession}, which
   * returns {@code null} (never throws) on a detached, {@code EmptyGraph}, or non-YTDB traversal.
   * Reading the flag from the session's {@code ContextConfiguration} (rather than the JVM-global
   * {@link GlobalConfiguration}) lets operators — and tests — flip it per-session without mutating
   * global state. That {@code getConfiguration()} is {@code @Nullable}; a null result is treated
   * as "decline" so the kill-switch read never dereferences a null configuration.
   *
   * <p>This strategy is the only reader of the kill-switch on the translation path.
   * {@link RepeatDeclineStrategy} deliberately does not consult it, so the two cannot disagree
   * about whether the translator is on when a thread flips the flag mid-compilation; see that
   * class's "Why the kill-switch is not consulted".
   */
  @Nullable private static DatabaseSessionEmbedded resolveSessionIfEnabled(
      Traversal.Admin<?, ?> traversal) {
    var session = YTDBStrategyUtil.resolveYtdbSession(traversal);
    if (session == null) {
      return null;
    }
    // getConfiguration() is @Nullable (it delegates to storage.getContextConfiguration()). A
    // null ContextConfiguration cannot be dereferenced for the flag, so treat it as "not
    // enabled" and decline explicitly rather than relying on the throw-safety net to catch an
    // NPE — declining is the safe default when the kill-switch cannot be resolved.
    var configuration = session.getConfiguration();
    if (configuration == null) {
      return null;
    }
    var enabled =
        configuration.getValueAsBoolean(
            GlobalConfiguration.QUERY_GREMLIN_TO_MATCH_TRANSLATOR_ENABLED);
    return enabled ? session : null;
  }

  /**
   * Scans the entire step list and returns {@code true} as soon as a boundary step ({@link
   * AbstractMatchPlanStep}) is found (the idempotency gate). Keying on the boundary base rather
   * than the concrete {@link YTDBMatchPlanStep} detects every boundary form — the single-plan step
   * and any sibling concrete step over the same base — so a re-applied strategy leaves an
   * already-translated traversal alone regardless of which boundary form it carries. The scan
   * covers the whole list, not just the start step, because a wrapping traversal source or test
   * harness could place ordinary steps in front of a previously-translated boundary.
   */
  private static boolean containsBoundaryStep(Traversal.Admin<?, ?> traversal) {
    for (Step<?, ?> step : traversal.getSteps()) {
      if (step instanceof AbstractMatchPlanStep<?, ?>) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} iff the traversal's start step is a vertex-emitting {@link GraphStep}.
   * Gates on the plain TinkerPop {@code GraphStep} rather than {@code YTDBGraphStep} because
   * this strategy runs before {@code YTDBGraphStepStrategy} produces the YTDB subclass; see the
   * class Javadoc "Gating cascade". {@code GraphStep.returnsVertex()} distinguishes {@code
   * g.V()} (accept) from {@code g.E()} (decline — edge starts are a later milestone).
   */
  private static boolean hasVertexGraphStart(Traversal.Admin<?, ?> traversal) {
    return traversal.getStartStep() instanceof GraphStep<?, ?> graphStep
        && graphStep.returnsVertex();
  }

  /**
   * Replaces the traversal's entire step list with one boundary step built from {@code translation}
   * (all-or-nothing). Single-plan translations build one {@link YTDBMatchPlanStep}; multi-plan
   * translations build every child plan eagerly, install each child's parameters onto its own context,
   * and splice one {@link MultiPlanMatchStep}. The step list is swapped only after every required plan
   * exists, so a planner throw still leaves the original traversal intact.
   *
   * <p>Plan build order matters for the throw-safety net: the plan is built <em>before</em>
   * the step list is mutated, so a planner throw is caught by {@link #apply}'s net with the
   * original step list still intact. Only once the plan exists does {@code removeAllSteps} +
   * {@code addStep} run, and those TinkerPop calls do not throw for a well-formed boundary.
   *
   * <p>The plan is built with caching disabled (see class Javadoc): the inherited {@code
   * statement} field on the planner stays {@code null}, which the planner accepts only when
   * {@code useCache=false}.
   */
  private void applyTranslation(
      Traversal.Admin<?, ?> traversal,
      DatabaseSessionEmbedded session,
      GremlinToMatchTranslator.TranslationResult translation,
      long planningStart,
      GremlinShapeExtractor.Extraction extraction) {
    if (translation.isMultiPlan()) {
      var plans = buildChildPlans(session, translation, planningStart);
      replaceAllStepsWithBoundary(traversal, plans, translation);
      return;
    }
    InternalExecutionPlan plan = planBuilder.buildPlan(session, translation, planningStart);
    var copyOnOpen = isSharedPlanTemplate(session, translation, plan);
    replaceAllStepsWithBoundary(traversal, plan, translation, copyOnOpen);
    if (populateTranslationCache && copyOnOpen && extraction.complete()) {
      GremlinPlanCache.putTranslation(
          extraction.key(),
          new GremlinTranslationTemplate.Translate(
              plan,
              translation.boundaryAlias(),
              translation.outputType(),
              translation.returnClass(),
              translation.shaping(),
              translation.inputParameters().size()),
          session);
    }
  }

  /**
   * True when {@code plan} is the closed template living in {@link GremlinPlanCache} and the
   * boundary step must copy it on first open rather than own it. RID-bearing and non-cacheable
   * plans (e.g. {@code CountFromClassStep}) stay eager: the step closes them.
   */
  private static boolean isSharedPlanTemplate(
      DatabaseSessionEmbedded session,
      GremlinToMatchTranslator.TranslationResult translation,
      InternalExecutionPlan plan) {
    if (!translation.cacheEligible() || !plan.canBeCached()) {
      return false;
    }
    var inputs = translation.inputs();
    if (inputs == null) {
      return false;
    }
    return GremlinPlanCache.instance(session).contains(GremlinPlanFingerprint.fingerprint(inputs));
  }

  /**
   * Production plan builder: routes the translated {@link MatchPlanInputs} through the additive
   * {@link MatchExecutionPlanner#MatchExecutionPlanner(MatchPlanInputs) constructor} and builds
   * the plan eagerly, with the walk's positional parameters installed on the planning context so
   * root selection and IndexOrdered cost estimates see bound {@code ?} values (mirrors
   * {@code SQLMatchStatement}). Cache-eligible shapes get/put through {@link GremlinPlanCache};
   * RID-bearing shapes always build uncached.
   *
   * <p>Package-private rather than private so a test can wrap it in a fixture {@link
   * MatchPlanBuilder} that parks between union children: the concurrent-invalidation guard below is
   * only observable when a second thread invalidates mid-build, and a test that reimplemented the
   * guard would pin its own copy instead of this one.
   */
  static InternalExecutionPlan buildPlan(
      DatabaseSessionEmbedded session,
      GremlinToMatchTranslator.TranslationResult translation,
      long planningStart) {
    assert !translation.isMultiPlan()
        : "single-plan buildPlan helper cannot build a multi-plan translation";
    if (!translation.cacheEligible()) {
      return buildPlanUncached(
          session, requireInputs(translation), translation.inputParameters());
    }
    var inputs = requireInputs(translation);
    var fingerprint = GremlinPlanFingerprint.fingerprint(inputs);
    var cached = GremlinPlanCache.template(fingerprint, session);
    if (cached != null) {
      return cached;
    }
    var plan = buildPlanUncached(session, inputs, translation.inputParameters());
    // Cache only if no metadata invalidation landed after planningStart (captured before the walk).
    // A concurrent DDL that fires between the schema read and this put would otherwise leave a plan
    // built against the pre-change schema in the shared per-database cache, served to every later
    // query of this shape. Mirrors the YqlExecutionPlanCache guard in MatchExecutionPlanner.
    if (GremlinPlanCache.getLastInvalidation(session) < planningStart) {
      GremlinPlanCache.put(fingerprint, plan, session);
    }
    var stored = GremlinPlanCache.instance(session).peekStored(fingerprint);
    if (stored != null) {
      // The cache owns the closed template; drop the live build so the boundary step copies on open
      // instead of executing this instance.
      plan.close();
      return stored;
    }
    return plan;
  }

  /**
   * Builds every child {@link InternalExecutionPlan} for a multi-plan translation inside the
   * concurrent-DDL-guarded path (via {@link #planBuilder}), installs each child's positional
   * parameters onto that child's own context, and closes already-built children if a later child
   * throws. Each child is a single-plan translation: cache-eligible children hit {@link
   * GremlinPlanCache} under their own fingerprint; RID-bearing children bypass. The multi-plan
   * carrier itself is never one cache entry.
   */
  private List<InternalExecutionPlan> buildChildPlans(
      DatabaseSessionEmbedded session,
      GremlinToMatchTranslator.TranslationResult translation,
      long planningStart) {
    var builtPlans = new ArrayList<InternalExecutionPlan>(translation.childPlans().size());
    try {
      for (var child : translation.childPlans()) {
        var childInputs = child.inputs();
        // Lone post-union count(): push RETURN count(*) into each child so YQL count /
        // CountFromClass / per-child GremlinPlanCache stay available; MultiPlanMatchStep sums.
        if (PostConcatSupport.isPushDownCountOnly(translation.postConcatOps())) {
          childInputs = PostConcatSupport.rewriteToCountStar(childInputs);
        }
        var childTranslation =
            GremlinToMatchTranslator.TranslationResult.singlePlan(
                childInputs,
                translation.boundaryAlias(),
                translation.outputType(),
                translation.returnClass(),
                child.parameters(),
                child.cacheEligible(),
                translation.shaping());
        var childPlan = planBuilder.buildPlan(session, childTranslation, planningStart);
        // buildPlan returns the shared closed template for cache-eligible children. Copy now so
        // MultiPlanMatchStep owns (and later closes) a unique plan rather than the cache entry.
        if (child.cacheEligible() && childPlan.canBeCached()) {
          var isolatedCtx = new BasicCommandContext();
          isolatedCtx.setParentWithoutOverridingChild(childPlan.getContext());
          childPlan = childPlan.copy(isolatedCtx);
        }
        if (!child.parameters().isEmpty()) {
          childPlan.getContext().setInputParameters(child.parameters());
        }
        builtPlans.add(childPlan);
      }
      return List.copyOf(builtPlans);
    } catch (RuntimeException | Error e) {
      closePlans(builtPlans, e);
      throw e;
    }
  }

  /**
   * Builds a MATCH plan with the walk's positional parameters installed on the planning context —
   * the same order {@link com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchStatement}
   * uses. Root selection and IndexOrdered cost estimates call {@code WHERE.estimate} during
   * planning; without bound {@code ?} values a UNIQUE {@code id = ?} collapses to
   * {@code classCount / 2} and a smaller mid-walk alias (e.g. Forum behind {@code IS DEFINED})
   * can win the root and full-scan. Parameters are also re-installed at execute time on the
   * boundary step; planning needs them first so the cached plan shape is the selective one.
   */
  private static InternalExecutionPlan buildPlanUncached(
      DatabaseSessionEmbedded session,
      MatchPlanInputs inputs,
      Map<Object, Object> inputParameters) {
    var ctx = new BasicCommandContext(session);
    if (!inputParameters.isEmpty()) {
      ctx.setInputParameters(inputParameters);
    }
    return new MatchExecutionPlanner(inputs)
        .createExecutionPlan(ctx, /* enableProfiling */ false, /* useCache */ false);
  }

  private static MatchPlanInputs requireInputs(
      GremlinToMatchTranslator.TranslationResult translation) {
    var inputs = translation.inputs();
    assert inputs != null : "single-plan translation must carry MatchPlanInputs";
    if (inputs == null) {
      throw new IllegalArgumentException(
          "Single-plan translation is missing MatchPlanInputs for boundary build.");
    }
    return inputs;
  }

  private static void closePlans(List<InternalExecutionPlan> plans, Throwable primary) {
    for (var plan : plans) {
      try {
        plan.close();
      } catch (RuntimeException | Error closeFailure) {
        primary.addSuppressed(closeFailure);
      }
    }
  }

  /**
   * Removes every existing step and installs the boundary step as the traversal's sole step.
   * Raw types are unavoidable here: the strategy receives a {@link Traversal.Admin}{@code <?,
   * ?>} (the {@code TraversalStrategy} contract) but the boundary step's {@code <S, E>} type
   * variables have no concrete binding at this point, so both collapse into the same raw
   * container. {@link GremlinToMatchTranslator.TranslationResult#returnClass()} is the runtime
   * source of truth for the emitted element class.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void spliceFromTranslationCache(
      Traversal.Admin<?, ?> traversalRaw,
      GremlinTranslationTemplate.Translate cached,
      Map<Object, Object> bindings) {
    var boundary =
        new YTDBMatchPlanStep(
            traversalRaw,
            cached.returnClass(),
            cached.planTemplate(),
            cached.boundaryAlias(),
            cached.outputType(),
            bindings,
            cached.shaping(),
            true);
    TraversalHelper.removeAllSteps(traversalRaw);
    traversalRaw.addStep(boundary);
  }

  /**
   * Removes every existing step and installs the boundary step as the traversal's sole step.
   * Raw types are unavoidable here: the strategy receives a {@link Traversal.Admin}{@code <?,
   * ?>} (the {@code TraversalStrategy} contract) but the boundary step's {@code <S, E>} type
   * variables have no concrete binding at this point, so both collapse into the same raw
   * container. {@link GremlinToMatchTranslator.TranslationResult#returnClass()} is the runtime
   * source of truth for the emitted element class.
   *
   * @param copyOnOpen when {@code true}, {@code plan} is a shared cache template copied on first
   *     open rather than owned by the step
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void replaceAllStepsWithBoundary(
      Traversal.Admin<?, ?> traversalRaw,
      InternalExecutionPlan plan,
      GremlinToMatchTranslator.TranslationResult translation,
      boolean copyOnOpen) {
    var boundary =
        new YTDBMatchPlanStep(
            traversalRaw,
            translation.returnClass(),
            plan,
            translation.boundaryAlias(),
            translation.outputType(),
            translation.inputParameters(),
            translation.shaping(),
            copyOnOpen);
    TraversalHelper.removeAllSteps(traversalRaw);
    traversalRaw.addStep(boundary);
  }

  /**
   * Multi-plan splice: replaces every step with one {@link MultiPlanMatchStep} carrying the
   * already-built child plans. The base positional-parameter map stays empty — each child's
   * parameters were installed on that child's own context in {@link #buildChildPlans}.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void replaceAllStepsWithBoundary(
      Traversal.Admin<?, ?> traversalRaw,
      List<InternalExecutionPlan> plans,
      GremlinToMatchTranslator.TranslationResult translation) {
    var boundary =
        new MultiPlanMatchStep(
            traversalRaw,
            translation.returnClass(),
            plans,
            translation.boundaryAlias(),
            translation.outputType(),
            translation.shaping(),
            translation.postConcatOps());
    TraversalHelper.removeAllSteps(traversalRaw);
    traversalRaw.addStep(boundary);
  }

  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
    return NO_ORDERING;
  }

  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
    return NO_ORDERING;
  }

  /**
   * Injection seam for the whole-traversal translation step. Production wires {@link
   * GremlinToMatchTranslator#translate}; tests pass a fixture (or a throwing fixture for the
   * throw-safety net) without a real walker. Named rather than a bare {@code Function} so the
   * call site reads {@code translator.translate(traversal)}, and returns {@code null} to decline
   * rather than an empty {@link java.util.Optional} — the seam is package-private, so a nullable
   * return is simpler than wrapping at the single call site.
   */
  @FunctionalInterface
  interface TraversalTranslator {
    @Nullable GremlinToMatchTranslator.TranslationResult translate(Traversal.Admin<?, ?> traversal);

    /**
     * Translates with the order mode already resolved by the caller. The default
     * ignores the resolved value, which suits every fixture translator in the tests: a fixture
     * returns a fixed result and reads no setting. Production overrides it so the walk and the
     * shape key read one and the same answer.
     */
    @Nullable default GremlinToMatchTranslator.TranslationResult translate(
        Traversal.Admin<?, ?> traversal, @Nullable Boolean orderIncludesMissingKey) {
      return translate(traversal);
    }

    /** Translates with all order settings resolved by the caller. */
    @Nullable default GremlinToMatchTranslator.TranslationResult translate(
        Traversal.Admin<?, ?> traversal,
        @Nullable Boolean orderIncludesMissingKey,
        ResolvedOrderByNullsPlacement orderByNullsPlacements) {
      return translate(traversal, orderIncludesMissingKey);
    }
  }

  /**
   * Injection seam for building the MATCH execution plan from translated inputs. Production wires
   * {@link #buildPlan}; tests pass a stub so the splice path runs without a real {@link
   * MatchExecutionPlanner}. Named rather than a bare {@code BiFunction} so the call site reads
   * {@code planBuilder.buildPlan(session, inputs)}.
   */
  @FunctionalInterface
  interface MatchPlanBuilder {
    InternalExecutionPlan buildPlan(
        DatabaseSessionEmbedded session,
        GremlinToMatchTranslator.TranslationResult translation,
        long planningStart);
  }
}
