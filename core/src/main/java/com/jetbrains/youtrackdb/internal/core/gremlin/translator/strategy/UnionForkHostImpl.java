package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.MatchPlanInputs;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Walker-owned {@link UnionForkHost}: keeps the parent {@link Traversal.Admin} and step cursor
 * private so {@link UnionStepRecogniser} never receives them.
 */
final class UnionForkHostImpl implements UnionForkHost {

  private final Traversal.Admin<?, ?> parent;
  private final StepStreamCursor cursor;
  private final List<?> steps;
  private final WalkerContext ctx;
  private final Map<Class<?>, StepRecogniser> recognisers;
  private final ResolvedOrderByNullsPlacement orderByNullsPlacements;

  /**
   * Memoised prefix snapshot. {@link #recognisedPrefixSteps()} is called once by the recogniser and
   * once per union arm by {@link #walkFork}; rebuilding it each time made prefix copying O(arms ×
   * prefix). The cursor cannot move backwards and the recogniser consumes nothing between those
   * calls, so one snapshot serves them all.
   */
  private List<Step<?, ?>> prefixSnapshot;

  UnionForkHostImpl(
      @Nonnull Traversal.Admin<?, ?> parent,
      @Nonnull StepStreamCursor cursor,
      @Nonnull WalkerContext ctx,
      @Nonnull Map<Class<?>, StepRecogniser> recognisers,
      @Nonnull ResolvedOrderByNullsPlacement orderByNullsPlacements) {
    this.parent = parent;
    this.cursor = cursor;
    this.steps = parent.getSteps();
    this.ctx = ctx;
    this.recognisers = recognisers;
    this.orderByNullsPlacements = orderByNullsPlacements;
  }

  @Nonnull
  @Override
  public List<Step<?, ?>> recognisedPrefixSteps() {
    if (prefixSnapshot != null) {
      return prefixSnapshot;
    }
    // After UnionStepRecogniser.take(), position is past the union. The prefix is every step
    // strictly before that union index (position - 1).
    int unionIndex = cursor.position() - 1;
    if (unionIndex <= 0) {
      prefixSnapshot = List.of();
      return prefixSnapshot;
    }
    var prefix = new ArrayList<Step<?, ?>>(unionIndex);
    for (int i = 0; i < unionIndex; i++) {
      prefix.add((Step<?, ?>) steps.get(i));
    }
    prefixSnapshot = List.copyOf(prefix);
    return prefixSnapshot;
  }

  @Override
  public boolean postUnionSuffixTranslatable() {
    return GremlinStepWalker.postUnionSuffixTranslatable(cursor, recognisers);
  }

  @Nullable @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public GremlinToMatchTranslator.TranslationResult walkFork(
      @Nonnull List<Step<?, ?>> childSuffix) {
    var prefix = recognisedPrefixSteps();
    Graph graph = parent.getGraph().orElse(null);
    Traversal.Admin<?, ?> forked =
        graph != null ? new DefaultGraphTraversal<>(graph) : new DefaultGraphTraversal<>();
    forked.setStrategies(parent.getStrategies());
    for (Step<?, ?> step : prefix) {
      forked.addStep(step.clone());
    }
    for (Step<?, ?> step : childSuffix) {
      forked.addStep(step.clone());
    }
    // The forked list is prefix ++ childSuffix, flat, so the child's steps sit at top level as far
    // as the walk can tell. Handing the prefix length in as the fold latch's child-scope boundary
    // is what keeps a leading has() in the arm from reading as folded: natively the arm is a child
    // traversal, rebuildTraversal never descends into it, and its HasStep survives unfolded for
    // TinkerPop's comparator to answer. See GremlinStepWalker.walk(Traversal.Admin, int).
    return GremlinStepWalker.production()
        .walk(forked, prefix.size(), ctx.orderIncludesMissingKey(), orderByNullsPlacements);
  }

  @Override
  public void stashAcceptedChildren(
      @Nonnull List<MatchPlanInputs> childInputs,
      @Nonnull List<Map<Object, Object>> childInputParameters,
      @Nonnull List<Boolean> childCacheEligible) {
    ctx.stashUnionChildren(childInputs, childInputParameters, childCacheEligible);
  }
}
