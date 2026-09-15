package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization;

import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.GremlinToMatchStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;

/**
 * Applies the direction-specific null placement settings to native Gremlin {@code order()} steps.
 * TinkerPop's default comparators already place nulls first for ascending order and last for
 * descending order. The strategy rebuilds each {@link OrderGlobalStep} /
 * {@link OrderLocalStep} with wrapped framework {@link Order#asc} / {@link Order#desc}
 * comparators so null placement matches YQL {@code ORDER BY}.
 *
 * <p>Runs after {@link GremlinToMatchStrategy}, {@link YTDBOrderRidTieBreakStrategy}, and
 * {@link YTDBStandardOrderSemanticsStrategy}. A recognized shape loses its order step to the MATCH
 * splice. Earlier native strategies must finish the sort slots and missing-key behavior before
 * comparator wrapping.
 *
 * <p>Only the two framework order constants are wrapped. {@link Order#shuffle} and caller-supplied
 * comparators keep their own null handling. The strategy does not walk nested traversals: the
 * framework already visits each child once.
 */
public final class YTDBOrderNullsStrategy
    extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
    implements ProviderOptimizationStrategy {

  private static final YTDBOrderNullsStrategy INSTANCE = new YTDBOrderNullsStrategy();

  private YTDBOrderNullsStrategy() {
  }

  public static YTDBOrderNullsStrategy instance() {
    return INSTANCE;
  }

  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
    return Set.of(
        GremlinToMatchStrategy.class,
        YTDBOrderRidTieBreakStrategy.class,
        YTDBStandardOrderSemanticsStrategy.class);
  }

  @Override
  public void apply(Admin<?, ?> traversal) {
    // Resolve configuration once for the whole apply. Every wrap below reuses this value.
    var placements = YTDBStrategyUtil.orderByNullsPlacements(traversal);
    if (placements == null) {
      return;
    }

    for (OrderGlobalStep<?, ?> step : TraversalHelper.getStepsOfAssignableClass(
        OrderGlobalStep.class, traversal)) {
      rebuildGlobal(step, placements);
    }
    for (OrderLocalStep<?, ?> step : TraversalHelper.getStepsOfAssignableClass(
        OrderLocalStep.class, traversal)) {
      rebuildLocal(step, placements);
    }
  }

  /**
   * Rebuilds a global order step when it holds a framework {@code asc}/{@code desc} comparator.
   * {@code getComparators()} is unmodifiable (and synthesizes identity+asc for a bare {@code
   * order()}), so the pairs are copied onto a fresh step and the original is replaced.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void rebuildGlobal(
      OrderGlobalStep<?, ?> step, ResolvedOrderByNullsPlacement placements) {
    var replacements = wrappedComparators((List<?>) step.getComparators(), placements);
    if (replacements != null) {
      OrderStepModulators.replaceGlobalComparators((OrderGlobalStep) step, replacements);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void rebuildLocal(
      OrderLocalStep<?, ?> step, ResolvedOrderByNullsPlacement placements) {
    var replacements = wrappedComparators((List<?>) step.getComparators(), placements);
    if (replacements != null) {
      OrderStepModulators.replaceLocalComparators((OrderLocalStep) step, replacements);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Pair<Admin, Comparator>> wrappedComparators(
      List<?> slots, ResolvedOrderByNullsPlacement placements) {
    var pairs = (List<Pair<Admin, Comparator>>) (List<?>) slots;
    List<Pair<Admin, Comparator>> replacements = null;
    for (var index = 0; index < pairs.size(); index++) {
      var pair = pairs.get(index);
      var comparator = maybeWrap(pair.getValue1(), placements);
      if (comparator != pair.getValue1() && replacements == null) {
        replacements = new ArrayList<>(pairs);
      }
      if (replacements != null) {
        replacements.set(index, Pair.with(pair.getValue0(), comparator));
      }
    }
    return replacements;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Comparator maybeWrap(
      Comparator comparator, ResolvedOrderByNullsPlacement placements) {
    if (comparator == Order.asc && placements.ascending() == OrderByNullsPlacement.LAST) {
      return wrap(comparator, false);
    }
    if (comparator == Order.desc && placements.descending() == OrderByNullsPlacement.FIRST) {
      return wrap(comparator, true);
    }
    // shuffle and caller-supplied comparators keep their own null handling.
    return comparator;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Comparator wrap(Comparator delegate, boolean nullsFirst) {
    return (a, b) -> {
      if (a == null || b == null) {
        if (a == null && b == null) {
          return 0;
        }
        if (a == null) {
          return nullsFirst ? -1 : 1;
        }
        return nullsFirst ? 1 : -1;
      }
      return delegate.compare(a, b);
    };
  }
}
