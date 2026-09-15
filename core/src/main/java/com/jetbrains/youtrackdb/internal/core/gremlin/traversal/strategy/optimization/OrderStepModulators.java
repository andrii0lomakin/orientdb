package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;

/**
 * State-preserving rebuilds of {@code order()} steps, shared by strategies that replace sort slots.
 *
 * <h2>Why a rebuild rather than a child replacement</h2>
 *
 * {@code replaceLocalChild} cannot express a positional replacement. It matches a slot by
 * {@code equals}, and every {@code IdentityTraversal} equals every other one, so it rewrites the
 * first equal-looking slot instead of the requested one. There is no positional setter on
 * {@code ComparatorHolder} either, so the step is rebuilt with the slot list the caller wants and
 * swapped in at the same index. The index is found by reference, not through
 * {@code TraversalHelper.stepIndex}, which matches on {@code hashCode} and could find an earlier
 * equal-looking order step.
 *
 * <p>A global rebuild must preserve state established by earlier strategies. The limit controls
 * how many sorted traversers survive. Missing-key filtering controls whether a traverser without a
 * sort key survives. Losing either state would change query semantics while replacing only a
 * modulator. Labels must also survive because later traversal steps address them by name.
 *
 * <p>The fork always enables missing-key filtering for local order steps. It exposes no path that
 * disables this state. A new local step therefore preserves the only reachable state by default.
 *
 * <p>The class holds one copy of each rebuild on purpose. Modulator and comparator strategies must
 * preserve step state identically.
 */
final class OrderStepModulators {

  private OrderStepModulators() {
    // Static helper — no instances.
  }

  /**
   * The current modulators of {@code comparators}, as a mutable list a caller can substitute into
   * before handing it back to one of the replace methods.
   */
  @SuppressWarnings("rawtypes")
  static List<Admin> modulatorsOf(
      List<? extends Pair<? extends Admin<?, ?>, ? extends Comparator<?>>> comparators) {
    List<Admin> modulators = new ArrayList<>(comparators.size());
    for (var slot : comparators) {
      modulators.add(slot.getValue0());
    }
    return modulators;
  }

  /**
   * Rebuilds {@code step} with {@code modulators} in its comparator slots, keeping every comparator
   * and every step label, and swaps the rebuilt step in at the same index.
   *
   * <p>A bare {@code order()} keeps its fast path. Its comparator field is empty, and
   * {@code getComparators} synthesises the single identity slot. Installing the modulator on the
   * existing step therefore preserves the limit and missing-key filtering state without a rebuild.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void replaceGlobalModulators(OrderGlobalStep step, List<Admin> modulators) {
    var comparators = (List<Pair<Admin, Comparator>>) step.getComparators();
    if (step.getLocalChildren().isEmpty()) {
      step.modulateBy(modulators.getFirst(), comparators.getFirst().getValue1());
      return;
    }
    var replacements = new ArrayList<Pair<Admin, Comparator>>(comparators.size());
    for (var index = 0; index < comparators.size(); index++) {
      replacements.add(Pair.with(modulators.get(index), comparators.get(index).getValue1()));
    }
    replaceGlobalComparators(step, replacements);
  }

  /** Rebuilds a global step with replacement slots while preserving limit, filtering, and labels. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void replaceGlobalComparators(
      OrderGlobalStep step, List<Pair<Admin, Comparator>> comparators) {
    var replacement = new OrderGlobalStep(step.getTraversal());
    replacement.setLimit(step.getLimit());
    if (step.isFilteringUnproductiveTraversers()) {
      replacement.enableFilteringUnproductiveTraversers();
    }
    for (var comparator : comparators) {
      replacement.addComparator(comparator.getValue0(), comparator.getValue1());
    }
    swapStep(step, replacement);
  }

  /** Rebuilds a local step with replacement slots while preserving labels. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void replaceLocalComparators(
      OrderLocalStep step, List<Pair<Admin, Comparator>> comparators) {
    var replacement = new OrderLocalStep(step.getTraversal());
    for (var comparator : comparators) {
      replacement.addComparator(comparator.getValue0(), comparator.getValue1());
    }
    swapStep(step, replacement);
  }

  /** Swaps {@code replacement} in at the exact index {@code step} occupies, labels included. */
  private static void swapStep(Step<?, ?> step, Step<?, ?> replacement) {
    var traversal = step.getTraversal();
    var steps = traversal.getSteps();
    var index = 0;
    while (index < steps.size() && steps.get(index) != step) {
      index++;
    }
    TraversalHelper.copyLabels(step, replacement, false);
    traversal.removeStep(index);
    traversal.addStep(index, replacement);
  }
}
