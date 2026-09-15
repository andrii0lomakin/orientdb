package com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest;

import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;

/** Selects standard order semantics for sample scenarios that require productive order keys. */
public final class SampleScenarioOrderSemantics {

  public static final String REPEAT_SCENARIO =
      "g_V3_repeatXout_order_byXperformancesX_sampleX2X_aggregateXxXX_"
          + "untilXloops_isX2XX_capXxX_unfold";
  public static final String SEQUENTIAL_SCENARIO =
      "g_V3_out_order_byXperformancesX_sampleX2X_aggregateXxX_out_order_"
          + "byXperformancesX_sampleX2X_aggregateXxX_capXxX_unfold";

  private static final Set<String> STANDARD_ORDER_SCENARIOS =
      Set.of(REPEAT_SCENARIO, SEQUENTIAL_SCENARIO);

  private boolean standardOrderSemantics;

  /** Replaces the previous scenario selection to prevent semantics leaking between scenarios. */
  public void select(String scenarioName) {
    standardOrderSemantics = STANDARD_ORDER_SCENARIOS.contains(scenarioName);
  }

  /** Decorates the source only when the current scenario needs productive order semantics. */
  public GraphTraversalSource decorate(GraphTraversalSource source) {
    if (standardOrderSemantics) {
      return source.withStrategies(StandardOrderSemanticsStrategy.instance());
    }
    return source;
  }
}
