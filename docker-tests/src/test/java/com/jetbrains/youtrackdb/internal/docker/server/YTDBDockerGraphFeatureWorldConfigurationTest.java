package com.jetbrains.youtrackdb.internal.docker.server;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

/** Guards Docker feature-world scenario routing without starting containers. */
public class YTDBDockerGraphFeatureWorldConfigurationTest {

  private static final String REPEAT_SCENARIO =
      "g_V3_repeatXout_order_byXperformancesX_sampleX2X_aggregateXxXX_"
          + "untilXloops_isX2XX_capXxX_unfold";
  private static final String SEQUENTIAL_SCENARIO =
      "g_V3_out_order_byXperformancesX_sampleX2X_aggregateXxX_out_order_"
          + "byXperformancesX_sampleX2X_aggregateXxX_capXxX_unfold";

  private final YTDBDockerGraphBinaryFormatFeatureTest.YTDBGraphWorld world =
      new YTDBDockerGraphBinaryFormatFeatureTest.YTDBGraphWorld();

  /** Both upstream sampling forms receive standard order semantics in the Docker world. */
  @Test
  public void affectedSampleScenarios_receiveStandardOrderStrategy() {
    assertStandardStrategySelected(REPEAT_SCENARIO);
    assertStandardStrategySelected(SEQUENTIAL_SCENARIO);
  }

  /** Selecting an unrelated scenario resets the semantics selected by an earlier scenario. */
  @Test
  public void unrelatedScenario_afterAffectedScenarioReceivesDefaultSemantics() {
    world.selectScenario(REPEAT_SCENARIO);
    world.selectScenario("g_V_sampleX1X");

    var source = EmptyGraph.instance().traversal();
    assertSame(source, world.decorate(source));
  }

  private void assertStandardStrategySelected(String scenarioName) {
    world.selectScenario(scenarioName);

    var source = world.decorate(EmptyGraph.instance().traversal());
    assertTrue(source.getStrategies().getStrategy(StandardOrderSemanticsStrategy.class)
        .filter(strategy -> strategy == StandardOrderSemanticsStrategy.instance())
        .isPresent());
  }
}
