package com.jetbrains.youtrackdb.internal.core.gremlin;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.GraphFeatureWorld;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.SampleScenarioOrderSemantics;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.YTDBGraphFeatureTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Guards feature-suite isolation and the intended scenario exclusions. */
@Category(SequentialTest.class)
public class GraphFeatureWorldConfigurationTest {

  private final SampleScenarioOrderSemantics sampleScenarioSemantics =
      new SampleScenarioOrderSemantics();

  @Test
  public void graphCacheKey_separatesOrderModesForOneTestClass() {
    var defaultMode = GraphFeatureWorld.cacheKey(YTDBGraphFeatureTest.class, false);
    var standardMode = GraphFeatureWorld.cacheKey(YTDBGraphFeatureTest.class, true);

    assertThat(defaultMode).isNotEqualTo(standardMode);
  }

  @Test
  public void ignoredScenarioCounts_matchBothFeatureExecutions() {
    assertThat(GraphFeatureWorld.ignoredScenarioCount(false)).isEqualTo(1);
    assertThat(GraphFeatureWorld.ignoredScenarioCount(true)).isEqualTo(7);
  }

  /** Both upstream sampling forms require standard order semantics. */
  @Test
  public void affectedSampleScenarios_receiveStandardOrderStrategy() {
    assertStandardStrategySelected(SampleScenarioOrderSemantics.REPEAT_SCENARIO);
    assertStandardStrategySelected(SampleScenarioOrderSemantics.SEQUENTIAL_SCENARIO);
  }

  /** A later unrelated scenario replaces the selection and receives the unchanged source. */
  @Test
  public void unrelatedScenario_afterAffectedScenarioReceivesDefaultSemantics() {
    sampleScenarioSemantics.select(SampleScenarioOrderSemantics.REPEAT_SCENARIO);
    sampleScenarioSemantics.select("g_V_sampleX1X");

    var source = EmptyGraph.instance().traversal();
    assertThat(sampleScenarioSemantics.decorate(source)).isSameAs(source);
  }

  /** Similar names do not match because routing uses complete upstream scenario names. */
  @Test
  public void partialScenarioName_receivesDefaultSemantics() {
    sampleScenarioSemantics.select(SampleScenarioOrderSemantics.REPEAT_SCENARIO + "_suffix");

    var source = EmptyGraph.instance().traversal();
    assertThat(sampleScenarioSemantics.decorate(source)).isSameAs(source);
  }

  private void assertStandardStrategySelected(String scenarioName) {
    sampleScenarioSemantics.select(scenarioName);

    var source = sampleScenarioSemantics.decorate(EmptyGraph.instance().traversal());
    assertThat(source.getStrategies().getStrategy(StandardOrderSemanticsStrategy.class))
        .contains(StandardOrderSemanticsStrategy.instance());
  }
}
