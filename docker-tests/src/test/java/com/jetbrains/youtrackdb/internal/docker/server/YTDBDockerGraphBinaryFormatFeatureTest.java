package com.jetbrains.youtrackdb.internal.docker.server;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.jetbrains.youtrackdb.internal.docker.server.features.YTDBDockerGraphFeatureTestHooks;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import io.cucumber.java.Scenario;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.junit.runner.RunWith;

@SuppressWarnings("deprecation")
@RunWith(Cucumber.class)
@CucumberOptions(
    tags = "not @MultiProperties "
        + "and not @GraphComputerOnly "
        + "and not @UserSuppliedVertexPropertyIds "
        + "and not @UserSuppliedEdgeIds "
        + "and not @UserSuppliedVertexIds "
        + "and not @TinkerServiceRegistry "
        + "and not @DisallowNullPropertyValues "
        + "and not @InsertionOrderingRequired "
        + "and not @DataUUID "
        + "and not @DataDateTime",
    glue = {"org.apache.tinkerpop.gremlin.features",
        "com.jetbrains.youtrackdb.internal.docker.server.features"},
    objectFactory = GuiceFactory.class,
    features = {"classpath:/org/apache/tinkerpop/gremlin/test/features"},
    plugin = {"progress", "junit:target/cucumber.xml"})
public class YTDBDockerGraphBinaryFormatFeatureTest {

  @SuppressWarnings("NewClassNamingConvention")
  public static final class ServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(World.class).to(YTDBGraphWorld.class);
    }
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static class YTDBGraphWorld implements World {

    private static final Set<String> STANDARD_ORDER_SCENARIOS = Set.of(
        "g_V3_repeatXout_order_byXperformancesX_sampleX2X_aggregateXxXX_"
            + "untilXloops_isX2XX_capXxX_unfold",
        "g_V3_out_order_byXperformancesX_sampleX2X_aggregateXxX_out_order_"
            + "byXperformancesX_sampleX2X_aggregateXxX_capXxX_unfold");

    private boolean standardOrderSemantics;

    @Override
    public GraphTraversalSource getGraphTraversalSource(GraphData graphData) {
      var source = YTDBDockerGraphFeatureTestHooks.youTrackDB.openTraversal(
          YTDBDockerGraphFeatureTestHooks.getServerGraphName(graphData));
      return decorate(source);
    }

    @Override
    public void beforeEachScenario(Scenario scenario) {
      selectScenario(scenario.getName());
    }

    void selectScenario(String scenarioName) {
      // Replace the previous selection because Cucumber may reuse a World instance.
      standardOrderSemantics = STANDARD_ORDER_SCENARIOS.contains(scenarioName);
    }

    GraphTraversalSource decorate(GraphTraversalSource source) {
      if (standardOrderSemantics) {
        return source.withStrategies(StandardOrderSemanticsStrategy.instance());
      }
      return source;
    }

    @Override
    public String convertIdToScript(Object id, Class<? extends Element> type) {
      return "\"" + id + "\"";
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
      var fileName = Paths.get(pathToFileFromGremlin).getFileName().toString();

      String resourceName;
      if (fileName.endsWith(".kryo")) {
        resourceName = fileName.substring(0, fileName.length() - 5) + "-v3.kryo";
      } else if (fileName.endsWith(".json")) {
        resourceName = fileName.substring(0, fileName.length() - 5) + "-v3.json";
      } else if (fileName.endsWith(".xml")) {
        resourceName = fileName;
      } else {
        throw new IllegalArgumentException(fileName + " is not supported");
      }

      return YTDBDockerGraphFeatureTestHooks.SERVER_DATA_GRAPHS + "/" + resourceName;
    }

    @Override
    public void afterEachScenario() {
      try (var traversal = YTDBDockerGraphFeatureTestHooks.youTrackDB.openTraversal("graph")) {
        traversal.autoExecuteInTx(g -> g.V().drop());
      }
    }
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static final class WorldInjectorSource implements InjectorSource {

    @Override
    public Injector getInjector() {
      return Guice.createInjector(
          Stage.PRODUCTION,
          CucumberModules.createScenarioModule(),
          new ServiceModule());
    }
  }
}
