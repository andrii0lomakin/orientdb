package com.jetbrains.youtrackdb.internal.server.gremlin;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.SampleScenarioOrderSemantics;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import io.cucumber.java.Scenario;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.junit.AssumptionViolatedException;
import org.junit.runner.RunWith;

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
        "com.jetbrains.youtrackdb.internal.server.gremlin.features"},
    objectFactory = GuiceFactory.class,
    features = {"classpath:/org/apache/tinkerpop/gremlin/test/features",
        "classpath:/com/jetbrains/youtrackdb/internal/core/gremlin/gremlintest/features"},
    plugin = {"progress", "junit:target/cucumber.xml"})
public class YTDBRemoteGraphBinaryFormatFeatureTest {

  public static final ConcurrentHashMap<String, String> PATHS = new ConcurrentHashMap<>();
  public static final String YTDB_REMOTE_TEST = "ytdbRemoteTest";

  private static final Map<String, String> IGNORED_TESTS = Map.of(
      "g_injectXhello_hiX_concat_XV_valuesXnameXX",
      "YouTrackDB doesn't guarantee a consistent order of element's IDs");

  @SuppressWarnings("NewClassNamingConvention")
  public static final class ServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(World.class).to(YTDBGraphWorld.class);
    }
  }

  @SuppressWarnings("NewClassNamingConvention")
  public static class YTDBGraphWorld implements World {

    public static final YTDBGraphBinaryRemoteGraphProvider provider =
        new YTDBGraphBinaryRemoteGraphProvider();

    private final SampleScenarioOrderSemantics sampleScenarioOrderSemantics =
        new SampleScenarioOrderSemantics();

    @Override
    public GraphTraversalSource getGraphTraversalSource(GraphData graphData) {
      return sampleScenarioOrderSemantics.decorate(doGetTraversalSource(graphData));
    }

    private static YTDBGraphTraversalSource doGetTraversalSource(GraphData graphData) {
      final var graph = (switch (graphData) {
        case null -> initGraph(null);
        case CLASSIC -> initGraph(GraphData.CLASSIC);
        case MODERN -> initGraph(GraphData.MODERN);
        case CREW -> initGraph(GraphData.CREW);
        case GRATEFUL -> initGraph(GraphData.GRATEFUL);
        case SINK -> initGraph(GraphData.SINK);
      });

      return provider.traversal(graph);
    }

    private static Graph initGraph(GraphData graphData) {
      final var config =
          provider.standardGraphConfiguration(YTDBRemoteGraphBinaryFormatFeatureTest.class,
              YTDB_REMOTE_TEST,
              graphData);
      return provider.openTestGraph(config);
    }

    private static void cleanEmpty() {
      var traversal = doGetTraversalSource(null);
      traversal.autoExecuteInTx(g -> g.V().drop());
    }

    @Override
    public String convertIdToScript(Object id, Class<? extends Element> type) {
      return "\"" + id + "\"";
    }

    @Override
    public void beforeEachScenario(final Scenario scenario) {
      sampleScenarioOrderSemantics.select(scenario.getName());
      if (IGNORED_TESTS.containsKey(scenario.getName())) {
        throw new AssumptionViolatedException(IGNORED_TESTS.get(scenario.getName()));
      }

      cleanEmpty();
    }

    @Override
    public void afterEachScenario() {
      provider.closeConnections();
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
      var fileName = Paths.get(pathToFileFromGremlin).getFileName().toString();
      @SuppressWarnings("UnnecessaryLocalVariable")
      var realPath = PATHS.compute(fileName, (file, path) -> {
        try {

          if (file.endsWith(".kryo")) {
            var resourceName = fileName.substring(0, fileName.length() - 5) + "-v3.kryo";
            return TestHelper.generateTempFileFromResource(GryoResourceAccess.class, resourceName,
                "")
                .getAbsolutePath();
          }
          if (file.endsWith(".json")) {
            var resourceName = fileName.substring(0, fileName.length() - 5) + "-v3.json";
            return TestHelper.generateTempFileFromResource(GraphSONResourceAccess.class,
                resourceName,
                "")
                .getAbsolutePath();
          }
          if (file.endsWith(".xml")) {
            return TestHelper.generateTempFileFromResource(GraphMLResourceAccess.class, fileName,
                "").getAbsolutePath();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        throw new IllegalArgumentException(file + " is not supported");
      });

      return realPath;
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
