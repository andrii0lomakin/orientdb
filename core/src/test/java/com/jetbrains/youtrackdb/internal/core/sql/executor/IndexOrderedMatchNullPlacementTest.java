package com.jetbrains.youtrackdb.internal.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionStep;
import com.jetbrains.youtrackdb.internal.core.query.ResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.IndexOrderedEdgeStep;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Verifies null placement frozen into an index-ordered MATCH plan. */
@Category(SequentialTest.class)
public class IndexOrderedMatchNullPlacementTest extends DbTestBase {

  /**
   * One shared graph exercises both direction defaults and every explicit clause. The omitted-clause
   * cases reverse the shipped defaults, so replacing the plan-time resolved pair with the shipped
   * pair makes both default cases fail.
   */
  @Test
  public void defaultsAndExplicitClausesControlTheFrozenIndexScanPlacement() throws Exception {
    createGraphOnce();
    var storageConfig = session.getStorage().getContextConfiguration();
    var oldAscending =
        storageConfig.getValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC);
    var oldDescending =
        storageConfig.getValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC);

    try (var optimizerConfig = setIndexOrderedTestConfig()) {
      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, OrderByNullsPlacement.FIRST);

      assertPlacement("ASC", "", false);
      assertPlacement("DESC", "", true);
      assertPlacement("ASC", " NULLS FIRST", true);
      assertPlacement("ASC", " NULLS LAST", false);
      assertPlacement("DESC", " NULLS FIRST", true);
      assertPlacement("DESC", " NULLS LAST", false);
    } finally {
      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, oldAscending);
      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, oldDescending);
    }
  }

  /** Runs one query and verifies both the physical index scan and its null boundary. */
  private void assertPlacement(String direction, String clause, boolean nullsFirst) {
    session.begin();
    var query =
        "MATCH {class: NullMatchPerson, as: p, where: (name = 'person1')}"
            + ".in('NULL_MATCH_CREATOR'){class: NullMatchMessage, as: m, "
            + "where: (msgId >= 199)} RETURN m.creationDate AS cd, m.msgId AS mid "
            + "ORDER BY cd " + direction + clause + " LIMIT 4";
    try (var result = session.query(query)) {
      var rows = result.stream().toList();
      assertEquals("two dated and two null messages must be returned", 4, rows.size());
      var step = findIndexOrderedStep(result);
      var path = step.getChosenRuntimePath();
      assertTrue(
          "the MATCH plan must execute a B-tree scan, but used " + path,
          path == IndexOrderedEdgeStep.RuntimePath.INDEX_SCAN
              || path == IndexOrderedEdgeStep.RuntimePath.UNION_SCAN
              || path == IndexOrderedEdgeStep.RuntimePath.GLOBAL_SCAN);
      assertEquals("the first row must have the requested null state", nullsFirst,
          rows.getFirst().getProperty("cd") == null);
      assertEquals("the last row must have the opposite null state", !nullsFirst,
          rows.getLast().getProperty("cd") == null);
    }
    session.commit();
  }

  /** Creates the indexed graph once for all six assertions in the test method. */
  private void createGraphOnce() {
    session.execute("CREATE CLASS NullMatchPerson EXTENDS V").close();
    session.execute("CREATE CLASS NullMatchMessage EXTENDS V").close();
    session.execute("CREATE PROPERTY NullMatchMessage.creationDate DATETIME").close();
    session.execute("CREATE PROPERTY NullMatchMessage.msgId LONG").close();
    session.execute("CREATE CLASS NULL_MATCH_CREATOR EXTENDS E").close();
    session.execute(
        "CREATE INDEX NullMatchMessage.creationDate ON NullMatchMessage(creationDate) NOTUNIQUE")
        .close();

    session.begin();
    session.execute("CREATE VERTEX NullMatchPerson SET name = 'person1'").close();
    for (var i = 1; i <= 200; i++) {
      session.execute(
          "CREATE VERTEX NullMatchMessage SET creationDate = '2025-01-01 "
              + String.format("%02d", i / 60) + ":" + String.format("%02d", i % 60)
              + ":00', msgId = " + i)
          .close();
      linkMessage(i);
    }
    for (var i = 201; i <= 202; i++) {
      session.execute(
          "CREATE VERTEX NullMatchMessage SET creationDate = null, msgId = " + i).close();
      linkMessage(i);
    }
    session.commit();
  }

  private void linkMessage(int id) {
    session.execute(
        "CREATE EDGE NULL_MATCH_CREATOR FROM (SELECT FROM NullMatchMessage WHERE msgId = "
            + id + ") TO (SELECT FROM NullMatchPerson WHERE name = 'person1')")
        .close();
  }

  private static IndexOrderedEdgeStep findIndexOrderedStep(ResultSet result) {
    var plan = result.getExecutionPlan();
    assertNotNull("execution plan must be present", plan);
    var found = findIndexOrderedStep(plan.getSteps());
    assertNotNull("the plan must contain INDEX ORDERED MATCH:\n" + plan.prettyPrint(0, 2), found);
    return found;
  }

  @Nullable private static IndexOrderedEdgeStep findIndexOrderedStep(List<ExecutionStep> steps) {
    for (var step : steps) {
      if (step instanceof IndexOrderedEdgeStep indexOrdered) {
        return indexOrdered;
      }
      var nested = findIndexOrderedStep(step.getSubSteps());
      if (nested != null) {
        return nested;
      }
    }
    return null;
  }

  private static AutoCloseable setIndexOrderedTestConfig() {
    var oldMinLinkBag = GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG.getValue();
    var oldMaxScan = GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SCAN.getValue();
    var oldCostBias = GlobalConfiguration.QUERY_INDEX_ORDERED_COST_BIAS.getValue();
    var oldMaxSources = GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SOURCES.getValue();

    GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG.setValue(1);
    GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SCAN.setValue(10_000_000);
    GlobalConfiguration.QUERY_INDEX_ORDERED_COST_BIAS.setValue(1.0);
    GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SOURCES.setValue(100_000);
    return () -> {
      GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG.setValue(oldMinLinkBag);
      GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SCAN.setValue(oldMaxScan);
      GlobalConfiguration.QUERY_INDEX_ORDERED_COST_BIAS.setValue(oldCostBias);
      GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SOURCES.setValue(oldMaxSources);
    };
  }
}
