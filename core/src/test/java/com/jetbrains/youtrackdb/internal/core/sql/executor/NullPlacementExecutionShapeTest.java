package com.jetbrains.youtrackdb.internal.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Compares null placement across in-memory SELECT, indexed SELECT, and MATCH execution. */
@Category(SequentialTest.class)
public class NullPlacementExecutionShapeTest extends DbTestBase {

  private static final String CLASS_NAME = "NullPlacementShapeItem";
  private static final String SOURCE_CLASS = "NullPlacementShapeSource";
  private static final String EDGE_CLASS = "NullPlacementShapeEdge";

  private Object oldAscending;
  private Object oldDescending;
  private Object oldMinLinkBag;
  private Object oldMaxScan;
  private Object oldCostBias;

  @Before
  public void saveConfigurationAndCreateData() {
    oldAscending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    oldDescending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.getValue();
    oldMinLinkBag = GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG.getValue();
    oldMaxScan = GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SCAN.getValue();
    oldCostBias = GlobalConfiguration.QUERY_INDEX_ORDERED_COST_BIAS.getValue();
    GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG.setValue(1);
    GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SCAN.setValue(10_000_000);
    GlobalConfiguration.QUERY_INDEX_ORDERED_COST_BIAS.setValue(0.01);

    session.execute("CREATE CLASS " + CLASS_NAME + " EXTENDS V").close();
    session.execute("CREATE PROPERTY " + CLASS_NAME + ".label STRING").close();
    session.execute("CREATE PROPERTY " + CLASS_NAME + ".rank INTEGER").close();
    session.execute("CREATE CLASS " + SOURCE_CLASS + " EXTENDS V").close();
    session.execute("CREATE CLASS " + EDGE_CLASS + " EXTENDS E").close();
    session.execute(
        "CREATE INDEX " + CLASS_NAME + "_rank ON " + CLASS_NAME + " (rank) NOTUNIQUE")
        .close();

    session.begin();
    session.execute("CREATE VERTEX " + SOURCE_CLASS + " SET name = 'root'").close();
    createItem("absent", null, false);
    createItem("stored-null", null, true);
    createItem("expression-null", null, true);
    for (var rank = 1; rank <= 50; rank++) {
      createItem("value-" + rank, rank, true);
    }
    session.commit();
  }

  @After
  public void restoreConfiguration() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(oldAscending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(oldDescending);
    GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG.setValue(oldMinLinkBag);
    GlobalConfiguration.QUERY_INDEX_ORDERED_MAX_SCAN.setValue(oldMaxScan);
    GlobalConfiguration.QUERY_INDEX_ORDERED_COST_BIAS.setValue(oldCostBias);
  }

  /**
   * One data set produces identical null boundaries in all three physical plans for both directions.
   * Explicit FIRST and LAST clauses also override deliberately opposing defaults.
   */
  @Test
  public void allPlacementPairsAgreeAcrossExecutionShapes() {
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setPlacements(ascending, descending);
        assertShapesAgree("ASC", "", ascending == OrderByNullsPlacement.FIRST);
        assertShapesAgree("DESC", "", descending == OrderByNullsPlacement.FIRST);
      }
    }

    setPlacements(OrderByNullsPlacement.LAST, OrderByNullsPlacement.LAST);
    assertShapesAgree("ASC", " NULLS FIRST", true);
    setPlacements(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.FIRST);
    assertShapesAgree("DESC", " NULLS LAST", false);
  }

  /**
   * The sort key is an ifnull expression, not a stored property. Missing and stored-null operands
   * evaluate to null, and the configured placement controls those expression results.
   */
  @Test
  public void expressionEvaluatingToNullUsesConfiguredPlacement() {
    setPlacements(OrderByNullsPlacement.LAST, OrderByNullsPlacement.LAST);
    var sql =
        "SELECT label, ifnull(rank, null) AS sortRank FROM " + CLASS_NAME
            + " ORDER BY sortRank ASC";
    var run = run(sql, "sortRank");

    assertEquals(53, run.nullStates().size());
    assertEquals("the expression nulls must occupy the final three rows",
        nullStates(false), run.nullStates());
  }

  private void createItem(String label, Integer rank, boolean storeRank) {
    var command = "CREATE VERTEX " + CLASS_NAME + " SET label = '" + label + "'";
    if (storeRank) {
      command += ", rank = " + rank;
    }
    session.execute(command).close();
    session.execute(
        "CREATE EDGE " + EDGE_CLASS + " FROM (SELECT FROM " + CLASS_NAME
            + " WHERE label = '" + label + "') TO (SELECT FROM " + SOURCE_CLASS
            + " WHERE name = 'root')")
        .close();
  }

  private static void setPlacements(
      OrderByNullsPlacement ascending, OrderByNullsPlacement descending) {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(ascending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(descending);
  }

  private void assertShapesAgree(String direction, String clause, boolean nullsFirst) {
    var indexSql =
        "SELECT label, rank FROM " + CLASS_NAME + " ORDER BY rank " + direction + clause;
    var memorySql =
        "SELECT label, rank FROM " + CLASS_NAME + " ORDER BY rank " + direction + clause
            + ", label " + direction;
    var matchSql =
        "MATCH {class: " + SOURCE_CLASS + ", as:source, where: (name = 'root')}.in('"
            + EDGE_CLASS + "'){class: " + CLASS_NAME + ", as:item} "
            + "RETURN item.label AS label, item.rank AS rank ORDER BY rank " + direction + clause
            + " LIMIT 53";
    var indexed = run(indexSql, "rank");
    var inMemory = run(memorySql, "rank");
    var match = runMatch(matchSql);

    assertTrue("the indexed shape must fetch ordered index values:\n" + indexed.plan(),
        indexed.plan().contains("FETCH FROM INDEX"));
    assertFalse("the indexed shape must not sort in memory:\n" + indexed.plan(),
        indexed.plan().contains("ORDER BY"));
    assertTrue("the multi-key shape must sort in memory:\n" + inMemory.plan(),
        inMemory.plan().contains("ORDER BY"));

    var expectedNullStates = nullStates(nullsFirst);
    assertEquals(expectedNullStates, indexed.nullStates());
    assertEquals("in-memory SELECT must match indexed SELECT", indexed.nullStates(),
        inMemory.nullStates());
    assertEquals("index-ordered MATCH must match indexed SELECT", indexed.nullStates(),
        match.nullStates());
    assertEquals(indexed.labels(), inMemory.labels());
    assertEquals(indexed.labels(), match.labels());
  }

  private static List<Boolean> nullStates(boolean nullsFirst) {
    var states = new java.util.ArrayList<Boolean>(53);
    var leadingCount = nullsFirst ? 3 : 50;
    var trailingCount = nullsFirst ? 50 : 3;
    for (var i = 0; i < leadingCount; i++) {
      states.add(nullsFirst);
    }
    for (var i = 0; i < trailingCount; i++) {
      states.add(!nullsFirst);
    }
    return states;
  }

  private Run runMatch(String sql) {
    session.begin();
    Run run;
    try (var result = session.query(sql)) {
      var rows = result.stream().toList();
      var step = findIndexOrderedStep(result);
      var path = step.getChosenRuntimePath();
      assertTrue(
          "the MATCH shape must execute an index scan, but used " + path,
          path == IndexOrderedEdgeStep.RuntimePath.INDEX_SCAN
              || path == IndexOrderedEdgeStep.RuntimePath.UNION_SCAN
              || path == IndexOrderedEdgeStep.RuntimePath.GLOBAL_SCAN);
      run = new Run(
          result.getExecutionPlan().prettyPrint(0, 2),
          rows.stream().map(row -> row.getProperty("rank") == null).toList(),
          rows.stream().map(row -> row.<String>getProperty("label")).collect(Collectors.toSet()));
    }
    session.commit();
    return run;
  }

  private Run run(String sql, String sortProperty) {
    session.begin();
    Run run;
    try (var result = session.query(sql)) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();
      run = new Run(
          plan,
          rows.stream().map(row -> row.getProperty(sortProperty) == null).toList(),
          rows.stream().map(row -> row.<String>getProperty("label")).collect(Collectors.toSet()));
    }
    session.commit();
    return run;
  }

  private static IndexOrderedEdgeStep findIndexOrderedStep(ResultSet result) {
    var plan = result.getExecutionPlan();
    assertNotNull("MATCH execution plan must be present", plan);
    var step = findIndexOrderedStep(plan.getSteps());
    assertNotNull("MATCH must use INDEX ORDERED MATCH:\n" + plan.prettyPrint(0, 2), step);
    return step;
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

  private record Run(String plan, List<Boolean> nullStates, Set<String> labels) {
  }
}
