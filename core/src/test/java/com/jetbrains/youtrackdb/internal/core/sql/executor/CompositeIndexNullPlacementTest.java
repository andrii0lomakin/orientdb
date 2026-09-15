package com.jetbrains.youtrackdb.internal.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Verifies null-placement gates for ORDER BY shortcuts backed by composite indexes. */
@Category(SequentialTest.class)
public class CompositeIndexNullPlacementTest extends DbTestBase {

  private Object oldAscendingPlacement;

  @Before
  public void saveAscendingPlacement() {
    oldAscendingPlacement = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
  }

  @After
  public void restoreAscendingPlacement() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(oldAscendingPlacement);
  }

  /** A sort-only query must sort in memory when ASC requests non-natural NULLS LAST. */
  @Test
  public void sortOnlyNonNaturalPlacementDeclinesCompositeIndexOrdering() {
    var className = "CompositeNullSortOnly";
    createCompositeData(className);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);

    try (var result = session.query(
        "SELECT firstKey, secondKey FROM " + className + " ORDER BY firstKey ASC")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();

      assertTrue("Non-natural composite ordering needs an in-memory sort:\n" + plan,
          plan.contains("ORDER BY"));
      assertFalse("The sort-only shortcut must be declined:\n" + plan,
          plan.contains("FETCH FROM INDEX"));
      assertEquals(List.of(1, 2), nonNullValues(rows, "firstKey"));
      assertEquals(2, countNullValues(rows, "firstKey"));
      assertEquals(null, rows.getLast().getProperty("firstKey"));
    }
  }

  /** Equality on the leading key still needs a sort for non-natural trailing-key placement. */
  @Test
  public void equalityConditionNonNaturalPlacementKeepsIndexLookupAndAddsSort() {
    var className = "CompositeNullEquality";
    createCompositeData(className);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);

    try (var result = session.query(
        "SELECT firstKey, secondKey FROM " + className
            + " WHERE firstKey = 1 ORDER BY secondKey ASC")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();

      assertTrue("The equality predicate should retain its index lookup:\n" + plan,
          plan.contains("FETCH FROM INDEX"));
      assertTrue("Non-natural trailing-key placement needs an in-memory sort:\n" + plan,
          plan.contains("ORDER BY"));
      assertEquals(List.of(10, 20), nonNullValues(rows, "secondKey"));
      assertEquals(null, rows.getLast().getProperty("secondKey"));
    }
  }

  /**
   * A trailing sort item with an explicit non-natural clause must decline the sort-only shortcut.
   *
   * <p>The leading item asks for the natural placement of an ascending scan, which is nulls first,
   * so a gate that reads the leading item alone keeps the shortcut. The trailing item asks for nulls
   * last, which a composite key cannot produce, so the whole query needs an in-memory sort. The row
   * order proves the point. Under the shortcut the row with the null trailing key opens the group of
   * leading key 1, and the sorted answer closes that group with it.
   */
  @Test
  public void sortOnlyTrailingNonNaturalPlacementDeclinesCompositeIndexOrdering() {
    var className = "CompositeNullTrailingSortOnly";
    createCompositeData(className);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);

    try (var result = session.query(
        "SELECT firstKey, secondKey FROM " + className
            + " ORDER BY firstKey ASC, secondKey ASC NULLS LAST")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();

      assertTrue("A non-natural trailing item needs an in-memory sort:\n" + plan,
          plan.contains("ORDER BY"));
      assertFalse("The sort-only shortcut must be declined:\n" + plan,
          plan.contains("FETCH FROM INDEX"));
      assertEquals(
          "The null trailing key must close the group of leading key 1",
          Arrays.asList(1, 2, 10, 20, null, 5),
          values(rows, "secondKey"));
      assertEquals(
          "The leading key keeps its natural nulls-first placement",
          Arrays.asList(null, null, 1, 1, 1, 2),
          values(rows, "firstKey"));
    }
  }

  /**
   * A trailing sort item with an explicit non-natural clause must also decline the shortcut on the
   * condition-driven path.
   *
   * <p>The equality predicate on the leading key keeps its index lookup. The gate that decides
   * whether the lookup output is already fully sorted must read every sort item, so the non-natural
   * trailing item forces an in-memory sort on top of the lookup.
   */
  @Test
  public void equalityConditionTrailingNonNaturalPlacementAddsSort() {
    var className = "CompositeNullTrailingEquality";
    createCompositeData(className);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);

    try (var result = session.query(
        "SELECT firstKey, secondKey FROM " + className
            + " WHERE firstKey = 1 ORDER BY firstKey ASC, secondKey ASC NULLS LAST")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();

      assertTrue("The equality predicate should retain its index lookup:\n" + plan,
          plan.contains("FETCH FROM INDEX"));
      assertTrue("A non-natural trailing item needs an in-memory sort:\n" + plan,
          plan.contains("ORDER BY"));
      assertEquals(
          "The null trailing key must come last",
          Arrays.asList(10, 20, null),
          values(rows, "secondKey"));
    }
  }

  /**
   * An omitted direction means ascending on the condition-driven path. The configured nulls-last
   * placement is not natural for that scan, so the planner must retain an in-memory sort.
   */
  @Test
  public void equalityConditionOmittedDirectionsUseAscendingPlacement() {
    var className = "CompositeNullOmittedDirections";
    createCompositeData(className);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);

    try (var result = session.query(
        "SELECT firstKey, secondKey FROM " + className
            + " WHERE firstKey = 1 ORDER BY firstKey, secondKey")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();

      assertTrue("The equality predicate should retain its index lookup:\n" + plan,
          plan.contains("FETCH FROM INDEX"));
      assertTrue("Omitted directions must use ascending null placement:\n" + plan,
          plan.contains("ORDER BY"));
      assertEquals(
          "The configured ascending placement must put the null trailing key last",
          Arrays.asList(10, 20, null),
          values(rows, "secondKey"));
    }
  }

  /** Natural ASC NULLS FIRST keeps the sort-only composite-index shortcut. */
  @Test
  public void naturalPlacementKeepsCompositeIndexOrdering() {
    var className = "CompositeNullNatural";
    createCompositeData(className);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);

    try (var result = session.query(
        "SELECT firstKey, secondKey FROM " + className + " ORDER BY firstKey ASC")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      var rows = result.stream().toList();

      assertTrue("Natural composite ordering should use the index:\n" + plan,
          plan.contains("FETCH FROM INDEX"));
      assertFalse("Natural composite ordering should elide the in-memory sort:\n" + plan,
          plan.contains("ORDER BY"));
      assertEquals(null, rows.getFirst().getProperty("firstKey"));
      assertEquals(List.of(1, 2), nonNullValues(rows, "firstKey"));
    }
  }

  private void createCompositeData(String className) {
    session.execute("CREATE CLASS " + className).close();
    session.execute("CREATE PROPERTY " + className + ".firstKey INTEGER").close();
    session.execute("CREATE PROPERTY " + className + ".secondKey INTEGER").close();
    session.execute(
        "CREATE INDEX " + className + "_keys ON " + className
            + " (firstKey, secondKey) NOTUNIQUE")
        .close();

    session.begin();
    insert(className, null, 1);
    insert(className, null, 2);
    insert(className, 1, null);
    insert(className, 1, 10);
    insert(className, 1, 20);
    insert(className, 2, 5);
    session.commit();
  }

  private void insert(String className, Integer firstKey, Integer secondKey) {
    var entity = session.newEntity(className);
    entity.setProperty("firstKey", firstKey);
    entity.setProperty("secondKey", secondKey);
  }

  /** Every value of one property, in row order, so a test can assert the null position. */
  private static List<Integer> values(List<Result> rows, String property) {
    return rows.stream().map(row -> row.<Integer>getProperty(property)).toList();
  }

  private static List<Integer> nonNullValues(List<Result> rows, String property) {
    return rows.stream()
        .map(row -> row.<Integer>getProperty(property))
        .filter(value -> value != null)
        .distinct()
        .toList();
  }

  private static long countNullValues(List<Result> rows, String property) {
    return rows.stream().filter(row -> row.getProperty(property) == null).count();
  }
}
