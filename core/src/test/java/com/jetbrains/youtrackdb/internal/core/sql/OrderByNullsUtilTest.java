package com.jetbrains.youtrackdb.internal.core.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.LogRecordCollector;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests direction-specific null placement resolution and tolerant configuration reads. */
@Category(SequentialTest.class)
public class OrderByNullsUtilTest {

  private Object previousAscending;
  private Object previousDescending;

  @Before
  public void saveGlobals() {
    previousAscending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    previousDescending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.getValue();
  }

  @After
  public void restoreGlobals() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(previousDescending);
  }

  /** Explicit clauses remain absolute for either direction. */
  @Test
  public void explicitPlacementOverridesDirectionSettings() {
    assertTrue(OrderByNullsUtil.resolveNullsFirst(SQLOrderByItem.NULLS_FIRST, false, null));
    assertFalse(OrderByNullsUtil.resolveNullsFirst(SQLOrderByItem.NULLS_LAST, true, null));
  }

  /** Shipped settings preserve nulls-first ascending and nulls-last descending behavior. */
  @Test
  public void shippedValuesPreserveExistingBehavior() {
    assertEquals(ResolvedOrderByNullsPlacement.SHIPPED,
        OrderByNullsUtil.resolvePlacements(null));
    assertTrue(OrderByNullsUtil.resolveNullsFirst(null, true, null));
    assertFalse(OrderByNullsUtil.resolveNullsFirst(null, false, null));
  }

  /** All four independent combinations apply directly to their matching directions. */
  @Test
  public void allFourPlacementCombinationsAreIndependent() {
    assertCombination(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.FIRST, true, true);
    assertCombination(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST, true, false);
    assertCombination(OrderByNullsPlacement.LAST, OrderByNullsPlacement.FIRST, false, true);
    assertCombination(OrderByNullsPlacement.LAST, OrderByNullsPlacement.LAST, false, false);
  }

  /** Storage-local values override the matching runtime globals independently. */
  @Test
  public void storageValuesOverrideRuntimeGlobalsIndependently() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(OrderByNullsPlacement.FIRST);
    var storageConfig = new ContextConfiguration();
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.FIRST);

    assertEquals(
        new ResolvedOrderByNullsPlacement(
            OrderByNullsPlacement.FIRST, OrderByNullsPlacement.FIRST),
        OrderByNullsUtil.resolvePlacements(storageConfig));
  }

  /** Lower-case stored values name enum constants without changing their meaning. */
  @Test
  public void lowerCaseStoredValueIsAccepted() {
    var storageConfig = new ContextConfiguration();
    storageConfig.setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, "first");

    assertEquals(OrderByNullsPlacement.FIRST,
        OrderByNullsUtil.resolvePlacements(storageConfig).descending());
  }

  /** An unreadable storage value is reported and replaced by that key's runtime global. */
  @Test
  public void invalidStoredValueIsReportedAndUsesRuntimeGlobal() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    var storageConfig = new ContextConfiguration();
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, "NOT_A_PLACEMENT_TRACK_02");

    try (var logs = LogRecordCollector.attachTo(OrderByNullsUtil.class)) {
      assertEquals(OrderByNullsPlacement.LAST,
          OrderByNullsUtil.resolvePlacements(storageConfig).ascending());
      assertTrue(
          "the unreadable value must be reported, captured: " + logs.messages(),
          logs.warnedWithAll(
              "NOT_A_PLACEMENT_TRACK_02",
              GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getKey(),
              "runtime global value applies when it is readable"));
    }
  }

  /** A null command context uses both runtime globals without throwing. */
  @Test
  public void nullCommandContextUsesRuntimeGlobals() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(OrderByNullsPlacement.FIRST);

    assertEquals(
        ResolvedOrderByNullsPlacement.REVERSED,
        OrderByNullsUtil.resolvePlacementsForSort(null));
  }

  /** Explicit clauses override a pair passed directly to the composition helper. */
  @Test
  public void composeExplicitClauseOverridesPassedPlacements() {
    assertTrue(
        OrderByNullsUtil.composeNullsFirst(
            SQLOrderByItem.NULLS_FIRST, false, ResolvedOrderByNullsPlacement.SHIPPED));
    assertFalse(
        OrderByNullsUtil.composeNullsFirst(
            SQLOrderByItem.NULLS_LAST, true, ResolvedOrderByNullsPlacement.REVERSED));
  }

  /** A context without a session uses both runtime globals without throwing. */
  @Test
  public void contextWithoutSessionUsesRuntimeGlobals() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(OrderByNullsPlacement.FIRST);

    assertEquals(
        ResolvedOrderByNullsPlacement.REVERSED,
        OrderByNullsUtil.resolvePlacementsForSort(new BasicCommandContext()));
  }

  private static void assertCombination(
      OrderByNullsPlacement ascending,
      OrderByNullsPlacement descending,
      boolean expectedAscendingFirst,
      boolean expectedDescendingFirst) {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(ascending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(descending);

    var placements = OrderByNullsUtil.resolvePlacements(null);
    assertEquals(expectedAscendingFirst,
        OrderByNullsUtil.composeNullsFirst(null, true, placements));
    assertEquals(expectedDescendingFirst,
        OrderByNullsUtil.composeNullsFirst(null, false, placements));
  }
}
