package com.jetbrains.youtrackdb.internal.core.sql.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests explicit and configured placement for an {@link SQLOrderByItem}. */
@Category(SequentialTest.class)
public class SQLOrderByItemNullOrderingTest {

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

  /** Explicit NULLS FIRST remains absolute for ascending and descending items. */
  @Test
  public void explicitNullsFirstIsAbsolute() {
    assertTrue(item(SQLOrderByItem.ASC, SQLOrderByItem.NULLS_FIRST).resolveNullsFirst());
    assertTrue(item(SQLOrderByItem.DESC, SQLOrderByItem.NULLS_FIRST).resolveNullsFirst());
  }

  /** Explicit NULLS LAST remains absolute for ascending and descending items. */
  @Test
  public void explicitNullsLastIsAbsolute() {
    assertFalse(item(SQLOrderByItem.ASC, SQLOrderByItem.NULLS_LAST).resolveNullsFirst());
    assertFalse(item(SQLOrderByItem.DESC, SQLOrderByItem.NULLS_LAST).resolveNullsFirst());
  }

  /** Omitted clauses use each direction's shipped placement. */
  @Test
  public void omittedClauseUsesShippedDirectionPlacements() {
    assertTrue(item(SQLOrderByItem.ASC, null).resolveNullsFirst());
    assertFalse(item(SQLOrderByItem.DESC, null).resolveNullsFirst());
  }

  /** Omitted clauses use each direction's runtime setting without coupling the two values. */
  @Test
  public void omittedClauseUsesDirectionSpecificRuntimeSetting() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(OrderByNullsPlacement.FIRST);

    assertFalse(item(SQLOrderByItem.ASC, null).resolveNullsFirst());
    assertTrue(item(SQLOrderByItem.DESC, null).resolveNullsFirst());
  }

  /** A storage placement overrides only its matching runtime direction. */
  @Test
  public void storagePlacementOverridesMatchingRuntimeDirection() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);
    var config = new ContextConfiguration();
    config.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);

    assertFalse(item(SQLOrderByItem.ASC, null).resolveNullsFirst(config));
    assertFalse(item(SQLOrderByItem.DESC, null).resolveNullsFirst(config));
  }

  private static SQLOrderByItem item(String type, String nullOrdering) {
    var item = new SQLOrderByItem();
    item.setType(type);
    item.setNullOrdering(nullOrdering);
    return item;
  }
}
