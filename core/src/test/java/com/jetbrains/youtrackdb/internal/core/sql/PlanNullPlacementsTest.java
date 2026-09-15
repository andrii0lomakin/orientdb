package com.jetbrains.youtrackdb.internal.core.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import org.junit.Test;

/**
 * Tests the per-build null placement scope. The scope is what lets one plan build, the plan cache
 * stamp of that build, and a cache entry populated by that build all agree on one placement, even
 * when the configuration changes while the build runs.
 *
 * <p>Every test drives a private {@link ContextConfiguration}, so no test here touches a runtime
 * global and no isolation from other test classes is needed.
 */
public class PlanNullPlacementsTest {

  /**
   * A build reads one placement no matter how many sites ask for it. The test changes the
   * configuration in the middle of the open scope and asserts the second read still returns the
   * first pair, and that the recorded stamp is that same pair.
   */
  @Test
  public void openScopeSharesOneResolutionForTheWholeBuild() {
    var config = configWithAscending(OrderByNullsPlacement.FIRST);
    var placements = new PlanNullPlacements(() -> config);

    placements.open();
    try {
      var resolved = placements.resolve();
      assertEquals(OrderByNullsPlacement.FIRST, resolved.ascending());

      config.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);

      assertSame("one build must read one placement", resolved, placements.resolve());
      assertSame("the stamp must be the pair the build read", resolved, placements.recorded());
    } finally {
      placements.close();
    }

    assertNull("a closed scope records nothing", placements.recorded());
    assertEquals(
        "a later build reads the changed configuration",
        OrderByNullsPlacement.LAST,
        placements.resolve().ascending());
  }

  /**
   * A build that never asks for a placement records nothing, which is how the plan cache learns
   * that no sort clause can make the finished plan stale.
   */
  @Test
  public void buildThatReadsNoPlacementRecordsNothing() {
    var placements = new PlanNullPlacements(() -> configWithAscending(OrderByNullsPlacement.LAST));

    placements.open();
    try {
      assertNull("a build with no placement read must record nothing", placements.recorded());
    } finally {
      placements.close();
    }
  }

  /**
   * A nested build joins the open scope of its host instead of resolving its own value, and the
   * host keeps the recorded pair after the nested build closes. An embedded plan and the plan that
   * hosts it therefore carry one placement, and the host still publishes a stamp.
   */
  @Test
  public void nestedBuildJoinsTheOpenScope() {
    var config = configWithAscending(OrderByNullsPlacement.FIRST);
    var placements = new PlanNullPlacements(() -> config);

    placements.open();
    try {
      var host = placements.resolve();

      placements.open();
      try {
        config.setValue(
            GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
        assertSame("a nested build must join the open scope", host, placements.resolve());
      } finally {
        placements.close();
      }

      assertSame("the host keeps its stamp after the nested build", host, placements.recorded());
    } finally {
      placements.close();
    }

    assertNull("the outermost close drops the stamp", placements.recorded());
  }

  /**
   * A lifecycle reset clears both an unclosed scope and its memoized pair. A reused session must
   * therefore resolve the new borrower's current configuration instead of retaining stale state.
   */
  @Test
  public void resetDropsLeakedScopeAndResolvedPair() {
    var config = configWithAscending(OrderByNullsPlacement.FIRST);
    var placements = new PlanNullPlacements(() -> config);

    placements.open();
    assertEquals(OrderByNullsPlacement.FIRST, placements.resolve().ascending());

    config.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
    placements.reset();

    assertNull("a reset records no placement", placements.recorded());
    placements.open();
    try {
      assertEquals(
          "a fresh scope must resolve the current configuration",
          OrderByNullsPlacement.LAST,
          placements.resolve().ascending());
    } finally {
      placements.close();
    }
    assertNull("the fresh scope closes fully after reset", placements.recorded());
  }

  /** A read outside any scope resolves fresh and leaves nothing behind for a later build. */
  @Test
  public void readOutsideAnyScopeRemembersNothing() {
    var config = configWithAscending(OrderByNullsPlacement.LAST);
    var placements = new PlanNullPlacements(() -> config);

    assertEquals(OrderByNullsPlacement.LAST, placements.resolve().ascending());
    assertNull("a read outside a scope records nothing", placements.recorded());

    config.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.FIRST);
    assertEquals(
        "a read outside a scope must not be reused",
        OrderByNullsPlacement.FIRST,
        placements.resolve().ascending());
  }

  private static ContextConfiguration configWithAscending(OrderByNullsPlacement ascending) {
    var config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, ascending);
    return config;
  }
}
