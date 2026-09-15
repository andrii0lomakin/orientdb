package com.jetbrains.youtrackdb.internal.core.sql;

import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import javax.annotation.Nonnull;

/** Null placements resolved once for both directions at the start of a sort. */
public record ResolvedOrderByNullsPlacement(
    @Nonnull OrderByNullsPlacement ascending,
    @Nonnull OrderByNullsPlacement descending) {

  public static final ResolvedOrderByNullsPlacement SHIPPED =
      new ResolvedOrderByNullsPlacement(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);
  public static final ResolvedOrderByNullsPlacement REVERSED =
      new ResolvedOrderByNullsPlacement(OrderByNullsPlacement.LAST, OrderByNullsPlacement.FIRST);

  public ResolvedOrderByNullsPlacement {
    if (ascending == null || descending == null) {
      throw new IllegalArgumentException("Null placements must be present");
    }
  }

  /** Returns the configured placement for the requested direction. */
  public OrderByNullsPlacement forDirection(boolean ascendingDirection) {
    return ascendingDirection ? ascending : descending;
  }
}
