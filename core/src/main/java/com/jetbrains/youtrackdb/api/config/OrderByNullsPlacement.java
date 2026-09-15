package com.jetbrains.youtrackdb.api.config;

/**
 * Placement of null sort keys when an {@code ORDER BY} item omits an explicit {@code NULLS FIRST}
 * or {@code NULLS LAST} clause.
 *
 * <p>The ascending and descending configuration keys each use this enum independently. An explicit
 * clause on an item always overrides the configured placement.
 *
 * @see GlobalConfiguration#QUERY_ORDER_BY_NULLS_PLACEMENT_ASC
 * @see GlobalConfiguration#QUERY_ORDER_BY_NULLS_PLACEMENT_DESC
 */
public enum OrderByNullsPlacement {
  FIRST, LAST
}
