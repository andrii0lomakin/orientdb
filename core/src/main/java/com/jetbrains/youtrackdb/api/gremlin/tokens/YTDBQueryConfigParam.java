package com.jetbrains.youtrackdb.api.gremlin.tokens;

import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;

/// YTDB-specific parameters that can be passed to
/// [[com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSourceDSL#with(YTDBQueryConfigParam, Object)]] and
/// [[com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSourceDSL#with(YTDBQueryConfigParam)]]
/// methods to configure query behavior.
public enum YTDBQueryConfigParam {

  /// Controls whether the query is polymorphic, i.e., subclasses can be queried by their parent
  /// classes' names.
  polymorphicQuery(Boolean.class),

  /// Controls whether a global-scope `order()` step keeps a record that does not carry the
  /// ordered property. `true` orders such a record as a null key, the way YQL `ORDER BY` does.
  /// `false` restores standard order semantics behaviour, where the by-modulator produces nothing and
  /// the record is dropped. Overrides
  /// [[com.jetbrains.youtrackdb.api.config.GlobalConfiguration#QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY]]
  /// for one traversal. Local-scope order and the `select`, `values`, `group` and `dedup`
  /// modulators keep their filtering behaviour under either value.
  orderIncludesMissingKey(Boolean.class),

  /// Overrides the ascending null placement for one traversal. Stored as a string so the option is
  /// portable through the default remote serializers.
  orderByNullsPlacementAsc(String.class),

  /// Overrides the descending null placement for one traversal. Stored as a string so the option is
  /// portable through the default remote serializers.
  orderByNullsPlacementDesc(String.class),

  /// Client-provided query summary for query monitoring purposes.
  querySummary(String.class);

  private final Class<?> type;

  YTDBQueryConfigParam(Class<?> type) {
    this.type = type;
  }

  public Class<?> type() {
    return type;
  }

  /// Converts embedded enum use to the portable string representation. Other parameters retain
  /// their declared representation.
  public Object normalizeValue(Object value) {
    if (isNullPlacement() && value instanceof OrderByNullsPlacement placement) {
      return placement.name();
    }
    return value;
  }

  public boolean accepts(Object value) {
    var normalized = normalizeValue(value);
    if (!type.isInstance(normalized)) {
      return false;
    }
    if (!isNullPlacement()) {
      return true;
    }
    for (var placement : OrderByNullsPlacement.values()) {
      if (placement.name().equalsIgnoreCase((String) normalized)) {
        return true;
      }
    }
    return false;
  }

  public String validationDescription() {
    return isNullPlacement() ? "Accepted values: FIRST, LAST."
        : "Expected " + type.getSimpleName() + ".";
  }

  private boolean isNullPlacement() {
    return this == orderByNullsPlacementAsc || this == orderByNullsPlacementDesc;
  }
}
