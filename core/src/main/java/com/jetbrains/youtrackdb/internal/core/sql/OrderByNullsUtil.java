package com.jetbrains.youtrackdb.internal.core.sql;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;

/** Resolves null placement for {@code ORDER BY} and Gremlin {@code order()}. */
public final class OrderByNullsUtil {

  // Bound invalid-value reporting so arbitrary operator input cannot grow process memory forever.
  private static final int MAX_REPORTED_INVALID_VALUES = 256;
  private static final Set<String> REPORTED_INVALID_VALUES =
      Collections.synchronizedSet(new LinkedHashSet<>());

  private OrderByNullsUtil() {
  }

  /** Resolves placement for one sort key. Repeated comparisons must use a resolved pair instead. */
  public static boolean resolveNullsFirst(
      @Nullable String nullOrdering, boolean ascending, @Nullable ContextConfiguration config) {
    return composeNullsFirst(nullOrdering, ascending, resolvePlacements(config));
  }

  /** Composes an explicit clause and direction with placements resolved before comparison starts. */
  public static boolean composeNullsFirst(
      @Nullable String nullOrdering,
      boolean ascending,
      ResolvedOrderByNullsPlacement placements) {
    if (SQLOrderByItem.NULLS_FIRST.equals(nullOrdering)) {
      return true;
    }
    if (SQLOrderByItem.NULLS_LAST.equals(nullOrdering)) {
      return false;
    }
    return placements.forDirection(ascending) == OrderByNullsPlacement.FIRST;
  }

  /** Resolves both placements once for a whole sort or merge. */
  public static ResolvedOrderByNullsPlacement resolvePlacementsForSort(
      @Nullable CommandContext ctx) {
    var session = sessionOf(ctx);
    return resolvePlacements(session == null ? null : session.getConfiguration());
  }

  @Nullable private static DatabaseSessionEmbedded sessionOf(@Nullable CommandContext ctx) {
    if (ctx == null) {
      return null;
    }
    try {
      return ctx.getDatabaseSession();
    } catch (DatabaseException e) {
      return null;
    }
  }

  /**
   * Resolves both direction-specific keys. Precedence is database setting, readable server setting,
   * then shipped value. Each direction resolves independently.
   */
  public static ResolvedOrderByNullsPlacement resolvePlacements(
      @Nullable ContextConfiguration config) {
    return new ResolvedOrderByNullsPlacement(
        resolvePlacement(config, GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC),
        resolvePlacement(config, GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC));
  }

  private static OrderByNullsPlacement resolvePlacement(
      @Nullable ContextConfiguration config, GlobalConfiguration key) {
    if (config != null && config.getContextKeys().contains(key.getKey())) {
      var parsed = parse(config.getValue(key), key);
      if (parsed != null) {
        return parsed;
      }
    }
    var global = parse(key.getValue(), key);
    if (global != null) {
      return global;
    }
    return (OrderByNullsPlacement) key.getDefValue();
  }

  @Nullable private static OrderByNullsPlacement parse(
      @Nullable Object raw, GlobalConfiguration key) {
    if (raw == null) {
      return null;
    }
    if (raw instanceof OrderByNullsPlacement value) {
      return value;
    }
    var presentation = raw.toString();
    for (var constant : OrderByNullsPlacement.values()) {
      if (constant.name().equalsIgnoreCase(presentation)) {
        return constant;
      }
    }
    reportInvalid(presentation, key);
    return null;
  }

  private static void reportInvalid(String presentation, GlobalConfiguration key) {
    var reportKey = key.getKey() + "\u0000" + presentation.toUpperCase(Locale.ENGLISH);
    synchronized (REPORTED_INVALID_VALUES) {
      if (REPORTED_INVALID_VALUES.contains(reportKey)
          || REPORTED_INVALID_VALUES.size() >= MAX_REPORTED_INVALID_VALUES) {
        return;
      }
      REPORTED_INVALID_VALUES.add(reportKey);
    }
    LogManager.instance()
        .warn(
            OrderByNullsUtil.class,
            "Ignored the unreadable value '"
                + presentation
                + "' of the configuration key '"
                + key.getKey()
                + "'. The runtime global value applies when it is readable. Otherwise the shipped "
                + "value applies.");
  }
}
