package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBGraph;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBTransaction;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.ContradictoryOrderSemanticsException;
import com.jetbrains.youtrackdb.internal.core.sql.OrderByNullsUtil;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;

public final class YTDBStrategyUtil {

  private YTDBStrategyUtil() {
  }

  /// Reads one query option off the traversal the user configured.
  ///
  /// The option is looked up on the ROOT traversal rather than on {@code traversal} itself. A
  /// child traversal never carries the source's [OptionsStrategy] during the strategy pass: an
  /// anonymous child is built with the strategy list of TinkerPop's empty graph, and the parent
  /// list is copied onto it only when the parent locks, which happens after every strategy ran.
  /// Reading the child list would therefore answer `null` for every option on every nested step,
  /// so a documented per-traversal override would silently miss an order inside `union`,
  /// `choose` or any other child scope. {@code GremlinToMatchStrategy} records the same fact for
  /// its own veto marker.
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public static <T> @Nullable T getConfigValue(
      YTDBQueryConfigParam param, Admin<?, ?> traversal) {
    final var strategy =
        rootTraversal(traversal).getStrategies().getStrategy(OptionsStrategy.class).orElse(null);
    if (strategy == null) {
      return null;
    }
    return (T) strategy.getOptions().get(param.name());
  }

  /// Walks parent links to the outermost traversal. A root traversal reports [EmptyStep] as its
  /// parent, which ends the walk. The step count bound is a cycle guard: a malformed parent chain
  /// then yields the deepest traversal reached rather than hanging the compilation.
  private static Admin<?, ?> rootTraversal(Admin<?, ?> traversal) {
    var current = traversal;
    for (var guard = 0; guard < MAX_PARENT_DEPTH; guard++) {
      final var parent = current.getParent();
      if (parent == null || parent instanceof EmptyStep) {
        return current;
      }
      final var parentTraversal = parent.asStep().getTraversal();
      if (parentTraversal == null || parentTraversal == current) {
        return current;
      }
      current = parentTraversal;
    }
    return current;
  }

  /// Bound on the parent walk in [#rootTraversal]. Nesting deeper than this does not occur in a
  /// hand-written traversal, and the bound keeps a corrupt parent chain from looping forever.
  private static final int MAX_PARENT_DEPTH = 256;

  /// Resolves the YouTrackDB session backing {@code traversal}, or {@code null} when the traversal
  /// is not attached to a YTDB graph. Null-safe on non-YTDB graphs and TinkerPop's {@code
  /// EmptyGraph}: the {@code instanceof} gates decline before {@code tx()} is ever called, so a
  /// graph that does not support transactions never reaches the throwing call. Opens the
  /// transaction ({@code readWrite}) so callers can read session-scoped state.
  @Nullable public static DatabaseSessionEmbedded resolveYtdbSession(Admin<?, ?> traversal) {
    // The Graph and the Transaction from graph.tx() are borrowed from the traversal's long-lived
    // database graph, not opened here; closing them would tear the caller's graph down
    // mid-compilation, so the resource inspection is suppressed.
    @SuppressWarnings("resource")
    final var graph = traversal.getGraph().orElse(null);
    if (!(graph instanceof YTDBGraph)) {
      return null;
    }
    if (!(graph.tx() instanceof YTDBTransaction tx)) {
      return null;
    }
    tx.readWrite();
    return tx.getDatabaseSession();
  }

  /// Check whether the traversal should run as a polymorphic query. Returns {@code null} when the
  /// traversal has no attached YTDB graph (see {@link #resolveYtdbSession}) or its configuration
  /// cannot be resolved; otherwise the explicit {@code polymorphicQuery} option, or the {@code
  /// QUERY_GREMLIN_POLYMORPHIC_BY_DEFAULT} session default.
  @Nullable public static Boolean isPolymorphic(Admin<?, ?> traversal) {
    final var session = resolveYtdbSession(traversal);
    if (session == null) {
      return null;
    }

    final Boolean value = getConfigValue(YTDBQueryConfigParam.polymorphicQuery, traversal);
    if (value != null) {
      return value;
    }

    final var configuration = session.getConfiguration();
    if (configuration == null) {
      return null;
    }
    return configuration.getValueAsBoolean(
        GlobalConfiguration.QUERY_GREMLIN_POLYMORPHIC_BY_DEFAULT);
  }

  /// Resolves both null placements for one traversal. Each explicit per-traversal option overrides
  /// only its direction. The database setting, server setting, and shipped value then supply every
  /// direction without an explicit option.
  @Nullable public static ResolvedOrderByNullsPlacement orderByNullsPlacements(
      Admin<?, ?> traversal) {
    final var session = resolveYtdbSession(traversal);
    if (session == null || session.getConfiguration() == null) {
      return null;
    }

    final var configured = OrderByNullsUtil.resolvePlacements(session.getConfiguration());
    final var ascending =
        queryNullPlacement(YTDBQueryConfigParam.orderByNullsPlacementAsc, traversal);
    final var descending =
        queryNullPlacement(YTDBQueryConfigParam.orderByNullsPlacementDesc, traversal);
    return new ResolvedOrderByNullsPlacement(
        ascending == null ? configured.ascending() : ascending,
        descending == null ? configured.descending() : descending);
  }

  private static @Nullable OrderByNullsPlacement queryNullPlacement(
      YTDBQueryConfigParam param, Admin<?, ?> traversal) {
    final Object raw = getConfigValue(param, traversal);
    if (raw == null) {
      return null;
    }
    if (raw instanceof OrderByNullsPlacement placement) {
      return placement;
    }
    if (raw instanceof String presentation) {
      for (var placement : OrderByNullsPlacement.values()) {
        if (placement.name().equalsIgnoreCase(presentation)) {
          return placement;
        }
      }
    }
    throw new IllegalArgumentException(
        "Invalid value '" + raw + "' for parameter '" + param.name()
            + "'. Accepted values: FIRST, LAST.");
  }

  /// Resolves whether a global-scope `order()` step keeps a record without the ordered property.
  /// The explicit per-traversal option takes precedence over the database setting. The
  /// user-supplied standard-order strategy takes precedence only when the explicit option does not
  /// select record retention. The sole contradiction is an explicit
  /// `orderIncludesMissingKey=true` option with the standard-order strategy. An unresolved setting
  /// keeps records unless the strategy is present.
  public static boolean orderIncludesMissingKey(Admin<?, ?> traversal) {
    final Boolean explicit =
        getConfigValue(YTDBQueryConfigParam.orderIncludesMissingKey, traversal);
    if (Boolean.FALSE.equals(explicit)) {
      return false;
    }

    if (explicit == null) {
      final var session = resolveYtdbSession(traversal);
      if (session != null && session.getConfiguration() != null
          && !session.getConfiguration().getValueAsBoolean(
              GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY)) {
        return false;
      }
    }

    if (!hasStandardOrderSemanticsStrategy(traversal)) {
      return true;
    }
    if (Boolean.TRUE.equals(explicit)) {
      throw new ContradictoryOrderSemanticsException(
          "StandardOrderSemanticsStrategy conflicts with the explicit "
              + "orderIncludesMissingKey=true option. Remove the strategy with "
              + "withoutStrategies(StandardOrderSemanticsStrategy.class).");
    }
    return false;
  }

  /// Reports whether the root traversal carries the user strategy for standard order semantics.
  public static boolean hasStandardOrderSemanticsStrategy(Admin<?, ?> traversal) {
    return rootTraversal(traversal).getStrategies()
        .getStrategy(StandardOrderSemanticsStrategy.class).isPresent();
  }
}
