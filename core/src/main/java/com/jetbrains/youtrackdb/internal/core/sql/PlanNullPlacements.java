package com.jetbrains.youtrackdb.internal.core.sql;

import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One null placement resolution shared by everything a single plan build reads.
 *
 * <p>Null placement means the position of the rows whose sort key is null, either first or last. A
 * plan build can freeze that position into a step, for example into an index scan that concatenates
 * its null bucket before or after the ordered keys. Two independent reads of the configuration could
 * straddle a change, and one plan would then carry two positions at once. A scope makes the whole
 * build read one resolved pair.
 *
 * <p>A planner opens a scope when it may publish a plan to the shared plan cache. The session cache
 * path also opens a scope around lookup and population. Each owner closes its scope in a finally
 * block. A nested build joins the open scope, so an embedded plan agrees with its host.
 *
 * <p>{@link #recorded()} answers what the enclosing scope resolved. The resolution can come from a
 * plan build or from the session cache gate around that build. A scope that never asked for a
 * placement records nothing. The plan cache stores that answer next to the plan and compares it
 * before serving the plan again.
 *
 * <p>One instance belongs to one session and is touched only by the thread that owns the session, so
 * no field is synchronised.
 */
public final class PlanNullPlacements {

  private final Supplier<ContextConfiguration> configuration;

  /** Number of plan builds currently open on this session, so a nested build shares the value. */
  private int openScopes;

  /** The pair the open scope resolved, or {@code null} when no scope has resolved one yet. */
  @Nullable private ResolvedOrderByNullsPlacement resolved;

  public PlanNullPlacements(@Nonnull Supplier<ContextConfiguration> configuration) {
    this.configuration = configuration;
  }

  /** Opens a plan-build scope. Every call must be paired with a {@link #close()} in a finally. */
  public void open() {
    openScopes++;
  }

  /** Closes a plan-build scope, dropping the resolved pair when the outermost scope ends. */
  public void close() {
    if (openScopes > 0) {
      openScopes--;
    }
    if (openScopes == 0) {
      resolved = null;
    }
  }

  /** Drops every open scope and resolved pair when the owning session ends or is reused. */
  public void reset() {
    openScopes = 0;
    resolved = null;
  }

  /**
   * Returns the placement pair of the current build, resolving it on the first call of the scope.
   *
   * <p>A call outside any scope resolves a fresh pair and remembers nothing, so a later build cannot
   * inherit it.
   */
  @Nonnull
  public ResolvedOrderByNullsPlacement resolve() {
    var current = resolved;
    if (current != null) {
      return current;
    }
    current = OrderByNullsUtil.resolvePlacements(configuration.get());
    if (openScopes > 0) {
      resolved = current;
    }
    return current;
  }

  /**
   * Returns the pair the current build resolved, or {@code null} when it resolved none. Call it
   * inside the scope, because the outermost close drops the value.
   */
  @Nullable public ResolvedOrderByNullsPlacement recorded() {
    return openScopes > 0 ? resolved : null;
  }
}
