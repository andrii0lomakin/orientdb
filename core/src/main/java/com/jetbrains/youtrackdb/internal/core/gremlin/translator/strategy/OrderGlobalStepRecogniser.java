package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.BoundaryOutputType;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.lambda.RecordIdSortKeyTraversal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.ByModulatorTranslator;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchProjectionBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.parser.ProjectionExpressionFactories;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import java.util.ArrayList;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;

/**
 * Recogniser for {@link OrderGlobalStep}: {@code order()} / {@code order().by(...)} → {@link
 * SQLOrderBy}. Bare {@code order()} and identity modulators sort by {@code @rid} on an element
 * boundary only; {@code Order.shuffle} declines.
 *
 * <h2>The record identifier mappings are gated by boundary output type</h2>
 *
 * Two modulators claim a record identifier that the boundary may not have: a trailing identity, and
 * the {@link RecordIdSortKeyTraversal} that {@code YTDBOrderRidTieBreakStrategy} appends. A boundary
 * whose payload is a map or a scalar aggregate has no record identifier of its own, so
 * {@code alias.@rid} would sort by the entity behind the row rather than by the row the caller
 * receives — which is not what the native comparator does with that payload. Both mappings therefore
 * refuse a {@link BoundaryOutputType#MAP} and a {@link BoundaryOutputType#SCALAR} boundary, and the
 * refusal declines the whole shape. A decline is safe: both arms then run the native pipeline and
 * agree by construction.
 *
 * <h2>An {@code order()} after a grouping terminator</h2>
 *
 * A grouping terminator ({@code group()} / {@code groupCount()}) emits one map, so a following
 * {@code order()} sorts a one-element stream and returns the map whole, whatever key the
 * {@code by()} names. Measured on four people, two of whom carry {@code age}:
 * {@code g.V().groupCount().by(name).order().by(age)} returns all four entries natively — and so
 * does {@code order().by(zzz)} for a key nothing in the graph carries. The translation read that
 * {@code by(key)} as a sort over the grouping's <em>input</em> instead — it committed
 * {@code key IS DEFINED} as a pattern conjunct on the rows feeding the {@code GROUP BY} and appended
 * {@code ORDER BY <alias>.key} — returning {@code {Alice=1, Bob=1}}, the two ageless names removed by
 * a filter the query never asked for.
 *
 * <p>Two mechanisms have to line up for the map to survive, and only one of them is the absent sort.
 * {@code OrderGlobalStep.processAllStarts} projects the modulator once per traverser <em>before</em>
 * any comparison, and drops a traverser whose projection is non-productive, so a projection is
 * observable on a one-element stream even though the comparator is not. It survives here because a
 * {@code by(key)} over a {@code Map} is productive whatever the map holds: {@code ValueTraversal}
 * reads {@code map.get(key)} and yields the result, including {@code null} for an absent key, where
 * its {@code Element} branch marks an absent property as having no starts. Over elements the drop is
 * real — {@code g.V().order().by(k)} does exclude the elements that lack {@code k} — which is why the
 * pattern conjunct the translation emitted looked equivalent.
 *
 * <p>So a captured {@code GROUP BY} declines here, matching the grouping gates in
 * {@code GremlinAggregateAssembler} and {@code RangeGlobalStepRecogniser}. The cost is the
 * {@code group().by(k).order().by(k2)} surface, which runs on the native traverser pipeline instead.
 */
final class OrderGlobalStepRecogniser implements StepRecogniser {

  /** Singleton — the recogniser is stateless and cheap to share across walker instances. */
  static final OrderGlobalStepRecogniser INSTANCE = new OrderGlobalStepRecogniser();

  private OrderGlobalStepRecogniser() {
    // Singleton — instantiate via INSTANCE.
  }

  @Override
  public Outcome recognize(StepCursor cursor, RecognitionContext ctx) {
    var step = cursor.take();
    if (!(step instanceof OrderGlobalStep<?, ?> orderStep)) {
      return Outcome.DECLINE;
    }
    // Post-union order needs an in-memory sort of the concatenation; not in this cut (count /
    // limit / dedup cover the push-down / early-stop post-concat set). The walker's post-union
    // allow-list already declines the traversal before this recogniser is dispatched, so in
    // production this branch is a second line of defence: it keeps a direct invocation honest and
    // makes re-adding order() to the allow-list a decline rather than a silent mistranslation.
    if (ctx.hasUnionCarrier()) {
      return Outcome.DECLINE;
    }
    var boundary = ctx.boundaryAlias();
    if (boundary == null) {
      return Outcome.DECLINE;
    }
    // A second order() has no clear MATCH composition rule in Phase 1.
    if (ctxHasOrderBy(ctx)) {
      return Outcome.DECLINE;
    }
    // An order() after a grouping terminator sorts the single map that terminator emitted, not the
    // rows that fed it — see the class Javadoc's "An order() after a grouping terminator". Checked
    // before the comparator loop so the declining path commits no presence conjunct.
    if (ctx.groupBy() != null) {
      return Outcome.DECLINE;
    }

    var comparators = orderStep.getComparators();
    if (comparators == null || comparators.isEmpty()) {
      return Outcome.DECLINE;
    }

    var items = new ArrayList<SQLOrderByItem>(comparators.size());
    for (var pair : comparators) {
      var direction = ByModulatorTranslator.parseSortDirection(pair.getValue1());
      if (direction.isEmpty()) {
        return Outcome.DECLINE;
      }
      var ascending = SQLOrderByItem.ASC.equals(direction.get());
      var item =
          resolveSortItem(ctx, boundary, pair.getValue0(), ascending, ctx::resolveUserLabel);
      if (item == null) {
        return Outcome.DECLINE;
      }
      var placement = ctx.orderByNullsPlacements().forDirection(ascending);
      item.setNullOrdering(
          placement == OrderByNullsPlacement.FIRST
              ? SQLOrderByItem.NULLS_FIRST
              : SQLOrderByItem.NULLS_LAST);
      items.add(item);
    }

    // Contribution — reached only after every comparator resolved, so a declining modulator leaves
    // the context unmutated.
    for (var pair : comparators) {
      requireModulatedPropertyForOrder(ctx, boundary, pair.getValue0());
    }
    // TinkerPop may migrate an upstream {@code as(...)} label onto the {@code order()} step (e.g.
    // {@code inV().as("friend").order().by(name)} arrives as {@code OrderGlobalStep@[friend]}).
    // Register it on the current boundary so a following {@code select("friend")} resolves.
    if (!ctx.bindStepLabels(orderStep, boundary)) {
      return Outcome.DECLINE;
    }
    ctx.setOrderBy(MatchProjectionBuilder.orderBy(items));
    return Outcome.ACCEPTED;
  }

  /**
   * Identity modulators (bare {@code order()} / {@code by(Order.asc)}) sort by the value the walk
   * is already projecting — the property behind a preceding {@code values(key)} if there is one,
   * the element RID otherwise. {@code g.V().values("name").order()} sorts names, not RIDs, and
   * before this distinction existed it emitted the six names in RID order and called it sorted.
   * Other shapes go through {@link ByModulatorTranslator}. Built as AST — no YQL-text round-trip.
   *
   * <p>Contract with {@code YTDBOrderRidTieBreakStrategy}: identity → {@code @rid} is valid only on
   * element boundaries, which {@link #boundaryCarriesRecordId} enforces. Map-entry / projected-map
   * order must not reach this branch as a stand-in for a group key — those shapes decline today;
   * when enabled they must map {@code Column.keys} (or entry identity) to the GROUP BY key, never to
   * {@code @rid}. That strategy replaces a trailing identity over elements with its record
   * identifier sort key, which {@link ByModulatorTranslator} resolves to the same {@code @rid} item
   * under the same gate.
   */
  private static SQLOrderByItem resolveSortItem(
      RecognitionContext ctx,
      String alias,
      Traversal.Admin<?, ?> modulator,
      boolean ascending,
      java.util.function.Function<String, String> labelResolver) {
    if (modulator instanceof IdentityTraversal) {
      var projection = ctx.lastPropertyProjection();
      if (projection != null) {
        return ProjectionExpressionFactories.orderByProperty(
            projection.alias(), projection.propertyKey(), ascending);
      }
      if (!boundaryCarriesRecordId(ctx)) {
        return null;
      }
      return ProjectionExpressionFactories.orderByRecordAttribute(alias, "@rid", ascending);
    }
    // The appended sort key means "the record identifier of this row", so it needs the same gate.
    // Falling back to the last property projection is wrong here: the strategy installs this key
    // only over an element stream, and a property key would be a different sort than native runs.
    if (modulator instanceof RecordIdSortKeyTraversal && !boundaryCarriesRecordId(ctx)) {
      return null;
    }
    return ByModulatorTranslator.translateOrderModulator(alias, modulator, ascending, labelResolver)
        .orElse(null);
  }

  /**
   * Whether the row the boundary hands back is one record, so {@code alias.@rid} identifies it. A
   * map payload and a scalar aggregate payload are built from the projection instead, and neither
   * has an identifier of its own — see the class Javadoc. A {@code null} output type means no step
   * has pinned a boundary yet, which the caller already refused, so it is treated as permissive
   * rather than as a fourth case.
   */
  private static boolean boundaryCarriesRecordId(RecognitionContext ctx) {
    var outputType = ctx.boundaryOutputType();
    return outputType != BoundaryOutputType.MAP && outputType != BoundaryOutputType.SCALAR;
  }

  /**
   * Emits the {@code key IS DEFINED} conjunct one sort comparator implies, if
   * {@link OrderKeyPresencePolicy} says the translator owns that drop. Every order-key presence
   * emission in the translator passes through this one call, so the setting that decides it is
   * read in one place rather than at each call site.
   */
  private static void requireModulatedPropertyForOrder(
      RecognitionContext ctx, String boundary, Traversal.Admin<?, ?> modulator) {
    if (!OrderKeyPresencePolicy.emitsPatternPresenceConjunct(ctx)) {
      return;
    }
    ByModulatorTranslator.orderModulatorPresenceTarget(boundary, modulator, ctx::resolveUserLabel)
        .ifPresent(
            target -> ByModulatorPresence.requireModulatedProperty(
                ctx, target.alias(), target.propertyKey()));
  }

  private static boolean ctxHasOrderBy(RecognitionContext ctx) {
    return ctx.orderBy() != null;
  }

  @Override
  public boolean contributeShape(Step<?, ?> step, GremlinShapeEncoder encoder) {
    if (!(step instanceof OrderGlobalStep<?, ?> orderStep)) {
      return false;
    }
    var comparators = orderStep.getComparators();
    encoder.appendToken("ord", Integer.toString(comparators == null ? 0 : comparators.size()));
    if (comparators != null) {
      for (var pair : comparators) {
        encoder.appendToken(String.valueOf(pair.getValue1()));
      }
    }
    return true;
  }
}
