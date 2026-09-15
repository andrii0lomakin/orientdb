package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.lambda.RecordIdSortKeyTraversal;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;

/**
 * Cheap pre-walk extraction of the translation-cache key and this invocation's {@code ?} bindings.
 * Dispatches each step to the same class-keyed recogniser registry the walker uses: the recogniser
 * that would translate the step is the one that lists the tokens it reads. An unregistered step is
 * encoded by class name and labels only (the walker declines it). A recogniser that returns
 * {@code false} from {@link StepRecogniser#contributeShape}, or a lambda modulator the extractor
 * cannot name, marks the extraction incomplete so {@code apply} will not cache a {@code Translate}.
 *
 * <p>The key opens with the strategy-flag section. It carries every resolved setting that changes
 * the emitted plan for one step sequence. The section includes polymorphism ({@code poly}), edge
 * label verification ({@code elv}), and the productive-order setting ({@code oim}). It includes
 * resolved null placements ({@code onp}) only when a global order step can embed them. It also
 * includes upstream {@code ProductiveByStrategy}'s productive keys ({@code pb}).
 *
 * <p>Lambda {@code by()} modulators ({@link ValueTraversal}, {@link TokenTraversal}, {@link
 * IdentityTraversal}, {@link RecordIdSortKeyTraversal}) have an empty step list; their property key
 * / token lives on the traversal itself. Encoding them here, once, covers {@code order}/{@code group}/{@code project}/{@code
 * select} without each recogniser re-implementing the split.
 */
final class GremlinShapeExtractor {

  private final Map<Class<?>, StepRecogniser> recognisers;

  private final Set<Class<?>> transparentSteps;

  private final GremlinShapeEncoder encoder;

  private GremlinShapeExtractor(
      Map<Class<?>, StepRecogniser> recognisers,
      Set<Class<?>> transparentSteps,
      GremlinShapeEncoder encoder) {
    this.recognisers = recognisers;
    this.transparentSteps = transparentSteps;
    this.encoder = encoder;
  }

  /**
   * @param orderIncludesMissingKey the productive-order setting ALREADY RESOLVED for this
   *     compilation, or {@code null} when the caller has none. The value is passed in rather than
   *     resolved here so the key and the plan built beside it read one and the same answer. A
   *     second read could see a runtime flip and file the plan under the other setting's key, in
   *     a cache that is storage-wide and outlives the session.
   */
  static Extraction extract(
      @Nonnull Map<Class<?>, StepRecogniser> recognisers,
      @Nonnull Set<Class<?>> transparentSteps,
      @Nonnull Traversal.Admin<?, ?> traversal,
      @Nonnull DatabaseSessionEmbedded session,
      @Nullable Boolean orderIncludesMissingKey,
      @Nonnull ResolvedOrderByNullsPlacement orderByNullsPlacements) {
    var extractor =
        new GremlinShapeExtractor(
            recognisers, transparentSteps, new GremlinShapeEncoder(session.getSchema()));
    extractor.appendStrategyFlags(
        traversal, orderIncludesMissingKey, orderByNullsPlacements);
    extractor.visit(traversal);
    return new Extraction(extractor.encoder.key(), extractor.encoder.bindings(),
        extractor.encoder.complete());
  }

  record Extraction(@Nonnull String key, @Nonnull Map<Object, Object> bindings, boolean complete) {
  }

  private void appendStrategyFlags(
      Traversal.Admin<?, ?> traversal,
      @Nullable Boolean orderIncludesMissingKey,
      ResolvedOrderByNullsPlacement orderByNullsPlacements) {
    Boolean polymorphic = YTDBStrategyUtil.isPolymorphic(traversal);
    encoder.appendToken("poly", polymorphic == null ? "n" : (polymorphic ? "1" : "0"));
    encoder.appendToken(
        "elv",
        traversal.getStrategies().getStrategy(EdgeLabelVerificationStrategy.class).isPresent()
            ? "1"
            : "0");
    var productiveKeys =
        traversal
            .getStrategies()
            .getStrategy(ProductiveByStrategy.class)
            .map(ProductiveByStrategy::getProductiveKeys)
            .orElse(null);
    // The resolved productive-order setting changes the emitted pattern: under the shipped default
    // the order-key IS DEFINED conjunct is omitted, under the opt-out it is emitted. The cache is
    // storage-wide, so without this token a plan built under one setting would be spliced verbatim
    // into a traversal running under the other, in another session.
    encoder.appendToken(
        "oim",
        orderIncludesMissingKey == null ? "n" : (orderIncludesMissingKey ? "1" : "0"));
    // Only a global order step can embed these values in a translated plan. Search every child
    // because union arms and other nested traversals are encoded into the same shape key.
    if (containsGlobalOrder(traversal)) {
      encoder.appendToken(
          "onp",
          orderByNullsPlacements.ascending().name()
              + "/"
              + orderByNullsPlacements.descending().name());
    }
    if (productiveKeys == null) {
      encoder.appendToken("pb", "-");
    } else {
      encoder.appendToken("pb", Integer.toString(productiveKeys.size()));
      for (String key : new TreeSet<>(productiveKeys)) {
        encoder.appendToken(key);
      }
    }
  }

  private static boolean containsGlobalOrder(Traversal.Admin<?, ?> traversal) {
    for (Step<?, ?> step : traversal.getSteps()) {
      if (step instanceof OrderGlobalStep) {
        return true;
      }
      if (step instanceof TraversalParent parent) {
        for (var child : parent.getLocalChildren()) {
          if (containsGlobalOrder(child.asAdmin())) {
            return true;
          }
        }
        for (var child : parent.getGlobalChildren()) {
          if (containsGlobalOrder(child.asAdmin())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private void visit(Traversal.Admin<?, ?> traversal) {
    if (encodeLambda(traversal)) {
      return;
    }
    int counted = 0;
    for (Step<?, ?> step : traversal.getSteps()) {
      if (!transparentSteps.contains(step.getClass())) {
        counted++;
      }
    }
    encoder.appendToken("T", Integer.toString(counted));
    for (Step<?, ?> step : traversal.getSteps()) {
      if (transparentSteps.contains(step.getClass())) {
        continue;
      }
      encoder.appendToken("S", step.getClass().getName());
      var labels = GremlinStepLabels.userLabels(step);
      encoder.appendStringSeq("L", labels);
      var recogniser = recognisers.get(step.getClass());
      if (recogniser != null && !recogniser.contributeShape(step, encoder)) {
        encoder.markIncomplete();
      }
      if (step instanceof TraversalParent parent) {
        for (var child : parent.getLocalChildren()) {
          visit(child.asAdmin());
        }
        for (var child : parent.getGlobalChildren()) {
          visit(child.asAdmin());
        }
      }
    }
  }

  /**
   * {@code true} when {@code traversal} is a lambda (no step list) and has been encoded as such.
   * Unknown lambdas mark the extraction incomplete.
   */
  private boolean encodeLambda(Traversal.Admin<?, ?> traversal) {
    if (traversal instanceof ValueTraversal<?, ?> valueTraversal) {
      encoder.appendToken("vt", String.valueOf(valueTraversal.getPropertyKey()));
      return true;
    }
    if (traversal instanceof TokenTraversal<?, ?> tokenTraversal) {
      var token = tokenTraversal.getToken();
      encoder.appendToken("tt", token == null ? "-" : token.getAccessor());
      return true;
    }
    if (traversal instanceof IdentityTraversal) {
      encoder.appendToken("idtr", "1");
      return true;
    }
    // The appended record identifier sort key. Carrying its own token keeps a translated order()
    // shape cacheable; without it the lambda fallback below would mark every one incomplete.
    if (traversal instanceof RecordIdSortKeyTraversal) {
      encoder.appendToken("ridsk", "1");
      return true;
    }
    if (traversal.getSteps().isEmpty()) {
      encoder.appendToken("lambda", traversal.getClass().getName());
      encoder.markIncomplete();
      return true;
    }
    return false;
  }
}
