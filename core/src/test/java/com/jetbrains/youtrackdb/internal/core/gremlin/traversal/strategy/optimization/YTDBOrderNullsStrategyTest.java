package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.GraphBaseTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBTransaction;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.YTDBMatchPlanStep;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.GremlinToMatchStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * {@link YTDBOrderNullsStrategy} applies both direction-specific placement settings to native
 * Gremlin {@code order()}. Reversed placements require wrapped comparators because TinkerPop's
 * comparators already match the shipped placements.
 *
 * <p>Marked {@code @Category(SequentialTest)} because it mutates the process-wide
 * placement globals. The default surefire execution runs four test classes in parallel in one
 * virtual machine, so the mutation would leak between classes.
 */
@Category(SequentialTest.class)
public class YTDBOrderNullsStrategyTest extends GraphBaseTest {

  private Object previousAscending;
  private Object previousDescending;

  @Before
  public void saveGlobals() {
    previousAscending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    previousDescending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.getValue();
  }

  /** The null strategy runs after translation and standard-order missing-key handling. */
  @Test
  public void applyPriorWaitsForTranslatorAndStandardOrderSemantics() {
    assertThat(YTDBOrderNullsStrategy.instance().applyPrior())
        .containsExactlyInAnyOrder(
            GremlinToMatchStrategy.class,
            YTDBOrderRidTieBreakStrategy.class,
            YTDBStandardOrderSemanticsStrategy.class);
  }

  @After
  public void restoreConfiguration() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(previousDescending);
    var tx = (YTDBTransaction) graph.tx();
    tx.readWrite();
    tx.getDatabaseSession()
        .getConfiguration()
        .setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, null);
    tx.getDatabaseSession()
        .getConfiguration()
        .setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, null);
  }

  /**
   * Storage-local reversed placement puts null sort keys last for ascending native {@code
   * order().by(age)}. Record-retention mode keeps vertices without {@code age} in the sort stream.
   */
  @Test
  public void storageReversedPlacementPutsNullAgeVerticesLastAscending() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 25);
    graph.addVertex(T.label, "Person", "name", "Nobody");
    graph.addVertex(T.label, "Person", "name", "Nemo");
    graph.tx().commit();

    setStorageReversedPlacement();

    var names = graph.traversal().V().order().by("age").values("name").toList();

    assertThat(names.subList(0, 2)).containsExactly("Bob", "Alice");
    assertThat(names.subList(2, 4)).containsExactlyInAnyOrder("Nobody", "Nemo");
  }

  /**
   * Storage-local reversed placement puts null sort keys first for descending native {@code
   * order().by(age, desc)}.
   */
  @Test
  public void storageReversedPlacementPutsNullAgeVerticesFirstDescending() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 25);
    graph.addVertex(T.label, "Person", "name", "Nobody");
    graph.addVertex(T.label, "Person", "name", "Nemo");
    graph.tx().commit();

    setStorageReversedPlacement();

    var names = graph.traversal().V().order().by("age", Order.desc).values("name").toList();

    assertThat(names.subList(0, 2)).containsExactlyInAnyOrder("Nobody", "Nemo");
    assertThat(names.subList(2, 4)).containsExactly("Alice", "Bob");
  }

  /**
   * Direct strategy application preserves standard-order filtering when reversed placement
   * rebuilds a native global order step.
   */
  @Test
  public void applyPreservesMissingKeyFilteringOnRebuiltGlobalOrder() {
    graph.addVertex(T.label, "Person", "age", 30);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin =
        graph
            .traversal()
            .with(YTDBQueryConfigParam.orderIncludesMissingKey, false)
            .V()
            .order()
            .by("age")
            .asAdmin();
    YTDBStandardOrderSemanticsStrategy.instance().apply(admin);
    var orderBefore =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    orderBefore.setLimit(3);
    orderBefore.addLabel("ordered");
    assertThat(orderBefore.isFilteringUnproductiveTraversers()).isTrue();

    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderAfter =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    assertThat(orderAfter).isNotSameAs(orderBefore);
    assertThat(orderAfter.isFilteringUnproductiveTraversers()).isTrue();
    assertThat(orderAfter.getLimit()).isEqualTo(3);
    assertThat(orderAfter.getLabels()).containsExactly("ordered");
  }

  /** A rebuilt local order preserves its labels. */
  @Test
  public void applyPreservesLabelsOnRebuiltLocalOrder() {
    setStorageReversedPlacement();
    var admin = graph.traversal().inject(List.of(2, 1)).order(Scope.local).asAdmin();
    var orderBefore =
        TraversalHelper.getStepsOfAssignableClass(OrderLocalStep.class, admin).getFirst();
    orderBefore.addLabel("ordered");

    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderAfter =
        TraversalHelper.getStepsOfAssignableClass(OrderLocalStep.class, admin).getFirst();
    assertThat(orderAfter).isNotSameAs(orderBefore);
    assertThat(orderAfter.getLabels()).containsExactly("ordered");
  }

  /**
   * With the shipped {@code FIRST}, {@link YTDBOrderNullsStrategy#apply} returns before rebuilding
   * any order step.
   */
  @Test
  public void applyIsNoOpUnderShippedPlacement() {
    setGlobalPlacement(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    var tx = (YTDBTransaction) graph.tx();
    tx.readWrite();

    var admin = graph.traversal().V().order().by("age").asAdmin();
    var orderBefore =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();

    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderAfter =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    assertThat(orderAfter).isSameAs(orderBefore);
    assertThat(comparatorAt(orderAfter, 0)).isSameAs(Order.asc);
  }

  /** Each typed per-query option overrides only its direction. */
  @Test
  public void perQueryOverridesEachDirectionIndependently() {
    setGlobalPlacement(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);

    var ascendingAdmin =
        graph
            .traversal()
            .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, OrderByNullsPlacement.LAST)
            .inject(1)
            .order()
            .by(Order.asc)
            .by(Order.desc)
            .asAdmin();
    YTDBOrderNullsStrategy.instance().apply(ascendingAdmin);
    var ascendingStep =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, ascendingAdmin).getFirst();
    assertThat(comparatorAt(ascendingStep, 0)).isNotSameAs(Order.asc);
    assertThat(comparatorAt(ascendingStep, 1)).isSameAs(Order.desc);

    var descendingAdmin =
        graph
            .traversal()
            .with(YTDBQueryConfigParam.orderByNullsPlacementDesc, OrderByNullsPlacement.FIRST)
            .inject(1)
            .order()
            .by(Order.asc)
            .by(Order.desc)
            .asAdmin();
    YTDBOrderNullsStrategy.instance().apply(descendingAdmin);
    var descendingStep =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, descendingAdmin)
            .getFirst();
    assertThat(comparatorAt(descendingStep, 0)).isSameAs(Order.asc);
    assertThat(comparatorAt(descendingStep, 1)).isNotSameAs(Order.desc);
  }

  /** Per-query options beat every combination of the two server settings. */
  @Test
  public void perQueryOverridesAllFourGlobalCombinations() {
    for (var globalAscending : OrderByNullsPlacement.values()) {
      for (var globalDescending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(globalAscending, globalDescending);
        var admin =
            graph
                .traversal()
                .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, OrderByNullsPlacement.LAST)
                .with(YTDBQueryConfigParam.orderByNullsPlacementDesc, OrderByNullsPlacement.FIRST)
                .inject(1)
                .order()
                .by(Order.asc)
                .by(Order.desc)
                .asAdmin();

        YTDBOrderNullsStrategy.instance().apply(admin);

        var orderStep =
            TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
        assertThat(comparatorAt(orderStep, 0)).isNotSameAs(Order.asc);
        assertThat(comparatorAt(orderStep, 1)).isNotSameAs(Order.desc);
      }
    }
  }

  /** A local collection sort places a real null according to its per-query option. */
  @Test
  public void perQueryAscendingOverridePlacesNullLastInsideCollection() {
    var values =
        graph
            .traversal()
            .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, OrderByNullsPlacement.LAST)
            .inject(Arrays.asList(null, 2, 1))
            .order(Scope.local)
            .next();

    assertThat(values).containsExactly(1, 2, null);
  }

  /** Portable string options parse case-insensitively for native execution. */
  @Test
  public void perQueryPlacementParsesPortableStringIgnoringCase() {
    var values =
        graph
            .traversal()
            .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, "lAsT")
            .inject(Arrays.asList(null, 2, 1))
            .order(Scope.local)
            .next();

    assertThat(values).containsExactly(1, 2, null);
  }

  /** Embedded enum input is normalized to a GraphBinary-portable string option. */
  @Test
  public void perQueryPlacementEnumIsStoredAsPortableString() {
    var source =
        graph
            .traversal()
            .with(
                YTDBQueryConfigParam.orderByNullsPlacementAsc,
                OrderByNullsPlacement.LAST);
    var options =
        source.getStrategies().getStrategy(OptionsStrategy.class).orElseThrow().getOptions();

    assertThat(YTDBQueryConfigParam.orderByNullsPlacementAsc.type()).isEqualTo(String.class);
    assertThat(options.get(YTDBQueryConfigParam.orderByNullsPlacementAsc.name()))
        .isEqualTo("LAST");
  }

  /** Typed malformed strings fail when the option is set. */
  @Test
  public void perQueryPlacementRejectsMalformedTypedString() {
    assertThatThrownBy(
        () -> graph
            .traversal()
            .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, "MIDDLE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(YTDBQueryConfigParam.orderByNullsPlacementAsc.name())
        .hasMessageContaining("FIRST, LAST");
  }

  /** Untyped malformed strings still fail clearly during strategy application. */
  @Test
  public void perQueryPlacementRejectsMalformedUntypedString() {
    assertMalformedUntypedPlacement("MIDDLE");
  }

  /** Untyped values of the wrong type still fail clearly during strategy application. */
  @Test
  public void perQueryPlacementRejectsWrongUntypedType() {
    assertMalformedUntypedPlacement(17);
  }

  /**
   * Direct {@code apply} on a native {@code order()} traversal rebuilds framework comparators when
   * storage uses {@code LAST}. End-to-end runs often translate {@code order().by(...)} to
   * MATCH, so this pins the strategy body itself.
   */
  @Test
  public void applyWrapsNativeGlobalOrderUnderReversedPlacement() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin = graph.traversal().V().order().by("age").by("name", Order.desc).asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderStep =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    var ascWrapped = comparatorAt(orderStep, 0);
    assertThat(ascWrapped).isNotSameAs(Order.asc);
    assertThat(ascWrapped.compare(null, 1)).isPositive();
    assertThat(ascWrapped.compare(null, null)).isZero();
    assertThat(ascWrapped.compare(1, 2)).isNegative();

    var descWrapped = comparatorAt(orderStep, 1);
    assertThat(descWrapped).isNotSameAs(Order.desc);
    assertThat(descWrapped.compare(null, "a")).isNegative();
  }

  /**
   * Bare {@code order()} synthesizes identity+asc. Under {@code LAST} that asc comparator
   * is wrapped the same way as an explicit {@code by(..., asc)}.
   */
  @Test
  public void applyWrapsBareOrderUnderReversedPlacement() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin = graph.traversal().V().order().asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderStep =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    var wrapped = comparatorAt(orderStep, 0);
    assertThat(wrapped).isNotSameAs(Order.asc);
    assertThat(wrapped.compare(null, 1)).isPositive();
  }

  /**
   * {@code order(Scope.local)} uses {@link OrderLocalStep}. Under {@code LAST} its
   * framework comparators are rebuilt the same way as the global step.
   */
  @Test
  public void applyWrapsLocalOrderUnderReversedPlacement() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin = graph.traversal().V().fold().order(Scope.local).by("age").asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderStep =
        TraversalHelper.getStepsOfAssignableClass(OrderLocalStep.class, admin).getFirst();
    var wrapped = comparatorAt(orderStep, 0);
    assertThat(wrapped).isNotSameAs(Order.asc);
    assertThat(wrapped.compare(null, 1)).isPositive();
  }

  /**
   * The strategy does not walk nested traversals. Applying it to the parent leaves a child {@code
   * order()} untouched; the framework visits that child on its own pass.
   */
  @Test
  public void applyDoesNotRecurseIntoNestedOrder() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin = graph.traversal().V().map(__.order().by("age")).asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);

    var nestedOrder =
        TraversalHelper.getStepsOfAssignableClassRecursively(OrderGlobalStep.class, admin)
            .getFirst();
    assertThat(comparatorAt(nestedOrder, 0)).isSameAs(Order.asc);
  }

  /**
   * {@link Order#shuffle} is left alone. A step that mixes shuffle with asc wraps only the asc
   * comparator.
   */
  @Test
  public void applySkipsShuffleComparator() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin = graph.traversal().V().order().by(Order.shuffle).by("age").asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderStep =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    assertThat(comparatorAt(orderStep, 0)).isSameAs(Order.shuffle);
    assertThat(comparatorAt(orderStep, 1)).isNotSameAs(Order.asc);
  }

  /**
   * A second {@code apply} does not wrap an already-wrapped comparator again. Only framework {@code
   * asc}/{@code desc} constants are replaced.
   */
  @Test
  public void applyIsIdempotentUnderReversedPlacement() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    var admin = graph.traversal().V().order().by("age").asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);
    var firstWrap =
        comparatorAt(
            TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst(), 0);

    YTDBOrderNullsStrategy.instance().apply(admin);
    var second =
        comparatorAt(
            TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst(), 0);
    assertThat(second).isSameAs(firstWrap);
  }

  /** Caller-supplied comparators keep their own null handling and are not replaced. */
  @Test
  public void applyLeavesCallerComparatorAlone() {
    graph.addVertex(T.label, "Person", "age", 1);
    graph.tx().commit();

    setStorageReversedPlacement();

    Comparator<Integer> caller = Integer::compareTo;
    var admin = graph.traversal().V().order().by("age", caller).asAdmin();
    YTDBOrderNullsStrategy.instance().apply(admin);

    var orderStep =
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, admin).getFirst();
    assertThat(comparatorAt(orderStep, 0)).isSameAs(caller);
  }

  /** Native record sorting executes every global placement and reachable null shape. */
  @Test
  public void nativeRecordSortCoversGlobalPlacementMatrix() {
    seedNullShapeVertices();
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(ascending, descending);
        for (var shape : NullShape.values()) {
          assertThat(nativeRecordOrder(shape, Order.asc, null, null))
              .containsExactlyElementsOf(expected(Order.asc, ascending));
          assertThat(nativeRecordOrder(shape, Order.desc, null, null))
              .containsExactlyElementsOf(expected(Order.desc, descending));
        }
      }
    }
  }

  /** Local collection sorting executes every global placement and reachable null shape. */
  @Test
  public void localCollectionSortCoversGlobalPlacementMatrix() {
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(ascending, descending);
        for (var shape : NullShape.values()) {
          assertThat(localCollectionOrder(shape, Order.asc, null, null))
              .containsExactlyElementsOf(expected(Order.asc, ascending));
          assertThat(localCollectionOrder(shape, Order.desc, null, null))
              .containsExactlyElementsOf(expected(Order.desc, descending));
        }
      }
    }
  }

  /** Converted MATCH sorting executes every global placement for property-backed null shapes. */
  @Test
  public void convertedSortCoversGlobalPlacementMatrix() {
    seedNullShapeVertices();
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(ascending, descending);
        for (var shape : List.of(NullShape.ABSENT, NullShape.STORED)) {
          assertThat(convertedRecordOrder(shape, Order.asc, null, null))
              .containsExactlyElementsOf(expected(Order.asc, ascending));
          assertThat(convertedRecordOrder(shape, Order.desc, null, null))
              .containsExactlyElementsOf(expected(Order.desc, descending));
        }
      }
    }
  }

  /** Both query parameters beat every global pair for all native record null shapes. */
  @Test
  public void nativeRecordSortCoversBothQueryOverrides() {
    seedNullShapeVertices();
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(ascending, descending);
        for (var shape : NullShape.values()) {
          assertThat(
              nativeRecordOrder(
                  shape, Order.asc, YTDBQueryConfigParam.orderByNullsPlacementAsc, "last"))
              .containsExactly("low", "high", "null");
          assertThat(
              nativeRecordOrder(
                  shape, Order.desc, YTDBQueryConfigParam.orderByNullsPlacementDesc, "First"))
              .containsExactly("null", "high", "low");
        }
      }
    }
  }

  /** Both query parameters beat every global pair for all local collection null shapes. */
  @Test
  public void localCollectionSortCoversBothQueryOverrides() {
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(ascending, descending);
        for (var shape : NullShape.values()) {
          assertThat(
              localCollectionOrder(
                  shape, Order.asc, YTDBQueryConfigParam.orderByNullsPlacementAsc, "last"))
              .containsExactly("low", "high", "null");
          assertThat(
              localCollectionOrder(
                  shape, Order.desc, YTDBQueryConfigParam.orderByNullsPlacementDesc, "First"))
              .containsExactly("null", "high", "low");
        }
      }
    }
  }

  /** Both query parameters beat every global pair for property-backed MATCH null shapes. */
  @Test
  public void convertedSortCoversBothQueryOverrides() {
    seedNullShapeVertices();
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalPlacement(ascending, descending);
        for (var shape : List.of(NullShape.ABSENT, NullShape.STORED)) {
          assertThat(
              convertedRecordOrder(
                  shape, Order.asc, YTDBQueryConfigParam.orderByNullsPlacementAsc, "last"))
              .containsExactly("low", "high", "null");
          assertThat(
              convertedRecordOrder(
                  shape, Order.desc, YTDBQueryConfigParam.orderByNullsPlacementDesc, "First"))
              .containsExactly("null", "high", "low");
        }
      }
    }
  }

  /** Expression-null ordering declines MATCH conversion and executes natively under both placements. */
  @Test
  public void expressionNullOrderDeclinesMatchAndExecutesNativelyUnderBothPlacements() {
    seedNullShapeVertices();
    for (var ascending : OrderByNullsPlacement.values()) {
      setGlobalPlacement(ascending, OrderByNullsPlacement.LAST);
      var admin = recordTraversal(graph.traversal(), NullShape.EXPRESSION, Order.asc).asAdmin();

      admin.applyStrategies();

      assertThat(TraversalHelper.getStepsOfAssignableClass(YTDBMatchPlanStep.class, admin))
          .isEmpty();
      assertThat(admin.toList()).containsExactlyElementsOf(expected(Order.asc, ascending));
    }
  }

  private void assertMalformedUntypedPlacement(Object value) {
    assertThatThrownBy(
        () -> graph
            .traversal()
            .with(YTDBQueryConfigParam.orderByNullsPlacementAsc.name(), value)
            .V()
            .order()
            .by("age")
            .toList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(YTDBQueryConfigParam.orderByNullsPlacementAsc.name())
        .hasMessageContaining("FIRST, LAST");
  }

  private void seedNullShapeVertices() {
    for (var shape : NullShape.values()) {
      graph.addVertex(T.label, "Person", "shape", shape.name(), "role", "low", "age", 1);
      graph.addVertex(T.label, "Person", "shape", shape.name(), "role", "high", "age", 2);
      var nullVertex =
          graph.addVertex(T.label, "Person", "shape", shape.name(), "role", "null");
      if (shape == NullShape.STORED) {
        nullVertex.property("age", null);
      } else if (shape == NullShape.EXPRESSION) {
        nullVertex.property("age", 3);
        nullVertex.property("expressionNull", true);
      }
    }
    graph.tx().commit();
  }

  private List<Object> nativeRecordOrder(
      NullShape shape,
      Order order,
      YTDBQueryConfigParam option,
      String optionValue) {
    GraphTraversalSource source = graph.traversal().withoutStrategies(GremlinToMatchStrategy.class);
    if (option != null) {
      source = source.with(option.name(), optionValue);
    }
    return recordTraversal(source, shape, order).toList();
  }

  private List<Object> convertedRecordOrder(
      NullShape shape,
      Order order,
      YTDBQueryConfigParam option,
      String optionValue) {
    GraphTraversalSource source = graph.traversal();
    if (option != null) {
      source = source.with(option.name(), optionValue);
    }
    var admin = recordTraversal(source, shape, order).asAdmin();
    admin.applyStrategies();
    assertThat(TraversalHelper.getStepsOfAssignableClass(YTDBMatchPlanStep.class, admin))
        .isNotEmpty();
    return admin.toList();
  }

  private GraphTraversal<?, Object> recordTraversal(
      GraphTraversalSource source, NullShape shape, Order order) {
    var traversal = source.V().has("shape", shape.name()).order();
    if (shape == NullShape.EXPRESSION) {
      traversal =
          traversal.by(
              __.choose(__.has("expressionNull"), __.constant(null), __.values("age")), order);
    } else {
      traversal = traversal.by("age", order);
    }
    return traversal.values("role");
  }

  private List<String> localCollectionOrder(
      NullShape shape,
      Order order,
      YTDBQueryConfigParam option,
      String optionValue) {
    GraphTraversalSource source = graph.traversal();
    if (option != null) {
      source = source.with(option.name(), optionValue);
    }
    var rows = new ArrayList<Map<String, Object>>();
    rows.add(new HashMap<>(Map.of("role", "low", "age", 1)));
    rows.add(new HashMap<>(Map.of("role", "high", "age", 2)));
    var nullRow = new HashMap<String, Object>();
    nullRow.put("role", "null");
    if (shape == NullShape.STORED) {
      nullRow.put("age", null);
    } else if (shape == NullShape.EXPRESSION) {
      nullRow.put("age", 3);
      nullRow.put("expressionNull", true);
    }
    rows.add(nullRow);

    var traversal = source.inject(rows).order(Scope.local);
    if (shape == NullShape.EXPRESSION) {
      traversal =
          traversal.by(
              __.map(
                  traverser -> {
                    @SuppressWarnings("unchecked")
                    var row = (Map<String, Object>) traverser.get();
                    return Boolean.TRUE.equals(row.get("expressionNull"))
                        ? null
                        : row.get("age");
                  }),
              order);
    } else {
      traversal = traversal.by("age", order);
    }
    var ordered = traversal.next();
    return ordered.stream().map(row -> (String) row.get("role")).toList();
  }

  private static List<String> expected(Order order, OrderByNullsPlacement placement) {
    if (placement == OrderByNullsPlacement.FIRST) {
      return order == Order.asc
          ? List.of("null", "low", "high")
          : List.of("null", "high", "low");
    }
    return order == Order.asc
        ? List.of("low", "high", "null")
        : List.of("high", "low", "null");
  }

  private enum NullShape {
    ABSENT, STORED, EXPRESSION
  }

  private static void setGlobalPlacement(
      OrderByNullsPlacement ascending, OrderByNullsPlacement descending) {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(ascending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(descending);
  }

  private void setStorageReversedPlacement() {
    var tx = (YTDBTransaction) graph.tx();
    tx.readWrite();
    tx.getDatabaseSession()
        .getConfiguration()
        .setValue(
            GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
    tx.getDatabaseSession()
        .getConfiguration()
        .setValue(
            GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC, OrderByNullsPlacement.FIRST);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Comparator comparatorAt(OrderGlobalStep<?, ?> step, int index) {
    List<Pair> pairs = (List) step.getComparators();
    return (Comparator) pairs.get(index).getValue1();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Comparator comparatorAt(OrderLocalStep<?, ?> step, int index) {
    List<Pair> pairs = (List) step.getComparators();
    return (Comparator) pairs.get(index).getValue1();
  }
}
