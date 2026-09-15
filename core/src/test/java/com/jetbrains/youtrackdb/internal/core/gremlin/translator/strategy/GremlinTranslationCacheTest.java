package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.GraphBaseTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBTransaction;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.YTDBMatchPlanStep;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Translation-cache (pre-walk shape key) and copy-on-open behaviour: a second {@code has()} value
 * splices the cached template, a declining shape is cached as decline, schema invalidation clears
 * the map, and {@code getPlan()} before the first open is the shared template.
 */
@Category(SequentialTest.class)
public class GremlinTranslationCacheTest extends GraphBaseTest {

  private Object previousAscending;
  private Object previousDescending;

  private final TranslatorEquivalenceSupport support =
      new TranslatorEquivalenceSupport(this::graphSession);

  @Before
  public void enableTranslator() {
    previousAscending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    previousDescending = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.getValue();
    support.setTranslatorEnabled(true);
    GremlinPlanCache.instance(graphSession()).invalidate();
  }

  @After
  public void restoreNullPlacementGlobals() {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previousAscending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(previousDescending);
  }

  /**
   * Two walks that differ only in the {@code has()} value share a translation-cache entry, and the
   * second walk returns the second value's row — the harvested binding rebinds the cached plan.
   */
  @Test
  public void secondHasValue_hitsTranslationCache_andReturnsSecondRow() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 40);
    graph.tx().commit();

    var cache = GremlinPlanCache.instance(graphSession());
    var hitsBefore = cache.getTranslationHits();
    var missesBefore = cache.getTranslationMisses();
    var first = apply(() -> graph.traversal().V().has("age", 30));
    assertThat(sortedNames(first)).containsExactly("Alice");
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);
    assertThat(cache.getTranslationHits()).isEqualTo(hitsBefore);

    var second = apply(() -> graph.traversal().V().has("age", 40));
    assertThat(sortedNames(second)).containsExactly("Bob");
    assertThat(cache.getTranslationHits()).isEqualTo(hitsBefore + 1);
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);
  }

  /**
   * A walker-declined shape is stored as {@code Decline}; the second apply hits that entry and
   * leaves the native step list untouched.
   */
  @Test
  public void declinedShape_isCached_secondApplyLeavesNativeSteps() {
    graph.addVertex(T.label, "Person", "name", "Alice");
    graph.tx().commit();

    var cache = GremlinPlanCache.instance(graphSession());
    var hitsBefore = cache.getTranslationHits();
    var missesBefore = cache.getTranslationMisses();
    var first = graph.traversal().V().is(P.eq(1)).asAdmin();
    GremlinToMatchStrategy.instance().apply(first);
    assertThat(first.getStartStep()).isNotInstanceOf(YTDBMatchPlanStep.class);
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);

    var second = graph.traversal().V().is(P.eq(1)).asAdmin();
    GremlinToMatchStrategy.instance().apply(second);
    assertThat(second.getStartStep()).isNotInstanceOf(YTDBMatchPlanStep.class);
    assertThat(cache.getTranslationHits()).isEqualTo(hitsBefore + 1);
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);
  }

  /**
   * {@code P.lt(true)} and {@code P.lt("m")} must not share a translation-cache entry: the
   * comparability block is part of the shape, and serving the Boolean guard to the String walk
   * would keep extra rows.
   */
  @Test
  public void guardedLiteralTypes_doNotShareTranslationEntry() {
    graph.addVertex(T.label, "Types", "name", "s_alpha", "v", "alpha");
    graph.addVertex(T.label, "Types", "name", "s_zulu", "v", "zulu");
    graph.addVertex(T.label, "Types", "name", "b_true", "v", true);
    graph.addVertex(T.label, "Types", "name", "b_false", "v", false);
    graph.addVertex(T.label, "Types", "name", "n_ten", "v", 10);
    graph.tx().commit();

    var booleanKey =
        GremlinStepWalker.extractShape(
            graph.traversal().V().not(__.has("v", P.lt(true))).asAdmin(), graphSession())
            .key();
    var stringKey =
        GremlinStepWalker.extractShape(
            graph.traversal().V().not(__.has("v", P.lt("m"))).asAdmin(), graphSession())
            .key();
    assertThat(booleanKey).isNotEqualTo(stringKey);

    var booleanRun = apply(() -> graph.traversal().V().not(__.has("v", P.lt(true))));
    assertThat(sortedNames(booleanRun)).containsExactly("b_true", "n_ten", "s_alpha", "s_zulu");

    var stringRun = apply(() -> graph.traversal().V().not(__.has("v", P.lt("m"))));
    assertThat(sortedNames(stringRun)).containsExactly("b_false", "b_true", "n_ten", "s_zulu");
  }

  /**
   * After {@code apply} without iterating, {@code getPlan()} is the shared closed template. After
   * {@code toList()}, it is a live copy — copy-on-open, not copy-during-apply.
   */
  @Test
  public void getPlan_beforeIterate_isSharedTemplate_afterIterate_isCopy() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.tx().commit();

    var admin = graph.traversal().V().has("age", 30).asAdmin();
    GremlinToMatchStrategy.instance().apply(admin);
    assertThat(admin.getStartStep()).isInstanceOf(YTDBMatchPlanStep.class);
    @SuppressWarnings("unchecked")
    var step = (YTDBMatchPlanStep<?, Vertex>) admin.getStartStep();
    var template = step.getPlan();
    var walked = GremlinStepWalker.production()
        .walk(graph.traversal().V().has("age", P.eq(30)).asAdmin());
    assertThat(walked).isNotNull();
    assertThat(walked.inputs()).isNotNull();
    var fp = GremlinPlanFingerprint.fingerprint(walked.inputs());
    assertThat(GremlinPlanCache.instance(graphSession()).peekStored(fp)).isSameAs(template);

    admin.toList();
    assertThat(step.getPlan()).isNotSameAs(template);
  }

  /**
   * Step-local tokens the walker reads (select/project keys, Pop, valueMap keys, tail window, where
   * labels, dedup scope) must discriminate the shape key. A shared entry would splice the first
   * walk's MATCH projection / {@code ResultShaping} onto the second query.
   */
  @Test
  public void stepLocalTokens_discriminateShapeKeys() {
    assertThat(shapeKey(() -> graph.traversal().V().as("a").out("knows").as("b").select("a")))
        .isNotEqualTo(
            shapeKey(() -> graph.traversal().V().as("a").out("knows").as("b").select("b")));
    assertThat(shapeKey(() -> graph.traversal().V().as("a").select(Pop.last, "a")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().as("a").select(Pop.first, "a")));
    assertThat(shapeKey(() -> graph.traversal().V().valueMap("name")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().valueMap("age")));
    assertThat(shapeKey(() -> graph.traversal().V().elementMap("name")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().elementMap("age")));
    assertThat(shapeKey(() -> graph.traversal().V().project("x").by("name")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().project("y").by("name")));
    assertThat(shapeKey(() -> graph.traversal().V().values("name").tail(1)))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().values("name").tail(2)));
    assertThat(
        shapeKey(
            () -> graph.traversal().V().as("a").out("knows").as("b").where("a", P.eq("b"))))
        .isNotEqualTo(
            shapeKey(
                () -> graph.traversal().V().as("a").out("knows").as("b").where("b", P.eq("a"))));
    assertThat(shapeKey(() -> graph.traversal().V().as("a").out("knows").as("b").dedup("a")))
        .isNotEqualTo(
            shapeKey(() -> graph.traversal().V().as("a").out("knows").as("b").dedup("b")));
    assertThat(shapeKey(() -> graph.traversal().V().order().by("name")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().order().by("age")));
    assertThat(shapeKey(() -> graph.traversal().V().groupCount().by("name")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().groupCount().by("age")));
    assertThat(shapeKey(() -> graph.traversal().V().project("x").by("name")))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().project("x").by("age")));
    assertThat(shapeKey(() -> graph.traversal().V().has("name", TextP.regex("^mar"))))
        .isNotEqualTo(shapeKey(() -> graph.traversal().V().has("name", TextP.notRegex("^mar"))));
  }

  /**
   * After {@code select("a")} is cached, {@code select("b")} on the same hop must still return the
   * hop target, not the origin the first walk projected.
   */
  @Test
  public void selectDifferentLabels_doNotShareCachedPlan() {
    var alice = graph.addVertex(T.label, "Person", "name", "Alice");
    var bob = graph.addVertex(T.label, "Person", "name", "Bob");
    alice.addEdge("knows", bob);
    graph.tx().commit();

    var origin =
        apply(() -> graph.traversal().V().has("name", "Alice").as("a").out("knows").as("b")
            .select("a"));
    assertThat(sortedNames(origin)).containsExactly("Alice");

    var target =
        apply(() -> graph.traversal().V().has("name", "Alice").as("a").out("knows").as("b")
            .select("b"));
    assertThat(sortedNames(target)).containsExactly("Bob");
  }

  /**
   * After {@code tail(1)} is cached, {@code tail(2)} must keep its own window — the limit lives in
   * the boundary {@code ResultShaping}, not in the MATCH statement.
   */
  @Test
  public void tailWindows_doNotShareCachedShaping() {
    graph.addVertex(T.label, "Person", "name", "Abe");
    graph.addVertex(T.label, "Person", "name", "Zed");
    graph.tx().commit();

    var tail1 =
        apply(() -> graph.traversal().V().order().by("name").values("name").tail(1));
    assertThat(tail1).isEqualTo(List.of("Zed"));

    var tail2 =
        apply(() -> graph.traversal().V().order().by("name").values("name").tail(2));
    assertThat(tail2).isEqualTo(List.of("Abe", "Zed"));
  }

  /**
   * After {@code valueMap("name")} is cached, {@code valueMap("age")} must project age, not name.
   */
  @Test
  public void valueMapKeys_doNotShareCachedProjection() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.tx().commit();

    apply(() -> graph.traversal().V().has("name", "Alice").valueMap("name"));
    @SuppressWarnings("unchecked")
    var ageMaps =
        (List<java.util.Map<String, Object>>) apply(
            () -> graph.traversal().V().has("name", "Alice").valueMap("age"));
    assertThat(ageMaps).hasSize(1);
    assertThat(ageMaps.getFirst()).containsKey("age");
    assertThat(ageMaps.getFirst()).doesNotContainKey("name");
  }

  /**
   * {@code Text.regex} and {@code Text.notRegex} share {@code RegexPredicate}; only {@code
   * isNegate()} distinguishes them. After {@code regex("^mar")} is cached, {@code notRegex("^mar")}
   * must still return the names that do not match, not splice the positive MATCHES plan.
   */
  @Test
  public void regexAndNotRegex_doNotShareCachedPlan() {
    graph.addVertex(T.label, "Person", "name", "marko");
    graph.addVertex(T.label, "Person", "name", "vadas");
    graph.tx().commit();

    var regex = apply(() -> graph.traversal().V().has("name", TextP.regex("^mar")));
    assertThat(sortedNames(regex)).containsExactly("marko");

    var notRegex = apply(() -> graph.traversal().V().has("name", TextP.notRegex("^mar")));
    assertThat(sortedNames(notRegex)).containsExactly("vadas");
  }

  /**
   * {@code order().by("name")} and {@code order().by("age")} share the hop/start class list and
   * differ only in a {@code ValueTraversal} property key. After the first is cached, the second
   * must still sort by age.
   */
  @Test
  public void orderByPropertyKeys_doNotShareCachedPlan() {
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 20);
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.tx().commit();

    var byName = apply(() -> graph.traversal().V().order().by("name").values("name"));
    assertThat(byName).isEqualTo(List.of("Alice", "Bob"));

    var byAge = apply(() -> graph.traversal().V().order().by("age").values("name"));
    assertThat(byAge).isEqualTo(List.of("Bob", "Alice"));
  }

  /**
   * A lambda {@code by()} the extractor cannot name marks the extraction incomplete, so apply
   * neither reads nor writes the translation cache — fail-closed rather than a colliding template.
   */
  @Test
  public void unknownLambdaModulator_doesNotTouchTranslationCache() {
    graph.addVertex(T.label, "Person", "name", "Alice");
    graph.tx().commit();

    var cache = GremlinPlanCache.instance(graphSession());
    var hitsBefore = cache.getTranslationHits();
    var missesBefore = cache.getTranslationMisses();
    var admin = graph.traversal().V().order().by(new ConstantTraversal<>("x")).asAdmin();
    var extraction = GremlinStepWalker.extractShape(admin, graphSession());
    assertThat(extraction.complete()).isFalse();

    GremlinToMatchStrategy.instance().apply(admin);
    assertThat(admin.getStartStep()).isNotInstanceOf(YTDBMatchPlanStep.class);
    assertThat(cache.getTranslationHits()).isEqualTo(hitsBefore);
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore);
  }

  /** Every global placement combination is written explicitly into converted sort items. */
  @Test
  public void allFourGlobalNullPlacementCombinations_reachConvertedItems() {
    for (var ascending : OrderByNullsPlacement.values()) {
      for (var descending : OrderByNullsPlacement.values()) {
        setGlobalNullPlacements(ascending, descending);
        var orderBy =
            translatedOrderBy(
                () -> graph.traversal().V().order().by("age").by("name", Order.desc));

        assertThat(orderBy.getItems())
            .extracting(SQLOrderByItem::getNullOrdering)
            .containsExactly(nullOrdering(ascending), nullOrdering(descending));
      }
    }
  }

  /** The ascending per-query option overrides only the converted ascending item. */
  @Test
  public void ascendingPerQueryNullPlacement_reachesConvertedItem() {
    setGlobalNullPlacements(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);

    var orderBy =
        translatedOrderBy(
            () -> graph
                .traversal()
                .with(
                    YTDBQueryConfigParam.orderByNullsPlacementAsc,
                    OrderByNullsPlacement.LAST)
                .V()
                .order()
                .by("age")
                .by("name", Order.desc));

    assertThat(orderBy.getItems())
        .extracting(SQLOrderByItem::getNullOrdering)
        .containsExactly(SQLOrderByItem.NULLS_LAST, SQLOrderByItem.NULLS_LAST);
  }

  /** The descending per-query option overrides only the converted descending item. */
  @Test
  public void descendingPerQueryNullPlacement_reachesConvertedItem() {
    setGlobalNullPlacements(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);

    var orderBy =
        translatedOrderBy(
            () -> graph
                .traversal()
                .with(
                    YTDBQueryConfigParam.orderByNullsPlacementDesc,
                    OrderByNullsPlacement.FIRST)
                .V()
                .order()
                .by("age")
                .by("name", Order.desc));

    assertThat(orderBy.getItems())
        .extracting(SQLOrderByItem::getNullOrdering)
        .containsExactly(SQLOrderByItem.NULLS_FIRST, SQLOrderByItem.NULLS_FIRST);
  }

  /** Shape-identical traversals with different placements use separate translation entries. */
  @Test
  public void nullPlacement_missesTranslationCacheWithinOneLifetime() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Nobody");
    graph.tx().commit();

    var cache = GremlinPlanCache.instance(graphSession());
    var missesBefore = cache.getTranslationMisses();
    var hitsBefore = cache.getTranslationHits();

    var first =
        apply(
            () -> graph
                .traversal()
                .with(
                    YTDBQueryConfigParam.orderByNullsPlacementAsc,
                    OrderByNullsPlacement.FIRST)
                .V()
                .order()
                .by("age")
                .values("name"));
    assertThat(first).isEqualTo(List.of("Nobody", "Alice"));
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);

    apply(
        () -> graph
            .traversal()
            .with(
                YTDBQueryConfigParam.orderByNullsPlacementAsc,
                OrderByNullsPlacement.FIRST)
            .V()
            .order()
            .by("age")
            .values("name"));
    assertThat(cache.getTranslationHits()).isEqualTo(hitsBefore + 1);

    var last =
        apply(
            () -> graph
                .traversal()
                .with(
                    YTDBQueryConfigParam.orderByNullsPlacementAsc,
                    OrderByNullsPlacement.LAST)
                .V()
                .order()
                .by("age")
                .values("name"));
    assertThat(last).isEqualTo(List.of("Alice", "Nobody"));
    assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 2);
  }

  /** Placement changes do not partition a shape without any global order step. */
  @Test
  public void nullPlacement_doesNotPartitionShapeWithoutOrder() {
    setGlobalNullPlacements(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);
    var shipped = shapeKey(() -> graph.traversal().V().hasLabel("Person"));
    setGlobalNullPlacements(OrderByNullsPlacement.LAST, OrderByNullsPlacement.FIRST);
    var reversed = shapeKey(() -> graph.traversal().V().hasLabel("Person"));

    assertThat(shipped).isEqualTo(reversed);
  }

  /** A nested union-arm order still partitions the enclosing translation shape. */
  @Test
  public void nullPlacement_partitionsShapeForNestedUnionOrder() {
    setGlobalNullPlacements(OrderByNullsPlacement.FIRST, OrderByNullsPlacement.LAST);
    var shipped =
        rawShapeKey(
            () -> graph
                .traversal()
                .V()
                .union(__.order().by("age"), __.identity()));
    setGlobalNullPlacements(OrderByNullsPlacement.LAST, OrderByNullsPlacement.FIRST);
    var reversed =
        rawShapeKey(
            () -> graph
                .traversal()
                .V()
                .union(__.order().by("age"), __.identity()));

    assertThat(shipped).isNotEqualTo(reversed);
  }

  /** The per-session polymorphism flag is part of the shape key; toggling it must split entries. */
  @Test
  public void polymorphismFlag_discriminatesShapeKeys() {
    final var polyOn = captureShapeKeyWithPolymorphic(true);
    final var polyOff = captureShapeKeyWithPolymorphic(false);
    assertThat(polyOn).isNotEqualTo(polyOff);
  }

  /**
   * The resolved order mode is part of the shape key. The two values produce
   * different patterns for the same step list — one carries the order-key {@code IS DEFINED}
   * conjunct and the other does not — so they must not share an entry.
   */
  @Test
  public void productiveOrderSetting_discriminatesShapeKeys() {
    final var includingKey = captureOrderShapeKeyWith(true);
    final var standardOrderKey = captureOrderShapeKeyWith(false);
    assertThat(includingKey).isNotEqualTo(standardOrderKey);
  }

  /** The per-traversal option partitions the effective order mode in the shape key. */
  @Test
  public void perTraversalOrderOption_discriminatesShapeKeys() {
    var retaining = shapeKey(() -> graph.traversal()
        .with(YTDBQueryConfigParam.orderIncludesMissingKey, true).V().order().by("age"));
    var removing = shapeKey(() -> graph.traversal()
        .with(YTDBQueryConfigParam.orderIncludesMissingKey, false).V().order().by("age"));

    assertThat(retaining).isNotEqualTo(removing);
  }

  /** A user strategy partitions the effective order mode in the existing shape token. */
  @Test
  public void standardOrderStrategy_discriminatesShapeKeys() {
    var retaining = shapeKey(() -> graph.traversal().V().order().by("age"));
    var removing = shapeKey(() -> graph.traversal()
        .withStrategies(StandardOrderSemanticsStrategy.instance()).V().order().by("age"));

    assertThat(retaining).isNotEqualTo(removing);
  }

  /**
   * The detecting test for the storage-wide cache: translate a shape under one setting, FLIP the
   * setting INSIDE ONE CACHE LIFETIME with no invalidation in between, and translate again.
   *
   * <p>The second translation must MISS. Without the shape-key token it would hit, and the plan
   * built under the first setting would be spliced verbatim into a traversal running under the
   * second — across sessions, because the cache is storage-wide. The rows prove which semantics
   * each run actually got: three rows under the including default, two under the standard order semantics mode.
   */
  @Test
  public void flippingProductiveOrderSetting_missesTranslationCacheWithinOneLifetime() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 25);
    graph.addVertex(T.label, "Person", "name", "Nobody");
    graph.tx().commit();

    var config = graphSession().getConfiguration();
    var previous =
        config.getValueAsBoolean(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY);
    var cache = GremlinPlanCache.instance(graphSession());
    try {
      config.setValue(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, true);
      var missesBefore = cache.getTranslationMisses();
      var hitsBefore = cache.getTranslationHits();

      var including = apply(() -> graph.traversal().V().order().by("age").values("name"));
      assertThat(including)
          .as("the including default keeps the record that carries no age")
          .hasSize(3);
      assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);

      // Same shape, same cache, no invalidation: a warm hit, which proves the entry is live and
      // that the miss below is caused by the flip rather than by an empty cache.
      var warm = apply(() -> graph.traversal().V().order().by("age").values("name"));
      assertThat(warm).hasSize(3);
      assertThat(cache.getTranslationHits()).isEqualTo(hitsBefore + 1);
      assertThat(cache.getTranslationMisses()).isEqualTo(missesBefore + 1);

      config.setValue(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, false);

      var standardOrderResult = apply(() -> graph.traversal().V().order().by("age").values("name"));
      assertThat(cache.getTranslationMisses())
          .as("the flipped setting must key a different entry, so this translation misses")
          .isEqualTo(missesBefore + 2);
      assertThat(cache.getTranslationHits())
          .as("and it must not be served the plan built under the other setting")
          .isEqualTo(hitsBefore + 1);
      assertThat(standardOrderResult)
          .as("the standard order semantics mode drops the record that carries no age")
          .hasSize(2);
    } finally {
      config.setValue(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, previous);
    }
  }

  /**
   * Translation-cache shape keys are value-independent only within one runtime class. Different
   * String values share a key, but an Integer and a Long do not.
   */
  @Test
  public void runtimeValueClass_partitionsShapeKey() {
    var intThirty = shapeKey(() -> graph.traversal().V().has("age", P.eq(30)));
    var intNinetyNine = shapeKey(() -> graph.traversal().V().has("age", P.eq(99)));
    var longThirty = shapeKey(() -> graph.traversal().V().has("age", P.eq(30L)));

    assertThat(intThirty).isEqualTo(intNinetyNine);
    assertThat(intThirty).isNotEqualTo(longThirty);
  }

  /**
   * Row-level guard on limit literals: after {@code limit(1)} is cached, {@code limit(2)} must still
   * return two rows. A fingerprint that collapsed both limits to {@code ?} would serve the first
   * plan and silently truncate.
   */
  @Test
  public void differentLimits_afterCacheWarm_returnCorrectCardinality() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 1);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 2);
    graph.addVertex(T.label, "Person", "name", "Carol", "age", 3);
    graph.tx().commit();

    assertThat(apply(() -> graph.traversal().V().limit(1))).hasSize(1);
    assertThat(apply(() -> graph.traversal().V().limit(2)))
        .as("limit(2) must not reuse a cached limit(1) plan")
        .hasSize(2);
  }

  /**
   * Row-level guard on projection keys: after {@code values("name")} is cached, {@code values("age")}
   * must emit ages, not names.
   */
  @Test
  public void differentValuesKeys_afterCacheWarm_doNotCrossContaminate() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 40);
    graph.tx().commit();

    var names = apply(() -> graph.traversal().V().order().by("name").values("name"));
    assertThat(names).isEqualTo(List.of("Alice", "Bob"));

    var ages = apply(() -> graph.traversal().V().order().by("name").values("age"));
    assertThat(ages)
        .as("values(age) must not reuse a cached values(name) plan")
        .isEqualTo(List.of(30, 40));
  }

  /**
   * Integer vs Long {@code has(age)} partitions the shape key; after warming the Integer entry, the
   * Long walk must still answer correctly for its own binding (and must not be served the Integer
   * plan).
   */
  @Test
  public void integerThenLongAge_afterCacheWarm_bothReturnCorrectRows() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 40);
    graph.tx().commit();

    assertThat(sortedNames(apply(() -> graph.traversal().V().has("age", 30))))
        .containsExactly("Alice");
    assertThat(sortedNames(apply(() -> graph.traversal().V().has("age", 30L))))
        .as("Long 30 must not reuse the Integer-30 cached plan incorrectly")
        .containsExactly("Alice");
    assertThat(sortedNames(apply(() -> graph.traversal().V().has("age", 40L))))
        .containsExactly("Bob");
  }

  /**
   * Predicate order can differ between Gremlin shapes that still compile to one PQD. Warm with
   * {@code has(age).has(name)}, then run {@code has(name).has(age)} with different bindings — both
   * must return their own row (walk + PQD hit + shape backfill must not cross-bind).
   */
  @Test
  public void swappedHasOrder_afterCacheWarm_returnsOwnRow() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 40);
    graph.tx().commit();

    assertThat(
        sortedNames(
            apply(() -> graph.traversal().V().has("age", 30).has("name", "Alice"))))
        .containsExactly("Alice");
    assertThat(
        sortedNames(apply(() -> graph.traversal().V().has("name", "Bob").has("age", 40))))
        .as("swapped has() order must not return the previously cached person's row")
        .containsExactly("Bob");
  }

  private SQLOrderBy translatedOrderBy(
      java.util.function.Supplier<
          org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal<?, ?>> supplier) {
    var admin = supplier.get().asAdmin();
    var placements = YTDBStrategyUtil.orderByNullsPlacements(admin);
    assertThat(placements).isNotNull();
    var translation =
        GremlinToMatchTranslator.translate(
            admin, YTDBStrategyUtil.orderIncludesMissingKey(admin), placements);
    assertThat(translation).isNotNull();
    assertThat(translation.inputs()).isNotNull();
    assertThat(translation.inputs().orderBy()).isNotNull();
    return translation.inputs().orderBy();
  }

  private static void setGlobalNullPlacements(
      OrderByNullsPlacement ascending, OrderByNullsPlacement descending) {
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(ascending);
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(descending);
  }

  private static String nullOrdering(OrderByNullsPlacement placement) {
    return placement == OrderByNullsPlacement.FIRST
        ? SQLOrderByItem.NULLS_FIRST
        : SQLOrderByItem.NULLS_LAST;
  }

  private String shapeKey(
      java.util.function.Supplier<
          org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal<?, ?>> supplier) {
    var extraction = GremlinStepWalker.extractShape(supplier.get().asAdmin(), graphSession());
    assertThat(extraction.complete()).isTrue();
    return extraction.key();
  }

  private String rawShapeKey(
      java.util.function.Supplier<
          org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal<?, ?>> supplier) {
    return GremlinStepWalker.extractShape(supplier.get().asAdmin(), graphSession()).key();
  }

  private List<?> apply(
      java.util.function.Supplier<
          org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal<?, ?>> supplier) {
    var admin = supplier.get().asAdmin();
    GremlinToMatchStrategy.instance().apply(admin);
    assertThat(admin.getSteps()).hasSize(1);
    assertThat(admin.getSteps().getFirst()).isInstanceOf(YTDBMatchPlanStep.class);
    return admin.toList();
  }

  private static List<String> sortedNames(List<?> vertices) {
    return vertices.stream()
        .map(v -> ((Vertex) v).value("name"))
        .map(Object::toString)
        .sorted()
        .toList();
  }

  private com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded graphSession() {
    var tx = (YTDBTransaction) graph.tx();
    tx.readWrite();
    return tx.getDatabaseSession();
  }

  /** Returns the order shape key for the supplied order mode. */
  private String captureOrderShapeKeyWith(boolean value) {
    var config = graphSession().getConfiguration();
    var previous =
        config.getValueAsBoolean(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY);
    config.setValue(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, value);
    try {
      return shapeKey(() -> graph.traversal().V().order().by("age").values("name"));
    } finally {
      config.setValue(GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, previous);
    }
  }

  private String captureShapeKeyWithPolymorphic(boolean value) {
    var config = graphSession().getConfiguration();
    var previous =
        config.getValueAsBoolean(GlobalConfiguration.QUERY_GREMLIN_POLYMORPHIC_BY_DEFAULT);
    config.setValue(GlobalConfiguration.QUERY_GREMLIN_POLYMORPHIC_BY_DEFAULT, value);
    try {
      return shapeKey(() -> graph.traversal().V().hasLabel("Person"));
    } finally {
      config.setValue(GlobalConfiguration.QUERY_GREMLIN_POLYMORPHIC_BY_DEFAULT, previous);
    }
  }

}
