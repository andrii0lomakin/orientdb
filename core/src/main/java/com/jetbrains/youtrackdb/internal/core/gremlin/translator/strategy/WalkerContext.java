package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.BoundaryOutputType;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.ListShapingOp;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.PostConcatOp;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.ResultShaping;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Schema;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.MatchPlanInputs;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchPatternBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchProjectionBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.builder.MatchWhereBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGroupBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLimit;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNestedProjection;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLPositionalParameter;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSkip;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * The full walk state {@link GremlinStepWalker} owns and recognisers contribute to. It implements
 * {@link RecognitionContext}, the narrow view handed to recognisers: they reach the fields here only
 * through that interface's named methods, while the walker reads the fields directly to build the
 * result. The walker creates one context per walk.
 *
 * <h2>No mutation discipline</h2>
 *
 * A recogniser may contribute in any order. A {@link Outcome#DECLINE} makes the walker discard the
 * whole context (and its cursor), so a partial contribution never leaks — see
 * {@link RecognitionContext}.
 */
final class WalkerContext implements RecognitionContext {

  /** Pattern under construction. Recognisers contribute through {@link #addNode} / {@link #addEdge} /
   *  {@link #addEdgeAsNode}; the walker calls {@code build()} once at the end of a successful walk. */
  final MatchPatternBuilder patternBuilder = new MatchPatternBuilder();

  /** Per-alias WHERE clauses contributed outside the pattern builder (e.g. {@code @rid IN [...]}).
   *  Merged with the builder's own alias filters at result-build time; entries here override builder
   *  entries on the same alias. */
  final Map<String, SQLWhereClause> aliasFilters = new LinkedHashMap<>();

  /** Per-edge-alias WHERE clauses for non-adjacent edge filtering (the {@code outE(L).has(...).inV()}
   *  shape). Populated by {@link #putEdgeFilter} for observability; the same clause also travels on
   *  the edge path item via {@link #addEdgeAsNode}, so it is not re-read at result-build time. */
  final Map<String, SQLWhereClause> edgeFilters = new LinkedHashMap<>();

  /** Detached NOT pattern chains produced by edge-bearing {@code NotStep} recognisers. Wired into
   *  {@link GremlinStepWalker}'s {@code buildResult} as {@code MatchPlanInputs.notMatchExpressions}. */
  final List<SQLMatchExpression> notMatchExpressions = new ArrayList<>();

  /** Positional-parameter values collected during the walk, keyed by slot ({@code 0}, {@code 1}, …).
   *  Insertion order matches slot allocation order for deterministic rebinding on cache hit. */
  final LinkedHashMap<Integer, Object> inputParameters = new LinkedHashMap<>();

  /** Next positional-parameter slot to allocate. Shape-pure: incremented once per {@link #bindParam}
   *  call regardless of value. */
  private int nextParamSlot;

  /** When {@code true}, this walk carries inline RIDs ({@code g.V(ids)} or {@code hasId(...)}) and
   *  must bypass the plan cache. */
  private boolean ridBearing;

  /** RETURN-clause projection items. One entry per output column. */
  final List<SQLExpression> returnItems = new ArrayList<>();

  /** {@code AS} aliases for each entry in {@link #returnItems}. Same length, parallel positions;
   *  null entries are allowed when an item has no alias. */
  final List<SQLIdentifier> returnAliases = new ArrayList<>();

  /** Optional nested projections per entry in {@link #returnItems}. Same length, parallel positions;
   *  null entries are allowed when an item has no nested projection. */
  final List<SQLNestedProjection> returnNestedProjections = new ArrayList<>();

  /** Alias under which the matched element appears in each result row. Pinned by the recogniser
   *  owning the traversal's terminator. Required for a successful walk. */
  String boundaryAlias;

  /** How the boundary step projects each result row onto a TinkerPop traverser. Pinned alongside
   *  {@link #boundaryAlias}. Required for a successful walk. */
  BoundaryOutputType outputType;

  /** TinkerPop element class the boundary step emits (e.g. {@code Vertex.class}). Pinned alongside
   *  {@link #boundaryAlias}. Required for a successful walk. */
  Class<? extends Element> returnClass;

  /** When {@code true}, the assembled plan carries {@code RETURN DISTINCT} ({@code dedup()}). */
  boolean returnDistinct;

  /**
   * Gremlin {@code as(label)} → internal pattern alias. Populated by {@link #bindStepLabels} when a
   * recognised step carries user labels; consumed by named {@code dedup(labels...)} and later
   * projection recognisers.
   */
  final Map<String, String> userLabelToAlias = new LinkedHashMap<>();

  /** {@code GROUP BY} clause for {@code group()} / {@code groupCount()} terminators. */
  @Nullable SQLGroupBy groupBy;

  /** {@code ProductiveByStrategy}'s productive keys, or {@code null} when the strategy is absent.
   *  See {@link #setProductiveByKeys}. */
  @Nullable private Set<String> productiveByKeys;

  /** Whether a global-scope order() keeps a record missing the ordered key. See
   *  {@link #setOrderIncludesMissingKey}. */
  private boolean orderIncludesMissingKey;

  /** Direction-specific null placements for translated order items. */
  private ResolvedOrderByNullsPlacement orderByNullsPlacements =
      ResolvedOrderByNullsPlacement.SHIPPED;

  /** {@code ORDER BY} clause for {@code order()} terminators. */
  @Nullable SQLOrderBy orderBy;

  /** {@code LIMIT} for {@code limit()} / {@code range()} terminators. */
  @Nullable SQLLimit limit;

  /** {@code SKIP} for {@code skip()} / {@code range()} terminators. */
  @Nullable SQLSkip skip;

  /**
   * Boundary row-projection shaping — the seven flags plus the ordered list-shaping ops a
   * terminator pins so the boundary base ({@link
   * com.jetbrains.youtrackdb.internal.core.gremlin.translator.step.AbstractMatchPlanStep}) knows
   * how to project each row: row dropping, presence checks, valueMap list wrapping, group-map
   * accumulation, singleton-map unwrapping, elementMap token keys, and the {@code fold} /
   * {@code unfold} / {@code reverse} / {@code tail} stream stages. Defaults to {@link
   * ResultShaping#NONE} (the element path).
   *
   * <p>Two write paths reach it. A flag-pinning terminator replaces the whole record through {@link
   * #setResultShaping}; a list-shaping terminator appends one op through {@link
   * #appendListShapingOp}, which preserves whatever is already pinned. See {@link
   * RecognitionContext#setResultShaping} for what keeps a replace from clobbering an append.
   */
  ResultShaping shaping = ResultShaping.NONE;

  /**
   * Alias whose entity a pinned {@code dropOnAbsent} checks. Set beside the shaping write that
   * turns the flag on; cleared by every {@link #setResultShaping} so a later replace cannot promote
   * against a stale alias. {@link UnionStepRecogniser} copies agreed child shaping without
   * re-declaring the alias, so a post-union slice cannot promote and stays declined.
   */
  @Nullable String presenceDropAlias;

  /**
   * Field-access expression from the immediately preceding single-key property extraction, consumed
   * by aggregate recognisers ({@code values("age").mean()}).
   */
  @Nullable RecognitionContext.PropertyProjection lastPropertyProjection;

  /**
   * The fold latch — see {@link RecognitionContext#atTraversalStart()} for what it means and
   * {@link GremlinStepWalker}'s dispatch loop for the update rule. Starts {@code false}: no step
   * precedes the traversal's own {@code GraphStep}, so nothing is folded until one is consumed.
   */
  private boolean atTraversalStart;

  /** Whether the traversal runs as a polymorphic query, resolved once from the traversal's YTDB
   *  session and query options ({@code YTDBStrategyUtil.isPolymorphic}) by {@link GremlinStepWalker}.
   *
   *  <p>The {@code hasLabel(L)} recogniser reads it to pick the boundary-node re-typing (see {@link
   *  RecognitionContext#polymorphic()}): polymorphic re-types to {@code {class: L}} (MATCH matches
   *  subclasses), non-polymorphic re-types to {@code L} plus an exact {@code @class = 'L'} filter.
   *  The vertex-source and bare-hop recognisers root every node at the generic {@code V} class
   *  ({@link #VERTEX_ROOT_CLASS}) regardless of it — native Gremlin never class-filters those
   *  shapes, so narrowing one would drop subclass instances the native pipeline keeps. The
   *  resolution also carries a decline side effect: a {@code null} result declines the whole walk in
   *  the walker. */
  private final boolean polymorphic;

  /** Whether the traversal opts into {@code EdgeLabelVerificationStrategy}, resolved once by
   *  {@link GremlinStepWalker} so {@link GremlinPatternAssembler#resolveEdgeLabel} reads a boolean
   *  rather than scanning the strategy list per hop. */
  private final boolean edgeLabelVerification;

  /** Schema snapshot the walk resolves types against, or {@code null} when the traversal has no
   *  attached YTDB session. Used by {@link #isDeclaredStringProperty(String, String)} to pick the
   *  {@code startingWith} translation form and by {@link #isVertexClass(String)} for hasLabel
   *  re-typing; a {@code null} schema resolves every property as "not a declared String". */
  @Nullable private final Schema schema;

  /** The recogniser registry a {@link #walkChild} sub-walk drives child sub-traversals against — the
   *  same registry the walker dispatches the top-level walk with, so a child dispatches identically.
   *  {@code null} for the test constructors that never drive a sub-walk; {@link #walkChild} fails
   *  loudly if reached with a null registry, since production always supplies one. */
  @Nullable private final Map<Class<?>, StepRecogniser> recognisers;

  /**
   * Narrow union fork seam installed by {@link GremlinStepWalker#walk}. Holds the parent traversal
   * privately so {@link UnionStepRecogniser} never receives a {@code Traversal.Admin}. {@code null}
   * for test-constructed contexts and sub-walks.
   */
  @Nullable private UnionForkHost unionForkHost;

  /**
   * Ordered child {@link MatchPlanInputs} stashed by {@link UnionStepRecogniser} on accept. When
   * non-empty, {@link GremlinStepWalker}'s {@code buildResult} emits a multi-plan
   * {@link GremlinToMatchTranslator.TranslationResult} and ignores the prefix-only pattern on this
   * context (the prefix was re-walked into each child).
   */
  @Nullable private List<MatchPlanInputs> unionChildInputs;

  /** Parallel positional-parameter maps for {@link #unionChildInputs}; same length when set. */
  @Nullable private List<Map<Object, Object>> unionChildInputParameters;

  /**
   * Parallel per-child {@code GremlinPlanCache} eligibility flags for {@link #unionChildInputs};
   * same length when set. RID-bearing children are {@code false}.
   */
  @Nullable private List<Boolean> unionChildCacheEligible;

  /** Ordered post-concat reductions ({@code count}/{@code limit}/{@code dedup}) after a union. */
  private final List<PostConcatOp> postConcatOps = new ArrayList<>();

  /** Stateless builder used to AND-compose same-alias filter contributions in {@link
   *  #putAliasFilter}; construction is trivial so a shared instance is fine. */
  private static final MatchWhereBuilder WHERE = new MatchWhereBuilder();

  /** Reserved prefix for translator-minted anonymous vertex aliases: {@code $g2m_anon_0},
   *  {@code $g2m_anon_1}, … The {@code $g2m_} namespace is the translator's private space,
   *  distinct from GQL's {@code $c} and from {@code MatchExecutionPlanner.DEFAULT_ALIAS_PREFIX},
   *  so a minted alias cannot collide with either front-end. User labels starting with {@code $}
   *  are refused by the walker's reserved-prefix pre-flight scan, so the namespace stays private. */
  static final String ANON_VERTEX_ALIAS_PREFIX = "$g2m_anon_";

  /** Reserved prefix for translator-minted anonymous edge aliases: {@code $g2m_edge_0},
   *  {@code $g2m_edge_1}, … Used by the non-adjacent edge-filter recogniser to name the edge in
   *  the edge-as-node MATCH form. Same reserved {@code $g2m_} namespace as {@link
   *  #ANON_VERTEX_ALIAS_PREFIX}. */
  static final String EDGE_ALIAS_PREFIX = "$g2m_edge_";

  /**
   * Generic vertex root class {@code "V"} -- the polymorphic base every vertex-rooted traversal
   * roots at when no explicit user class is given. Shared by {@link StartStepRecogniser} (the {@code
   * g.V()} boundary node) and {@link GremlinPatternAssembler} (each bare-hop and edge-as-node target
   * node), which both register their node under it with no {@code @class} filter, so the emitted
   * MATCH keeps the full polymorphic vertex set native Gremlin returns. One definition so the two
   * sites cannot drift onto different roots -- a drift would silently reintroduce a subclass
   * undercount.
   */
  static final String VERTEX_ROOT_CLASS = "V";

  /**
   * The single definition of the reserved {@code $} alias-namespace prefix. The translator mints
   * every internal alias under it ({@link #ANON_VERTEX_ALIAS_PREFIX} and {@link #EDGE_ALIAS_PREFIX}
   * both begin with {@code $}), so a user identifier in this space could reach a MATCH WHERE
   * identifier the executor resolves as a query context variable ({@code $parent}, or any {@code
   * $name} bound in the execution context) rather than a record property — diverging from native
   * Gremlin, which treats {@code $foo} as a plain, absent property name.
   *
   * <p>Both reserved-namespace guards read this one constant so they cannot drift apart on its
   * value, though they react differently: {@link GremlinStepWalker}'s reserved-prefix {@code as(...)}
   * label pre-flight <em>rejects</em> a label starting with it (throwing a {@code
   * ReservedAliasException} — a user alias colliding with the minted namespace is prohibited input),
   * while {@link GremlinPredicateAdapter}'s {@code has(...)}-key guard <em>declines</em> to native
   * through {@link #isReservedHasKey(String)}.
   */
  static final String RESERVED_ALIAS_PREFIX = "$";

  /** TinkerPop's hidden-key namespace prefix ({@code ~label} / {@code ~id}, produced by {@code
   *  hasLabel} / {@code hasId}). A {@code has(...)} key in this space is a reserved token, not a
   *  plain property, so {@link #isReservedHasKey(String)} declines it. */
  static final String HIDDEN_KEY_PREFIX = "~";

  /** YouTrackDB's record-attribute namespace prefix ({@code @class} / {@code @rid} / {@code
   *  @version}). The shared identifier resolver treats a bare {@code @}-prefixed identifier as
   *  record metadata rather than a property, so a {@code has(...)} key in this space would diverge
   *  from native Gremlin — which treats {@code @foo} as an ordinary, absent property name.
   *  {@link #isReservedHasKey(String)} declines it. */
  static final String RECORD_ATTRIBUTE_PREFIX = "@";

  /**
   * The one place the {@code has(...)}-key reserved-namespace decline set is expressed. Returns
   * {@code true} when {@code key} lands in a namespace whose bare identifier the MATCH executor
   * would resolve as something other than a plain record property — the minted-alias {@code $}
   * space ({@link #RESERVED_ALIAS_PREFIX}), TinkerPop's hidden-key {@code ~} space ({@link
   * #HIDDEN_KEY_PREFIX}), or YouTrackDB's record-attribute {@code @} space ({@link
   * #RECORD_ATTRIBUTE_PREFIX}) — so the predicate adapter declines rather than translate a
   * divergent filter. Centralising the three prefixes here keeps the decline set from drifting as
   * predicate coverage grows in later tracks. Null / blank keys are the caller's concern (not a
   * namespace one). The walker's label pre-flight deliberately consumes only {@link
   * #RESERVED_ALIAS_PREFIX}: an {@code as(...)} label can collide only with the minted-alias
   * namespace, never with {@code ~} / {@code @}.
   */
  static boolean isReservedHasKey(String key) {
    return key.startsWith(RESERVED_ALIAS_PREFIX)
        || key.startsWith(HIDDEN_KEY_PREFIX)
        || key.startsWith(RECORD_ATTRIBUTE_PREFIX);
  }

  /** Anonymous-vertex alias sequence ({@code $g2m_anon_0}, {@code $g2m_anon_1}, …), minted by
   *  {@link #nextAnonVertexAlias()}. Per-context: a fresh {@link WalkerContext} per walk restarts
   *  it at 0, so the sequence is deterministic per query rather than monotonic across the JVM. */
  private final AliasSequence anonVertexAliases = new AliasSequence(ANON_VERTEX_ALIAS_PREFIX);

  /** Anonymous-edge alias sequence ({@code $g2m_edge_0}, {@code $g2m_edge_1}, …), minted by
   *  {@link #nextEdgeAlias()}; see {@link #anonVertexAliases}. */
  private final AliasSequence edgeAliases = new AliasSequence(EDGE_ALIAS_PREFIX);

  /** Convenience constructor with no schema snapshot — used by unit tests that exercise recogniser
   *  logic without a live session. Every property resolves as "not a declared String", so a
   *  {@code startingWith} routes to the strict full-scan form. Carries no registry, so it cannot drive
   *  a sub-walk. */
  WalkerContext(boolean polymorphic, boolean edgeLabelVerification) {
    this(polymorphic, edgeLabelVerification, null, null);
  }

  /** Registry-less constructor — used by unit tests that pin single-recogniser mutations without a
   *  sub-walk. */
  WalkerContext(boolean polymorphic, boolean edgeLabelVerification, @Nullable Schema schema) {
    this(polymorphic, edgeLabelVerification, schema, null);
  }

  /** Full constructor the walker uses: carries the recogniser registry so a combinator recogniser can
   *  drive a child sub-walk through {@link #walkChild}. */
  WalkerContext(
      boolean polymorphic,
      boolean edgeLabelVerification,
      @Nullable Schema schema,
      @Nullable Map<Class<?>, StepRecogniser> recognisers) {
    this.polymorphic = polymorphic;
    this.edgeLabelVerification = edgeLabelVerification;
    this.schema = schema;
    this.recognisers = recognisers;
  }

  // --- RecognitionContext: resolved flags -------------------------------------------------------

  @Override
  public boolean polymorphic() {
    return polymorphic;
  }

  /** The top-level walk is the one whose projection becomes the boundary's RETURN. */
  @Override
  public boolean projectsReturnedPayload() {
    return true;
  }

  @Override
  public boolean edgeLabelVerificationEnabled() {
    return edgeLabelVerification;
  }

  /**
   * Records {@code ProductiveByStrategy}'s productive-key set for the traversal being walked:
   * {@code null} when the strategy is absent, an empty set when every key is productive (the
   * strategy's own default), and otherwise the configured keys. Resolved once by the walker for
   * the same reason the polymorphism and edge-label-verification flags are.
   */
  void setProductiveByKeys(@Nullable Set<String> keys) {
    this.productiveByKeys = keys;
  }

  /**
   * Records whether a global-scope {@code order().by(key)} keeps a record missing {@code key}.
   * The walker resolves the value from the option, {@code StandardOrderSemanticsStrategy},
   * and the database setting.
   *
   * <p>The field starts {@code false}, the standard order semantics answer. A unit-test context
   * that never calls this setter keeps the pre-existing emission behavior. The context does not
   * inherit the shipped default.
   */
  void setOrderIncludesMissingKey(boolean orderIncludesMissingKey) {
    this.orderIncludesMissingKey = orderIncludesMissingKey;
  }

  @Override
  public boolean orderIncludesMissingKey() {
    return orderIncludesMissingKey;
  }

  void setOrderByNullsPlacements(ResolvedOrderByNullsPlacement placements) {
    orderByNullsPlacements = placements;
  }

  @Override
  public ResolvedOrderByNullsPlacement orderByNullsPlacements() {
    return orderByNullsPlacements;
  }

  /**
   * Membership in the configured set means <em>not</em> productive. {@code
   * ProductiveByStrategy.hasKeyNotKnownAsProductive} is
   * {@code productiveKeys.isEmpty() || !productiveKeys.contains(key)}, and the strategy wraps the
   * modulator in {@code coalesce(…, null)} — making it productive — exactly when that answers true.
   * A key the caller lists is one it asserts is already present on every element, so the strategy
   * leaves that {@code by(key)} alone and its Gremlin-side drop survives.
   */
  @Override
  public boolean byModulatorIsProductive(String propertyKey) {
    return productiveByKeys != null
        && (productiveByKeys.isEmpty() || !productiveByKeys.contains(propertyKey));
  }

  // --- RecognitionContext: boundary read --------------------------------------------------------

  @Nullable @Override
  public String boundaryAlias() {
    return boundaryAlias;
  }

  @Nullable @Override
  public String boundaryClassName() {
    return boundaryAlias == null ? null
        : patternBuilder.registeredAliasClasses().get(boundaryAlias);
  }

  @Nullable @Override
  public BoundaryOutputType boundaryOutputType() {
    return outputType;
  }

  @Nullable @Override
  public SQLOrderBy orderBy() {
    return orderBy;
  }

  @Nullable @Override
  public SQLLimit limit() {
    return limit;
  }

  @Nullable @Override
  public SQLSkip skip() {
    return skip;
  }

  @Override
  public boolean returnDistinct() {
    return returnDistinct;
  }

  // --- RecognitionContext: the fold latch --------------------------------------------------------

  @Override
  public boolean atTraversalStart() {
    return atTraversalStart;
  }

  @Override
  public void setAtTraversalStart(boolean atStart) {
    this.atTraversalStart = atStart;
  }

  // --- RecognitionContext: schema-aware type gating ---------------------------------------------

  @Override
  public boolean isDeclaredStringProperty(@Nullable String className, String propertyKey) {
    if (schema == null || className == null || propertyKey == null) {
      // No class context or no schema: the type is unknown, so it is not a *declared* String. The
      // caller (startingWith routing) then chooses the strict full-scan form.
      return false;
    }
    var clazz = schema.getClass(className);
    if (clazz == null) {
      return false;
    }
    // getProperty walks superclasses (per its own contract), so a property the leaf class inherits
    // is found too. No subclass sweep: a subclass cannot override an inherited property's type
    // (checkParametersConflict forbids type overrides), and a subclass-only property is not
    // declared on this class, so "declared String on className" is exactly this lookup.
    var property = clazz.getProperty(propertyKey);
    if (property == null) {
      return false;
    }
    return property.getType() == PropertyType.STRING;
  }

  @Override
  public boolean isVertexClass(String className) {
    if (schema == null || className == null) {
      // No schema to verify against: decline the re-type so a hasLabel never builds a scan over an
      // unverifiable class (the walker already declines a schema-less traversal, so this is defensive).
      return false;
    }
    var clazz = schema.getClass(className);
    return clazz != null && clazz.isVertexType();
  }

  // --- RecognitionContext: alias minting --------------------------------------------------------

  /** Mints the next anonymous vertex alias ({@code $g2m_anon_0}, {@code $g2m_anon_1}, …). Each call
   *  returns a fresh alias and advances the per-context counter, so a multi-hop chain gets distinct
   *  intermediate-node names. */
  @Override
  public String nextAnonVertexAlias() {
    return anonVertexAliases.next();
  }

  /** Mints the next anonymous edge alias ({@code $g2m_edge_0}, {@code $g2m_edge_1}, …). Each call
   *  returns a fresh alias and advances the per-context counter. */
  @Override
  public String nextEdgeAlias() {
    return edgeAliases.next();
  }

  // --- RecognitionContext: contributions --------------------------------------------------------

  @Override
  public void addNode(String alias, String className) {
    patternBuilder.addNode(alias, className, null, false);
  }

  @Override
  public void addEdge(
      String fromAlias,
      String toAlias,
      MatchPatternBuilder.Direction dir,
      @Nullable String edgeLabel) {
    patternBuilder.addEdge(fromAlias, toAlias, dir, edgeLabel, null, null, null);
  }

  @Override
  public void addEdgeAsNode(
      String fromAlias,
      String edgeAlias,
      String toAlias,
      MatchPatternBuilder.Direction edgeDir,
      @Nullable String edgeLabel,
      MatchPatternBuilder.Direction closingVertexDir,
      @Nullable SQLWhereClause edgeFilter) {
    patternBuilder.addEdgeAsNode(
        fromAlias, edgeAlias, toAlias, edgeDir, edgeLabel, closingVertexDir, edgeFilter);
  }

  @Override
  public void putAliasFilter(String alias, SQLWhereClause where) {
    var existing = aliasFilters.get(alias);
    if (existing == null) {
      aliasFilters.put(alias, where);
      return;
    }
    // A second contribution to the same alias AND-composes rather than replaces: a has(...)
    // recogniser routinely contributes two clauses to one alias — a g.V(ids) @rid IN then a
    // has(...) predicate, or a hasLabel(L) @class narrowing then a has(...) predicate. Overwriting
    // would silently drop the earlier filter and return a wrong (over-large) multiset.
    var merged = WHERE.and(existing.getBaseExpression(), where.getBaseExpression());
    aliasFilters.put(alias, WHERE.wrap(merged));
  }

  @Override
  public void putEdgeFilter(String edgeAlias, SQLWhereClause where) {
    edgeFilters.put(edgeAlias, where);
  }

  @Override
  public boolean positivePatternHasAlias(String alias) {
    return patternBuilder.hasAlias(alias);
  }

  @Override
  public void addNotMatchExpression(SQLMatchExpression expression) {
    notMatchExpressions.add(expression);
  }

  @Override
  public SQLPositionalParameter bindParam(Object value) {
    var slot = nextParamSlot++;
    inputParameters.put(slot, value);
    return SQLPositionalParameter.forSlot(slot);
  }

  @Override
  public void markRidBearing() {
    ridBearing = true;
  }

  /** Whether this walk is RID-bearing and must bypass the plan cache. */
  boolean ridBearing() {
    return ridBearing;
  }

  @Override
  public void pinBoundary(String alias, BoundaryOutputType type,
      Class<? extends Element> returnClass) {
    this.boundaryAlias = alias;
    this.outputType = type;
    this.returnClass = returnClass;
  }

  @Override
  public void setSingleReturnColumn(String alias) {
    // Clear first so a re-pin (a chain hop replacing the prior boundary's column) cannot leave a
    // stale column keyed on the previous alias; the three parallel lists stay in lock-step.
    returnItems.clear();
    returnAliases.clear();
    returnNestedProjections.clear();
    returnItems.add(MatchProjectionBuilder.aliasColumn(alias));
    returnAliases.add(MatchProjectionBuilder.columnAlias(alias));
    returnNestedProjections.add(null);
  }

  @Override
  public boolean bindStepLabels(Step<?, ?> step, String internalAlias) {
    var labels = GremlinStepLabels.userLabels(step);
    if (labels.isEmpty()) {
      return true;
    }
    for (String userLabel : labels) {
      var existing = userLabelToAlias.get(userLabel);
      if (existing != null && !existing.equals(internalAlias)) {
        return false;
      }
    }
    for (String userLabel : labels) {
      userLabelToAlias.put(userLabel, internalAlias);
      patternBuilder.registerUserLabel(internalAlias, userLabel);
    }
    return true;
  }

  @Nullable @Override
  public String resolveUserLabel(String userLabel) {
    return userLabelToAlias.get(userLabel);
  }

  @Override
  public void clearReturnProjection() {
    returnItems.clear();
    returnAliases.clear();
    returnNestedProjections.clear();
  }

  @Override
  public void appendReturnColumn(SQLExpression expression, @Nullable String returnAlias) {
    returnItems.add(expression);
    returnAliases.add(returnAlias == null ? null : MatchProjectionBuilder.columnAlias(returnAlias));
    returnNestedProjections.add(null);
  }

  @Override
  public List<SQLExpression> returnItems() {
    return returnItems;
  }

  @Override
  public void setReturnDistinct(boolean distinct) {
    this.returnDistinct = distinct;
  }

  @Override
  public void setGroupBy(@Nullable SQLGroupBy groupBy) {
    this.groupBy = groupBy;
  }

  @Nullable @Override
  public SQLGroupBy groupBy() {
    return groupBy;
  }

  @Override
  public void setOrderBy(@Nullable SQLOrderBy orderBy) {
    this.orderBy = orderBy;
  }

  @Override
  public void setLimit(@Nullable SQLLimit limit) {
    this.limit = limit;
  }

  @Override
  public void setSkip(@Nullable SQLSkip skip) {
    this.skip = skip;
  }

  @Override
  public void setResultShaping(@Nonnull ResultShaping shaping) {
    this.shaping = shaping;
    // A wholesale replace drops any alias a previous drop-on-absent write recorded. The writer that
    // turns dropOnAbsent back on has to re-declare it; UnionStepRecogniser's agreed-shaping path
    // does not, so a post-union slice cannot promote and stays declined.
    this.presenceDropAlias = null;
  }

  @Override
  public void setPresenceDropAlias(@Nullable String alias) {
    this.presenceDropAlias = alias;
  }

  @Override
  public boolean dropsRowsOnAbsentProperty() {
    return shaping.dropOnAbsent();
  }

  @Override
  public boolean promotePresenceDropToPatternFilter() {
    if (!shaping.dropOnAbsent()) {
      return true;
    }
    if (presenceDropAlias == null) {
      return false;
    }
    for (var key : shaping.presencePropertyKeys()) {
      ByModulatorPresence.requireProjectedProperty(this, presenceDropAlias, key);
    }
    // dropOnAbsent stays on: after the conjunct no row lacking the key survives, so the shaping
    // drop is redundant, but it cannot remove a row the conjunct left.
    return true;
  }

  @Override
  public void appendListShapingOp(@Nonnull ListShapingOp op) {
    var ops = new ArrayList<>(shaping.listShapingOps());
    ops.add(op);
    this.shaping = shaping.withListShapingOps(ops);
  }

  /**
   * Always true: the shaping this context holds is the one its own boundary base reads, so an
   * appended op reaches the projected payload stream. The answer is about that boundary rather than
   * about being the outermost walk — a union arm runs as its own top-level walk with its own
   * {@code WalkerContext} and answers {@code true} as well, and declining an arm whose trailing
   * {@code fold()} appended an op belongs to {@link UnionStepRecogniser}'s child loop. {@link
   * SubTraversalPredicateAdapter#supportsListShaping()} answers {@code false}, which is where the
   * decline for a combinator child comes from; {@link RecognitionContext#supportsListShaping()}
   * carries the canonical rationale for the whole channel.
   */
  @Override
  public boolean supportsListShaping() {
    return true;
  }

  /**
   * Reads the ops off the shaping this context holds, so the answer grows the moment {@link
   * #appendListShapingOp} lands one and empties if a later {@link #setResultShaping} replaces the
   * record. The walker's dispatch loop reads it as the terminators' last-step gate and as the
   * before/after comparison around each accept; {@link RecognitionContext#listShapingOps()} carries
   * the contract, including why the second reader needs the list rather than a flag.
   */
  @Nonnull
  @Override
  public List<ListShapingOp> listShapingOps() {
    return shaping.listShapingOps();
  }

  /** The boundary row-projection shaping the terminator pinned, read by the walker at result-build
   *  time. {@link ResultShaping#NONE} until a terminator sets it. */
  ResultShaping shaping() {
    return shaping;
  }

  @Override
  public void
      setLastPropertyProjection(@Nullable RecognitionContext.PropertyProjection projection) {
    this.lastPropertyProjection = projection;
  }

  @Nullable @Override
  public RecognitionContext.PropertyProjection lastPropertyProjection() {
    return lastPropertyProjection;
  }

  @Override
  public SubTraversalPredicateAdapter walkChild(Traversal.Admin<?, ?> child) {
    if (recognisers == null) {
      // Only a test-constructed registry-less context can reach here; the walker always builds the
      // context with its registry. Fail loud rather than silently declining, so a wiring bug surfaces
      // as an error instead of a mystery decline.
      throw new IllegalStateException(
          "walkChild requires a WalkerContext constructed with a recogniser registry");
    }
    return GremlinStepWalker.subWalk(child, this, recognisers);
  }

  /** Installs the union fork seam; called once by {@link GremlinStepWalker#walk}. */
  void setUnionForkHost(@Nonnull UnionForkHost host) {
    this.unionForkHost = host;
  }

  @Nullable @Override
  public UnionForkHost unionForkHost() {
    return unionForkHost;
  }

  /**
   * Stashes the ordered union-child plan inputs, positional-parameter maps, and per-child plan-cache
   * eligibility. Called via {@link UnionForkHost#stashAcceptedChildren} after the agreement gate
   * passes; {@code buildResult} then emits a multi-plan translation.
   */
  void stashUnionChildren(
      @Nonnull List<MatchPlanInputs> childInputs,
      @Nonnull List<Map<Object, Object>> childInputParameters,
      @Nonnull List<Boolean> childCacheEligible) {
    assert childInputs.size() == childInputParameters.size()
        : "union carrier requires one parameter map per child input";
    assert childInputs.size() == childCacheEligible.size()
        : "union carrier requires one cache-eligibility flag per child input";
    this.unionChildInputs = List.copyOf(childInputs);
    this.unionChildInputParameters =
        childInputParameters.stream().map(Map::copyOf).toList();
    this.unionChildCacheEligible = List.copyOf(childCacheEligible);
  }

  /** Whether {@link UnionStepRecogniser} accepted and stashed a multi-plan carrier. */
  @Override
  public boolean hasUnionCarrier() {
    return unionChildInputs != null && !unionChildInputs.isEmpty();
  }

  @Nonnull
  List<MatchPlanInputs> unionChildInputs() {
    assert hasUnionCarrier();
    return unionChildInputs;
  }

  @Nonnull
  List<Map<Object, Object>> unionChildInputParameters() {
    assert hasUnionCarrier();
    return unionChildInputParameters;
  }

  @Nonnull
  List<Boolean> unionChildCacheEligible() {
    assert hasUnionCarrier();
    return unionChildCacheEligible;
  }

  @Override
  public boolean anyUnionChildHasCardinalityClause() {
    assert hasUnionCarrier();
    for (var childInputs : unionChildInputs) {
      if (childInputs.limit() != null
          || childInputs.skip() != null
          || childInputs.returnDistinct()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void appendPostConcatOp(@Nonnull PostConcatOp op) {
    postConcatOps.add(op);
  }

  @Override
  @Nonnull
  public List<PostConcatOp> postConcatOps() {
    return List.copyOf(postConcatOps);
  }

  /**
   * Prefixed monotonic alias generator: one instance per alias namespace. Each {@link #next()}
   * returns {@code prefix + n} and advances the counter, so a namespace's aliases are distinct and
   * ordered ({@code prefix0}, {@code prefix1}, …). Reset is by construction — the enclosing {@link
   * WalkerContext} is rebuilt per walk, so each sequence restarts at 0 and stays deterministic per
   * query rather than monotonic across the JVM.
   */
  private static final class AliasSequence {

    private final String prefix;
    private int n;

    AliasSequence(String prefix) {
      this.prefix = prefix;
    }

    String next() {
      return prefix + n++;
    }
  }
}
