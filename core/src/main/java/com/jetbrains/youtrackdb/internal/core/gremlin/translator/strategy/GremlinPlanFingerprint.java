package com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy;

import com.jetbrains.youtrackdb.internal.core.sql.executor.match.MatchPlanInputs;
import com.jetbrains.youtrackdb.internal.core.sql.executor.match.PatternNode;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchPathItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

/**
 * Synthesises a value-independent fingerprint from post-walk {@link MatchPlanInputs} for the
 * {@link GremlinPlanCache}. The key enumerates every planner-visible field on the record: positive
 * pattern topology (including per-node {@code optional}), alias classes, alias filters, positive
 * {@code matchExpressions}, detached NOT expressions, return projection (items / aliases / nested
 * projections), result-shaping ({@code GROUP BY} / {@code ORDER BY} / {@code UNWIND} / {@code LIMIT}
 * / {@code SKIP} / {@code DISTINCT}), and return-mode flags ({@code $elements} / {@code $paths} /
 * {@code $patterns} / {@code $pathElements}). It never uses {@link
 * com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchStatement#toGenericStatement()}, which
 * omits {@code notMatchExpressions}. Positional parameters render as {@code ?}; structural tokens
 * (class names, {@code ~label}, RIDs, type-guard literals) stay verbatim so distinct labels and NOT
 * shapes do not collide. Limit / skip literals stay in the key (they are not positional slots), so
 * {@code limit(2)} and {@code limit(5)} cannot share a cached plan.
 *
 * <p><b>Every variable-length token is length-prefixed</b> ({@code len:content}) through {@link
 * #appendToken} / {@link #appendRendered}. The key is shared across sessions on one database and
 * carries untrusted identifiers (property keys, {@code as} / {@code select} labels, {@code
 * hasLabel} classes), so a raw concatenation with fixed delimiters ({@code [ ] : ; -> AS}) would
 * let a crafted identifier embedding those delimiters reproduce another walk's key and be served
 * its cached plan. Length-prefixing makes each token self-delimiting, so no combination of
 * user strings can forge a collision. Section markers ({@code P:} / {@code ;E:} / {@code ;F:} / …)
 * stay as fixed separators between the length-prefixed runs.
 */
final class GremlinPlanFingerprint {

  private static final Map<Object, Object> NO_PARAMS = Collections.emptyMap();

  private GremlinPlanFingerprint() {
    // Static utility — no instances.
  }

  /**
   * Builds the cache key for {@code inputs}. Callers must pass the pre-plan {@link MatchPlanInputs}
   * snapshot while {@code aliasFilters} is still insertion-ordered.
   */
  static String fingerprint(@Nonnull MatchPlanInputs inputs) {
    var sb = new StringBuilder(256);
    appendPattern(sb, inputs);
    appendAliasFilters(sb, inputs.aliasFilters());
    appendMatchExpressions(sb, inputs.matchExpressions());
    appendNotExpressions(sb, inputs.notMatchExpressions());
    appendReturnProjection(sb, inputs);
    appendResultShaping(sb, inputs);
    appendReturnModes(sb, inputs);
    return sb.toString();
  }

  /**
   * Appends a variable-length token as {@code len:content} so any delimiter characters inside
   * {@code token} cannot merge with or split from adjacent tokens. Injectivity, not readability, is
   * the goal — the key is never parsed back, only compared.
   */
  private static void appendToken(StringBuilder sb, String token) {
    sb.append(token.length()).append(':').append(token);
  }

  /**
   * Renders {@code render} into a scratch buffer and appends the result as one length-prefixed
   * token, so an AST fragment (a filter, a projection item, a NOT expression) that emits
   * user-controlled identifiers cannot forge a collision through embedded delimiters.
   */
  private static void appendRendered(StringBuilder sb, Consumer<StringBuilder> render) {
    var scratch = new StringBuilder();
    render.accept(scratch);
    appendToken(sb, scratch.toString());
  }

  private static void appendPattern(StringBuilder sb, MatchPlanInputs inputs) {
    sb.append("P:");
    var pattern = inputs.pattern();
    var aliasClasses = inputs.aliasClasses();
    for (var entry : pattern.aliasToNode.entrySet()) {
      appendToken(sb, entry.getKey());
      var cls = aliasClasses.get(entry.getKey());
      appendToken(sb, cls == null ? "" : cls);
      // optional:true changes scheduler / null-row behaviour; omitting it would let a required
      // hop and an optional hop share a plan.
      sb.append(entry.getValue().optional ? '1' : '0');
    }
    sb.append(";E:");
    for (PatternNode node : pattern.aliasToNode.values()) {
      for (var edge : node.out) {
        appendToken(sb, edge.out.alias);
        appendToken(sb, edge.in.alias);
        appendRendered(sb, scratch -> appendPathItemStructural(scratch, edge.item));
      }
    }
  }

  /** Renders a path item with edge labels and direction verbatim (not collapsed to {@code ?}). */
  private static void appendPathItemStructural(StringBuilder sb, SQLMatchPathItem item) {
    item.toString(NO_PARAMS, sb);
  }

  private static void appendMatchExpressionStructural(StringBuilder sb, SQLMatchExpression expr) {
    if (expr.getOrigin() != null) {
      expr.getOrigin().toString(NO_PARAMS, sb);
    }
    for (var item : expr.getItems()) {
      appendPathItemStructural(sb, item);
    }
  }

  /**
   * Appends each alias filter, rendered with {@code toString(NO_PARAMS, …)} rather than
   * {@code toGenericStatement}, for the same reason limit / skip below use it: an alias filter can
   * carry a literal that is part of the plan's <em>shape</em> rather than a rebindable value, and
   * {@code toGenericStatement} collapses every literal — including those — to {@code ?}.
   *
   * <p>The per-record range type guard is the case that forces it. It emits
   * {@code key.type() IN ['STRING']} with the comparability-block names as inline string literals,
   * so under {@code toGenericStatement} a guard naming {@code STRING} and a guard naming
   * {@code BOOLEAN} both render as {@code IN [?]} and produce a byte-identical key. The second
   * traversal to compile would then be served the first one's cached plan, guard included, and
   * answer a different row set. Only the {@code ;F:} section is exposed — {@code ;E:} already
   * renders bound path items verbatim — so an edge-free pattern (a root-level {@code not(…)} or
   * {@code or(…)} arm, a filter behind a barrier) is where the collision lands.
   *
   * <p>Value-independence is preserved. Every production comparison value binds through
   * {@code ParamSink} into an {@link
   * com.jetbrains.youtrackdb.internal.core.sql.parser.SQLPositionalParameter}, which renders as
   * {@code null} under an empty parameter map, so two traversals differing only in a bound value
   * still share one key and still share one plan.
   */
  private static void appendAliasFilters(StringBuilder sb,
      Map<String, SQLWhereClause> aliasFilters) {
    sb.append(";F:");
    for (var entry : aliasFilters.entrySet()) {
      appendToken(sb, entry.getKey());
      appendRendered(sb, scratch -> entry.getValue().toString(NO_PARAMS, scratch));
    }
  }

  private static void appendNotExpressions(StringBuilder sb,
      List<SQLMatchExpression> notExprs) {
    sb.append(";N:");
    for (var notExpr : notExprs) {
      appendRendered(sb, scratch -> appendMatchExpressionStructural(scratch, notExpr));
    }
  }

  /**
   * Positive MATCH expression list. Gremlin today leaves this empty and builds {@link
   * MatchPlanInputs#pattern()} directly; SQL still carries the expressions. Fingerprinting them
   * keeps two inputs that differ only in {@code matchExpressions} from sharing a plan if a future
   * front-end sets them without regenerating an equivalent pattern topology.
   */
  private static void appendMatchExpressions(StringBuilder sb,
      List<SQLMatchExpression> matchExprs) {
    sb.append(";M:");
    for (var expr : matchExprs) {
      appendRendered(sb, scratch -> appendMatchExpressionStructural(scratch, expr));
    }
  }

  private static void appendReturnProjection(StringBuilder sb, MatchPlanInputs inputs) {
    sb.append(";R:");
    var items = inputs.returnItems();
    var aliases = inputs.returnAliases();
    var nestedProjections = inputs.returnNestedProjections();
    for (int i = 0; i < items.size(); i++) {
      var item = items.get(i);
      var alias = aliases.get(i);
      var nestedProjection = nestedProjections.get(i);
      appendRendered(sb, item::toGenericStatement);
      // Mark alias presence with a fixed byte, then length-prefix the alias itself, so a null alias
      // and an empty-string alias stay distinct and an alias embedding delimiters cannot merge with
      // the next item.
      if (alias == null) {
        sb.append('-');
      } else {
        sb.append('+');
        appendRendered(sb, alias::toGenericStatement);
      }
      // Nested projections are planner-visible output shape, stored parallel to the return item.
      // Omitting them would let `RETURN a` and `RETURN a:{name}` (or two different nested
      // projections on the same item) share a cached plan key even though the planner carries the
      // projection structure through MatchPlanInputs.
      if (nestedProjection == null) {
        sb.append('-');
      } else {
        sb.append('+');
        appendRendered(sb, nestedProjection::toGenericStatement);
      }
    }
  }

  /**
   * Appends Track 6 result-shaping clauses. Limit / skip use {@code toString} (not
   * {@code toGenericStatement}): {@link com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNumber}
   * collapses every integer to {@code ?}, which would let {@code limit(2)} and {@code limit(5)}
   * collide. Group / order / unwind keep {@code toGenericStatement} — their discriminators are
   * property names, directions, and explicit null placements, not rebound literals.
   */
  private static void appendResultShaping(StringBuilder sb, MatchPlanInputs inputs) {
    sb.append(";G:");
    if (inputs.groupBy() != null) {
      appendRendered(sb, scratch -> inputs.groupBy().toGenericStatement(scratch));
    }
    sb.append(";O:");
    if (inputs.orderBy() != null) {
      appendRendered(sb, scratch -> inputs.orderBy().toGenericStatement(scratch));
    }
    sb.append(";U:");
    if (inputs.unwind() != null) {
      appendRendered(sb, scratch -> inputs.unwind().toGenericStatement(scratch));
    }
    sb.append(";L:");
    if (inputs.limit() != null) {
      appendRendered(sb, scratch -> inputs.limit().toString(NO_PARAMS, scratch));
    }
    sb.append(";S:");
    if (inputs.skip() != null) {
      appendRendered(sb, scratch -> inputs.skip().toString(NO_PARAMS, scratch));
    }
    sb.append(";D:").append(inputs.returnDistinct());
  }

  /**
   * Return-mode flags ({@code RETURN $elements} / {@code $paths} / {@code $patterns} /
   * {@code $pathElements}). These select different planner projection paths; omitting them would
   * let e.g. {@code RETURN a} and {@code RETURN $paths} collide when both happen to carry empty
   * return-item lists after normalisation.
   */
  private static void appendReturnModes(StringBuilder sb, MatchPlanInputs inputs) {
    sb.append(";RM:")
        .append(inputs.returnElements() ? '1' : '0')
        .append(inputs.returnPaths() ? '1' : '0')
        .append(inputs.returnPatterns() ? '1' : '0')
        .append(inputs.returnPathElements() ? '1' : '0');
  }
}
