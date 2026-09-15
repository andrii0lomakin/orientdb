package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.lambda;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Collate;
import java.util.HashMap;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.junit.Test;

/**
 * The collated sort key compares two property values exactly as the engine comparison does, through
 * {@link Collate#compareForOrderBy}. No database is needed — a collation is a value type.
 */
public class CollatedSortKeyTest {

  private static final Collate CASE_INSENSITIVE = Collate.caseInsensitiveCollate();

  private static final Collate DEFAULT = Collate.defaultCollate();

  /**
   * Scenario: two names whose letter case is the only difference in their first character, under the
   * case-insensitive collation. Expected: alphabetical order, so the lower-case {@code ada} precedes
   * the capitalised {@code Bob}, where a code-point comparison puts every capital first.
   */
  @Test
  public void compareTo_caseInsensitiveCollation_ignoresLetterCase() {
    var ada = CollatedSortKey.of("ada", CASE_INSENSITIVE);
    var bob = CollatedSortKey.of("Bob", CASE_INSENSITIVE);

    assertThat(ada).isLessThan(bob);
  }

  /**
   * Scenario: two spellings of one name under the case-insensitive collation. Expected: they do not
   * tie — the collation falls back to the raw comparison when the folded forms match, which is what
   * keeps a tie group in a stable order on both arms.
   */
  @Test
  public void compareTo_caseInsensitiveCollation_breaksTiesOnTheRawValue() {
    var capitalised = CollatedSortKey.of("Ada", CASE_INSENSITIVE);
    var lowerCase = CollatedSortKey.of("ada", CASE_INSENSITIVE);

    assertThat(capitalised).isLessThan(lowerCase);
  }

  /**
   * Scenario: the same two names under the default collation. Expected: code-point order, so the
   * capital sorts first. The default collation must change nothing about plain comparison.
   */
  @Test
  public void compareTo_defaultCollation_comparesByCodePoint() {
    var ada = CollatedSortKey.of("ada", DEFAULT);
    var bob = CollatedSortKey.of("Bob", DEFAULT);

    assertThat(ada).isGreaterThan(bob);
  }

  /** A null map value projects as a real null, so the order comparator handles its placement. */
  @Test
  public void traversal_nullMapValue_producesRealNull() {
    var row = new HashMap<String, Object>();
    row.put("name", null);
    var traversal = new CollatedSortKeyTraversal<HashMap<String, Object>>(
        "name", CASE_INSENSITIVE);

    traversal.addStart(new B_O_Traverser<>(row, 1L));

    assertThat(traversal.next()).isNull();
  }

  /** A present null element property projects as a real null before collation. */
  @Test
  public void traversal_nullElementProperty_producesRealNull() {
    var element = mock(Element.class);
    @SuppressWarnings("unchecked")
    var property = (Property<Object>) mock(Property.class);
    when(element.property("name")).thenReturn(property);
    when(property.isPresent()).thenReturn(true);
    when(property.value()).thenReturn(null);
    var traversal = new CollatedSortKeyTraversal<Element>("name", CASE_INSENSITIVE);

    traversal.addStart(new B_O_Traverser<>(element, 1L));

    assertThat(traversal.next()).isNull();
  }

  /** A productive bypass that returns null also projects a real null before collation. */
  @Test
  public void traversal_nullBypassValue_producesRealNull() {
    var traversal = new CollatedSortKeyTraversal<Object>("name", CASE_INSENSITIVE);
    traversal.setBypassTraversal(new ConstantTraversal<>(null));

    traversal.addStart(new B_O_Traverser<>("ignored", 1L));

    assertThat(traversal.next()).isNull();
  }

  /**
   * Scenario: a text value against a number, which one schema-less column can hold. Expected: they
   * are reported equal rather than throwing, which is what the engine comparison answers for the
   * same pair, so a sort over such a column returns rows on both arms.
   */
  @Test
  public void compareTo_incompatibleValueClasses_reportsEquality() {
    var text = CollatedSortKey.of("ada", CASE_INSENSITIVE);
    var number = CollatedSortKey.of(42, CASE_INSENSITIVE);

    assertThat(text).isEqualByComparingTo(number);
  }

  /** Two keys over one value are equal and hash alike, whatever their letter case folds to. */
  @Test
  public void equals_sameValueAndCollation_areEqual() {
    var first = CollatedSortKey.of("ada", CASE_INSENSITIVE);
    var second = CollatedSortKey.of("ada", CASE_INSENSITIVE);

    assertThat(first).isEqualTo(second).isEqualTo(first);
    assertThat(first.hashCode()).isEqualTo(second.hashCode());
    assertThat(first.value()).isEqualTo("ada");
  }

  /** A key of one collation is not equal to a key of another, because they order differently. */
  @Test
  public void equals_differentCollations_areNotEqual() {
    assertThat(CollatedSortKey.of("ada", CASE_INSENSITIVE))
        .isNotEqualTo(CollatedSortKey.of("ada", DEFAULT))
        .isNotEqualTo("ada");
  }

  /** The text form names the collation and the value, so a shape key stays stable across runs. */
  @Test
  public void toString_namesTheCollationAndTheValue() {
    assertThat(CollatedSortKey.of("Ada", CASE_INSENSITIVE)).hasToString("ci(Ada)");
  }
}
