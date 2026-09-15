package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.lambda;

import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Collate;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A {@code by(propertyKey)} modulator that projects the property to a {@link CollatedSortKey}, so a
 * native sort follows the collation the property declares instead of TinkerPop orderability.
 * Installed by {@code YTDBOrderCollationStrategy} in place of the {@link ValueTraversal} that
 * {@code by(propertyKey)} builds.
 *
 * <h2>It reproduces the end-of-stream behavior of a plain property projection</h2>
 *
 * {@code OrderGlobalStep} projects each modulator once per traverser and drops a traverser whose
 * projection is non-productive, which is how {@code g.V().order().by(k)} excludes an element that
 * carries no {@code k}. {@link ValueTraversal} produces that drop by reporting no starts for an
 * absent property, and this class does the same: the drop is part of the answer, not an
 * implementation detail of the comparison, so replacing the modulator must not change it.
 *
 * <p>The class is {@code final} and holds no subtype, which is also the idempotence marker: the
 * installing strategy replaces a {@link ValueTraversal} only, so a second application finds this
 * type instead and changes nothing.
 */
public final class CollatedSortKeyTraversal<S> extends AbstractLambdaTraversal<S, CollatedSortKey> {

  private final String propertyKey;

  private final Collate collate;

  /** The projection of the most recent start, read back by {@link #next()}. */
  @Nullable private CollatedSortKey key;

  /** {@code true} when the most recent start carried no such property — see the class Javadoc. */
  private boolean noStarts;

  public CollatedSortKeyTraversal(@Nonnull String propertyKey, @Nonnull Collate collate) {
    this.propertyKey = propertyKey;
    this.collate = collate;
  }

  @Nonnull
  public String getPropertyKey() {
    return propertyKey;
  }

  @Nonnull
  public Collate getCollate() {
    return collate;
  }

  @Override
  public @Nullable CollatedSortKey next() {
    if (noStarts) {
      throw new NoSuchElementException(this + " is empty");
    }
    return key;
  }

  @Override
  public boolean hasNext() {
    return !noStarts;
  }

  @Override
  public void addStart(Traverser.Admin<S> start) {
    if (bypassTraversal != null) {
      // A bypass traversal is installed by ProductiveByStrategy, which exists to turn the drop
      // above into a null row. Its answer is honoured rather than second-guessed.
      //
      // Read through Iterator<?> on purpose: the bypass yields the raw property value, while its
      // declared element type is this traversal's own key type, so a typed read would compile to a
      // cast that fails on the first row.
      Iterator<?> values = TraversalUtil.applyAll(start, bypassTraversal);
      if (values.hasNext()) {
        key = collate(values.next());
      } else {
        noStarts = true;
      }
      return;
    }
    var value = start.get();
    if (value instanceof Element element) {
      var property = element.property(propertyKey);
      if (property.isPresent()) {
        key = collate(property.value());
      } else {
        noStarts = true;
      }
    } else if (value instanceof Map<?, ?> map) {
      // A map row has no absent-key drop: reading a missing key yields null, which is what
      // ValueTraversal does with the same row.
      key = collate(map.get(propertyKey));
    } else {
      throw new IllegalStateException(
          "The by(\"" + propertyKey + "\") modulator can only be applied to a traverser that is an"
              + " Element or a Map - it is being applied to [" + value + "] a "
              + (value == null ? "null" : value.getClass().getSimpleName()) + " class instead");
    }
  }

  /** Returns a real null so the order comparator places nulls before applying collation. */
  @Nullable private CollatedSortKey collate(@Nullable Object value) {
    return value == null ? null : CollatedSortKey.of(value, collate);
  }

  @Override
  public void reset() {
    super.reset();
    noStarts = false;
  }

  /** Stable text form — the shape cache and the step renderers both key on a modulator's text. */
  @Override
  public String toString() {
    return "collated(" + propertyKey + "," + collate.getName() + ")";
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof CollatedSortKeyTraversal<?> traversal
        && propertyKey.equals(traversal.propertyKey)
        && collate.getName().equals(traversal.collate.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(CollatedSortKeyTraversal.class, propertyKey, collate.getName());
  }
}
