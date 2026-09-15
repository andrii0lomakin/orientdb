package com.jetbrains.youtrackdb.internal.common.comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem;
import java.lang.reflect.Proxy;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

public class GremlinOrderComparatorTest {

  @Test
  public void ordersEveryAdjacentTinkerPopTypePriority() {
    Object[] values = {
        null,
        false,
        1,
        new Date(1),
        "a",
        UUID.fromString("00000000-0000-0000-0000-000000000001"),
        element(Vertex.class),
        element(Edge.class),
        element(VertexProperty.class),
        property(),
        path(),
        Set.of(1),
        List.of(1),
        Map.of("a", 1),
        Map.entry("a", 1),
        new Object()
    };

    for (int i = 0; i < values.length - 1; i++) {
      assertTrue("priority at " + i,
          GremlinOrderComparator.INSTANCE.compare(values[i], values[i + 1]) < 0);
    }
  }

  /**
   * Translator provenance enables TinkerPop type priority only for translated items. Unmarked SQL
   * ordering retains its legacy incompatible-type tie.
   */
  @Test
  public void translatorMarkerEnablesTypePriorityForOrderItem() {
    var text = new ResultInternal(null);
    text.setProperty("value", "10");
    var bool = new ResultInternal(null);
    bool.setProperty("value", true);

    var marked = new SQLOrderByItem();
    marked.setAlias("value");
    marked.setGremlinToMatchTranslatorProduced(true);
    var unmarked = new SQLOrderByItem();
    unmarked.setAlias("value");

    assertTrue(marked.compare(text, bool, null, ResolvedOrderByNullsPlacement.SHIPPED) > 0);
    assertEquals(
        0, unmarked.compare(text, bool, null, ResolvedOrderByNullsPlacement.SHIPPED));
  }

  @Test
  public void comparesNestedListsLexicographically() {
    assertTrue(
        GremlinOrderComparator.INSTANCE.compare(List.of(List.of(1, 2)), List.of(List.of(1, 3)))
            < 0);
  }

  @Test
  public void comparesSetsAfterSortingTheirContents() {
    Set<Integer> first = new LinkedHashSet<>(List.of(3, 1));
    Set<Integer> second = new LinkedHashSet<>(List.of(3, 2));

    assertTrue(GremlinOrderComparator.INSTANCE.compare(first, second) < 0);
  }

  @Test
  public void comparesMapsBySortedEntries() {
    Map<String, Integer> first = new LinkedHashMap<>();
    first.put("b", 1);
    first.put("a", 1);
    Map<String, Integer> second = new LinkedHashMap<>();
    second.put("b", 1);
    second.put("a", 2);

    assertTrue(GremlinOrderComparator.INSTANCE.compare(first, second) < 0);
  }

  private static Object element(Class<?> type) {
    return Proxy.newProxyInstance(
        type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
          if (method.getName().equals("id")) {
            return 1;
          }
          return null;
        });
  }

  private static Property<Object> property() {
    return (Property<Object>) Proxy.newProxyInstance(
        Property.class.getClassLoader(), new Class<?>[] {Property.class}, (proxy, method, args) -> {
          if (method.getName().equals("key")) {
            return "a";
          }
          if (method.getName().equals("value")) {
            return 1;
          }
          return null;
        });
  }

  private static Path path() {
    return (Path) Proxy.newProxyInstance(
        Path.class.getClassLoader(), new Class<?>[] {Path.class}, (proxy, method, args) -> {
          if (method.getName().equals("iterator")) {
            return List.of(1).iterator();
          }
          return null;
        });
  }
}
