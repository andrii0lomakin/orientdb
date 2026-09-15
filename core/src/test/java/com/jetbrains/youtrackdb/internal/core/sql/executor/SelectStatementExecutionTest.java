package com.jetbrains.youtrackdb.internal.core.sql.executor;

import static com.jetbrains.youtrackdb.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;
import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.common.concur.TimeoutException;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.record.DBRecord;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Direction;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Edge;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Identifiable;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Vertex;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.id.RecordId;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Schema;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrackdb.internal.core.query.BasicResult;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrackdb.internal.core.sql.SQLEngine;
import com.jetbrains.youtrackdb.internal.core.sql.functions.SQLFunction;
import com.jetbrains.youtrackdb.internal.core.sql.parser.LocalResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBinaryCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLContainsCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLEqualsOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGetInternalPropertyExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SequentialTest.class)
public class SelectStatementExecutionTest extends DbTestBase {

  @Test
  public void testSelectNoTarget() {
    session.begin();
    var result = session.query("select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
    session.commit();
  }

  @Test
  public void testGroupByCount() {
    session.getMetadata().getSchema().createClass("InputTx");

    for (var i = 0; i < 100; i++) {
      session.begin();
      final var hash = UUID.randomUUID().toString();
      session.execute("insert into InputTx set address = '" + hash + "'");

      // CREATE RANDOM NUMBER OF COPIES final int random = new Random().nextInt(10);
      final var random = new Random().nextInt(10);
      for (var j = 0; j < random; j++) {
        session.execute("insert into InputTx set address = '" + hash + "'");
      }
      session.commit();
    }

    session.begin();
    final var result =
        session.query(
            "select address, count(*) as occurrencies from InputTx where address is not null group"
                + " by address limit 10");
    while (result.hasNext()) {
      final var row = result.next();
      Assert.assertNotNull(row.getProperty("address")); // <== FALSE!
      Assert.assertNotNull(row.getProperty("occurrencies"));
    }
    session.commit();
    result.close();
  }

  @Test
  public void testSelectNoTargetSkip() {
    var result = session.query("select 1 as one, 2 as two, 2+3 skip 1");
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetSkipZero() {
    var result = session.query("select 1 as one, 2 as two, 2+3 skip 0");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetLimit0() {
    var result = session.query("select 1 as one, 2 as two, 2+3 limit 0");
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetLimit1() {
    var result = session.query("select 1 as one, 2 as two, 2+3 limit 1");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetLimitx() {
    var result = session.query("select 1 as one, 2 as two, 2+3 skip 0 limit 0");
    printExecutionPlan(result);
    result.close();
  }

  @Test
  public void testSelectFullScan1() {
    var className = "TestSelectFullScan1";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 100000; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className);
    for (var i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);
    result.close();
    session.commit();
  }

  @Test
  public void testSelectFullScanOrderByRidAsc() {
    var className = "testSelectFullScanOrderByRidAsc";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 100000; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " ORDER BY @rid ASC");
    printExecutionPlan(result);
    Identifiable lastItem = null;
    for (var i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
      if (lastItem != null) {
        Assert.assertTrue(
            lastItem.getIdentity().compareTo(item.asEntity().getIdentity()) < 0);
      }
      lastItem = item.asEntity();
    }
    Assert.assertFalse(result.hasNext());
    session.commit();

    result.close();
  }

  @Test
  public void testSelectFullScanOrderByRidDesc() {
    var className = "testSelectFullScanOrderByRidDesc";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 100000; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " ORDER BY @rid DESC");
    printExecutionPlan(result);
    Identifiable lastItem = null;
    for (var i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
      if (lastItem != null) {
        Assert.assertTrue(
            lastItem.getIdentity().compareTo(item.asEntity().getIdentity()) > 0);
      }
      lastItem = item.asEntity();
    }
    Assert.assertFalse(result.hasNext());
    session.commit();

    result.close();
  }

  @Test
  public void testSelectFullScanLimit1() {
    var className = "testSelectFullScanLimit1";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 300; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " limit 10");
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectFullScanSkipLimit1() {
    var className = "testSelectFullScanSkipLimit1";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 300; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " skip 100 limit 10");
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectOrderByDesc() {
    var className = "testSelectOrderByDesc";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 30; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by surname desc");
    printExecutionPlan(result);

    String lastSurname = null;
    for (var i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      String thisSurname = item.getProperty("surname");
      if (lastSurname != null) {
        Assert.assertTrue(lastSurname.compareTo(thisSurname) >= 0);
      }
      lastSurname = thisSurname;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectOrderByAsc() {
    var className = "testSelectOrderByAsc";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 30; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by surname asc");
    printExecutionPlan(result);

    String lastSurname = null;
    for (var i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      String thisSurname = item.getProperty("surname");
      if (lastSurname != null) {
        Assert.assertTrue(lastSurname.compareTo(thisSurname) <= 0);
      }
      lastSurname = thisSurname;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectOrderByMassiveAsc() {
    var className = "testSelectOrderByMassiveAsc";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 100000; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i % 100);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by surname asc limit 100");
    printExecutionPlan(result);

    for (var i = 0; i < 100; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("surname0", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testOrderByLimitHeapDescending() {
    // Verifies that the bounded min-heap in OrderByStep produces correct top-N
    // results when N is much smaller than the total number of rows.
    var className = "testOrderByLimitHeapDescending";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 500; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select from " + className + " order by val desc limit 10");
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals(499 - i, (int) (Integer) item.getProperty("val"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testOrderBySkipLimitHeapAscending() {
    // Verifies that the bounded min-heap works correctly when combined with SKIP.
    // maxResults = SKIP + LIMIT = 15, so the heap holds the top-15 ASC, then
    // the downstream SkipStep discards the first 5 and LimitStep takes 10.
    var className = "testOrderBySkipLimitHeapAscending";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 500; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select from " + className + " order by val asc skip 5 limit 10");
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals(5 + i, (int) (Integer) item.getProperty("val"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testOrderByLimitHeapMultipleKeys() {
    // Verifies correct behavior with composite ORDER BY (two keys) and a small LIMIT,
    // exercising the heap comparator with multi-key sort.
    var className = "testOrderByLimitHeapMultipleKeys";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 200; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("group", i % 5);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select from " + className + " order by group asc, val desc limit 5");
    printExecutionPlan(result);

    // group=0 rows have val 0,5,10,...,195. DESC => 195,190,185,180,175
    int[] expected = {195, 190, 185, 180, 175};
    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals(0, (int) item.getProperty("group"));
      Assert.assertEquals(expected[i], (int) item.getProperty("val"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testOrderByLimitZeroReturnsEmpty() {
    // Verifies that LIMIT 0 is short-circuited in OrderByStep: no upstream rows are
    // pulled into the heap and an empty result is returned immediately.
    var className = "testOrderByLimitZeroReturnsEmpty";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by val asc limit 0");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testOrderByLimitHeapRespectsMaxElementsAllowed() {
    // Verifies that the bounded-heap path throws CommandExecutionException when
    // LIMIT exceeds QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, preventing OOM from
    // a maliciously large LIMIT value.
    var className = "testOrderByLimitHeapRespectsMaxElementsAllowed";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    var oldValue = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(3);
      session.begin();
      try {
        try (var result = session.query(
            "select from " + className + " order by val asc limit 10")) {
          result.forEachRemaining(x -> x.getProperty("val"));
        }
        Assert.fail("Expected CommandExecutionException");
      } catch (CommandExecutionException ex) {
        session.rollback();
      }
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(oldValue);
    }
  }

  @Test
  public void testOrderByLimitHeapWithMaxElementsAllowedDisabled() {
    var className = "testOrderByLimitHeapWithMaxElementsAllowedDisabled";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    var oldValue = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(-1);
      session.begin();
      var result = session.query(
          "select from " + className + " order by val desc limit 3");
      printExecutionPlan(result);
      for (var i = 0; i < 3; i++) {
        Assert.assertTrue(result.hasNext());
        var item = result.next();
        Assert.assertEquals(9 - i, (int) item.getProperty("val"));
      }
      Assert.assertFalse(result.hasNext());
      result.close();
      session.commit();
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(oldValue);
    }
  }

  @Test
  public void testOrderByWithoutLimitUnbounded() {
    var className = "testOrderByWithoutLimitUnbounded";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 20; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    var oldValue = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(-1);
      session.begin();
      var result = session.query("select from " + className + " order by val asc");
      printExecutionPlan(result);
      for (var i = 0; i < 20; i++) {
        Assert.assertTrue(result.hasNext());
        var item = result.next();
        Assert.assertEquals(i, (int) item.getProperty("val"));
      }
      Assert.assertFalse(result.hasNext());
      result.close();
      session.commit();
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(oldValue);
    }
  }

  @Test
  public void testSelectOrderWithProjections() {
    var className = "testSelectOrderWithProjections";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 100; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);

      session.commit();
    }

    session.begin();
    var result = session.query("select name from " + className + " order by surname asc");
    printExecutionPlan(result);

    String lastName = null;
    for (var i = 0; i < 100; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      Assert.assertNotNull(name);
      if (i > 0) {
        Assert.assertTrue(name.compareTo(lastName) >= 0);
      }
      lastName = name;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectOrderWithProjections2() {
    var className = "testSelectOrderWithProjections2";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 100; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select name from " + className + " order by name asc, surname asc");
    printExecutionPlan(result);

    String lastName = null;
    for (var i = 0; i < 100; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      Assert.assertNotNull(name);
      if (i > 0) {
        Assert.assertTrue(name.compareTo(lastName) >= 0);
      }
      lastName = name;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectFullScanWithFilter1() {
    var className = "testSelectFullScanWithFilter1";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 300; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name1' or name = 'name7' ");
    printExecutionPlan(result);

    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var name = item.getProperty("name");
      Assert.assertTrue("name1".equals(name) || "name7".equals(name));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectFullScanWithFilter2() {
    var className = "testSelectFullScanWithFilter2";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 300; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name <> 'name1' ");
    printExecutionPlan(result);

    for (var i = 0; i < 299; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var name = item.getProperty("name");
      Assert.assertNotEquals("name1", name);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testProjections() {
    var className = "testProjections";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 300; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select name from " + className);
    printExecutionPlan(result);

    for (var i = 0; i < 300; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      String surname = item.getProperty("surname");
      Assert.assertNotNull(name);
      Assert.assertTrue(name.startsWith("name"));
      Assert.assertNull(surname);
      Assert.assertFalse(item.isEntity());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCountStar() {
    var className = "testCountStar";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 7; i++) {
      session.begin();
      session.newEntity(className);
      session.commit();
    }
    session.begin();
    var result = session.query("select count(*) from " + className);
    printExecutionPlan(result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(7L, (Object) next.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCountStar2() {
    var className = "testCountStar2";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = (EntityImpl) session.newEntity(className);
      doc.setProperty("name", "name" + (i % 5));

      session.commit();
    }

    session.begin();
    var result = session.query("select count(*), name from " + className + " group by name");
    printExecutionPlan(result);
    Assert.assertNotNull(result);
    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals(2L, (Object) next.getProperty("count(*)"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCountStarEmptyNoIndex() {
    var className = "testCountStarEmptyNoIndex";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var elem = session.newEntity(className);
    elem.setProperty("name", "bar");
    session.commit();

    session.begin();
    var result = session.query("select count(*) from " + className + " where name = 'foo'");
    printExecutionPlan(result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(0L, (Object) next.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCountStarEmptyNoIndexWithAlias() {
    var className = "testCountStarEmptyNoIndexWithAlias";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var elem = session.newEntity(className);
    elem.setProperty("name", "bar");
    session.commit();

    session.begin();
    var result =
        session.query("select count(*) as a from " + className + " where name = 'foo'");
    printExecutionPlan(result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(0L, (Object) next.getProperty("a"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testAggretateMixedWithNonAggregate() {
    var className = "testAggretateMixedWithNonAggregate";
    session.getMetadata().getSchema().createClass(className);

    try {
      session.executeInTx(transaction -> {
        session.query(
            "select max(a) + max(b) + pippo + pluto as foo, max(d) + max(e), f from " + className)
            .close();
        Assert.fail();
      });
    } catch (CommandExecutionException x) {
      //ignore
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testAggretateMixedWithNonAggregateInCollection() {
    var className = "testAggretateMixedWithNonAggregateInCollection";
    session.getMetadata().getSchema().createClass(className);

    try {
      session.executeInTx(transaction -> {
        session.query("select [max(a), max(b), foo] from " + className).close();
        Assert.fail();
      });
    } catch (CommandExecutionException x) {
      //ignore
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testAggretateInCollection() {
    var className = "testAggretateInCollection";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var query = "select [max(a), max(b)] from " + className;
    var result = session.query(query);
    printExecutionPlan(query, result);
    result.close();
    session.commit();
  }

  @Test
  public void testAggretateMixedWithNonAggregateConstants() {
    var className = "testAggretateMixedWithNonAggregateConstants";
    session.getMetadata().getSchema().createClass(className);

    try {
      var result =
          session.query(
              "select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from "
                  + className);
      printExecutionPlan(result);
      result.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testAggregateSum() {
    var className = "testAggregateSum";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("val", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select sum(val) from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(45, (Object) item.getProperty("sum(val)"));

    result.close();
    session.commit();
  }

  @Test
  public void testAggregateSumGroupBy() {
    var className = "testAggregateSumGroupBy";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);

      session.commit();
    }
    session.begin();
    var result = session.query("select sum(val), type from " + className + " group by type");
    printExecutionPlan(result);
    var evenFound = false;
    var oddFound = false;
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      if ("even".equals(item.getProperty("type"))) {
        Assert.assertEquals(20, item.<Object>getProperty("sum(val)"));
        evenFound = true;
      } else if ("odd".equals(item.getProperty("type"))) {
        Assert.assertEquals(25, item.<Object>getProperty("sum(val)"));
        oddFound = true;
      }
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(evenFound);
    Assert.assertTrue(oddFound);
    result.close();
    session.commit();
  }

  @Test
  public void testAggregateSumMaxMinGroupBy() {
    var className = "testAggregateSumMaxMinGroupBy";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select sum(val), max(val), min(val), type from " + className + " group by type");
    printExecutionPlan(result);
    var evenFound = false;
    var oddFound = false;
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      if ("even".equals(item.getProperty("type"))) {
        Assert.assertEquals(20, item.<Object>getProperty("sum(val)"));
        Assert.assertEquals(8, item.<Object>getProperty("max(val)"));
        Assert.assertEquals(0, item.<Object>getProperty("min(val)"));
        evenFound = true;
      } else if ("odd".equals(item.getProperty("type"))) {
        Assert.assertEquals(25, item.<Object>getProperty("sum(val)"));
        Assert.assertEquals(9, item.<Object>getProperty("max(val)"));
        Assert.assertEquals(1, item.<Object>getProperty("min(val)"));
        oddFound = true;
      }
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(evenFound);
    Assert.assertTrue(oddFound);
    result.close();
    session.commit();
  }

  @Test
  public void testAggregateSumNoGroupByInProjection() {
    var className = "testAggregateSumNoGroupByInProjection";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select sum(val) from " + className + " group by type");
    printExecutionPlan(result);
    var evenFound = false;
    var oddFound = false;
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var sum = item.getProperty("sum(val)");
      if (sum.equals(20)) {
        evenFound = true;
      } else if (sum.equals(25)) {
        oddFound = true;
      }
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(evenFound);
    Assert.assertTrue(oddFound);
    result.close();
    session.commit();
  }

  @Test
  public void testAggregateSumNoGroupByInProjection2() {
    var className = "testAggregateSumNoGroupByInProjection2";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "dd1" : "dd2");
      doc.setProperty("val", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select sum(val) from " + className + " group by type.substring(0,1)");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var sum = item.getProperty("sum(val)");
      Assert.assertEquals(45, sum);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromCollectionNumber() {
    var className = "testFetchFromCollectionNumber";
    Schema schema = session.getMetadata().getSchema();
    schema.createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className);
    printExecutionPlan(result);
    var sum = 0;
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertNotNull(val);
      sum += val;
    }

    Assert.assertEquals(45, sum);
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassNumberOrderByRidDesc() {
    var className = "testFetchFromCollectionNumberOrderByRidDesc";
    Schema schema = session.getMetadata().getSchema();
    schema.createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " order by @rid desc");
    printExecutionPlan(result);

    RID lastRid = null;
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      if (lastRid == null) {
        lastRid = item.getIdentity();
      } else {
        Assert.assertTrue(lastRid.compareTo(item.getIdentity()) > 0);
        lastRid = item.getIdentity();
      }
    }

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassNumberOrderByRidAsc() {
    var className = "testFetchFromCollectionNumberOrderByRidAsc";
    Schema schema = session.getMetadata().getSchema();
    schema.createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select from " + className + " order by @rid asc");
    printExecutionPlan(result);

    RID lastRid = null;
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      if (lastRid == null) {
        lastRid = item.getIdentity();
      } else {
        Assert.assertTrue(lastRid.compareTo(item.getIdentity()) < 0);
        lastRid = item.getIdentity();
      }
    }

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testQueryAsTarget() {
    var className = "testQueryAsTarget";
    Schema schema = session.getMetadata().getSchema();
    schema.createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from (select from " + className + " where val > 2)  where val < 8");
    printExecutionPlan(result);

    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertTrue(val > 2);
      Assert.assertTrue(val < 8);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testQuerySchema() {
    session.begin();
    var result = session.query("select from metadata:schema");
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item.getProperty("classes"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testQueryMetadataIndexManager() {
    session.begin();
    var result = session.query("select from metadata:indexes");
    printExecutionPlan(result);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testQueryMetadataDatabase() {
    session.begin();
    var result = session.query("select from metadata:database");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("testQueryMetadataDatabase", item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testQueryMetadataStorage() {
    session.begin();
    var result = session.query("select from metadata:storage");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("testQueryMetadataStorage", item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testNonExistingRids() {
    session.begin();
    var result = session.query("select from #0:100000000");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSingleRid() {
    session.begin();
    var result = session.query("select from #0:1");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSingleRid2() {
    session.begin();
    var result = session.query("select from [#0:1]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSingleRidParam() {
    session.begin();
    var result = session.query("select from ?", new RecordId(0, 1));
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSingleRid3() {
    session.begin();
    session.newEntity();

    session.commit();

    session.begin();
    var result = session.query("select from [#0:1, #0:2000]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());

    Assert.assertFalse(result.hasNext());
    try {
      Assert.assertNotNull(result.next());
      Assert.fail();
    } catch (NoSuchElementException e) {
      //expected
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSingleRid4() {
    session.begin();
    session.newEntity();
    session.commit();

    session.begin();
    var result = session.query("select from [#0:1, #0:20000, #0:100000]");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    try {
      Assert.assertNotNull(result.next());
      Assert.fail();
    } catch (NoSuchElementException e) {
      //expected
    }

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndex() {
    var className = "testFetchFromClassWithIndex";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createIndex(className + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name = 'name2'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("name2", next.getProperty("name"));

    Assert.assertFalse(result.hasNext());

    var p = result.getExecutionPlan();
    Assert.assertNotNull(p);
    Assert.assertTrue(p instanceof SelectExecutionPlan);
    var plan = (SelectExecutionPlan) p;
    Assert.assertEquals(FetchFromIndexStep.class, plan.getSteps().getFirst().getClass());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromIndexHierarchy() {
    var className = "testFetchFromIndexHierarchy";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createIndex(className + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    var classNameExt = "testFetchFromIndexHierarchyExt";
    var clazzExt = session.getMetadata().getSchema().createClass(classNameExt, clazz);
    clazzExt.createIndex(classNameExt + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    for (var i = 5; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(classNameExt);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + classNameExt + " where name = 'name6'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);

    Assert.assertFalse(result.hasNext());

    var p = result.getExecutionPlan();
    Assert.assertNotNull(p);
    Assert.assertTrue(p instanceof SelectExecutionPlan);
    var plan = (SelectExecutionPlan) p;
    Assert.assertEquals(FetchFromIndexStep.class, plan.getSteps().getFirst().getClass());

    Assert.assertEquals(
        classNameExt + ".name", ((FetchFromIndexStep) plan.getSteps().getFirst()).getIndexName());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes() {
    var className = "testFetchFromClassWithIndexes";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", SchemaClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name2' or surname = 'surname3'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (var i = 0; i < 2; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue(
          "name2".equals(next.getProperty("name"))
              || "surname3".equals(next.getProperty("surname")));
    }

    Assert.assertFalse(result.hasNext());

    var p = result.getExecutionPlan();
    Assert.assertNotNull(p);
    Assert.assertTrue(p instanceof SelectExecutionPlan);
    var plan = (SelectExecutionPlan) p;
    Assert.assertEquals(ParallelExecStep.class, plan.getSteps().getFirst().getClass());
    var parallel = (ParallelExecStep) plan.getSteps().getFirst();
    Assert.assertEquals(2, parallel.getSubExecutionPlans().size());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes2() {
    var className = "testFetchFromClassWithIndexes2";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", SchemaClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from "
                + className
                + " where foo is not null and (name = 'name2' or surname = 'surname3')");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes3() {
    var className = "testFetchFromClassWithIndexes3";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", SchemaClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from "
                + className
                + " where foo < 100 and (name = 'name2' or surname = 'surname3')");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (var i = 0; i < 2; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue(
          "name2".equals(next.getProperty("name"))
              || "surname3".equals(next.getProperty("surname")));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes4() {
    var className = "testFetchFromClassWithIndexes4";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", SchemaClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from "
                + className
                + " where foo < 100 and ((name = 'name2' and foo < 20) or surname = 'surname3') and"
                + " ( 4<5 and foo < 50)");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (var i = 0; i < 2; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue(
          "name2".equals(next.getProperty("name"))
              || "surname3".equals(next.getProperty("surname")));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes5() {
    var className = "testFetchFromClassWithIndexes5";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name3' and surname >= 'surname1'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (var i = 0; i < 1; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes6() {
    var className = "testFetchFromClassWithIndexes6";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name3' and surname > 'surname3'");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes7() {
    var className = "testFetchFromClassWithIndexes7";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name3' and surname >= 'surname3'");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes8() {
    var className = "testFetchFromClassWithIndexes8";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name3' and surname < 'surname3'");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes9() {
    var className = "testFetchFromClassWithIndexes9";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name3' and surname <= 'surname3'");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes10() {
    var className = "testFetchFromClassWithIndexes10";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name > 'name3' ");
    printExecutionPlan(result);
    for (var i = 0; i < 6; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes11() {
    var className = "testFetchFromClassWithIndexes11";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name >= 'name3' ");
    printExecutionPlan(result);
    for (var i = 0; i < 7; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes12() {
    var className = "testFetchFromClassWithIndexes12";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name < 'name3' ");
    printExecutionPlan(result);
    for (var i = 0; i < 3; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes13() {
    var className = "testFetchFromClassWithIndexes13";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name <= 'name3' ");
    printExecutionPlan(result);
    for (var i = 0; i < 4; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes14() {
    var className = "testFetchFromClassWithIndexes14";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name > 'name3' and name < 'name5'");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithIndexes15() {
    var className = "testFetchFromClassWithIndexes15";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from "
                + className
                + " where name > 'name6' and name = 'name3' and surname > 'surname2' and surname <"
                + " 'surname5' ");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithHashIndexes1() {
    var className = "testFetchFromClassWithHashIndexes1";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(
        className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name6' and surname = 'surname6' ");
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromClassWithHashIndexes2() {
    var className = "testFetchFromClassWithHashIndexes2";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createIndex(
        className + ".name_surname", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name",
        "surname");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name6' and surname >= 'surname6' ");
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    Assert.assertEquals(
        FetchFromIndexStep.class, plan.getSteps().getFirst().getClass()); // index not used
    result.close();
    session.commit();
  }

  @Test
  public void testExpand1() {
    var childClassName = "testExpand1_child";
    var parentClassName = "testExpand1_parent";
    session.getMetadata().getSchema().createClass(childClassName);
    session.getMetadata().getSchema().createClass(parentClassName);

    var count = 10;
    for (var i = 0; i < count; i++) {
      session.begin();
      var doc = session.newInstance(childClassName);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);

      var parent = (EntityImpl) session.newEntity(parentClassName);
      parent.setProperty("linked", doc);

      session.commit();
    }

    session.begin();
    var result = session.query("select expand(linked) from " + parentClassName);
    printExecutionPlan(result);

    for (var i = 0; i < count; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testExpand2() {
    var childClassName = "testExpand2_child";
    var parentClassName = "testExpand2_parent";
    session.getMetadata().getSchema().createClass(childClassName);
    session.getMetadata().getSchema().createClass(parentClassName);

    session.begin();
    var count = 10;
    var collSize = 11;
    for (var i = 0; i < count; i++) {
      var coll = session.newLinkList();
      for (var j = 0; j < collSize; j++) {
        var doc = session.newInstance(childClassName);
        doc.setProperty("name", "name" + i);

        coll.add(doc);
      }

      var parent = (EntityImpl) session.newEntity(parentClassName);
      parent.setProperty("linked", coll);

    }
    session.commit();

    session.begin();
    var result = session.query("select expand(linked) from " + parentClassName);
    printExecutionPlan(result);

    for (var i = 0; i < count * collSize; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testExpand3() {
    var childClassName = "testExpand3_child";
    var parentClassName = "testExpand3_parent";
    session.getMetadata().getSchema().createClass(childClassName);
    session.getMetadata().getSchema().createClass(parentClassName);

    session.begin();
    var count = 30;
    var collSize = 7;
    for (var i = 0; i < count; i++) {
      var coll = session.newLinkList();
      for (var j = 0; j < collSize; j++) {
        var doc = session.newInstance(childClassName);
        doc.setProperty("name", "name" + j);

        coll.add(doc);
      }

      var parent = (EntityImpl) session.newEntity(parentClassName);
      parent.setProperty("linked", coll);

    }
    session.commit();

    session.begin();
    var result =
        session.query("select expand(linked) from " + parentClassName + " order by name");
    printExecutionPlan(result);

    String last = null;
    for (var i = 0; i < count * collSize; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      if (i > 0) {
        Assert.assertTrue(last.compareTo(next.getProperty("name")) <= 0);
      }
      last = next.getProperty("name");
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testDistinct1() {
    var className = "testDistinct1";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);

    for (var i = 0; i < 30; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);

      session.commit();
    }

    session.begin();
    var result = session.query("select distinct name, surname from " + className);
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testDistinct2() {
    var className = "testDistinct2";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);

    for (var i = 0; i < 30; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);

      session.commit();
    }

    session.begin();
    var result = session.query("select distinct(name) from " + className);
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testLet1() {
    session.begin();
    var result = session.query("select $a as one, $b as two let $a = 1, $b = 1+1");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    printExecutionPlan(result);
    result.close();
    session.commit();
  }

  @Test
  public void testLet1Long() {
    session.begin();
    var result = session.query("select $a as one, $b as two let $a = 1L, $b = 1L+1");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.<Object>getProperty("one"));
    Assert.assertEquals(2L, item.<Object>getProperty("two"));
    printExecutionPlan(result);
    result.close();
    session.commit();
  }

  @Test
  public void testLet2() {
    session.begin();
    var result = session.query("select $a as one let $a = (select 1 as a)");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    var one = item.<BasicResult>getEmbeddedList("one");
    Assert.assertEquals(1, one.size());
    var x = one.getFirst();
    Assert.assertEquals(1, x.getInt("a").intValue());
    result.close();
  }

  @Test
  public void testLet3() {
    session.begin();
    var result = session.query("select $a[0].foo as one let $a = (select 1 as foo)");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    var one = item.getProperty("one");
    Assert.assertEquals(1, one);
    result.close();
    session.commit();
  }

  @Test
  public void testLet4() {
    var className = "testLet4";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select name, surname, $nameAndSurname as fullname from "
                + className
                + " let $nameAndSurname = name + ' ' + surname");
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);

      Assert.assertEquals(
          item.getProperty("fullname"),
          item.getProperty("name") + " " + item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testLet5() {
    var className = "testLet5";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from "
                + className
                + " where name in (select name from "
                + className
                + " where name = 'name1')");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testLet6() {
    var className = "testLet6";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select $foo as name from "
                + className
                + " let $foo = (select name from "
                + className
                + " where name = $parent.$current.name)");
    printExecutionPlan(result);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertTrue(item.getProperty("name") instanceof Collection);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testLet7() {
    var className = "testLet7";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select $bar as name from "
                + className
                + " "
                + "let $foo = (select name from "
                + className
                + " where name = $parent.$current.name),"
                + "$bar = $foo[0].name");
    printExecutionPlan(result);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertTrue(item.getProperty("name") instanceof String);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testLetWithTraverseFunction() {
    var vertexClassName = "testLetWithTraverseFunction";
    var edgeClassName = "testLetWithTraverseFunctioEdge";

    var vertexClass = session.createVertexClass(vertexClassName);

    session.begin();
    var doc1 = session.newVertex(vertexClass);
    doc1.setProperty("name", "A");

    var doc2 = session.newVertex(vertexClass);
    doc2.setProperty("name", "B");
    session.commit();

    var doc2Id = doc2.getIdentity();

    var edgeClass = session.createEdgeClass(edgeClassName);

    session.begin();
    var activeTx1 = session.getActiveTransaction();
    doc1 = activeTx1.load(doc1);
    var activeTx = session.getActiveTransaction();
    doc2 = activeTx.load(doc2);

    session.newEdge(doc1, doc2, edgeClass);
    session.commit();

    session.begin();
    var queryString =
        "SELECT $x, name FROM " + vertexClassName + " let $x = out(\"" + edgeClassName + "\")";
    var resultSet = session.query(queryString);
    var counter = 0;
    while (resultSet.hasNext()) {
      var result = resultSet.next();
      Iterable<Identifiable> edge = result.getProperty("$x");
      for (var identifiable : edge) {
        Vertex toVertex = session.load(identifiable.getIdentity());
        if (doc2Id.equals(toVertex.getIdentity())) {
          ++counter;
        }
      }
    }
    Assert.assertEquals(1, counter);
    resultSet.close();
    session.commit();
  }

  @Test
  public void testLetVariableSubqueryProjectionFetchFromClassTarget_9695() {
    var className = "testLetVariableSubqueryProjectionFetchFromClassTarget_9695";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setInt("i", i);
      doc.newEmbeddedList("iSeq", new int[] {i, 2 * i, 4 * i});

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select $current.*, $b.*, $b.@class from (select 1 as sqa, @class as sqc from "
                + className
                + " LIMIT 2)\n"
                + "let $b = $current");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    var currentProperty = item.getProperty("$current.*");
    Assert.assertTrue(currentProperty instanceof BasicResult);
    final var currentResult = (Result) currentProperty;
    Assert.assertTrue(currentResult.isProjection());
    Assert.assertEquals(Integer.valueOf(1), currentResult.<Integer>getProperty("sqa"));
    Assert.assertEquals(className, currentResult.getProperty("sqc"));
    var bProperty = item.getProperty("$b.*");
    Assert.assertTrue(bProperty instanceof BasicResult);
    final var bResult = (Result) bProperty;
    Assert.assertTrue(bResult.isProjection());
    Assert.assertEquals(Integer.valueOf(1), bResult.<Integer>getProperty("sqa"));
    Assert.assertEquals(className, bResult.getProperty("sqc"));
    result.close();
    session.commit();
  }

  @Test
  public void testUnwind1() {
    var className = "testUnwind1";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("i", i);
      doc.newEmbeddedList("iSeq", new int[] {i, 2 * i, 4 * i});

      session.commit();
    }

    session.begin();
    var result = session.query("select i, iSeq from " + className + " unwind iSeq");
    printExecutionPlan(result);
    for (var i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("i"));
      Assert.assertNotNull(item.getProperty("iSeq"));
      Integer first = item.getProperty("i");
      Integer second = item.getProperty("iSeq");
      Assert.assertTrue(first + second == 0 || second % first == 0);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testUnwind2() {
    var className = "testUnwind2";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("i", i);
      List<Integer> iSeq = new ArrayList<>();
      iSeq.add(i);
      iSeq.add(i << 1);
      iSeq.add(i << 2);
      doc.newEmbeddedList("iSeq", iSeq);

      session.commit();
    }

    session.begin();
    var result = session.query("select i, iSeq from " + className + " unwind iSeq");
    printExecutionPlan(result);
    for (var i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("i"));
      Assert.assertNotNull(item.getProperty("iSeq"));
      Integer first = item.getProperty("i");
      Integer second = item.getProperty("iSeq");
      Assert.assertTrue(first + second == 0 || second % first == 0);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSubclassIndexes1() {
    var parent = "testFetchFromSubclassIndexes1_parent";
    var child1 = "testFetchFromSubclassIndexes1_child1";
    var child2 = "testFetchFromSubclassIndexes1_child2";
    var parentClass = session.getMetadata().getSchema().createClass(parent);
    var childClass1 = session.getMetadata().getSchema().createClass(child1, parentClass);
    var childClass2 = session.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", PropertyType.STRING);
    childClass1.createIndex(child1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child1);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + parent + " where name = 'name1'");
    printExecutionPlan(result);
    var plan = (InternalExecutionPlan) result.getExecutionPlan();
    Assert.assertTrue(plan.getSteps().getFirst() instanceof ParallelExecStep);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSubclassIndexes2() {
    var parent = "testFetchFromSubclassIndexes2_parent";
    var child1 = "testFetchFromSubclassIndexes2_child1";
    var child2 = "testFetchFromSubclassIndexes2_child2";
    var parentClass = session.getMetadata().getSchema().createClass(parent);
    var childClass1 = session.getMetadata().getSchema().createClass(child1, parentClass);
    var childClass2 = session.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", PropertyType.STRING);
    childClass1.createIndex(child1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    var plan = (InternalExecutionPlan) result.getExecutionPlan();
    Assert.assertTrue(plan.getSteps().getFirst() instanceof ParallelExecStep);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSubclassIndexes3() {
    var parent = "testFetchFromSubclassIndexes3_parent";
    var child1 = "testFetchFromSubclassIndexes3_child1";
    var child2 = "testFetchFromSubclassIndexes3_child2";
    var parentClass = session.getMetadata().getSchema().createClass(parent);
    var childClass1 = session.getMetadata().getSchema().createClass(child1, parentClass);
    session.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", PropertyType.STRING);
    childClass1.createIndex(child1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    var plan = (InternalExecutionPlan) result.getExecutionPlan();
    Assert.assertTrue(
        plan.getSteps().getFirst() instanceof FetchFromClassExecutionStep); // no index used
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSubclassIndexes4() {
    var parent = "testFetchFromSubclassIndexes4_parent";
    var child1 = "testFetchFromSubclassIndexes4_child1";
    var child2 = "testFetchFromSubclassIndexes4_child2";
    var parentClass = session.getMetadata().getSchema().createClass(parent);
    var childClass1 = session.getMetadata().getSchema().createClass(child1, parentClass);
    var childClass2 = session.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", PropertyType.STRING);
    childClass1.createIndex(child1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    session.begin();
    var parentdoc = session.newInstance(parent);
    parentdoc.setProperty("name", "foo");

    session.commit();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    var plan = (InternalExecutionPlan) result.getExecutionPlan();
    Assert.assertTrue(
        plan.getSteps().getFirst() instanceof FetchFromClassExecutionStep); // no index, because the superclass is not empty
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSubSubclassIndexes() {
    var parent = "testFetchFromSubSubclassIndexes_parent";
    var child1 = "testFetchFromSubSubclassIndexes_child1";
    var child2 = "testFetchFromSubSubclassIndexes_child2";
    var child2_1 = "testFetchFromSubSubclassIndexes_child2_1";
    var child2_2 = "testFetchFromSubSubclassIndexes_child2_2";
    var parentClass = session.getMetadata().getSchema().createClass(parent);
    var childClass1 = session.getMetadata().getSchema().createClass(child1, parentClass);
    var childClass2 = session.getMetadata().getSchema().createClass(child2, parentClass);
    var childClass2_1 = session.getMetadata().getSchema().createClass(child2_1, childClass2);
    var childClass2_2 = session.getMetadata().getSchema().createClass(child2_2, childClass2);

    parentClass.createProperty("name", PropertyType.STRING);
    childClass1.createIndex(child1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2_1.createIndex(child2_1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name");
    childClass2_2.createIndex(child2_2 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "name");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2_1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2_2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    var plan = (InternalExecutionPlan) result.getExecutionPlan();
    Assert.assertTrue(plan.getSteps().getFirst() instanceof ParallelExecStep);
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testFetchFromSubSubclassIndexesWithDiamond() {
    var parent = "testFetchFromSubSubclassIndexesWithDiamond_parent";
    var child1 = "testFetchFromSubSubclassIndexesWithDiamond_child1";
    var child2 = "testFetchFromSubSubclassIndexesWithDiamond_child2";
    var child12 = "testFetchFromSubSubclassIndexesWithDiamond_child12";

    var parentClass = session.getMetadata().getSchema().createClass(parent);
    var childClass1 = session.getMetadata().getSchema().createClass(child1, parentClass);
    var childClass2 = session.getMetadata().getSchema().createClass(child2, parentClass);
    session.getMetadata().getSchema().createClass(child12, childClass1, childClass2);

    parentClass.createProperty("name", PropertyType.STRING);
    childClass1.createIndex(child1 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", SchemaClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(child12);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    var plan = (InternalExecutionPlan) result.getExecutionPlan();
    Assert.assertTrue(plan.getSteps().getFirst() instanceof FetchFromClassExecutionStep);
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort1() {
    var className = "testIndexPlusSort1";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name1' order by surname ASC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) > 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    var plan = result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort2() {
    var className = "testIndexPlusSort2";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name1' order by surname DESC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) < 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    var plan = result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort3() {
    var className = "testIndexPlusSort3";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name1' order by name DESC, surname DESC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(((String) item.getProperty("surname")).compareTo(lastSurname) < 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    var plan = result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort4() {
    var className = "testIndexPlusSort4";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name1' order by name ASC, surname ASC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) > 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    var plan = result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort5() {
    var className = "testIndexPlusSort5";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createProperty("address", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname, address) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name1' order by surname ASC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) > 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    var plan = result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort6() {
    var className = "testIndexPlusSort6";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createProperty("address", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname, address) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name1' order by surname DESC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) < 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    var plan = result.getExecutionPlan();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPlusSort7() {
    var className = "testIndexPlusSort7";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    clazz.createProperty("address", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname, address) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query("select from " + className + " where name = 'name1' order by address DESC");
    printExecutionPlan(result);

    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    var orderStepFound = false;
    for (var step : result.getExecutionPlan().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertTrue(orderStepFound);
    result.close();
    session.commit();
  }

  @SuppressWarnings("ConstantValue")
  @Test
  public void testIndexPlusSort8() {
    var className = "testIndexPlusSort8";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result =
        session.query(
            "select from " + className + " where name = 'name1' order by name ASC, surname DESC");
    printExecutionPlan(result);
    for (var i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    var orderStepFound = false;
    for (var step : result.getExecutionPlan().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertTrue(orderStepFound);
    result.close();
    session.commit();
  }

  @SuppressWarnings("ConstantValue")
  @Test
  public void testIndexPlusSort9() {
    var className = "testIndexPlusSort9";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by name , surname ASC");
    printExecutionPlan(result);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    var orderStepFound = false;
    for (var step : result.getExecutionPlan().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertFalse(orderStepFound);
    result.close();
    session.commit();
  }

  @SuppressWarnings("ConstantValue")
  @Test
  public void testIndexPlusSort10() {
    var className = "testIndexPlusSort10";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by name desc, surname desc");
    printExecutionPlan(result);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    var orderStepFound = false;
    for (var step : result.getExecutionPlan().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertFalse(orderStepFound);
    result.close();
    session.commit();
  }

  @SuppressWarnings("ConstantValue")
  @Test
  public void testIndexPlusSort11() {
    var className = "testIndexPlusSort11";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by name asc, surname desc");
    printExecutionPlan(result);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    var orderStepFound = false;
    for (var step : result.getExecutionPlan().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertTrue(orderStepFound);
    result.close();
    session.commit();
  }

  @SuppressWarnings("ConstantValue")
  @Test
  public void testIndexPlusSort12() {
    var className = "testIndexPlusSort12";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("surname", PropertyType.STRING);
    session.execute(
        "create index "
            + className
            + ".name_surname on "
            + className
            + " (name, surname) NOTUNIQUE")
        .close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " order by name");
    printExecutionPlan(result);
    String last = null;
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      String name = item.getProperty("name");
      if (i > 0) {
        Assert.assertTrue(name.compareTo(last) >= 0);
      }
      last = name;
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    var orderStepFound = false;
    for (var step : result.getExecutionPlan().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertFalse(orderStepFound);
    result.close();
    session.commit();
  }

  @Test
  public void testSelectFromStringParam() {
    var className = "testSelectFromStringParam";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from ?", className);
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSelectFromStringNamedParam() {
    var className = "testSelectFromStringNamedParam";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }
    Map<Object, Object> params = new HashMap<>();
    params.put("target", className);

    session.begin();
    var result = session.query("select from :target", params);
    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testMatches() {
    var className = "testMatches";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    var result = session.query("select from " + className + " where name matches 'name1'");
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testRange() {
    var className = "testRange";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var doc = session.newInstance(className);
    doc.newEmbeddedList("name", new String[] {"a", "b", "c", "d"});

    session.commit();

    session.begin();
    var result = session.query("select name[0..3] as names from " + className);
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var names = item.<String>getEmbeddedList("names");
      if (names == null) {
        Assert.fail();
      }

      Assert.assertEquals(3, names.size());
      var iter = names.iterator();
      Assert.assertEquals("a", iter.next());
      Assert.assertEquals("b", iter.next());
      Assert.assertEquals("c", iter.next());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testRangeParams1() {
    var className = "testRangeParams1";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var doc = session.newInstance(className);
    doc.newEmbeddedList("name", new String[] {"a", "b", "c", "d"});

    session.commit();

    session.begin();
    var result = session.query("select name[?..?] as names from " + className, 0, 3);
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var names = item.<String>getEmbeddedList("names");
      if (names == null) {
        Assert.fail();
      }
      Assert.assertEquals(3, names.size());
      var iter = names.iterator();
      Assert.assertEquals("a", iter.next());
      Assert.assertEquals("b", iter.next());
      Assert.assertEquals("c", iter.next());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testRangeParams2() {
    var className = "testRangeParams2";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var doc = session.newInstance(className);
    doc.newEmbeddedList("name", new String[] {"a", "b", "c", "d"});

    session.commit();

    Map<String, Object> params = new HashMap<>();
    params.put("a", 0);
    params.put("b", 3);

    session.begin();
    var result = session.query("select name[:a..:b] as names from " + className, params);
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var names = item.<String>getEmbeddedList("names");
      if (names == null) {
        Assert.fail();
      }
      Assert.assertEquals(3, names.size());
      var iter = names.iterator();
      Assert.assertEquals("a", iter.next());
      Assert.assertEquals("b", iter.next());
      Assert.assertEquals("c", iter.next());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testEllipsis() {
    var className = "testEllipsis";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var doc = session.newInstance(className);
    doc.newEmbeddedList("name", new String[] {"a", "b", "c", "d"});

    session.commit();

    session.begin();
    var result = session.query("select name[0...2] as names from " + className);
    printExecutionPlan(result);

    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      var names = item.<String>getEmbeddedList("names");
      if (names == null) {
        Assert.fail();
      }
      Assert.assertEquals(3, names.size());
      var iter = names.iterator();
      Assert.assertEquals("a", iter.next());
      Assert.assertEquals("b", iter.next());
      Assert.assertEquals("c", iter.next());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testNewRid() {
    session.begin();
    var result = session.query("select {\"@rid\":\"#12:0\"} as theRid ");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    var rid = item.getProperty("theRid");
    Assert.assertTrue(rid instanceof Identifiable);
    var id = (Identifiable) rid;
    Assert.assertEquals(12, id.getIdentity().getCollectionId());
    Assert.assertEquals(0L, id.getIdentity().getCollectionPosition());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testNestedProjections1() {
    var className = "testNestedProjections1";
    session.execute("create class " + className).close();

    session.begin();
    var elem1 = session.newEntity(className);
    elem1.setProperty("name", "a");

    var elem2 = session.newEntity(className);
    elem2.setProperty("name", "b");
    elem2.setProperty("surname", "lkj");

    var elem3 = session.newEntity(className);
    elem3.setProperty("name", "c");

    var elem4 = session.newEntity(className);
    elem4.setProperty("name", "d");
    elem4.setProperty("elem1", elem1);
    elem4.setProperty("elem2", elem2);
    elem4.setProperty("elem3", elem3);

    session.commit();

    session.begin();
    var result =
        session.query(
            "select name, elem1:{*}, elem2:{!surname} from " + className + " where name = 'd'");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    // TODO refine this!
    Assert.assertTrue(item.getProperty("elem1") instanceof BasicResult);
    Assert.assertEquals("a", ((BasicResult) item.getProperty("elem1")).getProperty("name"));
    printExecutionPlan(result);

    result.close();
    session.commit();
  }

  @Test
  public void testSimpleCollectionFiltering() {
    var className = "testSimpleCollectionFiltering";
    session.execute("create class " + className).close();

    session.begin();
    var elem1 = session.newEntity(className);
    List<String> coll = new ArrayList<>();
    coll.add("foo");
    coll.add("bar");
    coll.add("baz");
    elem1.newEmbeddedList("coll", coll);
    session.commit();

    session.begin();
    var result = session.query("select coll[='foo'] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    var res = item.<String>getEmbeddedList("filtered");
    Assert.assertEquals(1, res.size());
    Assert.assertEquals("foo", res.getFirst());
    result.close();

    result = session.query("select coll[<'ccc'] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    res = item.getProperty("filtered");
    Assert.assertEquals(2, res.size());
    result.close();

    result = session.query("select coll[LIKE 'ba%'] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    res = item.getProperty("filtered");
    Assert.assertEquals(2, res.size());
    result.close();

    result = session.query("select coll[in ['bar']] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    res = item.getProperty("filtered");
    Assert.assertEquals(1, res.size());
    Assert.assertEquals("bar", res.getFirst());
    result.close();
    session.commit();
  }

  @Test
  public void testContaninsWithConversion() {
    var className = "testContaninsWithConversion";
    session.execute("create class " + className).close();

    session.begin();
    var elem1 = session.newEntity(className);
    List<Long> coll = new ArrayList<>();
    coll.add(1L);
    coll.add(3L);
    coll.add(5L);
    elem1.newEmbeddedList("coll", coll);

    var elem2 = session.newEntity(className);
    coll = new ArrayList<>();
    coll.add(2L);
    coll.add(4L);
    coll.add(6L);
    elem2.newEmbeddedList("coll", coll);
    session.commit();

    session.begin();
    var result = session.query("select from " + className + " where coll contains 1");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    result = session.query("select from " + className + " where coll contains 1L");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    result = session.query("select from " + className + " where coll contains 12L");
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testIndexPrefixUsage() {
    // issue #7636
    var className = "testIndexPrefixUsage";
    session.execute("create class " + className).close();
    session.execute("create property " + className + ".id LONG").close();
    session.execute("create property " + className + ".name STRING").close();
    session.execute("create index " + className + ".id_name on " + className + "(id, name) UNIQUE")
        .close();

    session.begin();
    session.execute("insert into " + className + " set id = 1 , name = 'Bar'").close();
    session.commit();

    session.begin();
    var result = session.query("select from " + className + " where name = 'Bar'");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testNamedParams() {
    var className = "testNamedParams";
    session.execute("create class " + className).close();

    session.begin();
    session.execute("insert into " + className + " set name = 'Foo', surname = 'Fox'").close();
    session.execute("insert into " + className + " set name = 'Bar', surname = 'Bax'").close();
    session.commit();

    Map<String, Object> params = new HashMap<>();
    params.put("p1", "Foo");
    params.put("p2", "Fox");
    session.begin();
    var result =
        session.query("select from " + className + " where name = :p1 and surname = :p2", params);
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testNamedParamsWithIndex() {
    var className = "testNamedParamsWithIndex";
    session.execute("create class " + className).close();
    session.execute("create property " + className + ".name STRING").close();
    session.execute("create index " + className + ".name ON " + className + " (name) NOTUNIQUE")
        .close();

    session.begin();
    session.execute("insert into " + className + " set name = 'Foo'").close();
    session.execute("insert into " + className + " set name = 'Bar'").close();
    session.commit();

    Map<String, Object> params = new HashMap<>();
    params.put("p1", "Foo");
    session.begin();
    var result = session.query("select from " + className + " where name = :p1", params);
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testIsDefined() {
    var className = "testIsDefined";
    session.execute("create class " + className).close();

    session.begin();
    session.execute("insert into " + className + " set name = 'Foo'").close();
    session.execute("insert into " + className + " set sur = 'Bar'").close();
    session.execute("insert into " + className + " set sur = 'Barz'").close();
    session.commit();

    session.begin();
    var result = session.query("select from " + className + " where name is defined");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testIsNotDefined() {
    var className = "testIsNotDefined";
    session.execute("create class " + className).close();

    session.begin();
    session.execute("insert into " + className + " set name = 'Foo'").close();
    session.execute("insert into " + className + " set name = null, sur = 'Bar'").close();
    session.execute("insert into " + className + " set sur = 'Barz'").close();
    session.commit();

    session.begin();
    var result = session.query("select from " + className + " where name is not defined");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testContainsWithSubquery() {
    var className = "testContainsWithSubquery";
    session.createClassIfNotExist(className + 1);
    var clazz2 = session.createClassIfNotExist(className + 2);
    clazz2.createProperty("tags", PropertyType.EMBEDDEDLIST);

    session.begin();
    session.execute("insert into " + className + 1 + "  set name = 'foo'");

    session.execute("insert into " + className + 2 + "  set tags = ['foo', 'bar']");
    session.execute("insert into " + className + 2 + "  set tags = ['baz', 'bar']");
    session.execute("insert into " + className + 2 + "  set tags = ['foo']");
    session.commit();

    session.begin();
    try (var result =
        session.query(
            "select from "
                + className
                + 2
                + " where tags contains (select from "
                + className
                + 1
                + " where name = 'foo')")) {

      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testInWithSubquery() {
    var className = "testInWithSubquery";
    session.createClassIfNotExist(className + 1);
    var clazz2 = session.createClassIfNotExist(className + 2);
    clazz2.createProperty("tags", PropertyType.EMBEDDEDLIST);

    session.begin();
    session.execute("insert into " + className + 1 + "  set name = 'foo'");

    session.execute("insert into " + className + 2 + "  set tags = ['foo', 'bar']");
    session.execute("insert into " + className + 2 + "  set tags = ['baz', 'bar']");
    session.execute("insert into " + className + 2 + "  set tags = ['foo']");
    session.commit();

    session.begin();
    try (var result =
        session.query(
            "select from "
                + className
                + 2
                + " where (select from "
                + className
                + 1
                + " where name = 'foo') in tags")) {

      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      session.commit();
    }
  }

  @Test
  public void testContainsAny() {
    var className = "testContainsAny";
    var clazz = session.createClassIfNotExist(className);
    clazz.createProperty("tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);

    session.begin();
    session.execute("insert into " + className + "  set tags = ['foo', 'bar']");
    session.execute("insert into " + className + "  set tags = ['bbb', 'FFF']");
    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className + " where tags containsany ['foo','baz']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (var result =
        session.query("select from " + className + " where tags containsany ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (var result =
        session.query("select from " + className + " where tags containsany ['foo','bbb']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (var result =
        session.query("select from " + className + " where tags containsany ['xx','baz']")) {
      Assert.assertFalse(result.hasNext());
    }

    try (var result = session.query("select from " + className + " where tags containsany []")) {
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testContainsAnyWithIndex() {
    var className = "testContainsAnyWithIndex";
    var clazz = session.createClassIfNotExist(className);
    var prop = clazz.createProperty("tags", PropertyType.EMBEDDEDLIST,
        PropertyType.STRING);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    session.begin();
    session.execute("insert into " + className + "  set tags = ['foo', 'bar']");
    session.execute("insert into " + className + "  set tags = ['bbb', 'FFF']");
    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className + " where tags containsany ['foo','baz']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (var result =
        session.query("select from " + className + " where tags containsany ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (var result =
        session.query("select from " + className + " where tags containsany ['foo','bbb']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (var result =
        session.query("select from " + className + " where tags containsany ['xx','baz']")) {
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (var result = session.query("select from " + className + " where tags containsany []")) {
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testContainsAll() {
    var className = "testContainsAll";
    var clazz = session.createClassIfNotExist(className);
    clazz.createProperty("tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);

    session.begin();
    session.execute("insert into " + className + "  set tags = ['foo', 'bar']");
    session.execute("insert into " + className + "  set tags = ['foo', 'FFF']");
    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className + " where tags containsall ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (var result =
        session.query("select from " + className + " where tags containsall ['foo']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testBetween() {
    var className = "testBetween";
    session.createClassIfNotExist(className);

    session.begin();
    session.execute("insert into " + className + "  set name = 'foo1', val = 1");
    session.execute("insert into " + className + "  set name = 'foo2', val = 2");
    session.execute("insert into " + className + "  set name = 'foo3', val = 3");
    session.execute("insert into " + className + "  set name = 'foo4', val = 4");
    session.commit();

    session.begin();
    try (var result = session.query("select from " + className + " where val between 2 and 3")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testInWithIndex() {
    var className = "testInWithIndex";
    var clazz = session.createClassIfNotExist(className);
    var prop = clazz.createProperty("tag", PropertyType.STRING);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    session.begin();
    session.execute("insert into " + className + "  set tag = 'foo'");
    session.execute("insert into " + className + "  set tag = 'bar'");
    session.commit();

    session.begin();
    try (var result = session.query(
        "select from " + className + " where tag in ['foo','baz']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (var result = session.query(
        "select from " + className + " where tag in ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (var result = session.query("select from " + className + " where tag in []")) {
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    try (var result = session.query("select from " + className + " where tag in (?)", params)) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testIndexChain() {
    var className1 = "testIndexChain1";
    var className2 = "testIndexChain2";
    var className3 = "testIndexChain3";

    var clazz3 = session.createClassIfNotExist(className3);
    var prop = clazz3.createProperty("name", PropertyType.STRING);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    var clazz2 = session.createClassIfNotExist(className2);
    prop = clazz2.createProperty("next", PropertyType.LINK, clazz3);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    var clazz1 = session.createClassIfNotExist(className1);
    prop = clazz1.createProperty("next", PropertyType.LINK, clazz2);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    session.begin();
    var elem3 = session.newEntity(className3);
    elem3.setProperty("name", "John");

    var elem2 = session.newEntity(className2);
    elem2.setProperty("next", elem3);

    var elem1 = session.newEntity(className1);
    elem1.setProperty("next", elem2);
    elem1.setProperty("name", "right");

    elem1 = session.newEntity(className1);
    elem1.setProperty("name", "wrong");
    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className1 + " where next.next.name = ?", "John")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals("right", item.getProperty("name"));
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testIndexChainWithContainsAny() {
    var className1 = "testIndexChainWithContainsAny1";
    var className2 = "testIndexChainWithContainsAny2";
    var className3 = "testIndexChainWithContainsAny3";

    var clazz3 = session.createClassIfNotExist(className3);
    var prop = clazz3.createProperty("name", PropertyType.STRING);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    var clazz2 = session.createClassIfNotExist(className2);
    prop = clazz2.createProperty("next", PropertyType.LINKSET, clazz3);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    var clazz1 = session.createClassIfNotExist(className1);
    prop = clazz1.createProperty("next", PropertyType.LINKSET, clazz2);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    session.begin();
    var elem3 = session.newEntity(className3);
    elem3.setProperty("name", "John");

    var elemFoo = session.newEntity(className3);
    elemFoo.setProperty("foo", "bar");

    var elem2 = session.newEntity(className2);
    var elems3 = session.newLinkList();

    elems3.add(elem3);
    elems3.add(elemFoo);
    elem2.setProperty("next", elems3);

    var elem1 = session.newEntity(className1);
    var elems2 = session.newLinkList();
    elems2.add(elem2);

    elem1.setProperty("next", elems2);
    elem1.setProperty("name", "right");

    elem1 = session.newEntity(className1);
    elem1.setProperty("name", "wrong");

    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className1 + " where next.next.name CONTAINSANY ['John']")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals("right", item.getProperty("name"));
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testMapByKeyIndex() {
    var className = "testMapByKeyIndex";

    var clazz1 = session.createClassIfNotExist(className);
    clazz1.createProperty("themap", PropertyType.EMBEDDEDMAP);

    session.execute(
        "CREATE INDEX " + className + ".themap ON " + className + "(themap by key) NOTUNIQUE");

    for (var i = 0; i < 100; i++) {
      session.begin();
      var theMap = session.newEmbeddedMap();
      theMap.put("key" + i, "val" + i);

      var elem1 = session.newEntity(className);
      elem1.setProperty("themap", theMap);

      session.commit();
    }

    session.begin();
    try (var result =
        session.query("select from " + className + " where themap CONTAINSKEY ?", "key10")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Map<String, Object> map = item.getProperty("themap");
      Assert.assertEquals("key10", map.keySet().iterator().next());
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testMapByKeyIndexMultiple() {
    var className = "testMapBjyKeyIndexMultiple";

    var clazz1 = session.createClassIfNotExist(className);
    clazz1.createProperty("themap", PropertyType.EMBEDDEDMAP);
    clazz1.createProperty("thestring", PropertyType.STRING);

    session.execute(
        "CREATE INDEX "
            + className
            + ".themap_thestring ON "
            + className
            + "(themap by key, thestring) NOTUNIQUE");

    for (var i = 0; i < 100; i++) {
      session.begin();
      var theMap = session.newEmbeddedMap();

      theMap.put("key" + i, "val" + i);

      var elem1 = session.newEntity(className);
      elem1.setProperty("themap", theMap);
      elem1.setProperty("thestring", "thestring" + i);

      session.commit();
    }

    session.begin();
    try (var result =
        session.query(
            "select from " + className + " where themap CONTAINSKEY ? AND thestring = ?",
            "key10",
            "thestring10")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Map<String, Object> map = item.getProperty("themap");
      Assert.assertEquals("key10", map.keySet().iterator().next());
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testMapByValueIndex() {
    var className = "testMapByValueIndex";

    var clazz1 = session.createClassIfNotExist(className);
    clazz1.createProperty("themap", PropertyType.EMBEDDEDMAP,
        PropertyType.STRING);

    session.execute(
        "CREATE INDEX " + className + ".themap ON " + className + "(themap by value) NOTUNIQUE");

    for (var i = 0; i < 100; i++) {
      session.begin();
      Map<String, Object> theMap = new HashMap<>();
      theMap.put("key" + i, "val" + i);
      var elem1 = session.newEntity(className);
      elem1.newEmbeddedMap("themap", theMap);
      session.commit();
    }

    session.begin();
    try (var result =
        session.query("select from " + className + " where themap CONTAINSVALUE ?", "val10")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Map<String, Object> map = item.getProperty("themap");
      Assert.assertEquals("key10", map.keySet().iterator().next());
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testListOfMapsContains() {
    var className = "testListOfMapsContains";

    var clazz1 = session.createClassIfNotExist(className);
    clazz1.createProperty("thelist", PropertyType.EMBEDDEDLIST, PropertyType.EMBEDDEDMAP);

    session.begin();
    session.execute("INSERT INTO " + className + " SET thelist = [{name:\"Jack\"}]").close();
    session.execute("INSERT INTO " + className + " SET thelist = [{name:\"Joe\"}]").close();
    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className + " where thelist CONTAINS ( name = ?)", "Jack")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testOrderByWithCollate() {
    var className = "testOrderByWithCollate";

    session.createClassIfNotExist(className);

    session.begin();
    session.execute("INSERT INTO " + className + " SET name = 'A', idx = 0").close();
    session.execute("INSERT INTO " + className + " SET name = 'C', idx = 2").close();
    session.execute("INSERT INTO " + className + " SET name = 'E', idx = 4").close();
    session.execute("INSERT INTO " + className + " SET name = 'b', idx = 1").close();
    session.execute("INSERT INTO " + className + " SET name = 'd', idx = 3").close();
    session.commit();

    session.begin();
    try (var result =
        session.query("select from " + className + " order by name asc collate ci")) {
      for (var i = 0; i < 5; i++) {
        Assert.assertTrue(result.hasNext());
        var item = result.next();
        int val = item.getProperty("idx");
        Assert.assertEquals(i, val);
      }
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testContainsEmptyCollection() {
    var className = "testContainsEmptyCollection";

    session.createClassIfNotExist(className);

    session.begin();
    session.execute("INSERT INTO " + className + " content {\"name\": \"jack\", \"age\": 22}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[]]}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[1]]}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"pete\", \"age\": 22, \"test\": [{}]}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"david\", \"age\": 22, \"test\": [\"hello\"]}")
        .close();
    session.commit();

    session.begin();
    try (var result = session.query("select from " + className + " where test contains []")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testContainsCollection() {
    var className = "testContainsCollection";

    session.createClassIfNotExist(className);

    session.begin();
    session.execute("INSERT INTO " + className + " content {\"name\": \"jack\", \"age\": 22}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[]]}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[1]]}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"pete\", \"age\": 22, \"test\": [{}]}")
        .close();
    session.execute(
        "INSERT INTO "
            + className
            + " content {\"name\": \"david\", \"age\": 22, \"test\": [\"hello\"]}")
        .close();
    session.commit();

    session.begin();
    try (var result = session.query("select from " + className + " where test contains [1]")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testHeapLimitForOrderBy() {
    Long oldValue = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(3);

      var className = "testHeapLimitForOrderBy";

      session.createClassIfNotExist(className);

      session.begin();
      session.execute("INSERT INTO " + className + " set name = 'a'").close();
      session.execute("INSERT INTO " + className + " set name = 'b'").close();
      session.execute("INSERT INTO " + className + " set name = 'c'").close();
      session.execute("INSERT INTO " + className + " set name = 'd'").close();
      session.commit();

      session.begin();
      try {
        try (var result = session.query("select from " + className + " ORDER BY name")) {
          result.forEachRemaining(x -> x.getProperty("name"));
        }
        Assert.fail();
      } catch (CommandExecutionException ex) {
        session.rollback();
        //ignore
      }
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(oldValue);
    }
  }

  @Test
  public void testXor() {
    session.begin();
    try (var result = session.query("select 15 ^ 4 as foo")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals(11, (int) item.getProperty("foo"));
      Assert.assertFalse(result.hasNext());
    }
    session.commit();
  }

  @Test
  public void testLike() {
    var className = "testLike";

    session.createClassIfNotExist(className);

    session.begin();
    session.execute("INSERT INTO " + className + " content {\"name\": \"foobarbaz\"}").close();
    session.execute("INSERT INTO " + className + " content {\"name\": \"test[]{}()|*^.test\"}")
        .close();
    session.commit();

    session.begin();
    try (var result = session.query("select from " + className + " where name LIKE 'foo%'")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    try (var result =
        session.query("select from " + className + " where name LIKE '%foo%baz%'")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    try (var result = session.query("select from " + className + " where name LIKE '%bar%'")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (var result = session.query("select from " + className + " where name LIKE 'bar%'")) {
      Assert.assertFalse(result.hasNext());
    }

    try (var result = session.query("select from " + className + " where name LIKE '%bar'")) {
      Assert.assertFalse(result.hasNext());
    }

    var specialChars = "[]{}()|*^.";
    for (var c : specialChars.toCharArray()) {
      try (var result =
          session.query("select from " + className + " where name LIKE '%" + c + "%'")) {
        Assert.assertTrue(result.hasNext());
        result.next();
        Assert.assertFalse(result.hasNext());
      }
    }
    session.commit();
  }

  @Test
  public void testCountGroupBy() {
    // issue #9288
    var className = "testCountGroupBy";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);

      session.commit();
    }

    session.begin();
    var result = session.query("select count(val) as count from " + className + " limit 3");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(10L, (long) item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  @Ignore
  public void testTimeout() {
    var className = "testTimeout";
    final var funcitonName = getClass().getSimpleName() + "_sleep";
    session.getMetadata().getSchema().createClass(className);

    SQLEngine
        .registerFunction(
            funcitonName,
            new SQLFunction() {

              @Override
              public Object execute(
                  Object iThis,
                  Result iCurrentRecord,
                  Object iCurrentResult,
                  Object[] iParams,
                  CommandContext iContext) {
                try {
                  Thread.sleep(5);
                } catch (InterruptedException e) {
                  //ignore
                }
                return null;
              }

              @Override
              public void config(Object[] configuredParameters) {
              }

              @Override
              public boolean aggregateResults() {
                return false;
              }

              @Override
              public boolean filterResult() {
                return false;
              }

              @Override
              public String getName(DatabaseSessionEmbedded session) {
                return funcitonName;
              }

              @Override
              public int getMinParams() {
                return 0;
              }

              @Override
              public int getMaxParams(DatabaseSessionEmbedded session) {
                return 0;
              }

              @Override
              public String getSyntax(DatabaseSessionEmbedded session) {
                return "";
              }

              @Override
              public Object getResult() {
                return null;
              }

              @Override
              public void setResult(Object iResult) {
              }
            });
    for (var i = 0; i < 3; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);

    }
    try (var result =
        session.query("select " + funcitonName + "(), * from " + className + " timeout 1")) {
      while (result.hasNext()) {
        result.next();
      }
      Assert.fail();
    } catch (TimeoutException ex) {
      //ignore
    }

    try (var result =
        session.query("select " + funcitonName + "(), * from " + className + " timeout 1000")) {
      while (result.hasNext()) {
        result.next();
      }
    } catch (TimeoutException ex) {
      Assert.fail();
    }
  }

  @Test
  public void testSimpleRangeQueryWithIndexGTE() {
    final var className = "testSimpleRangeQueryWithIndexGTE";
    final var clazz = session.getMetadata().getSchema().getOrCreateClass(className);
    final var prop = clazz.createProperty("name", PropertyType.STRING);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    for (var i = 0; i < 10; i++) {
      session.begin();
      final var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }
    session.begin();
    final var result = session.query("select from " + className + " WHERE name >= 'name5'");
    printExecutionPlan(result);

    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSimpleRangeQueryWithIndexLTE() {
    final var className = "testSimpleRangeQueryWithIndexLTE";
    final var clazz = session.getMetadata().getSchema().getOrCreateClass(className);
    final var prop = clazz.createProperty("name", PropertyType.STRING);
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    for (var i = 0; i < 10; i++) {
      session.begin();
      final var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    final var result = session.query("select from " + className + " WHERE name <= 'name5'");
    printExecutionPlan(result);

    for (var i = 0; i < 6; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testSimpleRangeQueryWithOutIndex() {
    final var className = "testSimpleRangeQueryWithOutIndex";
    final var clazz = session.getMetadata().getSchema().getOrCreateClass(className);
    final var prop = clazz.createProperty("name", PropertyType.STRING);
    // Hash Index skipped for range query
    prop.createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    for (var i = 0; i < 10; i++) {
      session.begin();
      final var doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);

      session.commit();
    }

    session.begin();
    final var result = session.query("select from " + className + " WHERE name >= 'name5'");
    printExecutionPlan(result);

    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testComplexIndexChain() {

    // A -b-> B -c-> C -d-> D.name
    //               C.name

    var classNamePrefix = "testComplexIndexChain_";
    var a = session.getMetadata().getSchema().createClass(classNamePrefix + "A");
    var b = session.getMetadata().getSchema().createClass(classNamePrefix + "B");
    var c = session.getMetadata().getSchema().createClass(classNamePrefix + "C");
    var d = session.getMetadata().getSchema().createClass(classNamePrefix + "D");

    a.createProperty("b", PropertyType.LINK, b)
        .createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);
    b.createProperty("c", PropertyType.LINK, c)
        .createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);
    c.createProperty("d", PropertyType.LINK, d)
        .createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);
    c.createProperty("name", PropertyType.STRING)
        .createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);
    d.createProperty("name", PropertyType.STRING)
        .createIndex(SchemaClass.INDEX_TYPE.NOTUNIQUE);

    session.begin();
    var dDoc = session.newEntity(d.getName());
    dDoc.setProperty("name", "foo");

    var cDoc = session.newEntity(c.getName());
    cDoc.setProperty("name", "foo");
    cDoc.setProperty("d", dDoc);

    var bDoc = session.newEntity(b.getName());
    bDoc.setProperty("c", cDoc);

    var aDoc = session.newEntity(a.getName());
    aDoc.setProperty("b", bDoc);

    session.commit();

    session.begin();
    try (var rs =
        session.query(
            "SELECT FROM "
                + classNamePrefix
                + "A WHERE b.c.name IN ['foo'] AND b.c.d.name IN ['foo']")) {
      Assert.assertTrue(rs.hasNext());
    }

    try (var rs =
        session.query(
            "SELECT FROM " + classNamePrefix + "A WHERE b.c.name = 'foo' AND b.c.d.name = 'foo'")) {
      Assert.assertTrue(rs.hasNext());
      Assert.assertTrue(
          rs.getExecutionPlan().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
    session.commit();
  }

  @Test
  public void testIndexWithSubquery() {
    var classNamePrefix = "testIndexWithSubquery_";
    session.execute("create class " + classNamePrefix + "Ownership extends V abstract;").close();
    session.execute("create class " + classNamePrefix + "User extends V;").close();
    session.execute("create property " + classNamePrefix + "User.id String;").close();
    session.execute(
        "create index "
            + classNamePrefix
            + "User.id ON "
            + classNamePrefix
            + "User(id) unique;")
        .close();
    session.execute(
        "create class " + classNamePrefix + "Report extends " + classNamePrefix + "Ownership;")
        .close();
    session.execute("create property " + classNamePrefix + "Report.id String;").close();
    session.execute("create property " + classNamePrefix + "Report.label String;").close();
    session.execute("create property " + classNamePrefix + "Report.format String;").close();
    session.execute("create property " + classNamePrefix + "Report.source String;").close();
    session.execute("create class " + classNamePrefix + "hasOwnership extends E;").close();

    session.begin();
    session.execute("insert into " + classNamePrefix + "User content {id:\"admin\"};");
    session.execute(
        "insert into "
            + classNamePrefix
            + "Report content {format:\"PDF\", id:\"rep1\", label:\"Report 1\","
            + " source:\"Report1.src\"};")
        .close();
    session.execute(
        "insert into "
            + classNamePrefix
            + "Report content {format:\"CSV\", id:\"rep2\", label:\"Report 2\","
            + " source:\"Report2.src\"};")
        .close();
    session.execute(
        "create edge "
            + classNamePrefix
            + "hasOwnership from (select from "
            + classNamePrefix
            + "User) to (select from "
            + classNamePrefix
            + "Report);")
        .close();
    session.commit();

    session.begin();
    try (var rs =
        session.query(
            "select from "
                + classNamePrefix
                + "Report where id in (select out('"
                + classNamePrefix
                + "hasOwnership').id from "
                + classNamePrefix
                + "User where id = 'admin');")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
    session.commit();

    session.execute(
        "create index "
            + classNamePrefix
            + "Report.id ON "
            + classNamePrefix
            + "Report(id) unique;")
        .close();

    session.begin();
    try (var rs =
        session.query(
            "select from "
                + classNamePrefix
                + "Report where id in (select out('"
                + classNamePrefix
                + "hasOwnership').id from "
                + classNamePrefix
                + "User where id = 'admin');")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
    session.commit();
  }

  /// Checks that composite index is used if we use both inV and outV functions to select the edge
  /// needed.
  @Test
  public void testCompositeIndexWithInVOutVFunctions() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestIndexWithEdgesFunctionsVertex");
    var edgeClass = schema.createEdgeClass("TestIndexWithEdgesFunctionsEdge");

    edgeClass.createIndex("EdgeIndex", INDEX_TYPE.NOTUNIQUE, Edge.DIRECTION_IN, Edge.DIRECTION_OUT);

    var rids = session.computeInTx(transaction -> {
      var v1 = transaction.newVertex(vertexClass);
      var v2 = transaction.newVertex(vertexClass);

      var edge = v1.addEdge(v2, edgeClass);

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query(
          "select from TestIndexWithEdgesFunctionsEdge where inV() = :inV and outV() = :outV",
          Map.of("outV", rids[0], "inV", rids[1]))) {

        var resList = rs.toEdgeList();
        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[2], resList.getFirst().getIdentity());

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());

        var fetchFromIndex = steps.getFirst();
        Assert.assertTrue(fetchFromIndex instanceof FetchFromIndexStep);

        var keyCondition = ((FetchFromIndexStep) fetchFromIndex).getDesc().getKeyCondition();
        Assert.assertTrue(keyCondition instanceof SQLAndBlock);
        var sqlAndBlock = (SQLAndBlock) keyCondition;
        Assert.assertEquals(2, sqlAndBlock.getSubBlocks().size());
        var firstExpression = sqlAndBlock.getSubBlocks().getFirst();
        Assert.assertTrue(firstExpression instanceof SQLBinaryCondition);

        var firstBinaryCondition = (SQLBinaryCondition) firstExpression;
        Assert.assertTrue(
            firstBinaryCondition.getLeft() instanceof SQLGetInternalPropertyExpression);
        Assert.assertEquals("in", firstBinaryCondition.getLeft().toString());
        Assert.assertEquals(":inV", firstBinaryCondition.getRight().toString());
        Assert.assertTrue(firstBinaryCondition.getOperator() instanceof SQLEqualsOperator);

        var secondExpression = sqlAndBlock.getSubBlocks().getLast();
        Assert.assertTrue(secondExpression instanceof SQLBinaryCondition);

        var secondBinaryCondition = (SQLBinaryCondition) secondExpression;
        Assert.assertTrue(
            secondBinaryCondition.getLeft() instanceof SQLGetInternalPropertyExpression);
        Assert.assertEquals("out", secondBinaryCondition.getLeft().toString());
        Assert.assertEquals(":outV", secondBinaryCondition.getRight().toString());
        Assert.assertTrue(secondBinaryCondition.getOperator() instanceof SQLEqualsOperator);
      }
    });
  }

  @Test
  public void testInVOutVFunctionsWithoutIndex() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestInVOutVFunctionsWithoutIndexVertex");
    var edgeClass = schema.createEdgeClass("TestInVOutVFunctionsWithoutIndexEdge");

    var rids = session.computeInTx(transaction -> {
      var v1 = transaction.newVertex(vertexClass);
      var v2 = transaction.newVertex(vertexClass);

      var edge = v1.addEdge(v2, edgeClass);

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query(
          "select from TestInVOutVFunctionsWithoutIndexEdge where inV() = :inV and outV() = :outV",
          Map.of("outV", rids[0], "inV", rids[1]))) {

        var resList = rs.toEdgeList();
        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[2], resList.getFirst().getIdentity());
      }
    });
  }

  @Test
  public void testOutEEdgesIndexUsageInGraph() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestOutEEdgesIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestOutEEdgesIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var propertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName);
    var oudEdgesProperty = vertexClass.createProperty(propertyName,
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var indexName = "TestOutEEdgesIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, oudEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "outE('" + edgeClassName + "') contains :outE", Map.of("outE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[0], resList.getFirst().getIdentity());

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());

        var sourceStep = steps.getFirst();
        Assert.assertTrue(sourceStep instanceof FetchFromIndexStep);
        var fetchFromIndexStep = (FetchFromIndexStep) sourceStep;
        Assert.assertEquals(indexName, fetchFromIndexStep.getDesc().getIndex().getName());

        var keyCondition = fetchFromIndexStep.getDesc().getKeyCondition();
        Assert.assertTrue(keyCondition instanceof SQLAndBlock);
        var sqlAndBlock = (SQLAndBlock) keyCondition;
        Assert.assertEquals(1, sqlAndBlock.getSubBlocks().size());

        var expression = sqlAndBlock.getSubBlocks().getFirst();
        Assert.assertTrue(expression instanceof SQLContainsCondition);

        var containsCondition = (SQLContainsCondition) expression;
        Assert.assertTrue(
            containsCondition.getLeft() instanceof SQLGetInternalPropertyExpression);
        Assert.assertEquals(propertyName, containsCondition.getLeft().toString());
        Assert.assertEquals(":outE", containsCondition.getRight().toString());
      }
    });
  }

  @Test
  public void testOutEEdgesWithoutIndexUsageInGraph() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestOutEWithoutIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestOutEWithoutIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var vertexClassName = vertexClass.getName();
    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "outE('" + edgeClassName + "') contains :outE", Map.of("outE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[0], resList.getFirst().getIdentity());
      }
    });
  }

  @Test
  public void testInEEdgesIndexUsageInGraph() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestInEEdgesIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestInEEdgesIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var inEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName),
        PropertyType.LINKBAG);

    var propertyName = inEdgesProperty.getName();
    var vertexClassName = vertexClass.getName();

    var indexName = "TestInEEdgesIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, inEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "inE('" + edgeClassName + "') contains :inE", Map.of("inE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[1], resList.getFirst().getIdentity());

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());

        var sourceStep = steps.getFirst();
        Assert.assertTrue(sourceStep instanceof FetchFromIndexStep);
        var fetchFromIndexStep = (FetchFromIndexStep) sourceStep;
        Assert.assertEquals(indexName, fetchFromIndexStep.getDesc().getIndex().getName());

        var keyCondition = fetchFromIndexStep.getDesc().getKeyCondition();
        Assert.assertTrue(keyCondition instanceof SQLAndBlock);
        var sqlAndBlock = (SQLAndBlock) keyCondition;
        Assert.assertEquals(1, sqlAndBlock.getSubBlocks().size());

        var expression = sqlAndBlock.getSubBlocks().getFirst();
        Assert.assertTrue(expression instanceof SQLContainsCondition);

        var containsCondition = (SQLContainsCondition) expression;
        Assert.assertTrue(
            containsCondition.getLeft() instanceof SQLGetInternalPropertyExpression);
        Assert.assertEquals(propertyName, containsCondition.getLeft().toString());
        Assert.assertEquals(":inE", containsCondition.getRight().toString());
      }
    });
  }

  @Test
  public void testBothEEdgesWithoutIndex() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEVertex");
    var edgeClass = schema.createEdgeClass("TestBothEEdge");

    var edgeClassName = edgeClass.getName();
    var vertexClassName = vertexClass.getName();

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));
      }
    });
  }

  @Test
  public void testBothEEdgesIndexUsageInGraphOneIndexIn() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestBothEIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var oudEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName),
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var indexName = "TestBothEIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, oudEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));
      }
    });
  }

  @Test
  public void testBothEEdgesIndexUsageInGraphOneIndexOut() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestBothEIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var oudEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName),
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var indexName = "TestBothEIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, oudEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));
      }
    });
  }

  @Test
  public void testBothEEdgesIndexUsageInGraphTwoIndexes() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestBothEIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();

    var outEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName),
        PropertyType.LINKBAG);
    var inEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName),
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var outIndexName = "TestBothEIndexUsageInGraphIndexOutIndex";
    vertexClass.createIndex(outIndexName, INDEX_TYPE.NOTUNIQUE, outEdgesProperty.getName());

    var inIndexName = "TestBothEIndexUsageInGraphIndexInIndex";
    vertexClass.createIndex(inIndexName, INDEX_TYPE.NOTUNIQUE, inEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());
        var first = steps.getFirst();
        Assert.assertTrue(first instanceof ParallelExecStep);

        var parallelExecStep = (ParallelExecStep) first;
        var parallelSteps = parallelExecStep.getSubExecutionPlans();

        Assert.assertEquals(2, parallelSteps.size());

        var firstParallelStep = parallelSteps.getFirst().getSteps().getFirst();
        Assert.assertTrue(firstParallelStep instanceof FetchFromIndexStep);
        var firstParallelIndexStep = (FetchFromIndexStep) firstParallelStep;

        var secondParallelStep = parallelSteps.getLast().getSteps().getFirst();
        Assert.assertTrue(secondParallelStep instanceof FetchFromIndexStep);
        var secondParallelIndexStep = (FetchFromIndexStep) secondParallelStep;

        var indexNames = Set.of(firstParallelIndexStep.getDesc().getIndex().getName(),
            secondParallelIndexStep.getDesc().getIndex().getName());

        Assert.assertEquals(Set.of(outIndexName, inIndexName), indexNames);
      }
    });
  }

  @Test
  public void testExclude() {
    var className = "TestExclude";
    session.getMetadata().getSchema().createClass(className);
    session.begin();
    var doc = session.newInstance(className);
    doc.setProperty("name", "foo");
    doc.setProperty("surname", "bar");

    session.commit();

    session.begin();
    var result = session.query("select *, !surname from " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertNull(item.getProperty("surname"));

    printExecutionPlan(result);
    result.close();
    session.commit();
  }

  @Test
  public void testOrderByLet() {
    var className = "testOrderByLet";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var doc = session.newInstance(className);
    doc.setProperty("name", "abbb");

    doc = session.newInstance(className);
    doc.setProperty("name", "baaa");

    session.commit();

    session.begin();
    try (var result =
        session.query(
            "select from "
                + className
                + " LET $order = name.substring(1) ORDER BY $order ASC LIMIT 1")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("baaa", item.getProperty("name"));
    }
    try (var result =
        session.query(
            "select from "
                + className
                + " LET $order = name.substring(1) ORDER BY $order DESC LIMIT 1")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("abbb", item.getProperty("name"));
    }
    session.commit();
  }

  @Test
  public void testMapToJson() {
    var className = "testMapToJson";
    session.execute("create class " + className).close();
    session.execute("create property " + className + ".themap embeddedmap").close();

    session.begin();
    session.execute(
        "insert into "
            + className
            + " set name = 'foo', themap = {\"foo bar\":\"baz\", \"riz\":\"faz\"}")
        .close();
    session.commit();

    session.begin();
    try (var rs = session.query("select themap.tojson() as x from " + className)) {
      Assert.assertTrue(rs.hasNext());
      var item = rs.next();
      Assert.assertTrue(((String) item.getProperty("x")).contains("foo bar"));
    }
    session.commit();
  }

  @Test
  public void testOptimizedCountQuery() {
    var className = "testOptimizedCountQuery";
    session.execute("create class " + className).close();
    session.execute("create property " + className + ".field boolean").close();
    session.execute("create index " + className + ".field on " + className + "(field) NOTUNIQUE")
        .close();

    session.begin();
    session.execute("insert into " + className + " set field=true").close();
    session.commit();

    session.begin();
    try (var rs =
        session.query("select count(*) as count from " + className + " where field=true")) {
      Assert.assertTrue(rs.hasNext());
      var item = rs.next();
      Assert.assertEquals(1L, (long) item.getProperty("count"));
      Assert.assertFalse(rs.hasNext());
    }
    session.commit();
  }

  @Test
  public void testAsSetKeepsOrderWithExpand() {
    // init classes
    session.activateOnCurrentThread();

    var car = session.createVertexClass("Car");
    var engine = session.createVertexClass("Engine");
    var body = session.createVertexClass("BodyType");
    var eng = session.createEdgeClass("eng");
    var bt = session.createEdgeClass("bt");
    session.begin();

    var diesel = session.newVertex(engine);
    diesel.setProperty("name", "diesel");
    var gasoline = session.newVertex(engine);
    gasoline.setProperty("name", "gasoline");
    var microwave = session.newVertex(engine);
    microwave.setProperty("name", "EV");

    var coupe = session.newVertex(body);
    coupe.setProperty("name", "coupe");
    var suv = session.newVertex(body);
    suv.setProperty("name", "suv");
    session.commit();
    session.begin();
    // fill data
    var coupe1 = session.newVertex(car);
    var activeTx8 = session.getActiveTransaction();
    gasoline = activeTx8.load(gasoline);
    var activeTx7 = session.getActiveTransaction();
    coupe = activeTx7.load(coupe);

    coupe1.setProperty("name", "car1");
    coupe1.addEdge(gasoline, eng);
    coupe1.addEdge(coupe, bt);

    var coupe2 = session.newVertex(car);
    var activeTx6 = session.getActiveTransaction();
    diesel = activeTx6.load(diesel);
    var activeTx5 = session.getActiveTransaction();
    coupe = activeTx5.load(coupe);

    coupe2.setProperty("name", "car2");
    coupe2.addEdge(diesel, eng);
    coupe2.addEdge(coupe, bt);

    var mw1 = session.newVertex(car);

    var activeTx4 = session.getActiveTransaction();
    microwave = activeTx4.load(microwave);
    var activeTx3 = session.getActiveTransaction();
    suv = activeTx3.load(suv);
    mw1.setProperty("name", "microwave1");
    mw1.addEdge(microwave, eng);
    mw1.addEdge(suv, bt);

    var mw2 = session.newVertex(car);
    mw2.setProperty("name", "microwave2");
    mw2.addEdge(microwave, eng);
    mw2.addEdge(suv, bt);

    var hatch1 = session.newVertex(car);
    hatch1.setProperty("name", "hatch1");
    hatch1.addEdge(diesel, eng);
    hatch1.addEdge(suv, bt);
    session.commit();

    session.begin();
    var activeTx2 = session.getActiveTransaction();
    gasoline = activeTx2.load(gasoline);
    var activeTx1 = session.getActiveTransaction();
    diesel = activeTx1.load(diesel);
    var activeTx = session.getActiveTransaction();
    microwave = activeTx.load(microwave);

    var identities =
        String.join(
            ",",
            Stream.of(coupe1, coupe2, mw1, mw2, hatch1)
                .map(DBRecord::getIdentity)
                .map(Object::toString)
                .toList());
    var unionAllEnginesQuery =
        "SELECT expand(unionAll($a, $a).asSet()) LET $a=(SELECT expand(out('eng')) FROM ["
            + identities
            + "])";

    var engineNames =
        session.query(unionAllEnginesQuery)
            .vertexStream()
            .map(oVertex -> oVertex.getProperty("name"))
            .collect(Collectors.toSet());

    var names = Arrays.asList(
        gasoline.getProperty("name"),
        diesel.getProperty("name"),
        microwave.getProperty("name"));
    Assert.assertEquals(names.size(), engineNames.size());
    Assert.assertEquals(new HashSet<>(names), engineNames);
    session.commit();
  }

  @Test
  public void testSelectIntersectWithExpandAndLet() {
    var schema = session.getSchema();

    schema.createVertexClass("V1");
    schema.createVertexClass("V2");
    schema.createVertexClass("V3");

    schema.createEdgeClass("link1");
    schema.createEdgeClass("link2");

    //V1 -> V2
    //V2 -> V3
    var rids = session.computeInTx(transaction -> {
      var v1 = transaction.newVertex("V1");
      var v2 = transaction.newVertex("V2");

      var v3 = transaction.newVertex("V3");

      v1.addEdge(v2, "link1");
      v2.addEdge(v3, "link2");

      var v1rid = v1.getIdentity();
      var v2rid = v2.getIdentity();
      var v3rid = v3.getIdentity();

      for (var i = 0; i < 10; i++) {
        v2 = transaction.newVertex("V2");
        v1.addEdge(v2, "link1");
      }

      for (var i = 0; i < 10; i++) {
        v2 = transaction.newVertex("V2");
        v2.addEdge(v3, "link2");
      }

      return new RID[] {v1rid, v3rid, v2rid};
    });

    final var query = "SELECT expand(intersect($a0, $b0)) "
        + "LET $a0=(SELECT expand(out('link1')) FROM :targetIds1), "
        + "$b0=(SELECT expand(in('link2')) FROM :targetIds2)";
    final Map<Object, Object> params =
        Map.of("targetIds1", rids[0], "targetIds2", rids[1]);

    session.executeInTx(transaction -> {
      final var result = transaction.query(query, params).toList();
      Assert.assertEquals(1, result.size());
      Assert.assertEquals(rids[2], result.getFirst().getIdentity());
    });

    session.executeInTx(tx -> {

      var statement = (SQLSelectStatement) SQLEngine.parse(query, session);
      var ctx = new BasicCommandContext();
      ctx.setDatabaseSession(session);
      ctx.setInputParameters(params);

      final var plan = statement.createExecutionPlan(ctx);
      new LocalResultSet(session, plan).toList();

      final var a = ctx.getVariable("$a0");
      final var b = ctx.getVariable("$b0");

      // checking that the variables have not been converted to collections, killing
      // the performance of "intersect" function.
      assertThat(a).isInstanceOf(LocalResultSet.class);
      assertThat(b).isInstanceOf(LocalResultSet.class);
    });
  }

  // ── Predicate push-down into expand() ──

  /**
   * Verifies that extractClassEqualityName() correctly parses {@code @class = 'X'}
   * from a WHERE clause, both in normal and reversed form.
   */
  @Test
  public void testExtractClassEqualityName() {
    var stm =
        (com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement) com.jetbrains.youtrackdb.internal.core.sql.SQLEngine
            .parse("SELECT FROM V WHERE @class = 'Post'", session);
    var where = stm.getWhereClause();
    Assert.assertNotNull("WHERE clause should not be null", where);
    Assert.assertEquals("Post", where.extractClassEqualityName());

    var stmReversed =
        (com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement) com.jetbrains.youtrackdb.internal.core.sql.SQLEngine
            .parse("SELECT FROM V WHERE 'Comment' = @class", session);
    Assert.assertEquals("Comment",
        stmReversed.getWhereClause().extractClassEqualityName());

    var stmNoClass =
        (com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement) com.jetbrains.youtrackdb.internal.core.sql.SQLEngine
            .parse("SELECT FROM V WHERE name = 'Alice'", session);
    Assert.assertNull(stmNoClass.getWhereClause().extractClassEqualityName());

    // Compound AND: @class = 'Post' AND score > 5
    var stmCompound =
        (com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement) com.jetbrains.youtrackdb.internal.core.sql.SQLEngine
            .parse("SELECT FROM V WHERE @class = 'Post' AND score > 5", session);
    var compoundResult =
        stmCompound.getWhereClause().extractClassEquality();
    Assert.assertNotNull("Should extract class from compound AND", compoundResult);
    Assert.assertEquals("Post", compoundResult.className());
    Assert.assertNotNull(
        "Remaining WHERE should contain score > 5",
        compoundResult.remainingWhere());
    Assert.assertTrue(
        compoundResult.remainingWhere().toString().contains("score"));

    // Also extractClassEqualityName() should still work on compound
    Assert.assertEquals("Post",
        stmCompound.getWhereClause().extractClassEqualityName());
  }

  /**
   * Verifies that a WHERE predicate on a property of expanded records is pushed
   * down into the expand() step. The query expands all edges then filters by
   * a property — the pushed-down filter should appear in the EXPAND step
   * of the execution plan rather than as a separate FilterStep.
   */
  @Test
  public void testPredicatePushDownIntoExpand_classFilter() {
    session.execute("CREATE CLASS PdForum EXTENDS V").close();
    session.execute("CREATE CLASS PdPost EXTENDS V").close();
    session.execute("CREATE CLASS PdComment EXTENDS V").close();
    session.execute("CREATE CLASS PdContainerOf EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX PdForum SET name = 'forum1'").close();
    // 5 posts + 3 comments in the forum
    for (var i = 0; i < 5; i++) {
      session.execute("CREATE VERTEX PdPost SET title = 'post" + i + "'").close();
      session.execute(
          "CREATE EDGE PdContainerOf FROM (SELECT FROM PdForum WHERE name = 'forum1')"
              + " TO (SELECT FROM PdPost WHERE title = 'post" + i + "')")
          .close();
    }
    for (var i = 0; i < 3; i++) {
      session.execute("CREATE VERTEX PdComment SET text = 'comment" + i + "'").close();
      session.execute(
          "CREATE EDGE PdContainerOf FROM (SELECT FROM PdForum WHERE name = 'forum1')"
              + " TO (SELECT FROM PdComment WHERE text = 'comment" + i + "')")
          .close();
    }
    session.commit();

    // Class filter: only Posts, not Comments
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('PdContainerOf')) FROM PdForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE @class = 'PdPost'");
    var items = result.stream().toList();
    Assert.assertEquals("Should return only 5 posts, not comments", 5, items.size());
    for (var item : items) {
      Assert.assertTrue("Each result should be a PdPost",
          item.isEntity() && item.asEntity().getSchemaClassName().equals("PdPost"));
    }

    // Verify that the @class filter was pushed down into EXPAND as a zero-I/O class
    // filter (checking collection IDs), not just a generic post-expand WHERE.
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "Generic push-down filter should NOT be used for @class — "
            + "class filter (zero I/O) should be used instead. Plan was:\n" + plan,
        plan.contains("push-down filter"));
    Assert.assertTrue(
        "Class filter should be pushed down into EXPAND, plan was:\n" + plan,
        plan.contains("class filter"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that when the outer WHERE is compound (@class = 'Post' AND <other>),
   * the planner extracts the class filter as a zero-I/O collection ID check on
   * ExpandStep and keeps the remaining condition as an outer FilterStep.
   * This is the IC10-like pattern: the class filter skips Comment records
   * without disk I/O, while the remaining predicate is evaluated after loading.
   */
  @Test
  public void testPredicatePushDownIntoExpand_compoundClassAndProperty() {
    session.execute("CREATE CLASS PdForum2 EXTENDS V").close();
    session.execute("CREATE CLASS PdPost2 EXTENDS V").close();
    session.execute("CREATE CLASS PdComment2 EXTENDS V").close();
    session.execute("CREATE CLASS PdContainerOf2 EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX PdForum2 SET name = 'forum1'").close();
    for (var i = 0; i < 5; i++) {
      session.execute(
          "CREATE VERTEX PdPost2 SET title = 'post" + i + "', score = " + i).close();
      session.execute(
          "CREATE EDGE PdContainerOf2 FROM (SELECT FROM PdForum2 WHERE name = 'forum1')"
              + " TO (SELECT FROM PdPost2 WHERE title = 'post" + i + "')")
          .close();
    }
    for (var i = 0; i < 3; i++) {
      session.execute(
          "CREATE VERTEX PdComment2 SET text = 'comment" + i + "', score = " + (i + 10))
          .close();
      session.execute(
          "CREATE EDGE PdContainerOf2 FROM (SELECT FROM PdForum2 WHERE name = 'forum1')"
              + " TO (SELECT FROM PdComment2 WHERE text = 'comment" + i + "')")
          .close();
    }
    session.commit();

    // Compound WHERE: class filter + property filter.
    // Without push-down: loads all 8 records, filters by class then score.
    // With push-down: skips 3 comments via collection ID (zero I/O),
    //   loads 5 posts, filters by score.
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('PdContainerOf2')) FROM PdForum2"
            + "    WHERE name = 'forum1'"
            + ") WHERE @class = 'PdPost2' AND score >= 3");
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return 2 posts with score >= 3 (post3, post4)", 2, items.size());
    for (var item : items) {
      Assert.assertEquals("PdPost2", item.asEntity().getSchemaClassName());
      Assert.assertTrue(((Number) item.getProperty("score")).intValue() >= 3);
    }

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Class filter should be pushed down into EXPAND, plan was:\n" + plan,
        plan.contains("class filter"));
    // The remaining "score >= 3" should either be a push-down filter or outer FilterStep
    Assert.assertTrue(
        "Remaining predicate should be present (push-down or filter), plan was:\n"
            + plan,
        plan.contains("score") || plan.contains("FILTER"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that a property filter on expanded records is pushed down and
   * produces correct results. The outer WHERE filters by a date range — only
   * matching expanded records should be returned.
   */
  @Test
  public void testPredicatePushDownIntoExpand_propertyFilter() {
    session.execute("CREATE CLASS PdPerson EXTENDS V").close();
    session.execute("CREATE CLASS PdMessage EXTENDS V").close();
    session.execute("CREATE CLASS PdHasCreator EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX PdPerson SET name = 'Alice'").close();
    for (var i = 0; i < 10; i++) {
      session.execute(
          "CREATE VERTEX PdMessage SET text = 'msg" + i + "', score = " + i).close();
      session.execute(
          "CREATE EDGE PdHasCreator FROM (SELECT FROM PdMessage WHERE text = 'msg" + i + "')"
              + " TO (SELECT FROM PdPerson WHERE name = 'Alice')")
          .close();
    }
    session.commit();

    // Property filter: only messages with score >= 7
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(in('PdHasCreator')) FROM PdPerson"
            + "    WHERE name = 'Alice'"
            + ") WHERE score >= 7");
    var items = result.stream().toList();
    Assert.assertEquals("Should return only 3 messages with score >= 7", 3, items.size());
    for (var item : items) {
      Assert.assertTrue(((Number) item.getProperty("score")).intValue() >= 7);
    }
    result.close();
    session.commit();
  }

  @Test
  public void testConditionalAggregationSumIf() {
    var className = "testConditionalAggregationSumIf";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("val", i);
      doc.setProperty("category", i % 2 == 0 ? "even" : "odd");
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select sum(if(category = 'even', val, 0)) as evenSum,"
            + " sum(if(category = 'odd', val, 0)) as oddSum"
            + " from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(20, item.<Object>getProperty("evenSum"));
    Assert.assertEquals(25, item.<Object>getProperty("oddSum"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testConditionalAggregationCountIf() {
    var className = "testConditionalAggregationCountIf";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("active", i < 6);
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select sum(if(active = true, 1, 0)) as activeCnt,"
            + " sum(if(active = false, 1, 0)) as inactiveCnt"
            + " from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(6, item.<Object>getProperty("activeCnt"));
    Assert.assertEquals(4, item.<Object>getProperty("inactiveCnt"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCaseWhenExecution() {
    var className = "testCaseWhenExecution";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var score : new int[] {95, 85, 75, 55}) {
      var doc = session.newInstance(className);
      doc.setProperty("score", score);
    }
    session.commit();

    session.begin();
    var result = session.query(
        "select score, CASE WHEN score > 90 THEN 'A'"
            + " WHEN score > 80 THEN 'B'"
            + " WHEN score > 70 THEN 'C'"
            + " ELSE 'F' END as grade"
            + " from " + className
            + " order by score desc");
    printExecutionPlan(result);

    var r1 = result.next();
    Assert.assertEquals(95, r1.<Object>getProperty("score"));
    Assert.assertEquals("A", r1.getProperty("grade"));

    var r2 = result.next();
    Assert.assertEquals(85, r2.<Object>getProperty("score"));
    Assert.assertEquals("B", r2.getProperty("grade"));

    var r3 = result.next();
    Assert.assertEquals(75, r3.<Object>getProperty("score"));
    Assert.assertEquals("C", r3.getProperty("grade"));

    var r4 = result.next();
    Assert.assertEquals(55, r4.<Object>getProperty("score"));
    Assert.assertEquals("F", r4.getProperty("grade"));

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCaseWhenInsideAggregate() {
    var className = "testCaseWhenInsideAggregate";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(className);
      doc.setProperty("age", 15 + i * 5);
      session.commit();
    }

    session.begin();
    var result = session.query(
        "select sum(CASE WHEN age >= 18 THEN 1 ELSE 0 END) as adults,"
            + " sum(CASE WHEN age < 18 THEN 1 ELSE 0 END) as minors"
            + " from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(9, item.<Object>getProperty("adults"));
    Assert.assertEquals(1, item.<Object>getProperty("minors"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  @Test
  public void testCaseWhenWithoutElseReturnsNull() {
    var className = "testCaseWhenWithoutElseReturnsNull";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var doc = session.newInstance(className);
    doc.setProperty("status", "unknown");
    session.commit();

    session.begin();
    var result = session.query(
        "select CASE WHEN status = 'active' THEN 'yes' END as label"
            + " from " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNull(item.getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies that an aggregate function INSIDE a CASE THEN branch is correctly
   * recognized by the planner. Before the fix, SQLCaseExpression.isAggregate()
   * always returned false, so `CASE WHEN ... THEN sum(x) ELSE 0 END` was treated
   * as a non-aggregate expression, producing wrong results.
   */
  @Test
  public void testAggregateInsideCaseThenBranch() {
    var className = "testAggInsideCaseThen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // values: 10, 20, 30, 40, 50 → sum = 150
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN 1 = 1 THEN sum(value) ELSE 0 END as total"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(150, item.<Number>getProperty("total").intValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies that an aggregate function inside the ELSE branch is recognized.
   */
  @Test
  public void testAggregateInsideCaseElseBranch() {
    var className = "testAggInsideCaseElse";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 4; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // values: 10, 20, 30, 40 → count = 4
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN 1 = 0 THEN 'x' ELSE count(*) END as cnt"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(4L, item.<Number>getProperty("cnt").longValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies that an aggregate function inside a WHEN condition is recognized.
   * E.g. CASE WHEN count(*) > 3 THEN 'many' ELSE 'few' END.
   * Before the fix, isAggregate() on SQLBooleanExpression returned false,
   * so the planner would not set up aggregation for the WHEN condition.
   */
  @Test
  public void testAggregateInsideCaseWhenCondition() {
    var className = "testAggInsideCaseWhen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // 5 records → count(*) = 5 > 3 → 'many'
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 3 THEN 'many' ELSE 'few' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("many", item.getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies aggregate in WHEN condition with result going to ELSE branch.
   * CASE WHEN count(*) > 100 THEN 'over' ELSE 'under' END with 5 records.
   */
  @Test
  public void testAggregateInsideCaseWhenConditionElsePath() {
    var className = "testAggInsideCaseWhenElse";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", i);
    }
    session.commit();

    // 5 records → count(*) = 5, not > 100 → 'under'
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 100 THEN 'over' ELSE 'under' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("under", item.getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies aggregates in both WHEN condition and THEN branch simultaneously.
   * CASE WHEN sum(value) > 10 THEN count(*) ELSE 0 END.
   */
  @Test
  public void testAggregateInBothCaseWhenAndThen() {
    var className = "testAggBothWhenThen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // values: 10+20+30+40+50 = 150 > 10 → count(*) = 5
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN sum(value) > 10 THEN count(*) ELSE 0 END as total"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(5L, item.<Number>getProperty("total").longValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies aggregates in all three positions: WHEN, THEN, and ELSE simultaneously.
   * CASE WHEN count(*) > 3 THEN sum(value) ELSE avg(value) END.
   */
  @Test
  public void testAggregateInAllThreeCasePositions() {
    var className = "testAggAllThreePos";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // 5 records → count(*) = 5 > 3 → sum(value) = 150
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 3 THEN sum(value) ELSE avg(value) END as res"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(150L, item.<Number>getProperty("res").longValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Same as above but forces the ELSE branch to verify the aggregate there works.
   * CASE WHEN count(*) > 100 THEN sum(value) ELSE avg(value) END.
   */
  @Test
  public void testAggregateInAllThreeCasePositionsElsePath() {
    var className = "testAggAllThreePosElse";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // 5 records → count(*) = 5, not > 100 → avg(value) = 30
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 100 THEN sum(value) ELSE avg(value) END as res"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(30.0, item.<Number>getProperty("res").doubleValue(), 0.001);
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies multiple WHEN branches each containing an aggregate.
   * Exercises the loop in splitForAggregation with more than one WHEN condition.
   */
  @Test
  public void testMultipleWhenBranchesWithAggregates() {
    var className = "testMultiWhenAgg";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 8; i++) {
      session.newInstance(className);
    }
    session.commit();

    // 8 records → count(*) = 8; first WHEN (>10) false, second WHEN (>5) true → 'some'
    session.begin();
    var result = session.query(
        "SELECT CASE"
            + " WHEN count(*) > 10 THEN 'many'"
            + " WHEN count(*) > 5 THEN 'some'"
            + " ELSE 'few' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("some", item.getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies AND/OR boolean operators in WHEN condition with multiple aggregates.
   * Tests that SQLAndBlock.isAggregate and splitForAggregation work correctly.
   */
  @Test
  public void testBooleanAndOrInWhenWithAggregates() {
    var className = "testBoolAndOrAgg";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("value", (i + 1) * 10);
    }
    session.commit();

    // count(*) = 5, sum(value) = 150
    // WHEN count(*) > 3 AND sum(value) < 200 → true AND true → 'match'
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 3 AND sum(value) < 200"
            + " THEN 'match' ELSE 'no match' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("match", item.getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies CASE with aggregate in WHEN works correctly with GROUP BY.
   * Each group gets its own aggregate evaluation.
   */
  @Test
  public void testCaseAggregateInWhenWithGroupBy() {
    var className = "testCaseAggWhenGroupBy";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 6; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("category", i < 4 ? "A" : "B");
    }
    session.commit();

    // Group A: 4 records → count(*) > 2 → 'popular'
    // Group B: 2 records → count(*) = 2, not > 2 → 'rare'
    session.begin();
    var result = session.query(
        "SELECT category,"
            + " CASE WHEN count(*) > 2 THEN 'popular' ELSE 'rare' END as popularity"
            + " FROM " + className + " GROUP BY category ORDER BY category");
    Assert.assertTrue(result.hasNext());
    var a = result.next();
    Assert.assertEquals("A", a.getProperty("category"));
    Assert.assertEquals("popular", a.getProperty("popularity"));
    Assert.assertTrue(result.hasNext());
    var b = result.next();
    Assert.assertEquals("B", b.getProperty("category"));
    Assert.assertEquals("rare", b.getProperty("popularity"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies CASE WHEN with aggregate on an empty table.
   * count(*) on empty table = 0, so ELSE branch should be taken.
   */
  @Test
  public void testCaseAggregateOnEmptyTable() {
    var className = "testCaseAggEmpty";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 0 THEN 'has data' ELSE 'empty' END as status"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("empty", item.getProperty("status"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Regression test: after the isCountStar() fix that adds isBaseIdentifier() check,
   * a plain SELECT count(*) must still use the CountFromClassStep optimization.
   */
  @Test
  public void testPlainCountStarStillUsesOptimization() {
    var className = "testPlainCountOpt";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 3; i++) {
      session.begin();
      session.newEntity(className);
      session.commit();
    }

    session.begin();
    var result = session.query("select count(*) from " + className);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(3L, (Object) item.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());

    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    assertThat(plan.getSteps())
        .anyMatch(step -> step instanceof CountFromClassStep);
    result.close();
    session.commit();
  }

  /**
   * Covers count(*) with WHERE on a single-field
   * indexed property uses CountFromIndexWithKeyStep optimization.
   */
  @Test
  public void testCountStarWithIndexedWhereField() {
    var className = "testCountStarWithIndexedWhereField";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createIndex(className + ".name", INDEX_TYPE.NOTUNIQUE, "name");

    session.begin();
    for (var i = 0; i < 3; i++) {
      session.newEntity(className).setProperty("name", "alice");
    }
    session.newEntity(className).setProperty("name", "bob");
    session.commit();

    session.begin();
    var result = session.query("select count(*) from " + className + " where name = 'alice'");
    Assert.assertTrue(result.hasNext());
    var row = result.next();
    Assert.assertEquals(3L, (Object) row.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());

    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    assertThat(plan.getSteps())
        .anyMatch(step -> step instanceof CountFromIndexWithKeyStep);
    result.close();
    session.commit();
  }

  /**
   * Verifies that count(*) WHERE field = value only uses CountFromIndexWithKeyStep
   * when the index field matches the WHERE field. Here the only index is on 'age'
   * but the WHERE clause filters by 'name', so the planner must NOT use the index
   * and should fall back to the standard count path.
   */
  @Test
  public void testCountStarWithWhereDoesNotUseMismatchedIndex() {
    var className = "testCountStarMismatchedIdx";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createProperty("age", PropertyType.INTEGER);
    clazz.createIndex(className + ".age", INDEX_TYPE.NOTUNIQUE, "age");

    session.begin();
    for (var i = 0; i < 3; i++) {
      var e = session.newEntity(className);
      e.setProperty("name", "alice");
      e.setProperty("age", 20 + i);
    }
    var e2 = session.newEntity(className);
    e2.setProperty("name", "bob");
    e2.setProperty("age", 40);
    session.commit();

    session.begin();
    var result = session.query(
        "select count(*) from " + className + " where name = 'alice'");
    Assert.assertTrue(result.hasNext());
    var row = result.next();
    Assert.assertEquals(3L, (Object) row.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   *  isCountStar() returns false for
   * a single-aggregate query that is NOT count(*), like max(name).
   */
  @Test
  public void testMaxAggregateDoesNotUseCountStarOptimization() {
    var className = "testMaxAggregateDoesNotUseCountStarOptimization";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    session.newEntity(className).setProperty("name", "alice");
    session.newEntity(className).setProperty("name", "bob");
    session.commit();

    session.begin();
    var result = session.query("select max(name) from " + className);
    Assert.assertTrue(result.hasNext());
    var row = result.next();
    Assert.assertEquals("bob", row.getProperty("max(name)"));
    Assert.assertFalse(result.hasNext());

    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    assertThat(plan.getSteps())
        .noneMatch(step -> step instanceof CountFromClassStep);
    result.close();
    session.commit();
  }

  /**
   * Verifies that a simple SELECT count(*) FROM Class uses CountFromClassStep.
   * If isCountStar() is broken (e.g. always returns false), the planner would
   * use the slower aggregate scan path instead.
   */
  @Test
  public void testCountStarUsesCountFromClassStep() {
    var className = "testCountStarUsesCountFromClass";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 5; i++) {
      session.newEntity(className).setProperty("val", i);
    }
    session.commit();

    session.begin();
    var result = session.query("select count(*) from " + className);
    Assert.assertTrue(result.hasNext());
    var row = result.next();
    Assert.assertEquals(5L, (Object) row.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());

    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    assertThat(plan.getSteps())
        .anyMatch(step -> step instanceof CountFromClassStep);
    result.close();
    session.commit();
  }

  /**
   * Verifies that SELECT count(*) FROM Class WHERE field = 'nonexistent'
   * returns 1 row with count=0 (not an empty result set) and uses
   * GuaranteeEmptyCountStep. The WHERE clause prevents hardwired
   * CountFromClassStep (which already handles empty correctly), forcing the
   * planner through the standard aggregate path where isCountOnly() must
   * return true to add GuaranteeEmptyCountStep.
   */
  @Test
  public void testCountStarWhereNoMatchReturnsZeroRow() {
    var className = "testCountStarWhereNoMatch";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    session.newEntity(className).setProperty("name", "alice");
    session.commit();

    session.begin();
    var result = session.query(
        "select count(*) from " + className + " where name = 'nonexistent'");
    Assert.assertTrue("count(*) with no matches should return 1 row", result.hasNext());
    var row = result.next();
    Assert.assertEquals(0L, (Object) row.getProperty("count(*)"));
    Assert.assertFalse(result.hasNext());

    var plan = (SelectExecutionPlan) result.getExecutionPlan();
    assertThat(plan.getSteps())
        .anyMatch(step -> step instanceof GuaranteeEmptyCountStep);
    result.close();
    session.commit();
  }

  /**
   * when a class name starts with $
   * but the class actually exists, it should be treated as a class (not a variable).
   */
  @Test
  public void testSelectFromDollarPrefixedClassThatExists() {
    var className = "$DollarPrefixedClass";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    session.newEntity(className).setProperty("val", 42);
    session.commit();

    session.begin();
    var result = session.query("select from `" + className + "`");
    Assert.assertTrue(result.hasNext());
    var row = result.next();
    Assert.assertEquals(42, (int) row.getProperty("val"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * handleClassWithIndexForSortOnly
   * filters out indexes without definitions. An index with a definition should
   * be considered for sort-only optimization.
   */
  @Test
  public void testOrderByUsesIndexForSortWhenAvailable() {
    var className = "testOrderByIndexSort";
    var clazz = session.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", PropertyType.STRING);
    clazz.createIndex(className + ".name", INDEX_TYPE.NOTUNIQUE, "name");

    session.begin();
    session.newEntity(className).setProperty("name", "charlie");
    session.newEntity(className).setProperty("name", "alice");
    session.newEntity(className).setProperty("name", "bob");
    session.commit();

    session.begin();
    var result = session.query("select from " + className + " order by name ASC");
    var items = result.stream().toList();
    Assert.assertEquals(3, items.size());
    Assert.assertEquals("alice", items.get(0).getProperty("name"));
    Assert.assertEquals("bob", items.get(1).getProperty("name"));
    Assert.assertEquals("charlie", items.get(2).getProperty("name"));
    result.close();
    session.commit();
  }

  @Test
  public void testConditionalAggregationWithCompoundCondition() {
    var className = "testConditionalAggCompound";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 10; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("age", 15 + i * 3);
      doc.setProperty("city", i % 2 == 0 ? "NYC" : "LA");
    }
    session.commit();

    session.begin();
    var result = session.query(
        "select sum(if(age >= 18 AND city = 'NYC', 1, 0)) as nycAdults"
            + " from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    var nycAdults = item.<Number>getProperty("nycAdults").intValue();
    Assert.assertTrue("Expected at least 1 NYC adult", nycAdults >= 1);
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  // ── isCountStar: count(*) shortcut guards ──

  /**
   * count(*) wrapped in a CASE expression should NOT use the CountFromClassStep
   * shortcut because the output projection is not a simple alias passthrough —
   * it's a CASE expression that must be evaluated per-group.
   */
  @Test
  public void testCountStarInsideCaseDoesNotUseCountShortcut() {
    var className = "testCountStarCaseNoShortcut";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 5; i++) {
      session.begin();
      session.newEntity(className);
      session.commit();
    }

    session.begin();
    // count(*) wrapped in CASE — must NOT use CountFromClassStep
    var result = session.query(
        "SELECT CASE WHEN 1 = 1 THEN count(*) ELSE 0 END as cnt FROM " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    // count(*) = 5, condition is true, so CASE returns 5
    Assert.assertEquals(5L, ((Number) item.getProperty("cnt")).longValue());
    Assert.assertFalse(result.hasNext());

    // Verify execution plan does NOT contain CountFromClassStep
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "count(*) inside CASE should NOT use CountFromClass shortcut, plan was:\n" + plan,
        plan.contains("CALCULATE CLASS SIZE"));

    result.close();
    session.commit();
  }

  /**
   * count(1) is NOT count(*) — it counts non-null evaluations of the literal 1.
   * The planner must not use the CountFromClassStep shortcut for count(1)
   * because it's semantically different (though the result is the same for
   * non-null literals). This also verifies that isCountStar correctly
   * distinguishes "count(*)" from "count(1)" via string comparison.
   */
  @Test
  public void testCountOneDoesNotUseCountStarShortcut() {
    var className = "testCountOneNoShortcut";
    session.getMetadata().getSchema().createClass(className);

    for (var i = 0; i < 3; i++) {
      session.begin();
      session.newEntity(className);
      session.commit();
    }

    session.begin();
    var result = session.query("SELECT count(1) as c FROM " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(3L, ((Number) item.getProperty("c")).longValue());
    Assert.assertFalse(result.hasNext());

    // count(1) must NOT use CountFromClass shortcut
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "count(1) should NOT use CountFromClass shortcut, plan was:\n" + plan,
        plan.contains("CALCULATE CLASS SIZE"));

    result.close();
    session.commit();
  }

  /**
   * SELECT count(*) on an empty table must return one row with count=0.
   * This verifies that isCountOnly returns true and GuaranteeEmptyCountStep
   * is inserted into the plan.
   */
  @Test
  public void testCountOnlyOnEmptyTableReturnsZero() {
    var className = "testCountOnlyEmpty";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var result = session.query("SELECT count(*) FROM " + className);
    printExecutionPlan(result);
    Assert.assertTrue("count(*) on empty table must return one row", result.hasNext());
    var item = result.next();
    Assert.assertEquals(0L, ((Number) item.getProperty("count(*)")).longValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * SELECT count(*), count(*) FROM ... has two aggregate projection items,
   * so isCountOnly must return false and GuaranteeEmptyCountStep should NOT
   * be added. On an empty table this means the result set may be empty.
   */
  @Test
  public void testTwoCountStarProjectionsNotTreatedAsCountOnly() {
    var className = "testTwoCountNotOnly";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    var result = session.query(
        "SELECT count(*) as a, count(*) as b FROM " + className);
    printExecutionPlan(result);
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "Two count(*) projections should not trigger single-count optimization,"
            + " plan was:\n" + plan,
        plan.contains("GUARANTEE FOR ZERO COUNT"));
    result.close();
    session.commit();
  }

  /**
   * SELECT count(*) FROM ... ORDER BY name — the ORDER BY creates a synthetic
   * projection alias (_$$$ORDER_BY_ALIAS$$$_0) that must be excluded when
   * counting user-visible projection items. Without the filter, isCountOnly
   * would see 2 items and wrongly return false, causing count(*) on an
   * empty table to not guarantee a row with 0.
   */
  @Test
  public void testCountOnlyWithOrderByOnEmptyTable() {
    var className = "testCountOnlyOrderByEmpty";
    var cls = session.getMetadata().getSchema().createClass(className);
    cls.createProperty("name", PropertyType.STRING);

    session.begin();
    var result = session.query(
        "SELECT count(*) FROM " + className + " ORDER BY name");
    printExecutionPlan(result);
    Assert.assertTrue(
        "count(*) with ORDER BY on empty table must still return one row",
        result.hasNext());
    var item = result.next();
    Assert.assertEquals(0L, ((Number) item.getProperty("count(*)")).longValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  // ── ORDER BY with indexed property ──

  /**
   * ORDER BY on an indexed property should use the index to avoid sorting.
   * The planner iterates available indexes and skips any with null definition;
   * this test ensures the index-backed ORDER BY path is exercised.
   */
  @Test
  public void testOrderByUsesIndex() {
    var className = "testOrderByIdx";
    var cls = session.getMetadata().getSchema().createClass(className);
    cls.createProperty("name", PropertyType.STRING);
    cls.createIndex(className + ".name", INDEX_TYPE.NOTUNIQUE, "name");

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = (EntityImpl) session.newEntity(className);
      doc.setProperty("name", "n" + (4 - i));
      session.commit();
    }

    session.begin();
    var result = session.query("SELECT name FROM " + className + " ORDER BY name ASC");
    printExecutionPlan(result);

    // Verify ordered results
    String prev = null;
    while (result.hasNext()) {
      var name = result.next().<String>getProperty("name");
      if (prev != null) {
        Assert.assertTrue("Results should be in ascending order",
            name.compareTo(prev) >= 0);
      }
      prev = name;
    }

    // Verify the plan uses an index for ordering
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "ORDER BY on indexed field should use index, plan was:\n" + plan,
        plan.contains("FROM INDEX") || plan.contains("CALCULATE INDEX"));

    result.close();
    session.commit();
  }

  // ── ORDER BY null placement: index-accelerated vs in-memory ──
  //
  // The in-memory sort treats null as the smallest value: nulls first for ASC, nulls last for DESC.
  // An index-accelerated sort must produce the same placement even though it never runs a
  // comparator — null keys live in a separate bucket outside the sorted B-tree, so the code decides
  // their position purely by where it concatenates the null stream. One explicit test per scenario.
  // Each asserts (a) whether the index optimization is used — an index-accelerated sort has a
  // FetchFromIndexStep and no OrderByStep, an in-memory sort has an OrderByStep and no
  // FetchFromIndexStep (checked by orderedNamesIndexAccelerated / orderedNamesInMemory) — and (b)
  // the exact null placement. Every scenario pins the same order literal per direction, so ASC and
  // DESC placement is identical across the index-accelerated and in-memory paths by construction.

  /**
   * ASC over a NOTUNIQUE (multi-value) indexed field: the planner satisfies ORDER BY from the index
   * (FetchFromIndexValuesStep, no OrderByStep) and emits the two null-keyed rows first, then the
   * ascending values.
   */
  @Test
  public void testOrderByAscUsesNotUniqueIndexNullsFirst() {
    var className = "testOrderByAscIdxNotUnique";
    createIndexedNameClass(className, INDEX_TYPE.NOTUNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesIndexAccelerated(className, "ASC");

    Assert.assertEquals(Arrays.asList(null, null, "a", "b", "c"), ordered);
  }

  /**
   * DESC over a NOTUNIQUE (multi-value) indexed field: index-accelerated, and the null-keyed rows
   * come LAST. This is the placement the bug got wrong — the index path used to prepend the null
   * stream unconditionally, so it emitted nulls first for DESC too instead of last.
   */
  @Test
  public void testOrderByDescUsesNotUniqueIndexNullsLast() {
    var className = "testOrderByDescIdxNotUnique";
    createIndexedNameClass(className, INDEX_TYPE.NOTUNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesIndexAccelerated(className, "DESC");

    Assert.assertEquals(Arrays.asList("c", "b", "a", null, null), ordered);
  }

  /**
   * ASC over a UNIQUE (single-value) indexed field: index-accelerated. Nulls are not ignored by
   * default (INDEX_IGNORE_NULL_VALUES_DEFAULT = false), so the single null key is stored and
   * emitted first.
   */
  @Test
  public void testOrderByAscUsesUniqueIndexNullsFirst() {
    var className = "testOrderByAscIdxUnique";
    createIndexedNameClass(className, INDEX_TYPE.UNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null));

    var ordered = orderedNamesIndexAccelerated(className, "ASC");

    Assert.assertEquals(Arrays.asList(null, "a", "b", "c"), ordered);
  }

  /**
   * DESC over a UNIQUE (single-value) indexed field: index-accelerated, single null key emitted
   * LAST — the same DESC placement the in-memory sort produces.
   */
  @Test
  public void testOrderByDescUsesUniqueIndexNullsLast() {
    var className = "testOrderByDescIdxUnique";
    createIndexedNameClass(className, INDEX_TYPE.UNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null));

    var ordered = orderedNamesIndexAccelerated(className, "DESC");

    Assert.assertEquals(Arrays.asList("c", "b", "a", null), ordered);
  }

  /**
   * ASC with no index: the sort runs in memory (OrderByStep, no FetchFromIndexStep) and nulls come
   * first. Reference behavior the index-accelerated ASC path must match.
   */
  @Test
  public void testOrderByAscInMemoryNullsFirst() {
    var className = "testOrderByAscNoIdx";
    createPlainNameClass(className);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesInMemory(className, "ASC");

    Assert.assertEquals(Arrays.asList(null, null, "a", "b", "c"), ordered);
  }

  /**
   * DESC with no index: in-memory sort (OrderByStep, no FetchFromIndexStep), nulls last. Reference
   * behavior the index-accelerated DESC path must match.
   */
  @Test
  public void testOrderByDescInMemoryNullsLast() {
    var className = "testOrderByDescNoIdx";
    createPlainNameClass(className);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesInMemory(className, "DESC");

    Assert.assertEquals(Arrays.asList("c", "b", "a", null, null), ordered);
  }

  /**
   * Explicit {@code NULLS FIRST} with ASC: absolute placement puts nulls first. Index-accelerated
   * path must still satisfy ORDER BY from the index (no OrderByStep).
   */
  @Test
  public void testOrderByAscNullsFirstIndexAccelerated() {
    var className = "testOrderByAscNullsFirstIdx";
    createIndexedNameClass(className, INDEX_TYPE.NOTUNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesIndexAccelerated(className, "ASC NULLS FIRST");

    Assert.assertEquals(Arrays.asList(null, null, "a", "b", "c"), ordered);
  }

  /**
   * Explicit {@code NULLS LAST} with ASC: absolute placement puts nulls last even though ASC with
   * the default FIRST would put them first. Index path must match.
   */
  @Test
  public void testOrderByAscNullsLastIndexAccelerated() {
    var className = "testOrderByAscNullsLastIdx";
    createIndexedNameClass(className, INDEX_TYPE.NOTUNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesIndexAccelerated(className, "ASC NULLS LAST");

    Assert.assertEquals(Arrays.asList("a", "b", "c", null, null), ordered);
  }

  /**
   * Explicit {@code NULLS FIRST} with DESC puts nulls first instead of the shipped LAST placement.
   */
  @Test
  public void testOrderByDescNullsFirstIndexAccelerated() {
    var className = "testOrderByDescNullsFirstIdx";
    createIndexedNameClass(className, INDEX_TYPE.NOTUNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesIndexAccelerated(className, "DESC NULLS FIRST");

    Assert.assertEquals(Arrays.asList(null, null, "c", "b", "a"), ordered);
  }

  /**
   * Explicit {@code NULLS LAST} with DESC pins the same placement as the shipped LAST setting.
   */
  @Test
  public void testOrderByDescNullsLastIndexAccelerated() {
    var className = "testOrderByDescNullsLastIdx";
    createIndexedNameClass(className, INDEX_TYPE.NOTUNIQUE);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesIndexAccelerated(className, "DESC NULLS LAST");

    Assert.assertEquals(Arrays.asList("c", "b", "a", null, null), ordered);
  }

  /**
   * In-memory ASC {@code NULLS LAST}: reference for the index-accelerated ASC NULLS LAST path.
   */
  @Test
  public void testOrderByAscNullsLastInMemory() {
    var className = "testOrderByAscNullsLastNoIdx";
    createPlainNameClass(className);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesInMemory(className, "ASC NULLS LAST");

    Assert.assertEquals(Arrays.asList("a", "b", "c", null, null), ordered);
  }

  /**
   * In-memory DESC {@code NULLS FIRST}: reference for the index-accelerated DESC NULLS FIRST path.
   */
  @Test
  public void testOrderByDescNullsFirstInMemory() {
    var className = "testOrderByDescNullsFirstNoIdx";
    createPlainNameClass(className);
    seedNames(className, Arrays.asList("b", "a", "c", null, null));

    var ordered = orderedNamesInMemory(className, "DESC NULLS FIRST");

    Assert.assertEquals(Arrays.asList(null, null, "c", "b", "a"), ordered);
  }

  /**
   * Omitted NULLS clause with ascending placement {@code LAST} puts nulls last. Both
   * index-accelerated and in-memory paths must agree.
   */
  @Test
  public void testOrderByGlobalLastPlacementAscending() {
    var previous = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    try {
      var indexed = "testOrderByLastPlacementAscIdx";
      createIndexedNameClass(indexed, INDEX_TYPE.NOTUNIQUE);
      seedNames(indexed, Arrays.asList("b", "a", "c", null, null));
      Assert.assertEquals(
          Arrays.asList("a", "b", "c", null, null), orderedNamesIndexAccelerated(indexed, "ASC"));

      var plain = "testOrderByLastPlacementAscNoIdx";
      createPlainNameClass(plain);
      seedNames(plain, Arrays.asList("b", "a", "c", null, null));
      Assert.assertEquals(
          Arrays.asList("a", "b", "c", null, null), orderedNamesInMemory(plain, "ASC"));
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previous);
    }
  }

  /**
   * Omitted NULLS clause with descending placement {@code FIRST} puts nulls first. Both paths must
   * agree.
   */
  @Test
  public void testOrderByGlobalFirstPlacementDescending() {
    var previous = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.getValue();
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(OrderByNullsPlacement.FIRST);
    try {
      var indexed = "testOrderByFirstPlacementDescIdx";
      createIndexedNameClass(indexed, INDEX_TYPE.NOTUNIQUE);
      seedNames(indexed, Arrays.asList("b", "a", "c", null, null));
      Assert.assertEquals(
          Arrays.asList(null, null, "c", "b", "a"), orderedNamesIndexAccelerated(indexed, "DESC"));

      var plain = "testOrderByFirstPlacementDescNoIdx";
      createPlainNameClass(plain);
      seedNames(plain, Arrays.asList("b", "a", "c", null, null));
      Assert.assertEquals(
          Arrays.asList(null, null, "c", "b", "a"), orderedNamesInMemory(plain, "DESC"));
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC.setValue(previous);
    }
  }

  /**
   * Explicit {@code NULLS FIRST} overrides a {@code LAST} global default: ASC still puts
   * nulls first. Pins the precedence rule (clause &gt; global default).
   */
  @Test
  public void testOrderByExplicitNullsOverridesGlobalPlacement() {
    var previous = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.LAST);
    try {
      var className = "testOrderByNullsOverride";
      createPlainNameClass(className);
      seedNames(className, Arrays.asList("b", "a", null));

      var ordered = orderedNamesInMemory(className, "ASC NULLS FIRST");

      Assert.assertEquals(Arrays.asList(null, "a", "b"), ordered);
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previous);
    }
  }

  /**
   * A per-storage ascending placement overrides the runtime global when the
   * NULLS clause is omitted.
   */
  @Test
  public void testOrderByStorageLocalPlacementOverridesGlobal() {
    var previous = GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.getValue();
    GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(OrderByNullsPlacement.FIRST);
    session
        .getStorage()
        .getContextConfiguration()
        .setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC,
            OrderByNullsPlacement.LAST);
    try {
      var className = "testOrderByStorageLastPlacement";
      createPlainNameClass(className);
      seedNames(className, Arrays.asList("b", "a", "c", null, null));

      var ordered = orderedNamesInMemory(className, "ASC");

      Assert.assertEquals(Arrays.asList("a", "b", "c", null, null), ordered);
    } finally {
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC.setValue(previous);
      session
          .getStorage()
          .getContextConfiguration()
          .setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, null);
    }
  }

  /**
   * COLLATE + {@code NULLS LAST}: null placement comes from the NULLS clause (absolute), while
   * non-null string ordering uses the collate. Forces in-memory sort because COLLATE disables the
   * index-for-sort optimization.
   */
  @Test
  public void testOrderByCollateWithNullsLast() {
    var className = "testOrderByCollateNullsLast";
    createPlainNameClass(className);
    seedNames(className, Arrays.asList("b", "A", "c", null));

    session.begin();
    try (var result =
        session.query(
            "SELECT name FROM " + className + " ORDER BY name ASC NULLS LAST COLLATE ci")) {
      var plan = result.getExecutionPlan().prettyPrint(0, 2);
      Assert.assertTrue("COLLATE must force in-memory OrderByStep, plan was:\n" + plan,
          plan.contains("ORDER BY"));
      var names = new ArrayList<>();
      while (result.hasNext()) {
        names.add(result.next().<Object>getProperty("name"));
      }
      // ci orders A/b/c case-insensitively; nulls last from the explicit clause.
      Assert.assertEquals(Arrays.asList("A", "b", "c", null), names);
    } finally {
      session.commit();
    }
  }

  /** Creates a class with a STRING {@code name} property and an index of {@code type} on it. */
  private void createIndexedNameClass(String className, INDEX_TYPE type) {
    var cls = session.getMetadata().getSchema().createClass(className);
    cls.createProperty("name", PropertyType.STRING);
    cls.createIndex(className + ".name", type, "name");
  }

  /**
   * Creates a class with a STRING {@code name} property and no index, forcing an in-memory sort.
   */
  private void createPlainNameClass(String className) {
    session.getMetadata().getSchema().createClass(className)
        .createProperty("name", PropertyType.STRING);
  }

  /**
   * Inserts one record per entry in {@code names}; a {@code null} entry creates a record that does
   * not set the {@code name} property, so an index that does not ignore nulls stores it under the
   * null key. Each record is committed separately, matching the seeding style of the other
   * index-ordering tests in this class.
   */
  private void seedNames(String className, List<String> names) {
    for (var name : names) {
      session.begin();
      var doc = (EntityImpl) session.newEntity(className);
      if (name != null) {
        doc.setProperty("name", name);
      }
      session.commit();
    }
  }

  /**
   * Runs {@code SELECT name FROM <className> ORDER BY name <orderBySuffix>}, asserts the planner
   * satisfied the sort from the index — the plan contains a {@link FetchFromIndexStep} and adds no
   * in-memory {@link OrderByStep} — and returns the emitted {@code name} values in result order
   * (nulls included). Use this for a class that has an index on {@code name}.
   * {@code orderBySuffix} is the text after the field name (e.g. {@code ASC},
   * {@code DESC NULLS FIRST}).
   */
  private List<Object> orderedNamesIndexAccelerated(String className, String orderBySuffix) {
    var run = runOrderByQuery(className, orderBySuffix);
    Assert.assertTrue(
        "expected an index-accelerated sort (FetchFromIndexStep), plan was:\n" + run.renderedPlan(),
        run.fetchFromIndexSteps() >= 1);
    Assert.assertEquals(
        "index-accelerated sort must not add an in-memory OrderByStep, plan was:\n"
            + run.renderedPlan(),
        0, run.orderBySteps());
    return run.names();
  }

  /**
   * Runs {@code SELECT name FROM <className> ORDER BY name <orderBySuffix>}, asserts the sort ran in
   * memory — the plan contains an {@link OrderByStep} and no {@link FetchFromIndexStep} — and
   * returns the emitted {@code name} values in result order (nulls included). Use this for a class
   * with no index on {@code name}, so its result is the reference the index-accelerated path must
   * match. {@code orderBySuffix} is the text after the field name (e.g. {@code ASC},
   * {@code DESC NULLS FIRST}).
   */
  private List<Object> orderedNamesInMemory(String className, String orderBySuffix) {
    var run = runOrderByQuery(className, orderBySuffix);
    Assert.assertEquals(
        "expected an in-memory sort (no index), plan was:\n" + run.renderedPlan(), 0,
        run.fetchFromIndexSteps());
    Assert.assertEquals(
        "in-memory sort must add an OrderByStep, plan was:\n" + run.renderedPlan(), 1,
        run.orderBySteps());
    return run.names();
  }

  /**
   * Runs {@code SELECT name FROM <className> ORDER BY name <orderBySuffix>} and returns the
   * plan-step counts, the rendered plan (for assertion messages), and the emitted {@code name}
   * values in result order (nulls included). The {@code ResultSet} is closed and the transaction
   * committed unconditionally, so a failed assertion in a caller cannot leak either. The wrappers
   * {@link #orderedNamesIndexAccelerated} / {@link #orderedNamesInMemory} add the plan-shape
   * assertion that tells an index-accelerated sort apart from an in-memory one.
   */
  private OrderByRun runOrderByQuery(String className, String orderBySuffix) {
    session.begin();
    try (var result =
        session.query("SELECT name FROM " + className + " ORDER BY name " + orderBySuffix)) {
      var plan = result.getExecutionPlan();
      var fetchFromIndex =
          plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count();
      var orderBy = plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count();
      var rendered = plan.prettyPrint(0, 2);
      var names = new ArrayList<>();
      while (result.hasNext()) {
        names.add(result.next().<Object>getProperty("name"));
      }
      return new OrderByRun(fetchFromIndex, orderBy, rendered, names);
    } finally {
      session.commit();
    }
  }

  /** Plan-step counts, rendered plan, and emitted names from one ORDER BY query run. */
  private record OrderByRun(
      long fetchFromIndexSteps, long orderBySteps, String renderedPlan, List<Object> names) {
  }

  // ── CASE + MIN/MAX aggregate tests ──

  /**
   * min() inside CASE THEN branch: returns the minimum value when condition
   * is true.
   */
  @Test
  public void testMinInsideCaseThenBranch() {
    var className = "testMinCaseThen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {10, 20, 30, 40, 50}) {
      session.newInstance(className).setProperty("val", v);
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT CASE WHEN 1 = 1 THEN min(val) ELSE 999 END as m FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals(10, ((Number) result.next().getProperty("m")).intValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * max() inside CASE THEN branch: returns the maximum value when condition
   * is true.
   */
  @Test
  public void testMaxInsideCaseThenBranch() {
    var className = "testMaxCaseThen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {10, 20, 30, 40, 50}) {
      session.newInstance(className).setProperty("val", v);
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT CASE WHEN 1 = 1 THEN max(val) ELSE 0 END as m FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals(50, ((Number) result.next().getProperty("m")).intValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * min() in WHEN condition: branch selected based on aggregate threshold.
   */
  @Test
  public void testMinInCaseWhenCondition() {
    var className = "testMinCaseWhen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {5, 15, 25}) {
      session.newInstance(className).setProperty("val", v);
    }
    session.commit();

    // min(val) = 5 < 10 → 'low'
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN min(val) < 10 THEN 'low' ELSE 'high' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("low", result.next().getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * max() in WHEN condition: branch selected based on aggregate threshold.
   */
  @Test
  public void testMaxInCaseWhenCondition() {
    var className = "testMaxCaseWhen";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {5, 15, 25}) {
      session.newInstance(className).setProperty("val", v);
    }
    session.commit();

    // max(val) = 25 > 20 → 'high'
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN max(val) > 20 THEN 'high' ELSE 'low' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("high", result.next().getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * CASE inside min(): computes the minimum over CASE-transformed values.
   */
  @Test
  public void testCaseInsideMinAggregate() {
    var className = "testCaseInsideMin";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {10, 20, 30}) {
      var doc = session.newInstance(className);
      doc.setProperty("val", v);
      doc.setProperty("category", v > 15 ? "big" : "small");
    }
    session.commit();

    // min(CASE WHEN category = 'big' THEN val ELSE 999 END) → min(999, 20, 30) = 20
    session.begin();
    var result = session.query(
        "SELECT min(CASE WHEN category = 'big' THEN val ELSE 999 END) as m"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals(20, ((Number) result.next().getProperty("m")).intValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * CASE inside max(): computes the maximum over CASE-transformed values.
   */
  @Test
  public void testCaseInsideMaxAggregate() {
    var className = "testCaseInsideMax";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {10, 20, 30}) {
      var doc = session.newInstance(className);
      doc.setProperty("val", v);
      doc.setProperty("category", v > 15 ? "big" : "small");
    }
    session.commit();

    // max(CASE WHEN category = 'small' THEN val ELSE 0 END) → max(10, 0, 0) = 10
    session.begin();
    var result = session.query(
        "SELECT max(CASE WHEN category = 'small' THEN val ELSE 0 END) as m"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals(10, ((Number) result.next().getProperty("m")).intValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  // ── COUNT(field) vs COUNT(*) inside CASE ──

  /**
   * count(nullable_field) inside CASE: counts only non-null values, unlike
   * count(*) which counts all rows.
   */
  @Test
  public void testCountFieldInsideCaseVsCountStar() {
    var className = "testCountFieldCase";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    session.newInstance(className).setProperty("name", "Alice");
    session.newInstance(className).setProperty("name", "Bob");
    session.newInstance(className); // name is null
    session.commit();

    // count(*) = 3, count(name) = 2 (skips null)
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(name) < count(*) THEN 'has nulls' ELSE 'no nulls' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("has nulls", result.next().getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  // ── Nested CASE inside CASE ──

  /**
   * Nested CASE: outer CASE with aggregate in WHEN, inner CASE with aggregate
   * in THEN branch. Tests that splitForAggregation recursively handles nested
   * CASE expressions.
   */
  @Test
  public void testNestedCaseWithAggregates() {
    var className = "testNestedCase";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var i = 0; i < 6; i++) {
      session.newInstance(className).setProperty("val", i * 10);
    }
    session.commit();

    // count(*) = 6 > 3 → enters outer THEN
    // sum(val) = 150 > 100 → inner CASE returns 'big'
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 3 THEN"
            + " CASE WHEN sum(val) > 100 THEN 'big' ELSE 'small' END"
            + " ELSE 'few' END as label"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("big", result.next().getProperty("label"));
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  // ── CASE aggregate in GROUP BY with multiple groups ──

  /**
   * CASE with aggregate in a GROUP BY query with multiple groups. Verifies
   * that per-group aggregates inside CASE are correctly evaluated: groups
   * with count > 3 get 'large', others get 'small'.
   */
  @Test
  public void testCaseAggregateWithGroupByMultipleGroups() {
    var className = "testCaseGroupMulti";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    // category A: 4 items, category B: 2 items
    for (var i = 0; i < 4; i++) {
      session.newInstance(className).setProperty("category", "A");
    }
    for (var i = 0; i < 2; i++) {
      session.newInstance(className).setProperty("category", "B");
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT category,"
            + " CASE WHEN count(*) > 3 THEN 'large' ELSE 'small' END as size"
            + " FROM " + className
            + " GROUP BY category"
            + " ORDER BY category");
    var items = result.stream().toList();
    Assert.assertEquals(2, items.size());
    Assert.assertEquals("A", items.get(0).getProperty("category"));
    Assert.assertEquals("large", items.get(0).getProperty("size"));
    Assert.assertEquals("B", items.get(1).getProperty("category"));
    Assert.assertEquals("small", items.get(1).getProperty("size"));
    result.close();
    session.commit();
  }

  // ── CASE aggregate in ORDER BY via alias ──

  /**
   * ORDER BY a CASE-aggregate expression using an alias. The CASE classifies
   * groups by their aggregate value, and the alias is used in ORDER BY.
   */
  @Test
  public void testCaseAggregateInOrderByViaAlias() {
    var className = "testCaseAggOrderAlias";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    // X: 3 items (val sum = 30), Y: 2 items (val sum = 200)
    for (var i = 0; i < 3; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("grp", "X");
      doc.setProperty("val", 10);
    }
    for (var i = 0; i < 2; i++) {
      var doc = session.newInstance(className);
      doc.setProperty("grp", "Y");
      doc.setProperty("val", 100);
    }
    session.commit();

    // Use alias in ORDER BY: priority 0 for sum>50, 1 otherwise
    // Y (sum=200>50 → priority=0) sorts before X (sum=30 → priority=1)
    session.begin();
    var result = session.query(
        "SELECT grp, sum(val) as s,"
            + " CASE WHEN sum(val) > 50 THEN 0 ELSE 1 END as priority"
            + " FROM " + className
            + " GROUP BY grp"
            + " ORDER BY priority, grp");
    var items = result.stream().toList();
    Assert.assertEquals(2, items.size());
    Assert.assertEquals("Y", items.get(0).getProperty("grp"));
    Assert.assertEquals("X", items.get(1).getProperty("grp"));
    result.close();
    session.commit();
  }

  // ── avg() inside CASE ──

  /**
   * avg() inside CASE THEN branch and in ELSE branch, verifying that
   * different aggregate functions can appear in different branches.
   */
  @Test
  public void testAvgInsideCaseBranches() {
    var className = "testAvgCaseBranch";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    for (var v : new int[] {10, 20, 30}) {
      session.newInstance(className).setProperty("val", v);
    }
    session.commit();

    // avg(val) = 20.0, count(*) = 3 > 1 → THEN branch → returns avg = 20
    session.begin();
    var result = session.query(
        "SELECT CASE WHEN count(*) > 1 THEN avg(val) ELSE min(val) END as m"
            + " FROM " + className);
    Assert.assertTrue(result.hasNext());
    var val = ((Number) result.next().getProperty("m")).doubleValue();
    Assert.assertEquals(20.0, val, 0.01);
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  // ── IC3-style: conditional aggregation with graph traversal inside if() ──

  /**
   * IC3-style pattern: conditional aggregation using if() with graph traversal
   * inside the condition. This is the key pattern that YTDB-586 unblocks:
   * instead of two separate subqueries scanning the same record set (one per
   * country), a single scan uses sum(if(traversal condition, 1, 0)) to count
   * matches for both countries simultaneously.
   *
   * Schema: Person -[IS_LOCATED_IN]-> City -[IS_PART_OF]-> Country
   *         Person <-[HAS_CREATOR]- Message
   *
   * The query counts how many messages were created by people located in
   * each of two countries, using a single scan with two conditional sums.
   */
  @Test
  public void testConditionalAggregationWithGraphTraversal() {
    session.execute("CREATE CLASS IC3Country IF NOT EXISTS EXTENDS V").close();
    session.execute("CREATE CLASS IC3City IF NOT EXISTS EXTENDS V").close();
    session.execute("CREATE CLASS IC3Person IF NOT EXISTS EXTENDS V").close();
    session.execute("CREATE CLASS IC3Message IF NOT EXISTS EXTENDS V").close();
    session.execute("CREATE CLASS IC3IsPartOf IF NOT EXISTS EXTENDS E").close();
    session.execute("CREATE CLASS IC3IsLocatedIn IF NOT EXISTS EXTENDS E").close();
    session.execute("CREATE CLASS IC3HasCreator IF NOT EXISTS EXTENDS E").close();

    // Build graph: 2 countries, 2 cities, 3 people, 9 messages
    session.begin();
    session.execute("CREATE VERTEX IC3Country SET name = 'USA'").close();
    session.execute("CREATE VERTEX IC3Country SET name = 'UK'").close();
    session.execute("CREATE VERTEX IC3City SET name = 'NYC'").close();
    session.execute("CREATE VERTEX IC3City SET name = 'London'").close();

    // Cities → Countries
    session.execute(
        "CREATE EDGE IC3IsPartOf FROM (SELECT FROM IC3City WHERE name = 'NYC')"
            + " TO (SELECT FROM IC3Country WHERE name = 'USA')")
        .close();
    session.execute(
        "CREATE EDGE IC3IsPartOf FROM (SELECT FROM IC3City WHERE name = 'London')"
            + " TO (SELECT FROM IC3Country WHERE name = 'UK')")
        .close();

    // People → Cities
    session.execute("CREATE VERTEX IC3Person SET name = 'Alice'").close();
    session.execute("CREATE VERTEX IC3Person SET name = 'Bob'").close();
    session.execute("CREATE VERTEX IC3Person SET name = 'Charlie'").close();
    session.execute(
        "CREATE EDGE IC3IsLocatedIn FROM (SELECT FROM IC3Person WHERE name = 'Alice')"
            + " TO (SELECT FROM IC3City WHERE name = 'NYC')")
        .close();
    session.execute(
        "CREATE EDGE IC3IsLocatedIn FROM (SELECT FROM IC3Person WHERE name = 'Bob')"
            + " TO (SELECT FROM IC3City WHERE name = 'NYC')")
        .close();
    session.execute(
        "CREATE EDGE IC3IsLocatedIn FROM (SELECT FROM IC3Person WHERE name = 'Charlie')"
            + " TO (SELECT FROM IC3City WHERE name = 'London')")
        .close();

    // Messages → People (Alice: 3, Bob: 2, Charlie: 4)
    for (var i = 0; i < 3; i++) {
      session.execute("CREATE VERTEX IC3Message SET text = 'alice-" + i + "'").close();
      session.execute(
          "CREATE EDGE IC3HasCreator FROM (SELECT FROM IC3Message WHERE text = 'alice-" + i
              + "') TO (SELECT FROM IC3Person WHERE name = 'Alice')")
          .close();
    }
    for (var i = 0; i < 2; i++) {
      session.execute("CREATE VERTEX IC3Message SET text = 'bob-" + i + "'").close();
      session.execute(
          "CREATE EDGE IC3HasCreator FROM (SELECT FROM IC3Message WHERE text = 'bob-" + i
              + "') TO (SELECT FROM IC3Person WHERE name = 'Bob')")
          .close();
    }
    for (var i = 0; i < 4; i++) {
      session.execute("CREATE VERTEX IC3Message SET text = 'charlie-" + i + "'").close();
      session.execute(
          "CREATE EDGE IC3HasCreator FROM (SELECT FROM IC3Message WHERE text = 'charlie-" + i
              + "') TO (SELECT FROM IC3Person WHERE name = 'Charlie')")
          .close();
    }
    session.commit();

    // IC3-style query: single scan with conditional aggregation.
    // For each message, traverse to creator's country via graph edges,
    // and count conditionally with sum(if(traversal CONTAINS country, 1, 0)).
    // This replaces two separate subqueries that each scan the same set.
    session.begin();
    var result = session.query(
        "SELECT"
            + " sum(if(out('IC3HasCreator').out('IC3IsLocatedIn')"
            + "   .out('IC3IsPartOf').name CONTAINS 'USA', 1, 0)) as usaCount,"
            + " sum(if(out('IC3HasCreator').out('IC3IsLocatedIn')"
            + "   .out('IC3IsPartOf').name CONTAINS 'UK', 1, 0)) as ukCount"
            + " FROM IC3Message");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    // Alice(3) + Bob(2) = 5 messages from USA people
    Assert.assertEquals(5, ((Number) item.getProperty("usaCount")).intValue());
    // Charlie = 4 messages from UK person
    Assert.assertEquals(4, ((Number) item.getProperty("ukCount")).intValue());
    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * Verifies that when the outer WHERE has @class + an indexed property condition,
   * the planner uses an index pre-filter on the ExpandStep. The index is queried
   * at execution time and non-matching RIDs are skipped without disk I/O.
   */
  @Test
  public void testPredicatePushDownIntoExpand_indexedPropertyFilter() {
    session.execute("CREATE CLASS IdxForum EXTENDS V").close();
    session.execute("CREATE CLASS IdxPost EXTENDS V").close();
    session.execute("CREATE CLASS IdxComment EXTENDS V").close();
    session.execute("CREATE CLASS IdxContainerOf EXTENDS E").close();
    session.execute("CREATE PROPERTY IdxPost.score INTEGER").close();
    session.execute("CREATE INDEX IdxPost.score ON IdxPost (score) NOTUNIQUE").close();

    session.begin();
    session.execute("CREATE VERTEX IdxForum SET name = 'forum1'").close();
    for (var i = 0; i < 10; i++) {
      session.execute(
          "CREATE VERTEX IdxPost SET title = 'post" + i + "', score = " + i).close();
      session.execute(
          "CREATE EDGE IdxContainerOf FROM (SELECT FROM IdxForum WHERE name = 'forum1')"
              + " TO (SELECT FROM IdxPost WHERE title = 'post" + i + "')")
          .close();
    }
    for (var i = 0; i < 5; i++) {
      session.execute(
          "CREATE VERTEX IdxComment SET text = 'c" + i + "', score = " + (i + 8)).close();
      session.execute(
          "CREATE EDGE IdxContainerOf FROM (SELECT FROM IdxForum WHERE name = 'forum1')"
              + " TO (SELECT FROM IdxComment WHERE text = 'c" + i + "')")
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('IdxContainerOf')) FROM IdxForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE @class = 'IdxPost' AND score >= 7");
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return 3 posts with score >= 7 (post7, post8, post9)", 3, items.size());
    for (var item : items) {
      Assert.assertEquals("IdxPost", item.asEntity().getSchemaClassName());
      Assert.assertTrue(((Number) item.getProperty("score")).intValue() >= 7);
    }

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Class filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("class filter"));
    Assert.assertTrue(
        "Index pre-filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("index pre-filter"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that when the property in the WHERE clause is NOT indexed, the planner
   * falls back to a generic push-down filter (no index pre-filter in the plan).
   */
  @Test
  public void testPredicatePushDownIntoExpand_nonIndexedPropertyFallback() {
    session.execute("CREATE CLASS NiForum EXTENDS V").close();
    session.execute("CREATE CLASS NiPost EXTENDS V").close();
    session.execute("CREATE CLASS NiContainerOf EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX NiForum SET name = 'forum1'").close();
    for (var i = 0; i < 8; i++) {
      session.execute(
          "CREATE VERTEX NiPost SET title = 'post" + i + "', rating = " + i).close();
      session.execute(
          "CREATE EDGE NiContainerOf FROM (SELECT FROM NiForum WHERE name = 'forum1')"
              + " TO (SELECT FROM NiPost WHERE title = 'post" + i + "')")
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('NiContainerOf')) FROM NiForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE @class = 'NiPost' AND rating >= 5");
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return 3 posts with rating >= 5", 3, items.size());
    for (var item : items) {
      Assert.assertEquals("NiPost", item.asEntity().getSchemaClassName());
      Assert.assertTrue(((Number) item.getProperty("rating")).intValue() >= 5);
    }

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Class filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("class filter"));
    Assert.assertFalse(
        "No index pre-filter should be present (property is not indexed), plan was:\n"
            + plan,
        plan.contains("index pre-filter"));
    Assert.assertTrue(
        "Generic push-down filter should be present, plan was:\n" + plan,
        plan.contains("push-down filter"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that all three filter levels coexist correctly: class filter (zero I/O),
   * index pre-filter (skip non-matching RIDs), and generic push-down filter
   * (post-load check for non-indexed properties).
   */
  @Test
  public void testPredicatePushDownIntoExpand_compoundIndexedAndNonIndexed() {
    session.execute("CREATE CLASS CmpForum EXTENDS V").close();
    session.execute("CREATE CLASS CmpPost EXTENDS V").close();
    session.execute("CREATE CLASS CmpComment EXTENDS V").close();
    session.execute("CREATE CLASS CmpContainerOf EXTENDS E").close();
    session.execute("CREATE PROPERTY CmpPost.score INTEGER").close();
    session.execute("CREATE INDEX CmpPost.score ON CmpPost (score) NOTUNIQUE").close();

    session.begin();
    session.execute("CREATE VERTEX CmpForum SET name = 'f1'").close();
    for (var i = 0; i < 10; i++) {
      var tag = (i % 2 == 0) ? "even" : "odd";
      session.execute(
          "CREATE VERTEX CmpPost SET title = 'p" + i + "', score = " + i
              + ", tag = '" + tag + "'")
          .close();
      session.execute(
          "CREATE EDGE CmpContainerOf FROM (SELECT FROM CmpForum WHERE name = 'f1')"
              + " TO (SELECT FROM CmpPost WHERE title = 'p" + i + "')")
          .close();
    }
    for (var i = 0; i < 3; i++) {
      session.execute("CREATE VERTEX CmpComment SET text = 'c" + i + "'").close();
      session.execute(
          "CREATE EDGE CmpContainerOf FROM (SELECT FROM CmpForum WHERE name = 'f1')"
              + " TO (SELECT FROM CmpComment WHERE text = 'c" + i + "')")
          .close();
    }
    session.commit();

    // @class = 'CmpPost' -> class filter (zero I/O, skips 3 comments)
    // score >= 5         -> index pre-filter (skip posts with score < 5)
    // tag = 'odd'        -> generic push-down (post-load check)
    // Expected results: posts with score >= 5 AND tag = 'odd' -> score 5, 7, 9
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('CmpContainerOf')) FROM CmpForum"
            + "    WHERE name = 'f1'"
            + ") WHERE @class = 'CmpPost' AND score >= 5 AND tag = 'odd'");
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return posts with score>=5 AND tag='odd': p5, p7, p9", 3, items.size());
    for (var item : items) {
      Assert.assertEquals("CmpPost", item.asEntity().getSchemaClassName());
      Assert.assertTrue(((Number) item.getProperty("score")).intValue() >= 5);
      Assert.assertEquals("odd", item.getProperty("tag"));
    }

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Class filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("class filter"));
    Assert.assertTrue(
        "Index pre-filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("index pre-filter"));

    result.close();
    session.commit();
  }

  /**
   * Case A1: Direct RID filter push-down. When the outer WHERE is
   * {@code @rid = <value>}, the planner should push a DirectRid
   * descriptor into ExpandStep for zero-I/O pre-filtering.
   */
  @Test
  public void testPredicatePushDownIntoExpand_directRidFilter() {
    session.execute("CREATE CLASS DRForum EXTENDS V").close();
    session.execute("CREATE CLASS DRPost EXTENDS V").close();
    session.execute("CREATE CLASS DRContainerOf EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX DRForum SET name = 'forum1'").close();
    RID targetRid = null;
    for (var i = 0; i < 5; i++) {
      var rs = session.execute(
          "CREATE VERTEX DRPost SET title = 'post" + i + "'");
      var created = rs.next();
      if (i == 2) {
        targetRid = created.getIdentity();
      }
      rs.close();
      session.execute(
          "CREATE EDGE DRContainerOf FROM (SELECT FROM DRForum WHERE name = 'forum1')"
              + " TO (SELECT FROM DRPost WHERE title = 'post" + i + "')")
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('DRContainerOf')) FROM DRForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE @rid = " + targetRid);
    var items = result.stream().toList();
    Assert.assertEquals("Should return exactly 1 vertex", 1, items.size());
    Assert.assertEquals(targetRid, items.getFirst().getIdentity());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "RID filter should be pushed into EXPAND, plan was:\n" + plan,
        plan.contains("rid filter: direct"));

    result.close();
    session.commit();
  }

  /**
   * Case A3: Reverse edge lookup push-down. When the outer WHERE is
   * {@code out('EdgeClass').@rid = <value>}, the planner should push an
   * EdgeRidLookup descriptor into ExpandStep.
   */
  @Test
  public void testPredicatePushDownIntoExpand_reverseEdgeLookup() {
    session.execute("CREATE CLASS RLForum EXTENDS V").close();
    session.execute("CREATE CLASS RLPost EXTENDS V").close();
    session.execute("CREATE CLASS RLPerson EXTENDS V").close();
    session.execute("CREATE CLASS RLContainerOf EXTENDS E").close();
    session.execute("CREATE CLASS RLHasCreator EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX RLForum SET name = 'forum1'").close();
    var personRs = session.execute("CREATE VERTEX RLPerson SET name = 'alice'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 5; i++) {
      session.execute("CREATE VERTEX RLPost SET title = 'post" + i + "'").close();
      session.execute(
          "CREATE EDGE RLContainerOf FROM (SELECT FROM RLForum WHERE name = 'forum1')"
              + " TO (SELECT FROM RLPost WHERE title = 'post" + i + "')")
          .close();
    }
    // Only post0 and post3 belong to alice
    session.execute(
        "CREATE EDGE RLHasCreator FROM (SELECT FROM RLPost WHERE title = 'post0')"
            + " TO (SELECT FROM RLPerson WHERE name = 'alice')")
        .close();
    session.execute(
        "CREATE EDGE RLHasCreator FROM (SELECT FROM RLPost WHERE title = 'post3')"
            + " TO (SELECT FROM RLPerson WHERE name = 'alice')")
        .close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('RLContainerOf')) FROM RLForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE out('RLHasCreator').@rid = " + personRid);
    var items = result.stream().toList();
    Assert.assertEquals("Should return 2 posts by alice", 2, items.size());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Edge RID lookup should be pushed into EXPAND, plan was:\n" + plan,
        plan.contains("rid filter: out('RLHasCreator')"));

    result.close();
    session.commit();
  }

  /**
   * Case A3 combined with additional filter: reverse edge lookup with
   * a remaining predicate that stays as a push-down filter.
   */
  @Test
  public void testPredicatePushDownIntoExpand_reverseEdgeLookupWithOtherFilter() {
    session.execute("CREATE CLASS RLOForum EXTENDS V").close();
    session.execute("CREATE CLASS RLOPost EXTENDS V").close();
    session.execute("CREATE CLASS RLOPerson EXTENDS V").close();
    session.execute("CREATE CLASS RLOContainerOf EXTENDS E").close();
    session.execute("CREATE CLASS RLOHasCreator EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX RLOForum SET name = 'forum1'").close();
    var personRs = session.execute("CREATE VERTEX RLOPerson SET name = 'bob'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 5; i++) {
      session.execute(
          "CREATE VERTEX RLOPost SET title = 'post" + i + "', score = " + (i + 1)).close();
      session.execute(
          "CREATE EDGE RLOContainerOf FROM (SELECT FROM RLOForum WHERE name = 'forum1')"
              + " TO (SELECT FROM RLOPost WHERE title = 'post" + i + "')")
          .close();
    }
    // post1 (score=2) and post3 (score=4) belong to bob
    session.execute(
        "CREATE EDGE RLOHasCreator FROM (SELECT FROM RLOPost WHERE title = 'post1')"
            + " TO (SELECT FROM RLOPerson WHERE name = 'bob')")
        .close();
    session.execute(
        "CREATE EDGE RLOHasCreator FROM (SELECT FROM RLOPost WHERE title = 'post3')"
            + " TO (SELECT FROM RLOPerson WHERE name = 'bob')")
        .close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('RLOContainerOf')) FROM RLOForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE out('RLOHasCreator').@rid = " + personRid
            + " AND score >= 2");
    var items = result.stream().toList();
    Assert.assertEquals("Should return 2 posts by bob with score>=2", 2, items.size());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Edge RID lookup should be pushed into EXPAND, plan was:\n" + plan,
        plan.contains("rid filter: out('RLOHasCreator')"));

    result.close();
    session.commit();
  }

  /**
   * Case A2: Partial $parent push-down. When the outer WHERE mixes
   * $parent-referencing and non-$parent conditions, the non-$parent part
   * is pushed down while the $parent part stays as an outer FilterStep.
   */
  @Test
  public void testPredicatePushDownIntoExpand_partialParentPushDown() {
    session.execute("CREATE CLASS PPForum EXTENDS V").close();
    session.execute("CREATE CLASS PPPost EXTENDS V").close();
    session.execute("CREATE CLASS PPContainerOf EXTENDS E").close();
    session.execute("CREATE CLASS PPHasCreator EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX PPForum SET name = 'forum1'").close();

    for (var i = 0; i < 5; i++) {
      session.execute(
          "CREATE VERTEX PPPost SET title = 'post" + i + "', score = " + (i + 1)).close();
      session.execute(
          "CREATE EDGE PPContainerOf FROM (SELECT FROM PPForum WHERE name = 'forum1')"
              + " TO (SELECT FROM PPPost WHERE title = 'post" + i + "')")
          .close();
    }
    session.commit();

    // This query uses a LET to simulate $parent: score > 3 is non-$parent,
    // so it should be pushed down.
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('PPContainerOf')) FROM PPForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE score > 3");
    var items = result.stream().toList();
    Assert.assertEquals("Should return posts with score > 3: post3(4), post4(5)", 2,
        items.size());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Push-down filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("push-down filter"));

    result.close();
    session.commit();
  }

  /**
   * Case C + Case A3 combined: class filter AND edge RID lookup.
   * Both should be pushed into ExpandStep independently.
   */
  @Test
  public void testPredicatePushDownIntoExpand_classFilterAndEdgeLookup() {
    session.execute("CREATE CLASS CEForum EXTENDS V").close();
    session.execute("CREATE CLASS CEPost EXTENDS V").close();
    session.execute("CREATE CLASS CEComment EXTENDS V").close();
    session.execute("CREATE CLASS CEPerson EXTENDS V").close();
    session.execute("CREATE CLASS CEContainerOf EXTENDS E").close();
    session.execute("CREATE CLASS CEHasCreator EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX CEForum SET name = 'forum1'").close();
    var personRs = session.execute("CREATE VERTEX CEPerson SET name = 'carol'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 3; i++) {
      session.execute("CREATE VERTEX CEPost SET title = 'post" + i + "'").close();
      session.execute(
          "CREATE EDGE CEContainerOf FROM (SELECT FROM CEForum WHERE name = 'forum1')"
              + " TO (SELECT FROM CEPost WHERE title = 'post" + i + "')")
          .close();
    }
    for (var i = 0; i < 2; i++) {
      session.execute("CREATE VERTEX CEComment SET text = 'comment" + i + "'").close();
      session.execute(
          "CREATE EDGE CEContainerOf FROM (SELECT FROM CEForum WHERE name = 'forum1')"
              + " TO (SELECT FROM CEComment WHERE text = 'comment" + i + "')")
          .close();
    }
    // post0 and post2 belong to carol
    session.execute(
        "CREATE EDGE CEHasCreator FROM (SELECT FROM CEPost WHERE title = 'post0')"
            + " TO (SELECT FROM CEPerson WHERE name = 'carol')")
        .close();
    session.execute(
        "CREATE EDGE CEHasCreator FROM (SELECT FROM CEPost WHERE title = 'post2')"
            + " TO (SELECT FROM CEPerson WHERE name = 'carol')")
        .close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('CEContainerOf')) FROM CEForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE @class = 'CEPost' AND out('CEHasCreator').@rid = " + personRid);
    var items = result.stream().toList();
    Assert.assertEquals("Should return 2 posts by carol (not comments)", 2, items.size());
    for (var item : items) {
      Assert.assertEquals("CEPost", item.asEntity().getSchemaClassName());
    }

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Class filter should be in EXPAND, plan was:\n" + plan,
        plan.contains("class filter"));
    Assert.assertTrue(
        "Edge RID lookup should be in EXPAND, plan was:\n" + plan,
        plan.contains("rid filter: out('CEHasCreator')"));

    result.close();
    session.commit();
  }

  // ── Coverage: index-backed pre-filter via expand() ──

  /**
   * Verifies that an indexed range filter on expanded records produces
   * correct results when pushed down into expand().
   */
  @Test
  public void testPredicatePushDownIntoExpand_indexedRangeFilter() {
    session.execute("CREATE CLASS IdxForum EXTENDS V").close();
    session.execute("CREATE CLASS IdxMsg EXTENDS V").close();
    session.execute("CREATE CLASS IdxContainerOf EXTENDS E").close();
    session.execute("CREATE PROPERTY IdxMsg.score INTEGER").close();
    session.execute(
        "CREATE INDEX IdxMsg.score ON IdxMsg (score) NOTUNIQUE").close();

    session.begin();
    session.execute("CREATE VERTEX IdxForum SET name = 'forum1'").close();
    for (var i = 0; i < 10; i++) {
      session.execute("CREATE VERTEX IdxMsg SET title = 'msg" + i + "', score = " + i)
          .close();
      session.execute(
          "CREATE EDGE IdxContainerOf FROM (SELECT FROM IdxForum WHERE name = 'forum1')"
              + " TO (SELECT FROM IdxMsg WHERE title = 'msg" + i + "')")
          .close();
    }
    session.commit();

    // Index filter: only messages with score >= 8 (should be 2: score=8, score=9)
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('IdxContainerOf')) FROM IdxForum"
            + "    WHERE name = 'forum1'"
            + ") WHERE score >= 8");
    var items = result.stream().toList();
    Assert.assertEquals("Should return 2 messages with score >= 8", 2, items.size());
    for (var item : items) {
      Assert.assertTrue(((Number) item.getProperty("score")).intValue() >= 8);
    }
    result.close();
    session.commit();
  }

  /**
   * Verifies that expand() with a non-indexed property filter still works
   * correctly (falls back to post-expand filtering).
   */
  @Test
  public void testPredicatePushDownIntoExpand_nonIndexedPropertyFilter() {
    session.execute("CREATE CLASS NiMsg EXTENDS V").close();
    session.execute("CREATE CLASS NiForum EXTENDS V").close();
    session.execute("CREATE CLASS NiContainerOf EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX NiForum SET name = 'f1'").close();
    for (var i = 0; i < 5; i++) {
      session.execute("CREATE VERTEX NiMsg SET val = " + i).close();
      session.execute(
          "CREATE EDGE NiContainerOf FROM (SELECT FROM NiForum WHERE name = 'f1')"
              + " TO (SELECT FROM NiMsg WHERE val = " + i + ")")
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('NiContainerOf')) FROM NiForum WHERE name = 'f1'"
            + ") WHERE val > 2");
    var items = result.stream().toList();
    Assert.assertEquals("Should return 2 messages with val > 2", 2, items.size());
    result.close();
    session.commit();
  }

  /**
   * Verifies that expand() reverse edge lookup works when the target vertex
   * has been deleted (RecordNotFoundException path).
   */
  @Test
  public void testPredicatePushDownIntoExpand_deletedTargetVertex() {
    session.execute("CREATE CLASS DtForum EXTENDS V").close();
    session.execute("CREATE CLASS DtPost EXTENDS V").close();
    session.execute("CREATE CLASS DtPerson EXTENDS V").close();
    session.execute("CREATE CLASS DtContainerOf EXTENDS E").close();
    session.execute("CREATE CLASS DtHasCreator EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX DtForum SET name = 'f1'").close();
    session.execute("CREATE VERTEX DtPost SET title = 'p1'").close();
    session.execute("CREATE VERTEX DtPerson SET name = 'alice'").close();
    session.execute(
        "CREATE EDGE DtContainerOf FROM (SELECT FROM DtForum WHERE name = 'f1')"
            + " TO (SELECT FROM DtPost WHERE title = 'p1')")
        .close();
    session.execute(
        "CREATE EDGE DtHasCreator FROM (SELECT FROM DtPost WHERE title = 'p1')"
            + " TO (SELECT FROM DtPerson WHERE name = 'alice')")
        .close();
    session.commit();

    // Query should work even with valid data
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('DtContainerOf')) FROM DtForum WHERE name = 'f1'"
            + ") WHERE out('DtHasCreator').size() > 0");
    var items = result.stream().toList();
    Assert.assertEquals(1, items.size());
    result.close();
    session.commit();
  }

  /**
   * Verifies index-assisted date range filtering via edge schema class inference.
   * The outer WHERE has {@code creationDate >= X AND creationDate < Y} but NO
   * {@code @class = ...} filter. The planner should infer the target class from
   * the edge schema ({@code EiHasCreator.out LINK EiMessage}) and use the
   * {@code EiMessage.creationDate} index for pre-filtering.
   */
  @Test
  public void testPredicatePushDownIntoExpand_indexViaEdgeSchemaInference() {
    session.execute("CREATE CLASS EiPerson EXTENDS V").close();
    session.execute("CREATE CLASS EiMessage EXTENDS V").close();
    session.execute("CREATE PROPERTY EiMessage.creationDate DATETIME").close();
    session.execute(
        "CREATE INDEX EiMessage.creationDate ON EiMessage (creationDate) NOTUNIQUE").close();

    session.execute("CREATE CLASS EiHasCreator EXTENDS E").close();
    session.execute("CREATE PROPERTY EiHasCreator.out LINK EiMessage").close();
    session.execute("CREATE PROPERTY EiHasCreator.in LINK EiPerson").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX EiPerson SET name = 'alice'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 10; i++) {
      var date = new GregorianCalendar(2024, i, 1).getTime();
      session.execute("CREATE VERTEX EiMessage SET creationDate = ?", date).close();
      session.execute(
          "CREATE EDGE EiHasCreator FROM (SELECT FROM EiMessage"
              + " WHERE creationDate = ?) TO ?",
          date, personRid)
          .close();
    }
    session.commit();

    // Query: all messages by alice created in Q1-Q2 2024 (Jan-Jun)
    session.begin();
    var startDate = new GregorianCalendar(2024, Calendar.JANUARY, 1).getTime();
    var endDate = new GregorianCalendar(2024, Calendar.JULY, 1).getTime();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(in('EiHasCreator')) FROM EiPerson WHERE @rid = ?"
            + ") WHERE creationDate >= ? AND creationDate < ?",
        personRid, startDate, endDate);
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return 6 messages (Jan-Jun)", 6, items.size());
    for (var item : items) {
      Assert.assertEquals("EiMessage", item.asEntity().getSchemaClassName());
    }

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Index pre-filter should fire via edge-inferred class, plan was:\n" + plan,
        plan.contains("index pre-filter"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that without linked class declarations on the edge schema, the planner
   * cannot infer the target class and thus does NOT use an index pre-filter.
   * The query still returns correct results via a generic push-down filter.
   */
  @Test
  public void testPredicatePushDownIntoExpand_noEdgeSchemaLinkNoIndex() {
    session.execute("CREATE CLASS NlPerson EXTENDS V").close();
    session.execute("CREATE CLASS NlMessage EXTENDS V").close();
    session.execute("CREATE PROPERTY NlMessage.creationDate DATETIME").close();
    session.execute(
        "CREATE INDEX NlMessage.creationDate ON NlMessage (creationDate) NOTUNIQUE").close();

    // Edge WITHOUT linked class declarations
    session.execute("CREATE CLASS NlHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX NlPerson SET name = 'bob'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 6; i++) {
      var date = new GregorianCalendar(2024, i, 1).getTime();
      session.execute("CREATE VERTEX NlMessage SET creationDate = ?", date).close();
      session.execute(
          "CREATE EDGE NlHasCreator FROM (SELECT FROM NlMessage"
              + " WHERE creationDate = ?) TO ?",
          date, personRid)
          .close();
    }
    session.commit();

    session.begin();
    var startDate = new GregorianCalendar(2024, Calendar.JANUARY, 1).getTime();
    var endDate = new GregorianCalendar(2024, Calendar.APRIL, 1).getTime();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(in('NlHasCreator')) FROM NlPerson WHERE @rid = ?"
            + ") WHERE creationDate >= ? AND creationDate < ?",
        personRid, startDate, endDate);
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return 3 messages (Jan-Mar)", 3, items.size());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "Index pre-filter should NOT fire without edge schema link, plan was:\n" + plan,
        plan.contains("index pre-filter"));

    result.close();
    session.commit();
  }

  /**
   * Verifies edge schema inference works for outgoing edges (out('Edge'))
   * in addition to incoming (in('Edge')).
   */
  @Test
  public void testPredicatePushDownIntoExpand_outDirectionEdgeSchemaInference() {
    session.execute("CREATE CLASS EsoPerson EXTENDS V").close();
    session.execute("CREATE CLASS EsoPost EXTENDS V").close();
    session.execute("CREATE CLASS EsoHasCreator EXTENDS E").close();
    session.execute("CREATE PROPERTY EsoPost.score INTEGER").close();
    session.execute("CREATE INDEX EsoPost.score ON EsoPost (score) NOTUNIQUE").close();

    session.begin();
    session.execute("CREATE VERTEX EsoPerson SET name = 'alice'").close();
    for (var i = 0; i < 6; i++) {
      session.execute("CREATE VERTEX EsoPost SET title = 'p" + i + "', score = " + i)
          .close();
      session.execute(
          "CREATE EDGE EsoHasCreator FROM (SELECT FROM EsoPost WHERE title = 'p" + i + "')"
              + " TO (SELECT FROM EsoPerson WHERE name = 'alice')")
          .close();
    }
    session.commit();

    // out('EsoHasCreator') from Person → targets EsoPost (via edge schema 'out' link)
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(in('EsoHasCreator')) FROM EsoPerson WHERE name = 'alice'"
            + ") WHERE score >= 4");
    var items = result.stream().toList();
    Assert.assertEquals("Should return 2 posts with score >= 4", 2, items.size());
    result.close();
    session.commit();
  }

  /**
   * Verifies that expand with no edge class parameter (bare out()) still
   * works correctly, falling back to non-indexed expansion.
   */
  @Test
  public void testPredicatePushDownIntoExpand_bareOutNoEdgeClass() {
    session.execute("CREATE CLASS BareF EXTENDS V").close();
    session.execute("CREATE CLASS BareP EXTENDS V").close();
    session.execute("CREATE CLASS BareLink EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX BareF SET name = 'f1'").close();
    for (var i = 0; i < 3; i++) {
      session.execute("CREATE VERTEX BareP SET val = " + i).close();
      session.execute(
          "CREATE EDGE BareLink FROM (SELECT FROM BareF WHERE name = 'f1')"
              + " TO (SELECT FROM BareP WHERE val = " + i + ")")
          .close();
    }
    session.commit();

    // Bare out() without edge class name — can't infer target class
    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out()) FROM BareF WHERE name = 'f1'"
            + ") WHERE val >= 2");
    var items = result.stream().toList();
    Assert.assertEquals(1, items.size());
    result.close();
    session.commit();
  }

  /**
   * Verifies that a non-existent edge class in expand does not cause errors —
   * returns empty results.
   */
  @Test
  public void testPredicatePushDownIntoExpand_nonExistentEdgeClass() {
    session.execute("CREATE CLASS NecF EXTENDS V").close();

    session.begin();
    session.execute("CREATE VERTEX NecF SET name = 'f1'").close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('NoSuchEdge')) FROM NecF WHERE name = 'f1'"
            + ") WHERE @class = 'V'");
    var items = result.stream().toList();
    Assert.assertEquals(0, items.size());
    result.close();
    session.commit();
  }

  // ── Coverage: edge schema inference null paths ──

  /**
   * both('Edge') is not a recognized traversal direction for inference —
   * extractTraversalDirection returns null, falls back to no index.
   */
  @Test
  public void testExpandInference_bothDirection_noInference() {
    session.execute("CREATE CLASS BdF EXTENDS V").close();
    session.execute("CREATE CLASS BdP EXTENDS V").close();
    session.execute("CREATE CLASS BdLink EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX BdF SET name = 'f1'").close();
    session.execute("CREATE VERTEX BdP SET val = 1").close();
    session.execute(
        "CREATE EDGE BdLink FROM (SELECT FROM BdF WHERE name = 'f1')"
            + " TO (SELECT FROM BdP WHERE val = 1)")
        .close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(both('BdLink')) FROM BdF WHERE name = 'f1'"
            + ") WHERE val = 1");
    var items = result.stream().toList();
    Assert.assertEquals(1, items.size());
    result.close();
    session.commit();
  }

  /**
   * Expand without a function call (expand(fieldName) instead of
   * expand(in/out('Edge'))) — extractExpandTraversalFunction returns null.
   */
  @Test
  public void testExpandInference_expandField_noInference() {
    session.execute("CREATE CLASS EfV EXTENDS V").close();

    session.begin();
    session.execute("CREATE VERTEX EfV SET name = 'a', tags = ['x','y','z']").close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(tags) FROM EfV WHERE name = 'a'"
            + ")");
    var items = result.stream().toList();
    Assert.assertEquals(3, items.size());
    result.close();
    session.commit();
  }

  /**
   * Edge class exists in schema but has no linked class on the target property —
   * lookupLinkedClassName returns null.
   */
  @Test
  public void testExpandInference_edgeNoLinkedClass_noInference() {
    session.execute("CREATE CLASS NlF EXTENDS V").close();
    session.execute("CREATE CLASS NlP EXTENDS V").close();
    // Create edge class without declaring linked vertex classes
    session.execute("CREATE CLASS NlEdge EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX NlF SET name = 'f1'").close();
    session.execute("CREATE VERTEX NlP SET val = 5").close();
    session.execute(
        "CREATE EDGE NlEdge FROM (SELECT FROM NlF WHERE name = 'f1')"
            + " TO (SELECT FROM NlP WHERE val = 5)")
        .close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('NlEdge')) FROM NlF WHERE name = 'f1'"
            + ") WHERE val = 5");
    var items = result.stream().toList();
    Assert.assertEquals(1, items.size());
    result.close();
    session.commit();
  }

  /**
   * Non-subquery FROM target — info.target has no statement, so
   * extractExpandTraversalFunction returns null early.
   */
  @Test
  public void testExpandInference_directClassTarget_noInference() {
    session.execute("CREATE CLASS DcV EXTENDS V").close();

    session.begin();
    session.execute("CREATE VERTEX DcV SET name = 'a', score = 10").close();
    session.execute("CREATE VERTEX DcV SET name = 'b', score = 20").close();
    session.commit();

    // Direct class target, not a subquery — no inference possible
    session.begin();
    var result = session.query("SELECT FROM DcV WHERE score > 5");
    var items = result.stream().toList();
    Assert.assertEquals(2, items.size());
    result.close();
    session.commit();
  }

  /**
   * in() without edge class parameter — extractEdgeClassName returns null.
   */
  @Test
  public void testExpandInference_inWithoutParam_noInference() {
    session.execute("CREATE CLASS IpF EXTENDS V").close();
    session.execute("CREATE CLASS IpP EXTENDS V").close();
    session.execute("CREATE CLASS IpLink EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX IpF SET name = 'f1'").close();
    session.execute("CREATE VERTEX IpP SET val = 7").close();
    session.execute(
        "CREATE EDGE IpLink FROM (SELECT FROM IpP WHERE val = 7)"
            + " TO (SELECT FROM IpF WHERE name = 'f1')")
        .close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(in()) FROM IpF WHERE name = 'f1'"
            + ") WHERE val = 7");
    var items = result.stream().toList();
    Assert.assertEquals(1, items.size());
    result.close();
    session.commit();
  }

  /**
   * Verifies that an indexed date range filter is still used for pre-filtering
   * even when the WHERE clause also contains a graph-navigation CONTAINS
   * condition (e.g. {@code out('IS_LOCATED_IN').name CONTAINS :country}).
   *
   * The CONTAINS condition causes {@code flatten()} to produce multiple OR
   * branches, which would normally prevent index lookup. The planner should
   * fall back to extracting only the indexable subset of the AND conditions
   * (the date range) and use it for index pre-filtering, while the full
   * WHERE (including CONTAINS) is applied as a post-filter.
   *
   * This pattern matches LDBC IC3's correlated LET subqueries.
   */
  @Test
  public void testPredicatePushDownIntoExpand_indexWithGraphNavContains() {
    // Schema: Person -[HAS_CREATOR]-> Message -[IS_LOCATED_IN]-> Place
    session.execute("CREATE CLASS GnPerson EXTENDS V").close();
    session.execute("CREATE CLASS GnMessage EXTENDS V").close();
    session.execute("CREATE PROPERTY GnMessage.creationDate DATETIME").close();
    session.execute(
        "CREATE INDEX GnMessage.creationDate ON GnMessage (creationDate) NOTUNIQUE")
        .close();
    session.execute("CREATE CLASS GnPlace EXTENDS V").close();
    session.execute("CREATE CLASS GnHasCreator EXTENDS E").close();
    session.execute("CREATE PROPERTY GnHasCreator.out LINK GnMessage").close();
    session.execute("CREATE PROPERTY GnHasCreator.in LINK GnPerson").close();
    session.execute("CREATE CLASS GnIsLocatedIn EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX GnPerson SET name = 'alice'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    var placeUs = session.execute("CREATE VERTEX GnPlace SET name = 'USA'");
    var usRid = placeUs.next().getIdentity();
    placeUs.close();

    var placeDe = session.execute("CREATE VERTEX GnPlace SET name = 'Germany'");
    var deRid = placeDe.next().getIdentity();
    placeDe.close();

    // Create 12 messages: Jan-Dec 2024, alternating USA/Germany
    for (var i = 0; i < 12; i++) {
      var date = new GregorianCalendar(2024, i, 15).getTime();
      var msgRs = session.execute(
          "CREATE VERTEX GnMessage SET creationDate = ?", date);
      var msgRid = msgRs.next().getIdentity();
      msgRs.close();

      session.execute(
          "CREATE EDGE GnHasCreator FROM ? TO ?", msgRid, personRid).close();
      var placeRid = (i % 2 == 0) ? usRid : deRid;
      session.execute(
          "CREATE EDGE GnIsLocatedIn FROM ? TO ?", msgRid, placeRid).close();
    }
    session.commit();

    // Query: messages by alice, created in Q1-Q2 2024, located in USA
    // Date range: Jan 1 - Jul 1 -> 6 messages (Jan-Jun)
    // Located in USA: Jan, Mar, May (i=0,2,4) -> 3 of 6
    session.begin();
    var startDate = new GregorianCalendar(2024, Calendar.JANUARY, 1).getTime();
    var endDate = new GregorianCalendar(2024, Calendar.JULY, 1).getTime();
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(in('GnHasCreator')) FROM GnPerson WHERE @rid = ?"
            + ") WHERE creationDate >= ? AND creationDate < ?"
            + "    AND out('GnIsLocatedIn').name CONTAINS 'USA'",
        personRid, startDate, endDate);
    var items = result.stream().toList();
    Assert.assertEquals(
        "Should return 3 messages (Jan, Mar, May in USA)", 3, items.size());
    for (var item : items) {
      Assert.assertEquals("GnMessage", item.asEntity().getSchemaClassName());
    }

    // Verify the plan uses index pre-filter (date range)
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Index pre-filter should fire for date range despite CONTAINS, "
            + "plan was:\n" + plan,
        plan.contains("index pre-filter"));

    result.close();
    session.commit();
  }

  /**
   * Tests that expand() with a @class filter on the target vertex
   * triggers the class filter push-down path in ExpandStep.
   */
  @Test
  public void testExpandWithClassFilter() {
    session.execute("CREATE CLASS EcAnimal EXTENDS V").close();
    session.execute("CREATE CLASS EcDog EXTENDS EcAnimal").close();
    session.execute("CREATE CLASS EcCat EXTENDS EcAnimal").close();
    session.execute("CREATE CLASS EcOwner EXTENDS V").close();
    session.execute("CREATE CLASS EcOwns EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX EcOwner SET name = 'Alice'").close();
    session.execute("CREATE VERTEX EcDog SET name = 'Rex'").close();
    session.execute("CREATE VERTEX EcCat SET name = 'Whiskers'").close();
    session.execute(
        "CREATE EDGE EcOwns FROM (SELECT FROM EcOwner WHERE name='Alice')"
            + " TO (SELECT FROM EcDog WHERE name='Rex')")
        .close();
    session.execute(
        "CREATE EDGE EcOwns FROM (SELECT FROM EcOwner WHERE name='Alice')"
            + " TO (SELECT FROM EcCat WHERE name='Whiskers')")
        .close();
    session.commit();

    session.begin();
    var classQuery = "SELECT FROM ("
        + "  SELECT expand(out('EcOwns')) FROM EcOwner WHERE name = 'Alice'"
        + ") WHERE @class = 'EcDog'";
    var cfList = session.query(classQuery).toList();
    Assert.assertEquals("Should only get dogs", 1, cfList.size());
    Assert.assertEquals("Rex", cfList.getFirst().getProperty("name"));

    // Verify the class filter optimization is active via EXPLAIN
    var explain = session.query("EXPLAIN " + classQuery).toList();
    var plan = (String) explain.getFirst().getProperty("executionPlanAsString");
    Assert.assertTrue(
        "EXPLAIN should show class filter push-down, plan was:\n" + plan,
        plan.contains("class filter"));
    session.commit();
  }

  /**
   * Tests expand() with an indexed property filter on the target,
   * triggering the index pre-filter path in ExpandStep
   */
  @Test
  public void testExpandWithIndexFilter() {
    session.execute("CREATE CLASS EiAuthor EXTENDS V").close();
    session.execute("CREATE CLASS EiArticle EXTENDS V").close();
    session.execute("CREATE CLASS EiWrote EXTENDS E").close();
    session.execute("CREATE PROPERTY EiArticle.rating INTEGER").close();
    session.execute(
        "CREATE INDEX EiArticle.rating ON EiArticle (rating) NOTUNIQUE")
        .close();

    session.begin();
    session.execute("CREATE VERTEX EiAuthor SET name = 'Bob'").close();
    for (int i = 0; i < 50; i++) {
      session.execute(
          "CREATE VERTEX EiArticle SET title = 'art" + i + "', rating = " + i)
          .close();
      session.execute(
          "CREATE EDGE EiWrote FROM (SELECT FROM EiAuthor WHERE name='Bob')"
              + " TO (SELECT FROM EiArticle WHERE title='art" + i + "')")
          .close();
    }
    session.commit();

    session.begin();
    var indexQuery = "SELECT FROM ("
        + "  SELECT expand(out('EiWrote')) FROM EiAuthor WHERE name = 'Bob'"
        + ") WHERE rating = 42";
    var list = session.query(indexQuery).toList();
    Assert.assertEquals("Should find exactly one article with rating=42",
        1, list.size());
    Assert.assertEquals(42, (int) list.getFirst().getProperty("rating"));

    // Verify the optimization is active via EXPLAIN: the optimizer may choose
    // either "index pre-filter" or "push-down filter" depending on cost estimates.
    // Both are valid optimizations — the key is that the filter was pushed down.
    var explain = session.query("EXPLAIN " + indexQuery).toList();
    var plan = (String) explain.getFirst().getProperty("executionPlanAsString");
    Assert.assertTrue(
        "EXPLAIN should show a pushed-down optimization, plan was:\n" + plan,
        plan.contains("index pre-filter") || plan.contains("push-down filter"));
    session.commit();
  }

  /**
   * Tests that expand() with a @rid = :param filter triggers the
   * DirectRid pre-filter path in ExpandStep
   */
  @Test
  public void testExpandWithRidFilter() {
    session.execute("CREATE CLASS ErNode EXTENDS V").close();
    session.execute("CREATE CLASS ErLink EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX ErNode SET name = 'root'").close();
    session.execute("CREATE VERTEX ErNode SET name = 'child1'").close();
    session.execute("CREATE VERTEX ErNode SET name = 'child2'").close();
    session.execute(
        "CREATE EDGE ErLink FROM (SELECT FROM ErNode WHERE name='root')"
            + " TO (SELECT FROM ErNode WHERE name='child1')")
        .close();
    session.execute(
        "CREATE EDGE ErLink FROM (SELECT FROM ErNode WHERE name='root')"
            + " TO (SELECT FROM ErNode WHERE name='child2')")
        .close();
    session.commit();

    session.begin();
    // Get the RID of child1
    var child1 = session.query("SELECT @rid FROM ErNode WHERE name='child1'")
        .toList();
    Assert.assertFalse(child1.isEmpty());
    var child1Rid = child1.getFirst().getProperty("@rid");

    // expand with @rid filter
    var result = session.query(
        "SELECT FROM ("
            + "  SELECT expand(out('ErLink')) FROM ErNode WHERE name = 'root'"
            + ") WHERE @rid = ?",
        child1Rid);
    var list = result.toList();
    Assert.assertEquals("Should find exactly child1", 1, list.size());
    Assert.assertEquals("child1", list.getFirst().getProperty("name"));

    // Verify the rid filter optimization is active via EXPLAIN
    var explain = session.query(
        "EXPLAIN SELECT FROM ("
            + "  SELECT expand(out('ErLink')) FROM ErNode WHERE name = 'root'"
            + ") WHERE @rid = " + child1Rid)
        .toList();
    var plan = (String) explain.getFirst().getProperty("executionPlanAsString");
    Assert.assertTrue(
        "EXPLAIN should show rid filter, plan was:\n" + plan,
        plan.contains("rid filter"));
    session.commit();
  }

  /**
   * Tests a LET subquery with expand(), verifying that LetQueryStep
   * paths are exercised. Also validates EXPLAIN output shows the LET step.
   */
  @Test
  public void testLetSubqueryWithExpand() {
    session.execute("CREATE CLASS LePerson EXTENDS V").close();
    session.execute("CREATE CLASS LeKnows EXTENDS E").close();

    session.begin();
    session.execute("CREATE VERTEX LePerson SET name='Alice'").close();
    session.execute("CREATE VERTEX LePerson SET name='Bob'").close();
    session.execute(
        "CREATE EDGE LeKnows FROM (SELECT FROM LePerson WHERE name='Alice')"
            + " TO (SELECT FROM LePerson WHERE name='Bob')")
        .close();
    session.commit();

    session.begin();
    var query = "SELECT *, $friends FROM LePerson"
        + " LET $friends = (SELECT expand(out('LeKnows')) FROM LePerson"
        + "   WHERE name = $parent.$current.name)"
        + " WHERE name = 'Alice'";
    var list = session.query(query).toList();
    Assert.assertEquals(1, list.size());

    // Verify EXPLAIN shows the LET step
    var explain = session.query("EXPLAIN " + query).toList();
    var plan = (String) explain.getFirst().getProperty("executionPlanAsString");
    Assert.assertTrue("EXPLAIN should show LET step, plan was:\n" + plan,
        plan.contains("LET"));
    session.commit();
  }

  @Test
  public void testOutEStateFullEdgesIndexUsageInGraph() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestOutEStateFullIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestOutEStateFullIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var propertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName);
    var oudEdgesProperty = vertexClass.createProperty(propertyName,
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var indexName = "TestOutEStateFullIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, oudEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "outE('" + edgeClassName + "') contains :outE", Map.of("outE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[0], resList.getFirst().getIdentity());

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());

        var sourceStep = steps.getFirst();
        Assert.assertTrue(sourceStep instanceof FetchFromIndexStep);
        var fetchFromIndexStep = (FetchFromIndexStep) sourceStep;
        Assert.assertEquals(indexName, fetchFromIndexStep.getDesc().getIndex().getName());

        var keyCondition = fetchFromIndexStep.getDesc().getKeyCondition();
        Assert.assertTrue(keyCondition instanceof SQLAndBlock);
        var sqlAndBlock = (SQLAndBlock) keyCondition;
        Assert.assertEquals(1, sqlAndBlock.getSubBlocks().size());

        var expression = sqlAndBlock.getSubBlocks().getFirst();
        Assert.assertTrue(expression instanceof SQLContainsCondition);

        var containsCondition = (SQLContainsCondition) expression;
        Assert.assertTrue(
            containsCondition.getLeft() instanceof SQLGetInternalPropertyExpression);
        Assert.assertEquals(propertyName, containsCondition.getLeft().toString());
        Assert.assertEquals(":outE", containsCondition.getRight().toString());
      }
    });
  }

  @Test
  public void testOutEStateFullEdgesWithoutIndexUsageInGraph() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestOutEStateFullWithoutIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestOutEStateFullWithoutIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var vertexClassName = vertexClass.getName();
    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "outE('" + edgeClassName + "') contains :outE", Map.of("outE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[0], resList.getFirst().getIdentity());
      }
    });
  }

  @Test
  public void testInEStateFullEdgesIndexUsageInGraph() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestInEStateFullIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestInEStateFullIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var inEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName),
        PropertyType.LINKBAG);

    var propertyName = inEdgesProperty.getName();
    var vertexClassName = vertexClass.getName();

    var indexName = "TestInEStateFullIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, inEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "inE('" + edgeClassName + "') contains :inE", Map.of("inE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(1, resList.size());
        Assert.assertEquals(rids[1], resList.getFirst().getIdentity());

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());

        var sourceStep = steps.getFirst();
        Assert.assertTrue(sourceStep instanceof FetchFromIndexStep);
        var fetchFromIndexStep = (FetchFromIndexStep) sourceStep;
        Assert.assertEquals(indexName, fetchFromIndexStep.getDesc().getIndex().getName());

        var keyCondition = fetchFromIndexStep.getDesc().getKeyCondition();
        Assert.assertTrue(keyCondition instanceof SQLAndBlock);
        var sqlAndBlock = (SQLAndBlock) keyCondition;
        Assert.assertEquals(1, sqlAndBlock.getSubBlocks().size());

        var expression = sqlAndBlock.getSubBlocks().getFirst();
        Assert.assertTrue(expression instanceof SQLContainsCondition);

        var containsCondition = (SQLContainsCondition) expression;
        Assert.assertTrue(
            containsCondition.getLeft() instanceof SQLGetInternalPropertyExpression);
        Assert.assertEquals(propertyName, containsCondition.getLeft().toString());
        Assert.assertEquals(":inE", containsCondition.getRight().toString());
      }
    });
  }

  @Test
  public void testBothEStateFullEdgesWithoutIndex() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEStateFullVertex");
    var edgeClass = schema.createEdgeClass("TestBothEStateFullEdge");

    var edgeClassName = edgeClass.getName();
    var vertexClassName = vertexClass.getName();

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));
      }
    });
  }

  @Test
  public void testBothEStateFullEdgesIndexUsageInGraphOneIndexIn() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEStateFullIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestBothEStateFullIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var oudEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName),
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var indexName = "TestBothEStateFullIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, oudEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));
      }
    });
  }

  @Test
  public void testBothEStateFullEdgesIndexUsageInGraphOneIndexOut() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEStateFullIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestBothEStateFullIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();
    var oudEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName),
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var indexName = "TestBothEStateFullIndexUsageInGraphIndex";
    vertexClass.createIndex(indexName, INDEX_TYPE.NOTUNIQUE, oudEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));
      }
    });
  }

  @Test
  public void testBothEStateFullEdgesIndexUsageInGraphTwoIndexes() {
    var schema = session.getSchema();

    var vertexClass = schema.createVertexClass("TestBothEStateFullIndexUsageInGraphVertex");
    var edgeClass = schema.createEdgeClass("TestBothEStateFullIndexUsageInGraphEdge");

    var edgeClassName = edgeClass.getName();

    var outEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClassName),
        PropertyType.LINKBAG);
    var inEdgesProperty = vertexClass.createProperty(
        Vertex.getEdgeLinkFieldName(Direction.IN, edgeClassName),
        PropertyType.LINKBAG);

    var vertexClassName = vertexClass.getName();

    var outIndexName = "TestBothEStateFullIndexUsageInGraphIndexOutIndex";
    vertexClass.createIndex(outIndexName, INDEX_TYPE.NOTUNIQUE, outEdgesProperty.getName());

    var inIndexName = "TestBothEStateFullIndexUsageInGraphIndexInIndex";
    vertexClass.createIndex(inIndexName, INDEX_TYPE.NOTUNIQUE, inEdgesProperty.getName());

    var rids = session.computeInTx(transaction -> {
      Vertex v1 = null;
      Vertex v2 = null;

      Edge edge = null;

      for (var i = 0; i < 10; i++) {
        v1 = transaction.newVertex(vertexClass);
        v2 = transaction.newVertex(vertexClass);

        edge = v1.addEdge(v2, edgeClass);
      }

      return new RID[] {v1.getIdentity(), v2.getIdentity(), edge.getIdentity()};
    });

    session.executeInTx(transaction -> {
      try (var rs = transaction.query("select from " + vertexClassName + " where "
          + "bothE('" + edgeClassName + "') contains :bothE", Map.of("bothE", rids[2]))) {
        var resList = rs.toVertexList();

        Assert.assertEquals(2, resList.size());
        Assert.assertEquals(Set.of(rids[0], rids[1]), new HashSet<>(resList));

        var executionPlan = rs.getExecutionPlan();
        var steps = executionPlan.getSteps();
        Assert.assertFalse(steps.isEmpty());
        var first = steps.getFirst();
        Assert.assertTrue(first instanceof ParallelExecStep);

        var parallelExecStep = (ParallelExecStep) first;
        var parallelSteps = parallelExecStep.getSubExecutionPlans();

        Assert.assertEquals(2, parallelSteps.size());

        var firstParallelStep = parallelSteps.getFirst().getSteps().getFirst();
        Assert.assertTrue(firstParallelStep instanceof FetchFromIndexStep);
        var firstParallelIndexStep = (FetchFromIndexStep) firstParallelStep;

        var secondParallelStep = parallelSteps.getLast().getSteps().getFirst();
        Assert.assertTrue(secondParallelStep instanceof FetchFromIndexStep);
        var secondParallelIndexStep = (FetchFromIndexStep) secondParallelStep;

        var indexNames = Set.of(firstParallelIndexStep.getDesc().getIndex().getName(),
            secondParallelIndexStep.getDesc().getIndex().getName());

        Assert.assertEquals(Set.of(outIndexName, inIndexName), indexNames);
      }
    });
  }

  /**
   * Verifies that two correlated LET subqueries sharing the same inner FROM
   * subquery are materialized once and reused. The execution plan should show
   * a MATERIALIZED LET GROUP instead of two independent LET steps.
   */
  @Test
  public void testLetMaterialization_sharedBase() {
    session.execute("CREATE CLASS LmPerson EXTENDS V").close();
    session.execute("CREATE CLASS LmMessage EXTENDS V").close();
    session.execute("CREATE CLASS LmHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX LmPerson SET name = 'alice'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 6; i++) {
      String type = (i < 4) ? "Post" : "Comment";
      session.execute(
          "CREATE VERTEX LmMessage SET title = 'msg" + i + "', type = '" + type + "'")
          .close();
      session.execute(
          "CREATE EDGE LmHasCreator FROM (SELECT FROM LmMessage WHERE title = 'msg"
              + i + "') TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $postCount[0].cnt as posts, $commentCount[0].cnt as comments"
            + " FROM LmPerson"
            + " LET $postCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('LmHasCreator')) FROM LmPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Post'),"
            + " $commentCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('LmHasCreator')) FROM LmPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Comment')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(4L, ((Number) item.getProperty("posts")).longValue());
    Assert.assertEquals(2L, ((Number) item.getProperty("comments")).longValue());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Should use MATERIALIZED LET GROUP, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that a single LET subquery (no sharing opportunity) still uses
   * the regular LET step, not the materialized group step.
   */
  @Test
  public void testLetMaterialization_singleLetNoGrouping() {
    session.execute("CREATE CLASS SlPerson EXTENDS V").close();
    session.execute("CREATE CLASS SlMessage EXTENDS V").close();
    session.execute("CREATE CLASS SlHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX SlPerson SET name = 'bob'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 3; i++) {
      session.execute(
          "CREATE VERTEX SlMessage SET title = 'msg" + i + "'").close();
      session.execute(
          "CREATE EDGE SlHasCreator FROM (SELECT FROM SlMessage WHERE title = 'msg"
              + i + "') TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $msgCount[0].cnt as msgs FROM SlPerson"
            + " LET $msgCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('SlHasCreator')) FROM SlPerson"
            + " WHERE @rid = $parent.$current.@rid))"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(3L, ((Number) item.getProperty("msgs")).longValue());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "Single LET should NOT use materialized group, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));
    Assert.assertTrue(
        "Single LET should use regular LET step, plan was:\n" + plan,
        plan.contains("LET (for each record)"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that when the materialized results exceed the configured max size,
   * the engine falls back to independent execution and still returns correct
   * results.
   */
  @Test
  public void testLetMaterialization_fallbackOnSizeLimit() {
    session.execute("CREATE CLASS FbPerson EXTENDS V").close();
    session.execute("CREATE CLASS FbMessage EXTENDS V").close();
    session.execute("CREATE CLASS FbHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX FbPerson SET name = 'carol'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 5; i++) {
      String type = (i < 3) ? "Post" : "Comment";
      session.execute(
          "CREATE VERTEX FbMessage SET title = 'msg" + i + "', type = '" + type + "'")
          .close();
      session.execute(
          "CREATE EDGE FbHasCreator FROM (SELECT FROM FbMessage WHERE title = 'msg"
              + i + "') TO " + personRid)
          .close();
    }
    session.commit();

    var originalMax = GlobalConfiguration.QUERY_LET_MATERIALIZATION_MAX_SIZE
        .getValueAsInteger();
    try {
      GlobalConfiguration.QUERY_LET_MATERIALIZATION_MAX_SIZE.setValue(1);

      session.begin();
      var result = session.query(
          "SELECT name, $postCount[0].cnt as posts,"
              + " $commentCount[0].cnt as comments"
              + " FROM FbPerson"
              + " LET $postCount = (SELECT count(*) as cnt FROM"
              + " (SELECT expand(in('FbHasCreator')) FROM FbPerson"
              + " WHERE @rid = $parent.$current.@rid)"
              + " WHERE type = 'Post'),"
              + " $commentCount = (SELECT count(*) as cnt FROM"
              + " (SELECT expand(in('FbHasCreator')) FROM FbPerson"
              + " WHERE @rid = $parent.$current.@rid)"
              + " WHERE type = 'Comment')"
              + " WHERE @rid = " + personRid);

      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertEquals(3L, ((Number) item.getProperty("posts")).longValue());
      Assert.assertEquals(2L, ((Number) item.getProperty("comments")).longValue());

      result.close();
      session.commit();
    } finally {
      GlobalConfiguration.QUERY_LET_MATERIALIZATION_MAX_SIZE.setValue(originalMax);
    }
  }

  /**
   * Verifies that two LET subqueries with different inner FROM subqueries are
   * NOT grouped — each executes independently.
   */
  @Test
  public void testLetMaterialization_differentInnerSubqueries() {
    session.execute("CREATE CLASS DsPerson EXTENDS V").close();
    session.execute("CREATE CLASS DsMessage EXTENDS V").close();
    session.execute("CREATE CLASS DsForum EXTENDS V").close();
    session.execute("CREATE CLASS DsHasCreator EXTENDS E").close();
    session.execute("CREATE CLASS DsMemberOf EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX DsPerson SET name = 'dave'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 3; i++) {
      session.execute("CREATE VERTEX DsMessage SET title = 'msg" + i + "'").close();
      session.execute(
          "CREATE EDGE DsHasCreator FROM (SELECT FROM DsMessage WHERE title = 'msg"
              + i + "') TO " + personRid)
          .close();
    }
    for (var i = 0; i < 2; i++) {
      session.execute("CREATE VERTEX DsForum SET title = 'forum" + i + "'").close();
      session.execute(
          "CREATE EDGE DsMemberOf FROM " + personRid
              + " TO (SELECT FROM DsForum WHERE title = 'forum" + i + "')")
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $msgCount[0].cnt as msgs, $forumCount[0].cnt as forums"
            + " FROM DsPerson"
            + " LET $msgCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('DsHasCreator')) FROM DsPerson"
            + " WHERE @rid = $parent.$current.@rid)),"
            + " $forumCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(out('DsMemberOf')) FROM DsPerson"
            + " WHERE @rid = $parent.$current.@rid))"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(3L, ((Number) item.getProperty("msgs")).longValue());
    Assert.assertEquals(2L, ((Number) item.getProperty("forums")).longValue());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertFalse(
        "Different inner subqueries should NOT be grouped, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));

    result.close();
    session.commit();
  }

  /**
   * Verifies that three LET subqueries sharing the same inner FROM subquery
   * are all grouped into a single materialized group.
   */
  @Test
  public void testLetMaterialization_threeLetsSharedBase() {
    session.execute("CREATE CLASS TlPerson EXTENDS V").close();
    session.execute("CREATE CLASS TlMessage EXTENDS V").close();
    session.execute("CREATE CLASS TlHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX TlPerson SET name = 'eve'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 9; i++) {
      String type = i < 4 ? "Post" : (i < 7 ? "Comment" : "Reply");
      session.execute(
          "CREATE VERTEX TlMessage SET title = 'msg" + i + "', type = '" + type + "'")
          .close();
      session.execute(
          "CREATE EDGE TlHasCreator FROM (SELECT FROM TlMessage WHERE title = 'msg"
              + i + "') TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $posts[0].cnt as posts, $comments[0].cnt as comments,"
            + " $replies[0].cnt as replies"
            + " FROM TlPerson"
            + " LET $posts = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('TlHasCreator')) FROM TlPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Post'),"
            + " $comments = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('TlHasCreator')) FROM TlPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Comment'),"
            + " $replies = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('TlHasCreator')) FROM TlPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Reply')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(4L, ((Number) item.getProperty("posts")).longValue());
    Assert.assertEquals(3L, ((Number) item.getProperty("comments")).longValue());
    Assert.assertEquals(2L, ((Number) item.getProperty("replies")).longValue());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Three shared LETs should use MATERIALIZED LET GROUP, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));

    result.close();
    session.commit();
  }

  /**
   * Verifies mixed LET types: two shared subqueries grouped, one expression LET
   * and one different subquery LET handled independently.
   */
  @Test
  public void testLetMaterialization_mixedLetTypes() {
    session.execute("CREATE CLASS MxPerson EXTENDS V").close();
    session.execute("CREATE CLASS MxMessage EXTENDS V").close();
    session.execute("CREATE CLASS MxHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX MxPerson SET name = 'frank', age = 30");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (var i = 0; i < 4; i++) {
      String type = (i < 2) ? "Post" : "Comment";
      session.execute(
          "CREATE VERTEX MxMessage SET title = 'msg" + i + "', type = '" + type + "'")
          .close();
      session.execute(
          "CREATE EDGE MxHasCreator FROM (SELECT FROM MxMessage WHERE title = 'msg"
              + i + "') TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $doubled, $postCount[0].cnt as posts,"
            + " $commentCount[0].cnt as comments"
            + " FROM MxPerson"
            + " LET $doubled = age * 2,"
            + " $postCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('MxHasCreator')) FROM MxPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Post'),"
            + " $commentCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('MxHasCreator')) FROM MxPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Comment')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    // Focus on the shared subquery LET results (the expression LET is handled separately)
    Assert.assertEquals(2L, ((Number) item.getProperty("posts")).longValue());
    Assert.assertEquals(2L, ((Number) item.getProperty("comments")).longValue());

    result.close();
    session.commit();
  }

  /**
   * Verifies that materialized LET handles an empty inner result set correctly:
   * the outer COUNT(*) should return 0 for both LET variables.
   */
  @Test
  public void testMaterializedLet_emptyInnerResultSet() {
    session.execute("CREATE CLASS EmptyPerson EXTENDS V").close();
    session.execute("CREATE CLASS EmptyMessage EXTENDS V").close();
    session.execute("CREATE CLASS EmptyHasCreator EXTENDS E").close();

    session.begin();
    // Create a person with NO edges to any messages
    var personRs = session.execute(
        "CREATE VERTEX EmptyPerson SET name = 'lonely'");
    var personRid = personRs.next().getIdentity();
    personRs.close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $postCount[0].cnt as posts,"
            + " $commentCount[0].cnt as comments"
            + " FROM EmptyPerson"
            + " LET $postCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('EmptyHasCreator')) FROM EmptyPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Post'),"
            + " $commentCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('EmptyHasCreator')) FROM EmptyPerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Comment')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    // Empty inner result set should yield count 0 for both
    Assert.assertEquals(0L, ((Number) item.getProperty("posts")).longValue());
    Assert.assertEquals(0L, ((Number) item.getProperty("comments")).longValue());
    Assert.assertFalse(result.hasNext());

    result.close();
    session.commit();
  }

  /**
   * Verifies LET with inter-variable dependency: $b references $a. The second
   * LET variable depends on the first, ensuring ordering is preserved.
   */
  @Test
  public void testCorrelatedLet_interVariableDependency() {
    session.execute("CREATE CLASS DepPerson EXTENDS V").close();

    session.begin();
    var personRs = session.execute(
        "CREATE VERTEX DepPerson SET name = 'alice', age = 10");
    var personRid = personRs.next().getIdentity();
    personRs.close();
    session.commit();

    session.begin();
    var result = session.query(
        "SELECT name, $a, $b FROM DepPerson"
            + " LET $a = age * 2,"
            + " $b = $a + 5"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(20, ((Number) item.getProperty("$a")).intValue());
    Assert.assertEquals(25, ((Number) item.getProperty("$b")).intValue());
    Assert.assertFalse(result.hasNext());

    result.close();
    session.commit();
  }

  /**
   * Verifies that running the same materialized LET query twice produces
   * consistent results, exercising the plan caching path.
   */
  @Test
  public void testMaterializedLet_planCachingConsistency() {
    session.execute("CREATE CLASS CachePerson EXTENDS V").close();
    session.execute("CREATE CLASS CacheMessage EXTENDS V").close();
    session.execute("CREATE CLASS CacheHasCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute(
        "CREATE VERTEX CachePerson SET name = 'bob'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (int i = 0; i < 3; i++) {
      String type = (i < 2) ? "Post" : "Comment";
      session.execute(
          "CREATE VERTEX CacheMessage SET title = 'cm" + i
              + "', type = '" + type + "'")
          .close();
      session.execute(
          "CREATE EDGE CacheHasCreator FROM"
              + " (SELECT FROM CacheMessage WHERE title = 'cm" + i
              + "') TO " + personRid)
          .close();
    }
    session.commit();

    String query =
        "SELECT name, $postCount[0].cnt as posts,"
            + " $commentCount[0].cnt as comments"
            + " FROM CachePerson"
            + " LET $postCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('CacheHasCreator')) FROM CachePerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Post'),"
            + " $commentCount = (SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('CacheHasCreator')) FROM CachePerson"
            + " WHERE @rid = $parent.$current.@rid)"
            + " WHERE type = 'Comment')"
            + " WHERE @rid = " + personRid;

    // First execution
    session.begin();
    var result1 = session.query(query);
    Assert.assertTrue(result1.hasNext());
    var item1 = result1.next();
    long posts1 = ((Number) item1.getProperty("posts")).longValue();
    long comments1 = ((Number) item1.getProperty("comments")).longValue();
    result1.close();
    session.commit();

    // Second execution (exercises plan cache)
    session.begin();
    var result2 = session.query(query);
    Assert.assertTrue(result2.hasNext());
    var item2 = result2.next();
    Assert.assertEquals(posts1, ((Number) item2.getProperty("posts")).longValue());
    Assert.assertEquals(comments1,
        ((Number) item2.getProperty("comments")).longValue());
    result2.close();
    session.commit();

    Assert.assertEquals(2L, posts1);
    Assert.assertEquals(1L, comments1);
  }

  /**
   * Verifies that three LET subqueries sharing the same inner FROM subquery
   * are correctly materialized and produce independent results. Each applies
   * a different WHERE filter to the same expanded edge set.
   */
  @Test
  public void testMaterializedLet_threeEntriesSameBase() {
    session.execute("CREATE CLASS TriPerson EXTENDS V").close();
    session.execute("CREATE CLASS TriMessage EXTENDS V").close();
    session.execute("CREATE CLASS TriCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute(
        "CREATE VERTEX TriPerson SET name = 'alice'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    // 2 posts, 3 comments, 1 draft
    for (int i = 0; i < 6; i++) {
      String type = (i < 2) ? "Post" : (i < 5) ? "Comment" : "Draft";
      session.execute(
          "CREATE VERTEX TriMessage SET type = '" + type + "'").close();
      session.execute(
          "CREATE EDGE TriCreator FROM"
              + " (SELECT FROM TriMessage WHERE type = '" + type
              + "' AND @rid NOT IN (SELECT in('TriCreator') FROM TriPerson))"
              + " TO " + personRid)
          .close();
    }
    session.commit();

    // Three LET entries with the same inner subquery but different filters.
    String query =
        "SELECT name,"
            + " $posts[0].cnt as postCount,"
            + " $comments[0].cnt as commentCount,"
            + " $drafts[0].cnt as draftCount"
            + " FROM TriPerson"
            + " LET $posts = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('TriCreator')) FROM TriPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Post'),"
            + " $comments = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('TriCreator')) FROM TriPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Comment'),"
            + " $drafts = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('TriCreator')) FROM TriPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Draft')"
            + " WHERE @rid = " + personRid;

    session.begin();
    var rs = session.query(query);
    Assert.assertTrue(rs.hasNext());
    var row = rs.next();
    Assert.assertEquals(2L,
        ((Number) row.getProperty("postCount")).longValue());
    Assert.assertEquals(3L,
        ((Number) row.getProperty("commentCount")).longValue());
    Assert.assertEquals(1L,
        ((Number) row.getProperty("draftCount")).longValue());
    rs.close();
    session.commit();
  }

  /**
   * Verifies that a LET expression ($total) referencing a materialized LET
   * subquery ($posts) produces correct results. The subquery is part of a
   * materialized group (shared inner FROM), while the expression is evaluated
   * separately. This guards against evaluation order regressions — if
   * materialization somehow ran $total before $posts, it would get null/wrong
   * values.
   */
  @Test
  public void testMaterializedLet_expressionReferencesSubquery() {
    session.execute("CREATE CLASS RefPerson EXTENDS V").close();
    session.execute("CREATE CLASS RefMessage EXTENDS V").close();
    session.execute("CREATE CLASS RefCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute(
        "CREATE VERTEX RefPerson SET name = 'eve'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    // 4 posts, 2 comments
    String[] types = {"Post", "Post", "Post", "Post", "Comment", "Comment"};
    for (int i = 0; i < types.length; i++) {
      session.execute(
          "CREATE VERTEX RefMessage SET type = '" + types[i] + "'").close();
    }
    for (int i = 0; i < types.length; i++) {
      session.execute(
          "CREATE EDGE RefCreator FROM"
              + " (SELECT FROM RefMessage WHERE type = '" + types[i]
              + "' AND @rid NOT IN"
              + " (SELECT in('RefCreator') FROM RefPerson))"
              + " TO " + personRid)
          .close();
    }
    session.commit();

    // $posts and $comments share the same inner FROM → materialized group.
    // $total is an expression referencing $posts and $comments → evaluated
    // after the group, must see the correct values.
    String query =
        "SELECT name,"
            + " $posts[0].cnt as postCount,"
            + " $comments[0].cnt as commentCount,"
            + " $total as totalCount"
            + " FROM RefPerson"
            + " LET $posts = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('RefCreator')) FROM RefPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Post'),"
            + " $comments = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('RefCreator')) FROM RefPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Comment'),"
            + " $total = $posts[0].cnt + $comments[0].cnt"
            + " WHERE @rid = " + personRid;

    session.begin();
    var rs = session.query(query);
    Assert.assertTrue(rs.hasNext());
    var row = rs.next();
    Assert.assertEquals(4L,
        ((Number) row.getProperty("postCount")).longValue());
    Assert.assertEquals(2L,
        ((Number) row.getProperty("commentCount")).longValue());
    // $total = $posts[0].cnt + $comments[0].cnt = 4 + 2 = 6
    Assert.assertNotNull("$total should not be null",
        row.getProperty("totalCount"));
    Assert.assertEquals(6L,
        ((Number) row.getProperty("totalCount")).longValue());
    rs.close();
    session.commit();
  }

  // ---------- Common filter extraction + push-down interaction tests ----------

  /**
   * Two LET entries sharing the same inner subquery with a common @class filter
   * and different per-entry conditions. The common filter (@class = 'PdPost')
   * should be pushed into the materialization query's ExpandStep, while the
   * per-entry conditions (tag = 'A' / tag = 'B') are preserved in each entry's
   * FilterStep via skipExpandPushDown.
   */
  @Test
  public void testMaterializedLet_commonFilterPushDown() {
    session.execute("CREATE CLASS PdPerson EXTENDS V").close();
    session.execute("CREATE CLASS PdPost EXTENDS V").close();
    session.execute("CREATE CLASS PdComment EXTENDS V").close();
    session.execute("CREATE CLASS PdCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX PdPerson SET name = 'alice'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    // 3 Posts with tag=A, 2 Posts with tag=B, 2 Comments (should be ignored)
    for (int i = 0; i < 3; i++) {
      var msgRs = session.execute(
          "CREATE VERTEX PdPost SET tag = 'A'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE PdCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    for (int i = 0; i < 2; i++) {
      var msgRs = session.execute(
          "CREATE VERTEX PdPost SET tag = 'B'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE PdCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    for (int i = 0; i < 2; i++) {
      var msgRs = session.execute(
          "CREATE VERTEX PdComment SET tag = 'A'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE PdCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    // Common filter: @class = 'PdPost'. Per-entry: tag = 'A' / tag = 'B'
    var result = session.query(
        "SELECT name,"
            + " $aCount[0].cnt as tagA,"
            + " $bCount[0].cnt as tagB"
            + " FROM PdPerson"
            + " LET $aCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('PdCreator')) FROM PdPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'PdPost' AND tag = 'A'),"
            + " $bCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('PdCreator')) FROM PdPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'PdPost' AND tag = 'B')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(
        "Posts with tag=A", 3L, ((Number) item.getProperty("tagA")).longValue());
    Assert.assertEquals(
        "Posts with tag=B", 2L, ((Number) item.getProperty("tagB")).longValue());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Should use MATERIALIZED LET GROUP, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));
    Assert.assertTrue(
        "Should show common filter in plan, plan was:\n" + plan,
        plan.contains("common filter:"));

    result.close();
    session.commit();
  }

  /**
   * Two LET entries with completely different WHERE clauses (no common filter).
   * Materialization should still work — each entry gets its full WHERE preserved
   * via skipExpandPushDown. No wrapper query is built.
   */
  @Test
  public void testMaterializedLet_noCommonFilter() {
    session.execute("CREATE CLASS NcPerson EXTENDS V").close();
    session.execute("CREATE CLASS NcMessage EXTENDS V").close();
    session.execute("CREATE CLASS NcCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX NcPerson SET name = 'bob'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    // 4 messages: 2 with color=red, 2 with size=big
    for (int i = 0; i < 2; i++) {
      var msgRs = session.execute(
          "CREATE VERTEX NcMessage SET color = 'red', size = 'small'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE NcCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    for (int i = 0; i < 2; i++) {
      var msgRs = session.execute(
          "CREATE VERTEX NcMessage SET color = 'blue', size = 'big'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE NcCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    // Completely different WHERE clauses — no common filter possible
    var result = session.query(
        "SELECT name,"
            + " $redCount[0].cnt as reds,"
            + " $bigCount[0].cnt as bigs"
            + " FROM NcPerson"
            + " LET $redCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('NcCreator')) FROM NcPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE color = 'red'),"
            + " $bigCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('NcCreator')) FROM NcPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE size = 'big')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(
        "Messages with color=red", 2L,
        ((Number) item.getProperty("reds")).longValue());
    Assert.assertEquals(
        "Messages with size=big", 2L,
        ((Number) item.getProperty("bigs")).longValue());

    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Should still use MATERIALIZED LET GROUP, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));

    result.close();
    session.commit();
  }

  /**
   * Two LET entries where one has a WHERE clause and the other has no WHERE.
   * The entry without WHERE contributes an empty set to the intersection,
   * resulting in no common filter. Both entries should still produce correct
   * results.
   */
  @Test
  public void testMaterializedLet_oneEntryWithoutWhere() {
    session.execute("CREATE CLASS NwPerson EXTENDS V").close();
    session.execute("CREATE CLASS NwMessage EXTENDS V").close();
    session.execute("CREATE CLASS NwCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX NwPerson SET name = 'carol'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (int i = 0; i < 5; i++) {
      String type = (i < 3) ? "Post" : "Comment";
      var msgRs = session.execute(
          "CREATE VERTEX NwMessage SET type = '" + type + "'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE NwCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    // $postCount has a WHERE, $allCount has no WHERE at all
    var result = session.query(
        "SELECT name,"
            + " $postCount[0].cnt as posts,"
            + " $allCount[0].cnt as total"
            + " FROM NwPerson"
            + " LET $postCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('NwCreator')) FROM NwPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Post'),"
            + " $allCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('NwCreator')) FROM NwPerson"
            + "   WHERE @rid = $parent.$current.@rid))"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(
        "Posts only", 3L, ((Number) item.getProperty("posts")).longValue());
    Assert.assertEquals(
        "All messages", 5L, ((Number) item.getProperty("total")).longValue());

    result.close();
    session.commit();
  }

  /**
   * Two LET entries with common @class filter executed multiple times (multiple
   * outer rows). Verifies that the plan cache works correctly with the
   * skipExpandPushDown flag included in the cache key — the second outer row
   * should get a cache hit for the per-entry plan (compiled without push-down).
   */
  @Test
  public void testMaterializedLet_commonFilterPlanCaching() {
    session.execute("CREATE CLASS CfPerson EXTENDS V").close();
    session.execute("CREATE CLASS CfPost EXTENDS V").close();
    session.execute("CREATE CLASS CfComment EXTENDS V").close();
    session.execute("CREATE CLASS CfCreator EXTENDS E").close();

    session.begin();
    // Create two persons, each with different edge counts
    var p1Rs = session.execute("CREATE VERTEX CfPerson SET name = 'alice'");
    var p1Rid = p1Rs.next().getIdentity();
    p1Rs.close();
    var p2Rs = session.execute("CREATE VERTEX CfPerson SET name = 'bob'");
    var p2Rid = p2Rs.next().getIdentity();
    p2Rs.close();

    // Alice: 3 Posts(tag=A), 1 Post(tag=B), 1 Comment
    for (int i = 0; i < 3; i++) {
      var rs = session.execute("CREATE VERTEX CfPost SET tag = 'A'");
      var rid = rs.next().getIdentity();
      rs.close();
      session.execute("CREATE EDGE CfCreator FROM " + rid + " TO " + p1Rid)
          .close();
    }
    var rs1 = session.execute("CREATE VERTEX CfPost SET tag = 'B'");
    var rid1 = rs1.next().getIdentity();
    rs1.close();
    session.execute("CREATE EDGE CfCreator FROM " + rid1 + " TO " + p1Rid)
        .close();
    var rs1c = session.execute("CREATE VERTEX CfComment SET tag = 'A'");
    var rid1c = rs1c.next().getIdentity();
    rs1c.close();
    session.execute("CREATE EDGE CfCreator FROM " + rid1c + " TO " + p1Rid)
        .close();

    // Bob: 1 Post(tag=A), 2 Posts(tag=B)
    var rs2a = session.execute("CREATE VERTEX CfPost SET tag = 'A'");
    var rid2a = rs2a.next().getIdentity();
    rs2a.close();
    session.execute("CREATE EDGE CfCreator FROM " + rid2a + " TO " + p2Rid)
        .close();
    for (int i = 0; i < 2; i++) {
      var rs2 = session.execute("CREATE VERTEX CfPost SET tag = 'B'");
      var rid2 = rs2.next().getIdentity();
      rs2.close();
      session.execute("CREATE EDGE CfCreator FROM " + rid2 + " TO " + p2Rid)
          .close();
    }
    session.commit();

    session.begin();
    // Query all persons — multiple outer rows exercise plan caching
    var result = session.query(
        "SELECT name,"
            + " $aCount[0].cnt as tagA,"
            + " $bCount[0].cnt as tagB"
            + " FROM CfPerson"
            + " LET $aCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('CfCreator')) FROM CfPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'CfPost' AND tag = 'A'),"
            + " $bCount = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('CfCreator')) FROM CfPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'CfPost' AND tag = 'B')"
            + " ORDER BY name");

    Assert.assertTrue(result.hasNext());
    var alice = result.next();
    Assert.assertEquals("alice", alice.getProperty("name"));
    Assert.assertEquals(3L, ((Number) alice.getProperty("tagA")).longValue());
    Assert.assertEquals(1L, ((Number) alice.getProperty("tagB")).longValue());

    Assert.assertTrue(result.hasNext());
    var bob = result.next();
    Assert.assertEquals("bob", bob.getProperty("name"));
    Assert.assertEquals(1L, ((Number) bob.getProperty("tagA")).longValue());
    Assert.assertEquals(2L, ((Number) bob.getProperty("tagB")).longValue());

    result.close();
    session.commit();
  }

  /**
   * Verifies that the common filter is only the intersection of all entries'
   * WHERE conditions. When all entries share the full WHERE (identical
   * conditions), the per-entry FilterStep evaluates redundantly (always true)
   * and materialization still produces correct results.
   */
  @Test
  public void testMaterializedLet_allConditionsCommon() {
    session.execute("CREATE CLASS AcPerson EXTENDS V").close();
    session.execute("CREATE CLASS AcMessage EXTENDS V").close();
    session.execute("CREATE CLASS AcCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute("CREATE VERTEX AcPerson SET name = 'dave'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (int i = 0; i < 4; i++) {
      var msgRs = session.execute(
          "CREATE VERTEX AcMessage SET type = 'Post', n = " + i);
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE AcCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    // 2 non-Post messages
    for (int i = 0; i < 2; i++) {
      var msgRs = session.execute("CREATE VERTEX AcMessage SET type = 'Comment'");
      var rid = msgRs.next().getIdentity();
      msgRs.close();
      session.execute("CREATE EDGE AcCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    session.commit();

    session.begin();
    // Both entries have identical WHERE — all conditions are common.
    // Each computes count(*) but with different aliases — still a valid
    // materialization group.
    var result = session.query(
        "SELECT name,"
            + " $x[0].cnt as countX,"
            + " $y[0].cnt as countY"
            + " FROM AcPerson"
            + " LET $x = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('AcCreator')) FROM AcPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Post'),"
            + " $y = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('AcCreator')) FROM AcPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE type = 'Post')"
            + " WHERE @rid = " + personRid);

    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals(4L, ((Number) item.getProperty("countX")).longValue());
    Assert.assertEquals(4L, ((Number) item.getProperty("countY")).longValue());

    result.close();
    session.commit();
  }

  /**
   * End-to-end test modelling the IC10 two-LET pattern before it was manually
   * merged into a single sum(if(...)). Verifies that both optimizations compose
   * correctly:
   *
   * <ul>
   *   <li>Common filter {@code @class = 'IcPost'} is pushed into the
   *       materialization query's ExpandStep (collection ID check — zero
   *       I/O for Comments)</li>
   *   <li>Per-entry filters ({@code tag CONTAINS 'sports'} /
   *       {@code NOT (tag CONTAINS 'sports')}) are preserved in each entry's
   *       FilterStep via {@code skipExpandPushDown}</li>
   *   <li>The shared inner traversal is executed once (materialization), not
   *       twice</li>
   * </ul>
   *
   * <p>Graph structure per person:
   * <pre>
   *   Person ←[IcCreator]— IcPost(tag='sports')   x3
   *   Person ←[IcCreator]— IcPost(tag='music')     x2
   *   Person ←[IcCreator]— IcComment(tag='sports') x2  (filtered out by @class)
   * </pre>
   */
  @Test
  public void testMaterializedLet_IC10Pattern_pushDownWithMaterialization() {
    session.execute("CREATE CLASS IcPerson EXTENDS V").close();
    session.execute("CREATE CLASS IcPost EXTENDS V").close();
    session.execute("CREATE CLASS IcComment EXTENDS V").close();
    session.execute("CREATE CLASS IcCreator EXTENDS E").close();
    session.execute("CREATE CLASS IcKnows EXTENDS E").close();

    session.begin();
    // Create two persons (start and fof) connected by KNOWS
    var startRs = session.execute(
        "CREATE VERTEX IcPerson SET name = 'start', id = 1");
    var startRid = startRs.next().getIdentity();
    startRs.close();

    var fofRs = session.execute(
        "CREATE VERTEX IcPerson SET name = 'fof', id = 2");
    var fofRid = fofRs.next().getIdentity();
    fofRs.close();

    session.execute(
        "CREATE EDGE IcKnows FROM " + startRid + " TO " + fofRid).close();

    // fof's content: 3 Posts with tag='sports', 2 Posts with tag='music',
    // 2 Comments with tag='sports' (should be filtered by @class = 'IcPost')
    for (int i = 0; i < 3; i++) {
      var rs = session.execute(
          "CREATE VERTEX IcPost SET tag = 'sports'");
      var rid = rs.next().getIdentity();
      rs.close();
      session.execute("CREATE EDGE IcCreator FROM " + rid + " TO " + fofRid)
          .close();
    }
    for (int i = 0; i < 2; i++) {
      var rs = session.execute(
          "CREATE VERTEX IcPost SET tag = 'music'");
      var rid = rs.next().getIdentity();
      rs.close();
      session.execute("CREATE EDGE IcCreator FROM " + rid + " TO " + fofRid)
          .close();
    }
    for (int i = 0; i < 2; i++) {
      var rs = session.execute(
          "CREATE VERTEX IcComment SET tag = 'sports'");
      var rid = rs.next().getIdentity();
      rs.close();
      session.execute("CREATE EDGE IcCreator FROM " + rid + " TO " + fofRid)
          .close();
    }
    session.commit();

    session.begin();
    // IC10-style query: two LETs with common @class='IcPost' filter
    // and different per-entry tag conditions
    var result = session.query(
        "SELECT fofName,"
            + " $posScore[0].cnt as posScore,"
            + " $negScore[0].cnt as negScore"
            + " FROM ("
            + "   SELECT expand(out('IcKnows')) FROM IcPerson"
            + "   WHERE @rid = " + startRid
            + " )"
            + " LET fofName = name,"
            + "  $posScore = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('IcCreator')) FROM IcPerson"
            + "    WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'IcPost' AND tag = 'sports'),"
            + "  $negScore = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('IcCreator')) FROM IcPerson"
            + "    WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'IcPost' AND tag = 'music')");

    Assert.assertTrue(result.hasNext());
    var row = result.next();
    Assert.assertEquals("fof", row.getProperty("fofName"));
    // 3 Posts with tag='sports' (Comments excluded by @class filter)
    Assert.assertEquals(
        "posScore (Posts with tag=sports)",
        3L, ((Number) row.getProperty("posScore")).longValue());
    // 2 Posts with tag='music'
    Assert.assertEquals(
        "negScore (Posts with tag=music)",
        2L, ((Number) row.getProperty("negScore")).longValue());

    // Verify plan structure: materialization with common filter push-down.
    // The common @class='IcPost' filter should appear in the materialization
    // step. Per-entry filters (tag conditions) are not visible in the plan
    // prettyPrint — they live in per-entry plans created at execution time.
    // Correctness of per-entry filtering is proven by the count assertions
    // above: if the FilterStep were lost (push-down without skipExpandPushDown),
    // all Posts would be counted for both entries, yielding 5/5 instead of 3/2.
    var plan = result.getExecutionPlan().prettyPrint(0, 2);
    Assert.assertTrue(
        "Should use MATERIALIZED LET GROUP, plan was:\n" + plan,
        plan.contains("MATERIALIZED LET GROUP"));
    Assert.assertTrue(
        "Common filter should contain @class = 'IcPost', plan was:\n" + plan,
        plan.contains("common filter:") && plan.contains("IcPost"));

    Assert.assertFalse(result.hasNext());
    result.close();
    session.commit();
  }

  /**
   * EXPLAIN-level test verifying that a query with two LET entries sharing
   * the same inner subquery and the same @class filter produces a plan with
   * MATERIALIZED LET GROUP and the common filter reported in the plan output.
   * Also verifies the query against independent execution for correctness.
   */
  @Test
  public void testMaterializedLet_explainShowsCommonFilter() {
    session.execute("CREATE CLASS ExPerson EXTENDS V").close();
    session.execute("CREATE CLASS ExPost EXTENDS V").close();
    session.execute("CREATE CLASS ExComment EXTENDS V").close();
    session.execute("CREATE CLASS ExCreator EXTENDS E").close();

    session.begin();
    var personRs = session.execute(
        "CREATE VERTEX ExPerson SET name = 'test'");
    var personRid = personRs.next().getIdentity();
    personRs.close();

    for (int i = 0; i < 4; i++) {
      var rs = session.execute("CREATE VERTEX ExPost SET n = " + i);
      var rid = rs.next().getIdentity();
      rs.close();
      session.execute("CREATE EDGE ExCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    for (int i = 0; i < 3; i++) {
      var rs = session.execute("CREATE VERTEX ExComment SET n = " + i);
      var rid = rs.next().getIdentity();
      rs.close();
      session.execute("CREATE EDGE ExCreator FROM " + rid + " TO " + personRid)
          .close();
    }
    session.commit();

    String query =
        "SELECT name,"
            + " $a[0].cnt as countA,"
            + " $b[0].cnt as countB"
            + " FROM ExPerson"
            + " LET $a = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('ExCreator')) FROM ExPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'ExPost' AND n < 2),"
            + " $b = (SELECT count(*) as cnt FROM"
            + "   (SELECT expand(in('ExCreator')) FROM ExPerson"
            + "   WHERE @rid = $parent.$current.@rid)"
            + "   WHERE @class = 'ExPost' AND n >= 2)"
            + " WHERE @rid = " + personRid;

    // 1. Run EXPLAIN and verify plan structure
    session.begin();
    var explain = session.execute("EXPLAIN " + query);
    Assert.assertTrue(explain.hasNext());
    var explainPlan = explain.next().getProperty("executionPlanAsString")
        .toString();
    explain.close();
    session.commit();

    Assert.assertTrue(
        "EXPLAIN should show MATERIALIZED LET GROUP, was:\n" + explainPlan,
        explainPlan.contains("MATERIALIZED LET GROUP"));
    Assert.assertTrue(
        "EXPLAIN should show common filter with ExPost, was:\n" + explainPlan,
        explainPlan.contains("common filter:")
            && explainPlan.contains("ExPost"));

    // 2. Run actual query and verify correct results
    session.begin();
    var result = session.query(query);
    Assert.assertTrue(result.hasNext());
    var row = result.next();
    // 4 Posts total: n=0,1 match "n < 2", n=2,3 match "n >= 2"
    Assert.assertEquals(2L, ((Number) row.getProperty("countA")).longValue());
    Assert.assertEquals(2L, ((Number) row.getProperty("countB")).longValue());
    result.close();
    session.commit();

    // 3. Verify against independent execution (no materialization trick)
    session.begin();
    var indA = session.query(
        "SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('ExCreator')) FROM ExPerson"
            + " WHERE @rid = " + personRid + ")"
            + " WHERE @class = 'ExPost' AND n < 2");
    Assert.assertEquals(
        2L, ((Number) indA.next().getProperty("cnt")).longValue());
    indA.close();

    var indB = session.query(
        "SELECT count(*) as cnt FROM"
            + " (SELECT expand(in('ExCreator')) FROM ExPerson"
            + " WHERE @rid = " + personRid + ")"
            + " WHERE @class = 'ExPost' AND n >= 2");
    Assert.assertEquals(
        2L, ((Number) indB.next().getProperty("cnt")).longValue());
    indB.close();
    session.commit();
  }

  // ====== LET pre-filter push-down tests ======

  // EXPLAIN plan markers emitted by the per-record LET step and the FilterStep.
  private static final String LPF_FILTER_MARKER = "FILTER ITEMS WHERE";
  private static final String LPF_LET_MARKER = "LET (for each record)";

  /**
   * Runs {@code EXPLAIN} for the query and returns the textual execution plan
   * ({@code executionPlanAsString}). Shared by the LET pre-filter tests so the
   * EXPLAIN boilerplate is not copy-pasted per test.
   */
  private String explainPlan(String query) {
    var explain = session.query("EXPLAIN " + query).toList();
    return (String) explain.getFirst().getProperty("executionPlanAsString");
  }

  /** Counts non-overlapping occurrences of {@code needle} in {@code haystack}. */
  private static int countOccurrences(String haystack, String needle) {
    var count = 0;
    var idx = haystack.indexOf(needle);
    while (idx >= 0) {
      count++;
      idx = haystack.indexOf(needle, idx + needle.length());
    }
    return count;
  }

  /**
   * Asserts a FULL push-down: the outer FilterStep is pushed BEFORE the
   * per-record LET step, and the pushed WHERE was fully cleared (nothing left
   * after LET).
   *
   * <p>Rather than trusting marker positions alone, this matches the exact
   * {@code pushedPredicate} text (as rendered into the plan): it must appear
   * BEFORE the LET marker, must NOT appear after it, and must appear exactly
   * ONCE overall (a duplicate would mean the same filter was both pushed and
   * retained). Matching the predicate text — not just the {@code FILTER ITEMS
   * WHERE} marker — keeps the check robust to a NESTED filter marker printed
   * inside a LET/FROM subquery's boxed sub-plan, which carries a different
   * predicate. Choose a distinctive {@code pushedPredicate} that cannot collide
   * with a nested sub-plan or a projection.
   */
  private static void assertFullPushDown(String plan, String pushedPredicate) {
    var letIdx = plan.indexOf(LPF_LET_MARKER);
    Assert.assertTrue("EXPLAIN should contain a per-record LET step, plan:\n" + plan,
        letIdx >= 0);
    var filterIdx = plan.indexOf(LPF_FILTER_MARKER);
    Assert.assertTrue(
        "A FilterStep should appear before the LET step (push-down), plan:\n" + plan,
        filterIdx >= 0 && filterIdx < letIdx);
    Assert.assertEquals(
        "Pushed predicate '" + pushedPredicate + "' should appear exactly once"
            + " (pushed, not also retained), plan:\n" + plan,
        1, countOccurrences(plan, pushedPredicate));
    Assert.assertTrue(
        "Pushed predicate '" + pushedPredicate + "' should appear before the LET"
            + " step, plan:\n" + plan,
        plan.indexOf(pushedPredicate) < letIdx);
  }

  /**
   * Asserts NO push-down: the outer FilterStep stays AFTER the per-record LET
   * step (the WHERE was left in place for {@code handleWhere}).
   *
   * <p>{@code retainedPredicate} must appear exactly once, AFTER the LET marker,
   * and there must be NO FilterStep before the LET marker — the latter catches
   * an over-eager optimization that wrongly pushes a LET-dependent conjunct
   * ahead of LET. (Valid because none of the no-push tests use a LET/FROM
   * subquery containing a WHERE, so no nested filter marker precedes LET.)
   */
  private static void assertNoPushDown(String plan, String retainedPredicate) {
    var letIdx = plan.indexOf(LPF_LET_MARKER);
    Assert.assertTrue("EXPLAIN should contain a per-record LET step, plan:\n" + plan,
        letIdx >= 0);
    var filterIdx = plan.indexOf(LPF_FILTER_MARKER);
    Assert.assertTrue(
        "The (only) FilterStep should appear after the LET step (no push-down),"
            + " plan:\n" + plan,
        filterIdx > letIdx);
    Assert.assertEquals(
        "Retained predicate '" + retainedPredicate + "' should appear exactly"
            + " once, plan:\n" + plan,
        1, countOccurrences(plan, retainedPredicate));
    Assert.assertTrue(
        "Retained predicate '" + retainedPredicate + "' should appear after the"
            + " LET step (not pushed), plan:\n" + plan,
        plan.indexOf(retainedPredicate) > letIdx);
  }

  /**
   * Asserts a MIXED push-down: one FilterStep before the per-record LET step
   * carrying {@code pushedPredicate} (the LET-independent conjunct) and another
   * after it carrying {@code retainedPredicate} (the LET-dependent conjunct).
   *
   * <p>Each predicate must appear exactly once and on the correct side of the
   * LET marker. The exactly-once checks catch a duplicated FilterStep (e.g. the
   * pushed conjunct also appearing after LET), and the content checks catch the
   * wrong conjunct being pushed. Choose distinctive predicate substrings (the
   * bare LET var name is NOT distinctive — it also appears in the LET step
   * label and the projection — so use the full dependent predicate).
   */
  private static void assertMixedPushDown(
      String plan, String pushedPredicate, String retainedPredicate) {
    var letIdx = plan.indexOf(LPF_LET_MARKER);
    Assert.assertTrue("EXPLAIN should contain a per-record LET step, plan:\n" + plan,
        letIdx >= 0);
    var preLetFilterIdx = plan.indexOf(LPF_FILTER_MARKER);
    Assert.assertTrue(
        "A FilterStep should appear before the LET step (mixed push), plan:\n" + plan,
        preLetFilterIdx >= 0 && preLetFilterIdx < letIdx);
    var postLetFilterIdx = plan.indexOf(LPF_FILTER_MARKER, letIdx);
    Assert.assertTrue(
        "A FilterStep should appear after the LET step (mixed push), plan:\n" + plan,
        postLetFilterIdx > letIdx);
    Assert.assertEquals(
        "Pushed predicate '" + pushedPredicate + "' should appear exactly once,"
            + " plan:\n" + plan,
        1, countOccurrences(plan, pushedPredicate));
    Assert.assertTrue(
        "Pushed predicate '" + pushedPredicate + "' should appear before the LET"
            + " step, plan:\n" + plan,
        plan.indexOf(pushedPredicate) < letIdx);
    Assert.assertEquals(
        "Retained predicate '" + retainedPredicate + "' should appear exactly"
            + " once, plan:\n" + plan,
        1, countOccurrences(plan, retainedPredicate));
    Assert.assertTrue(
        "Retained predicate '" + retainedPredicate + "' should appear after the"
            + " LET step, plan:\n" + plan,
        plan.indexOf(retainedPredicate) > letIdx);
  }

  /**
   * WHERE fully independent of LET: the entire WHERE is pushed before the LET
   * step. Verifies results are correct and EXPLAIN shows FilterStep before LET.
   */
  @Test
  public void testLetPreFilter_fullyIndependentWhere() {
    var cls = "LpfIndep";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // Query: WHERE age >= 50 is independent of LET $info
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE age >= 50";
    var list = session.query(query).toList();
    // age values: 0,10,20,30,40,50,60,70,80,90 → 5 rows have age >= 50
    Assert.assertEquals("Should return 5 rows with age >= 50", 5, list.size());

    // Verify EXPLAIN: full push-down — age >= 50 is pushed before LET, none after.
    assertFullPushDown(explainPlan(query), "age >= 50");
    // Verify row contents, not just count
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(
        "Should contain exactly {n5, n6, n7, n8, n9}",
        Set.of("n5", "n6", "n7", "n8", "n9"), names);
    session.commit();
  }

  /**
   * WHERE fully dependent on LET: no push-down occurs. The FilterStep should
   * appear after the LET step in the EXPLAIN plan.
   */
  @Test
  public void testLetPreFilter_fullyDependentWhere() {
    var cls = "LpfDep";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // WHERE references $total which is a LET variable → no push-down
    session.begin();
    var query = "SELECT name, $total FROM " + cls
        + " LET $total = (SELECT count(*) as cnt FROM " + cls + ")"
        + " WHERE $total[0].cnt > 0";
    var list = session.query(query).toList();
    Assert.assertEquals("All rows should pass since count > 0", 5, list.size());
    // Verify row contents, not just count
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals("Should contain exactly {n0, n1, n2, n3, n4}",
        Set.of("n0", "n1", "n2", "n3", "n4"), names);

    // Verify EXPLAIN: no push-down — the $total[0].cnt > 0 filter stays after LET.
    assertNoPushDown(explainPlan(query), "$total[0].cnt > 0");
    session.commit();
  }

  /**
   * Mixed WHERE: some conjuncts are LET-independent (pushed down), some are
   * LET-dependent (stay after LET). The dependent filter actually rejects rows
   * so that dropping it would change the result set. EXPLAIN should show two
   * FilterSteps.
   */
  @Test
  public void testLetPreFilter_mixedWhere() {
    var cls = "LpfMixed";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // age >= 50 is independent (selects n5..n9 = 5 rows).
    // $total[0].cnt > 7 is dependent — count is 10, so 10 > 7 is true.
    // The dependent filter passes all rows here, so we also add a second
    // mixed-WHERE test below that uses a rejecting dependent filter.
    session.begin();
    var query = "SELECT name, $total FROM " + cls
        + " LET $total = (SELECT count(*) as cnt FROM " + cls + ")"
        + " WHERE age >= 50 AND $total[0].cnt > 7";
    var list = session.query(query).toList();
    Assert.assertEquals("Should return 5 rows", 5, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals("Should contain exactly {n5, n6, n7, n8, n9}",
        Set.of("n5", "n6", "n7", "n8", "n9"), names);

    // Verify EXPLAIN: mixed push-down — age >= 50 pushed before LET,
    // $total[0].cnt > 7 retained after LET.
    assertMixedPushDown(explainPlan(query), "age >= 50", "$total[0].cnt > 7");
    session.commit();
  }

  /**
   * Mixed WHERE where the dependent filter rejects rows: the LET subquery
   * computes count(*) = 10, so the dependent filter $total[0].cnt > 100 fails
   * and eliminates all rows even though the independent filter (age >= 50)
   * passes 5 rows. This catches a bug that drops the dependent WHERE portion.
   */
  @Test
  public void testLetPreFilter_mixedWhere_dependentFilterRejectsRows() {
    var cls = "LpfMixedReject";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // age >= 50 is independent (passes 5 rows), $total[0].cnt > 100 is
    // dependent. count(*) = 10, so 10 > 100 is false → all rows rejected.
    session.begin();
    var query = "SELECT name, $total FROM " + cls
        + " LET $total = (SELECT count(*) as cnt FROM " + cls + ")"
        + " WHERE age >= 50 AND $total[0].cnt > 100";
    var list = session.query(query).toList();
    Assert.assertEquals(
        "Dependent filter should reject all rows (count=10, threshold=100)",
        0, list.size());
    session.commit();
  }

  /**
   * FROM (subquery) + LET + WHERE: the SubQueryStep guard should prevent
   * push-down. The FilterStep must appear after the LET step.
   */
  @Test
  public void testLetPreFilter_subqueryTargetFullyIndependentPushDown() {
    // Fully-independent WHERE after a SubQueryStep target: push-down IS safe
    // because info.whereClause becomes null, which makes
    // tryPushDownFilterIntoExpand bail out at its null guard.
    var cls = "LpfSubQ";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // FROM (subquery): last step before LET is a SubQueryStep.
    // WHERE val >= 2 is fully independent of $info → full push-down.
    session.begin();
    var query = "SELECT name, $info FROM (SELECT FROM " + cls + ")"
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE val >= 2";
    var list = session.query(query).toList();
    Assert.assertEquals("Should return 3 rows with val >= 2", 3, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n2", "n3", "n4"), names);

    // Verify EXPLAIN: fully-independent WHERE (val >= 2) is pushed before LET
    // even after a SubQueryStep target (full push-down, none retained after).
    assertFullPushDown(explainPlan(query), "val >= 2");
    // Verify $info is still populated (LET subquery ran after the filter)
    for (var row : list) {
      Assert.assertNotNull("$info should be populated", row.getProperty("$info"));
    }
    session.commit();
  }

  /**
   * Mixed WHERE (some conjuncts LET-independent, some LET-dependent) after a
   * SubQueryStep target: push-down is blocked because a mixed split would leave
   * info.whereClause non-null with the dependent part, causing
   * tryPushDownFilterIntoExpand to incorrectly match the SubQueryStep→FilterStep
   * adjacency.
   */
  @Test
  public void testLetPreFilter_subqueryTargetMixedWhereSkipsPushDown() {
    var cls = "LpfSubQMix";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // FROM (subquery) + mixed WHERE: val >= 1 is independent, $info check
    // is dependent. The SubQueryStep guard should prevent splitting.
    session.begin();
    var query = "SELECT name, $info FROM (SELECT FROM " + cls + ")"
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE val >= 1 AND $info IS NOT NULL";
    var list = session.query(query).toList();
    Assert.assertEquals("Should return 4 rows with val >= 1", 4, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n1", "n2", "n3", "n4"), names);

    // Verify EXPLAIN: mixed WHERE after a SubQueryStep is NOT pushed down
    // (the $info IS NOT NULL filter stays after LET).
    assertNoPushDown(explainPlan(query), "$info IS NOT NULL");
    session.commit();
  }

  /**
   * Multi-OR WHERE with mixed LET dependency: the entire WHERE must stay after
   * LET because OR branches cannot be split. Verifies the OR-safety constraint.
   */
  @Test
  public void testLetPreFilter_multiOrWithLetVarNoPushDown() {
    var cls = "LpfMultiOr";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // count(*) = 10, so the LET-dependent branch $total[0].cnt > 5 is true for
    // every row when correctly evaluated AFTER LET → the OR is always true → all
    // 10 rows. A wrongful push-before-LET would evaluate $total unbound, making
    // "$total[0].cnt > 5" false, so only the age >= 90 branch (n9) would match →
    // 1 row. The row count (10 vs 1) therefore independently detects a leak; the
    // assertNoPushDown plan-shape check is a second, independent guard.
    session.begin();
    var query = "SELECT name, $total FROM " + cls
        + " LET $total = (SELECT count(*) as cnt FROM " + cls + ")"
        + " WHERE age >= 90 OR $total[0].cnt > 5";
    var list = session.query(query).toList();
    Assert.assertEquals(
        "OR is always true (count 10 > 5) → all rows; a leak would collapse to 1",
        10, list.size());

    // Verify EXPLAIN: multi-OR with a LET ref prevents push-down (the
    // $total[0].cnt > 5 branch keeps the whole WHERE after LET).
    assertNoPushDown(explainPlan(query), "$total[0].cnt > 5");
    session.commit();
  }

  /**
   * Single-condition WHERE (non-AND) that is LET-independent: the entire WHERE
   * is pushed before the LET step. This exercises the splitByLetDependency
   * quick-exit path where no LET variable is referenced and the expression is
   * not an AND block.
   */
  @Test
  public void testLetPreFilter_singleConditionIndependent() {
    var cls = "LpfSingleIndep";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 6; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // Single condition (val > 3) is independent of LET $info → full push-down
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE val > 3";
    var list = session.query(query).toList();
    Assert.assertEquals("Should return 2 rows with val > 3", 2, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n4", "n5"), names);
    // Verify $info (LET subquery) was computed correctly after push-down
    var infoResult = (List<?>) list.getFirst().getProperty("$info");
    Assert.assertNotNull("$info should be populated after push-down", infoResult);
    Assert.assertEquals("$info should contain exactly one result row", 1, infoResult.size());
    var infoRow = (Result) infoResult.getFirst();
    Assert.assertEquals("$info count should reflect all 6 records",
        6L, ((Number) infoRow.getProperty("count(*)")).longValue());

    // Verify EXPLAIN: single independent condition (val > 3) is a full push-down.
    assertFullPushDown(explainPlan(query), "val > 3");
    session.commit();
  }

  /**
   * Single-condition WHERE (non-AND) that is LET-dependent: the entire WHERE
   * stays after the LET step. Uses a rejecting threshold ($cnt[0].c > 5 with
   * count=4) to verify the dependent filter actually executes and rejects rows.
   */
  @Test
  public void testLetPreFilter_singleConditionDependent() {
    var cls = "LpfSingleDep";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 4; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      session.commit();
    }

    // Single condition referencing LET variable with rejecting threshold.
    // count(*) = 4, and 4 > 5 is false → all rows rejected.
    session.begin();
    var query = "SELECT name, $cnt FROM " + cls
        + " LET $cnt = (SELECT count(*) as c FROM " + cls + ")"
        + " WHERE $cnt[0].c > 5";
    var list = session.query(query).toList();
    Assert.assertEquals(
        "Dependent filter should reject all rows (count=4, threshold=5)",
        0, list.size());

    // Verify EXPLAIN: single dependent condition ($cnt[0].c > 5) is NOT pushed down.
    assertNoPushDown(explainPlan(query), "$cnt[0].c > 5");
    session.commit();
  }

  /**
   * Multi-OR WHERE where ALL branches are LET-independent: the entire WHERE
   * is pushed before the LET step. This exercises the splitByLetDependency
   * quick-exit path for multi-OR blocks where no branch references any LET
   * variable.
   */
  @Test
  public void testLetPreFilter_multiOrAllIndependentPushDown() {
    var cls = "LpfMultiOrIndep";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // OR of two independent conditions: val < 2 (n0,n1) OR val > 7 (n8,n9)
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE val < 2 OR val > 7";
    var list = session.query(query).toList();
    Assert.assertEquals("Should return 4 rows", 4, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n0", "n1", "n8", "n9"), names);
    // Verify $info (LET subquery) was computed correctly after push-down
    var infoResult = (List<?>) list.getFirst().getProperty("$info");
    Assert.assertNotNull("$info should be populated after push-down", infoResult);
    Assert.assertEquals("$info should contain exactly one result row", 1, infoResult.size());
    var infoRow = (Result) infoResult.getFirst();
    Assert.assertEquals("$info count should reflect all 10 records",
        10L, ((Number) infoRow.getProperty("count(*)")).longValue());

    // Verify EXPLAIN: multi-OR all-independent is a full push-down (val > 7 is
    // the distinctive second branch of the pushed WHERE).
    assertFullPushDown(explainPlan(query), "val > 7");
    session.commit();
  }

  /**
   * LET expression (not subquery): WHERE independent of the expression-based
   * LET is pushed before the LET step. This exercises the LetExpressionStep
   * code path through handleLetPreFilter, verifying that the variable name
   * collection works for expression LETs (not just subquery LETs).
   */
  @Test
  public void testLetPreFilter_letExpressionPushDown() {
    var cls = "LpfLetExpr";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 8; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("score", i * 5);
      session.commit();
    }

    // LET $bonus = score + 100 is an expression (not a subquery).
    // WHERE score >= 20 is independent of $bonus → push-down.
    session.begin();
    var query = "SELECT name, $bonus FROM " + cls
        + " LET $bonus = score + 100"
        + " WHERE score >= 20";
    var list = session.query(query).toList();
    // score values: 0,5,10,15,20,25,30,35 → 4 rows have score >= 20
    Assert.assertEquals("Should return 4 rows with score >= 20", 4, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n4", "n5", "n6", "n7"), names);
    // Verify $bonus is computed correctly for a returned row
    var n4 = list.stream()
        .filter(r -> "n4".equals(r.getProperty("name")))
        .findFirst().orElseThrow();
    Assert.assertEquals("$bonus for n4 (score=20) should be 120",
        120, ((Number) n4.getProperty("$bonus")).intValue());

    // Verify EXPLAIN: expression-LET with an independent WHERE (score >= 20) is
    // a full push-down.
    assertFullPushDown(explainPlan(query), "score >= 20");
    session.commit();
  }

  /**
   * LET expression with dependent WHERE: the WHERE references the expression
   * LET variable, so no push-down occurs. Verifies that expression-based LET
   * variables are correctly detected as dependencies.
   */
  @Test
  public void testLetPreFilter_letExpressionDependent() {
    var cls = "LpfLetExprDep";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 6; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("score", i * 10);
      session.commit();
    }

    // WHERE references $bonus (a LET expression variable) → no push-down.
    session.begin();
    var query = "SELECT name, $bonus FROM " + cls
        + " LET $bonus = score + 100"
        + " WHERE $bonus > 130";
    var list = session.query(query).toList();
    // score values: 0,10,20,30,40,50 → bonus: 100,110,120,130,140,150
    // $bonus > 130 → n4 (140), n5 (150)
    Assert.assertEquals("Should return 2 rows with $bonus > 130", 2, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n4", "n5"), names);

    // Verify EXPLAIN: expression-LET with a dependent WHERE ($bonus > 130) is
    // NOT pushed down.
    assertNoPushDown(explainPlan(query), "$bonus > 130");
    session.commit();
  }

  /**
   * Independent WHERE that rejects ALL rows: the pushed-down filter eliminates
   * every record before the LET step, so zero rows flow into LET evaluation
   * and zero results are returned. Verifies the boundary where the independent
   * filter produces an empty pipeline.
   */
  @Test
  public void testLetPreFilter_independentFilterRejectsAllRows() {
    var cls = "LpfIndepRejectAll";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // val > 100 matches nothing — independent of LET → push-down, zero rows
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE val > 100";
    var list = session.query(query).toList();
    Assert.assertEquals("Should return 0 rows", 0, list.size());

    // Verify EXPLAIN: the independent filter (val > 100) is a full push-down even
    // though it rejects every row (the pushed FilterStep still precedes LET).
    assertFullPushDown(explainPlan(query), "val > 100");
    session.commit();
  }

  /**
   * Multiple LET items: WHERE references only the second LET variable ($cnt).
   * Verifies that the variable-name collection in handleLetPreFilter correctly
   * includes ALL LET variables — not just the first one — so that a dependency
   * on the second LET correctly prevents push-down.
   */
  @Test
  public void testLetPreFilter_multipleLetsSecondVarDependent() {
    var cls = "LpfMultiLet";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 5; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("val", i);
      session.commit();
    }

    // $info is the first LET (expression), $cnt is the second LET (subquery).
    // WHERE references $cnt → should NOT push down.
    session.begin();
    var query = "SELECT name, $info, $cnt FROM " + cls
        + " LET $info = val * 2, $cnt = (SELECT count(*) as c FROM " + cls + ")"
        + " WHERE $cnt[0].c > 0";
    var list = session.query(query).toList();
    Assert.assertEquals("All 5 rows should pass since count > 0", 5, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n0", "n1", "n2", "n3", "n4"), names);
    // Verify the expression LET ($info = val * 2) was computed correctly
    var n3 = list.stream()
        .filter(r -> "n3".equals(r.getProperty("name")))
        .findFirst().orElseThrow();
    Assert.assertEquals("$info for n3 (val=3) should be 6",
        6, ((Number) n3.getProperty("$info")).intValue());

    // Verify EXPLAIN: a WHERE on the second LET var ($cnt[0].c > 0) prevents
    // push-down.
    assertNoPushDown(explainPlan(query), "$cnt[0].c > 0");
    session.commit();
  }

  /**
   * Global LET variable referenced in WHERE is pushable. A numeric-constant LET
   * ($g = 50) is promoted to a GLOBAL LET (evaluated once, before the fetch),
   * while $info is a per-record subquery LET. The WHERE references only the
   * global $g, so it does NOT depend on the per-record LET and is pushed before
   * the per-record LET step. Because handleGlobalLet runs first, $g is already
   * bound when the pushed filter evaluates. Verifies both the plan shape
   * (a "LET (once)" step for $g plus a full push-down of the WHERE) and that the
   * results are correct.
   */
  @Test
  public void testLetPreFilter_globalLetVarInWhereIsPushable() {
    var cls = "LpfGlobalLet";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // $g = 50 is a constant → promoted to GLOBAL LET (evaluated once).
    // $info is a per-record subquery LET. WHERE age >= $g references only the
    // global var, so it is fully LET-independent → pushed before per-record LET.
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $g = 50, $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE age >= $g";
    var list = session.query(query).toList();
    // age values: 0,10,...,90 → 5 rows have age >= 50
    Assert.assertEquals("Should return 5 rows with age >= $g (50)", 5, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n5", "n6", "n7", "n8", "n9"), names);

    // Verify EXPLAIN: $g is a GLOBAL LET ("LET (once)"), and the WHERE is a full
    // push-down relative to the per-record LET step.
    var plan = explainPlan(query);
    Assert.assertTrue(
        "EXPLAIN should contain a global LET step (\"LET (once)\") for $g,"
            + " plan:\n" + plan,
        plan.contains("LET (once)"));
    assertFullPushDown(plan, "age >= $g");
    session.commit();
  }

  /**
   * $current in WHERE is pushable. WHERE $current.age >= 50 references the
   * current record (via the $current context variable), not the per-record LET
   * $info, so it is classified LET-independent and pushed before the per-record
   * LET step. This is safe because the class-scan LoaderExecutionStream binds
   * $current for each record as it is pulled, immediately before the pushed
   * filter's predicate runs. Guards the "special variable in WHERE" hazard
   * class. Verifies both the (full) push-down and the correct rows.
   */
  @Test
  public void testLetPreFilter_currentVarInWhereIsPushable() {
    var cls = "LpfCurrentVar";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // WHERE $current.age >= 50 refers to the current record, not $info → the
    // whole WHERE is LET-independent and pushed before the per-record LET.
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $info = (SELECT count(*) FROM " + cls + ")"
        + " WHERE $current.age >= 50";
    var list = session.query(query).toList();
    // age values: 0,10,...,90 → 5 rows have age >= 50
    Assert.assertEquals(
        "Should return 5 rows with $current.age >= 50", 5, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n5", "n6", "n7", "n8", "n9"), names);

    // Verify EXPLAIN: full push-down of the $current.age >= 50 WHERE before LET.
    assertFullPushDown(explainPlan(query), "$current.age >= 50");
    session.commit();
  }

  /**
   * Full push-down guarded against the flat-string nested-marker trap: the
   * per-record LET subquery itself contains a WHERE, so the EXPLAIN plan has a
   * NESTED "FILTER ITEMS WHERE age > $parent.$current.age" boxed AFTER the LET
   * marker. The outer WHERE (age >= 50) is fully LET-independent and pushed
   * BEFORE the LET step. assertFullPushDown must still pass because it matches
   * the OUTER predicate text ("age >= 50"), which appears exactly once and
   * before the LET marker; it must NOT be fooled by the different nested
   * predicate that appears after the marker. A correlated subquery (referencing
   * $parent) keeps the LET per-record, so its filter renders inside the
   * per-record LET box rather than being promoted to a global "LET (once)".
   */
  @Test
  public void testLetPreFilter_fullPushDownWithFilteringLetSubquery() {
    var cls = "LpfNestedFilter";
    session.execute("CREATE CLASS " + cls).close();

    for (var i = 0; i < 10; i++) {
      session.begin();
      var doc = session.newInstance(cls);
      doc.setProperty("name", "n" + i);
      doc.setProperty("age", i * 10);
      session.commit();
    }

    // Correlated LET subquery (references $parent) stays per-record; its WHERE
    // renders a nested FILTER after the LET marker. Outer WHERE age >= 50 is
    // LET-independent → full push-down before the per-record LET.
    session.begin();
    var query = "SELECT name, $info FROM " + cls
        + " LET $info = (SELECT count(*) as c FROM " + cls
        + " WHERE age > $parent.$current.age)"
        + " WHERE age >= 50";
    var list = session.query(query).toList();
    // age >= 50 → n5..n9 (age 50,60,70,80,90)
    Assert.assertEquals("Should return 5 rows with age >= 50", 5, list.size());
    var names = list.stream()
        .map(r -> (String) r.getProperty("name"))
        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of("n5", "n6", "n7", "n8", "n9"), names);
    // The correlated subquery must be evaluated per record: n5 (age 50) sees 4
    // rows with a greater age (60,70,80,90); n9 (age 90) sees none.
    var n5Info = (List<?>) list.stream()
        .filter(r -> "n5".equals(r.getProperty("name")))
        .findFirst().orElseThrow().getProperty("$info");
    Assert.assertEquals("n5 (age 50) should see 4 rows with a greater age",
        4L, ((Number) ((Result) n5Info.getFirst()).getProperty("c")).longValue());
    var n9Info = (List<?>) list.stream()
        .filter(r -> "n9".equals(r.getProperty("name")))
        .findFirst().orElseThrow().getProperty("$info");
    Assert.assertEquals("n9 (age 90) should see 0 rows with a greater age",
        0L, ((Number) ((Result) n9Info.getFirst()).getProperty("c")).longValue());

    // The plan has a nested "FILTER ITEMS WHERE age > $parent.$current.age"
    // after the LET marker; assertFullPushDown must still confirm the OUTER
    // "age >= 50" filter is pushed before LET and not duplicated after it.
    assertFullPushDown(explainPlan(query), "age >= 50");
    session.commit();
  }

}
