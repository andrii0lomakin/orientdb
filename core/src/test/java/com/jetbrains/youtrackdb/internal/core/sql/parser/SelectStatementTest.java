package com.jetbrains.youtrackdb.internal.core.sql.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SelectStatementTest extends DbTestBase {

  protected static SimpleNode checkRightSyntax(String query) {
    var result = checkSyntax(query, true);
    var builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
  }

  protected static SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected static SimpleNode checkSyntax(String query, boolean isCorrect) {
    var osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        //        System.out.println(query);
        //        if(result!= null ) {
        //          System.out.println("->");
        //          StringBuilder builer = new StringBuilder();
        //          result.toString(null, builer);
        //          System.out.println(builer.toString());
        //          System.out.println("............");
        //        }
        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testParserSimpleSelect1() {
    var stm = checkRightSyntax("select from Foo");
    assertTrue(stm instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) stm;
    assertNull(select.getProjection());
    assertNotNull(select.getTarget());
    assertNull(select.getWhereClause());
  }

  @Test
  public void testParserSimpleSelect2() {
    var stm = checkRightSyntax("select bar from Foo");
    assertTrue(stm instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) stm;
    assertNotNull(select.getProjection());
    assertNotNull(select.getProjection().getItems());
    assertEquals(1, select.getProjection().getItems().size());
    assertNotNull(select.getTarget());
    assertNull(select.getWhereClause());
  }

  @Test
  public void testComments() {
    checkRightSyntax("select from Foo");

    checkRightSyntax("select /* aaa bbb ccc*/from Foo");
    checkRightSyntax("select /* aaa bbb \nccc*/from Foo");
    checkRightSyntax("select /** aaa bbb ccc**/from Foo");
    checkRightSyntax("select /** aaa bbb ccc*/from Foo");

    checkRightSyntax("/* aaa bbb ccc*/select from Foo");
    checkRightSyntax("select from Foo/* aaa bbb ccc*/");
    checkRightSyntax("/* aaa bbb ccc*/select from Foo/* aaa bbb ccc*/");

    checkWrongSyntax("select /** aaa bbb */ccc*/from Foo");

    checkWrongSyntax("select /**  /*aaa bbb */ccc*/from Foo");
    checkWrongSyntax("*/ select from Foo");
  }

  @Test
  public void testSimpleSelect() {
    checkRightSyntax("select from Foo");
    checkRightSyntax("select * from Foo");

    checkWrongSyntax("select from Foo bar");
    checkWrongSyntax("select * from Foo bar");

    checkWrongSyntax("select * Foo");
  }

  @Test
  public void testUnwind() {
    checkRightSyntax("select from Foo unwind foo");
    checkRightSyntax("select from Foo unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 order by foo unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 group by bar order by foo unwind foo, bar");
  }

  @Test
  public void testConditionalAggregationWithSingleCondition() {
    checkRightSyntax(
        "select sum(if(out('IS_LOCATED_IN').name contains 'PL', 1, 0)) as xCount from Person");
  }

  @Test
  public void testConditionalAggregationWithCompoundAndCondition() {
    checkRightSyntax(
        "select sum(if(age > 18 AND city = 'NYC', 1, 0)) as cnt from Person");
  }

  @Test
  public void testConditionalAggregationWithCompoundOrCondition() {
    checkRightSyntax(
        "select count(if(status = 'A' OR status = 'B', 1, 0)) as cnt from Record");
  }

  @Test
  public void testConditionalAggregationMultipleAggregatesInSelect() {
    checkRightSyntax(
        "select sum(if(out('IS_LOCATED_IN').name contains :countryX, 1, 0)) as xCount,"
            + " sum(if(out('IS_LOCATED_IN').name contains :countryY, 1, 0)) as yCount"
            + " from Person");
  }

  @Test
  public void testConditionalAggregationNestedFunctions() {
    checkRightSyntax(
        "select count(if(name is not null, 1, 0)) as cnt from Foo");
  }

  @Test
  public void testConditionalAggregationWithNotCondition() {
    checkRightSyntax(
        "select sum(if(NOT active = true, 1, 0)) as inactiveCnt from Person");
  }

  @Test
  public void testConditionalAggregationBetweenCondition() {
    checkRightSyntax(
        "select sum(if(age BETWEEN 18 AND 65, 1, 0)) as workingAge from Person");
  }

  @Test
  public void testNonAggregateIfWithConditionStillParses() {
    checkRightSyntax(
        "select if(name = 'test', 'yes', 'no') from Foo");
  }

  @Test
  public void testRegularFunctionCallsUnaffected() {
    checkRightSyntax("select sum(salary) from Employee");
    checkRightSyntax("select count(*) from Foo");
    checkRightSyntax("select min(age), max(age) from Person");
    checkRightSyntax("select avg(score) from Student where grade = 'A'");
  }

  @Test
  public void testCaseWhenSimple() {
    checkRightSyntax(
        "select CASE WHEN status = 'A' THEN 'active' ELSE 'inactive' END from Foo");
  }

  @Test
  public void testCaseWhenMultipleBranches() {
    checkRightSyntax(
        "select CASE WHEN score > 90 THEN 'A'"
            + " WHEN score > 80 THEN 'B'"
            + " WHEN score > 70 THEN 'C'"
            + " ELSE 'F' END as grade from Student");
  }

  @Test
  public void testCaseWhenWithoutElse() {
    checkRightSyntax(
        "select CASE WHEN active = true THEN 'yes' END from Person");
  }

  @Test
  public void testCaseWhenInsideAggregateFunction() {
    checkRightSyntax(
        "select sum(CASE WHEN age > 18 THEN 1 ELSE 0 END) as adultCount from Person");
  }

  @Test
  public void testCaseWhenWithCompoundCondition() {
    checkRightSyntax(
        "select CASE WHEN age > 18 AND city = 'NYC' THEN 'match' ELSE 'no' END from Person");
  }

  @Test
  public void testCaseWhenMissingEnd() {
    checkWrongSyntax(
        "select CASE WHEN status = 'A' THEN 'active' from Foo");
  }

  @Test
  public void testSubSelect() {
    checkRightSyntax("select from (select from Foo)");

    checkWrongSyntax("select from select from foo");
  }

  @Test
  public void testSimpleSelectWhere() {
    checkRightSyntax("select from Foo where name = 'foo'");
    checkRightSyntax("select * from Foo where name = 'foo'");

    checkRightSyntax("select from Foo where name = 'foo' and surname = \"bar\"");
    checkRightSyntax("select * from Foo where name = 'foo' and surname = \"bar\"");

    checkWrongSyntax("select * from Foo name = 'foo'");
    checkWrongSyntax("select from Foo bar where name = 'foo'");
    checkWrongSyntax("select * from Foo bar where name = 'foo'");
    checkWrongSyntax("select Foo where name = 'foo'");
    checkWrongSyntax("select * Foo where name = 'foo'");

    // issue #5221
    checkRightSyntax("select from $1");
  }

  @Test
  public void testIn() {
    var result = checkRightSyntax("select count(*) from OFunction where name in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testNotIn() {
    var result =
        checkRightSyntax("select count(*) from OFunction where name not in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof SQLStatement);
    var stm = (SQLStatement) result;
  }

  @Test
  public void testMath1() {
    var result =
        checkRightSyntax("select * from sqlSelectIndexReuseTestClass where prop1 = 1 + 1");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testMath2() {
    var result =
        checkRightSyntax("select * from sqlSelectIndexReuseTestClass where prop1 = foo + 1");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testMath5() {
    var result =
        checkRightSyntax(
            "select * from sqlSelectIndexReuseTestClass where prop1 = foo + 1 * bar - 5");

    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testFetchPlan1() {
    var result = checkRightSyntax("select 'Ay' as a , 'bEE' as b from Foo fetchplan *:1");

    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testFetchPlan2() {
    var result = checkRightSyntax("select 'Ay' as a , 'bEE' as b fetchplan *:1");

    assertTrue(result instanceof SQLSelectWithoutTargetStatement);
    var select = (SQLSelectWithoutTargetStatement) result;
  }

  @Test
  public void testContainsWithCondition() {
    var result =
        checkRightSyntax("select from Profile where customReferences.values() CONTAINS 'a'");

    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testNamedParam() {
    var result =
        checkRightSyntax("select from JavaComplexTestClass where enumField = :enumItem");

    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testBoolean() {
    var result = checkRightSyntax("select from Foo where bar = true");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testDottedAtField() {
    var result = checkRightSyntax("select from City where country.@class = 'Country'");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testQuotedFieldNameFrom() {
    var result =
        checkRightSyntax("select `from` from City where country.@class = 'Country'");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testQuotedTargetName() {
    checkRightSyntax("select from `edge`");
    checkRightSyntax("select from `from`");
    checkRightSyntax("select from `vertex`");
    checkRightSyntax("select from `select`");
  }

  @Test
  public void testQuotedFieldName() {
    checkRightSyntax("select `foo` from City where country.@class = 'Country'");
  }

  @Test
  public void testLongDotted() {
    var result =
        checkRightSyntax("select from Profile where location.city.country.name = 'Spain'");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testInIsNotAReservedWord() {
    var result =
        checkRightSyntax("select count(*) from TRVertex where in.type() not in [\"LINKSET\"] ");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectStatement);
    var select = (SQLSelectStatement) result;
  }

  @Test
  public void testSelectFunction() {
    var result = checkRightSyntax("select max(1,2,7,0,-2,3), 'pluto'");
    // result.dump("    ");
    assertTrue(result instanceof SQLSelectWithoutTargetStatement);
    var select = (SQLSelectWithoutTargetStatement) result;
  }

  @Test
  public void testEscape1() {
    var result =
        checkRightSyntax(
            "select from ouser where \"\\u005C\\u005C\" = \"\\u005C\\u005C\" ");
    assertTrue(result instanceof SQLSelectStatement);
  }

  @Test
  public void testWildcardSuffix() {
    checkRightSyntax("select foo.* from bar ");
  }

  @Test
  public void testEmptyCollection() {
    var query = "select from bar where name not in :param1";
    var osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      var stm = (SQLSelectStatement) result;
      Map<Object, Object> params = new HashMap<Object, Object>();
      params.put("param1", new HashSet<Object>());

      var parsed = new StringBuilder();
      stm.toString(params, parsed);
      assertEquals("SELECT FROM bar WHERE name NOT IN []", parsed.toString());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testEscape2() {
    checkWrongSyntax("select from collection:internal where \"\\u005C\" = \"\\u005C\" ");
  }

  @Test
  public void testSpatial() {

    checkRightSyntax(
        "select *,$distance from Place where [latitude,longitude,$spatial] NEAR"
            + " [41.893056,12.482778,{\"maxDistance\": 0.5}]");
    checkRightSyntax(
        "select * from Place where [latitude,longitude] WITHIN"
            + " [[51.507222,-0.1275],[55.507222,-0.1275]]");
  }

  @Test
  public void testSubConditions() {
    checkRightSyntax(
        "SELECT @rid as rid, localName FROM Person WHERE ( 'milano' IN out('lives').localName OR"
            + " 'roma' IN out('lives').localName ) ORDER BY age ASC");
  }

  @Test
  public void testRecordAttributes() {
    // issue #4430
    checkRightSyntax(
        "SELECT @this, @rid, @rid_id, @rid_pos, @version, @class, @type, @size, @fields, @raw from"
            + " V");
    checkRightSyntax(
        "SELECT @THIS, @RID, @RID_ID, @RID_POS, @VERSION, @CLASS, @TYPE, @SIZE, @FIELDS, @RAW from"
            + " V");
  }

  @Test
  public void testDoubleEquals() {
    // issue #4413
    checkRightSyntax("SELECT from V where name = 'foo'");
    checkRightSyntax("SELECT from V where name == 'foo'");
  }

  @Test
  public void testMatches() {

    checkRightSyntax("select from Person where name matches 'a'");

    checkRightSyntax(
        "select from Person where name matches"
            + " '(?i)(^\\\\Qname1\\\\E$)|(^\\\\Qname2\\\\E$)|(^\\\\Qname3\\\\E$)' and age=30");
  }

  @Test
  // issue #3718
  public void testComplexTarget1() {
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] LET $e = (SELECT FROM $current.prop1)");
    checkRightSyntax(
        "SELECT $e FROM [#1:1,#1:2] let $e = (SELECT FROM (SELECT FROM $parent.$current))");
  }

  @Test
  public void testEval() {
    checkRightSyntax(
        """
              select  sum(weight) , f.name as name from (
                  select weight, if(eval("out.name = 'one'"),out,in) as f  from (
                  select expand(bothE('E')) from V
              )
                  ) group by name
            """);
  }

  @Test
  public void testNewLine() {
    checkRightSyntax("INSERT INTO Country SET name=\"one\\ntwo\" RETURN @rid");
  }

  @Test
  public void testJsonWithUrl() {
    checkRightSyntax("insert into V content { \"url\": \"http://www.google.com\" } ");
  }

  @Test
  public void testGroupBy() {
    // issue #4245
    checkRightSyntax(
        """
            select in.name from (
            select expand(outE()) from V
            )
            group by in.name""");
  }

  @Test
  public void testInputParams() {

    checkRightSyntax("select from foo where name like  '%'+ :param1 + '%'");

    checkRightSyntax("select from foo where name like  'aaa'+ :param1 + 'a'");
  }

  @Test
  @Ignore
  public void testSlashInQuery() {
    checkRightSyntax("insert into test content {\"node_id\": \"MFmqvmht//sYYWB8=\"}");
    checkRightSyntax("insert into test content { \"node_id\": \"MFmqvmht\\/\\/GYsYYWB8=\"}");
  }

  @Test
  public void checkOrderBySyntax() {
    checkRightSyntax("select from test order by something ");
    checkRightSyntax("select from test order by something, somethingElse ");
    checkRightSyntax("select from test order by something asc, somethingElse desc");
    checkRightSyntax("select from test order by something asc, somethingElse ");
    checkRightSyntax("select from test order by something, somethingElse asc");
    checkRightSyntax("select from test order by something asc");
    checkRightSyntax("select from test order by something desc");
    checkRightSyntax("select from test order by (something desc)");
    checkRightSyntax("select from test order by (something asc)");
    checkRightSyntax("select from test order by (something asc),somethingElse");
    checkRightSyntax("select from test order by (something),(somethingElse)");
    checkRightSyntax("select from test order by something,(somethingElse)");
    checkRightSyntax("select from test order by (something asc),(somethingElse desc)");
  }

  @Test
  @Ignore
  public void testMultipleLucene() {
    checkRightSyntax("select from Foo where a lucene 'a'");
    checkWrongSyntax("select from Foo where a lucene 'a' and b lucene 'a'");

    checkWrongSyntax(
        "select union($a, $b) let $a = (select from Foo where a lucene 'a' and b lucene 'b'), $b ="
            + " (select from Foo where b lucene 'b')");
    checkRightSyntax(
        "select union($a, $b) let $a = (select from Foo where a lucene 'a'), $b = (select from Foo"
            + " where b lucene 'b')");
    checkWrongSyntax("select from (select from Foo) where a lucene 'a'");

    checkWrongSyntax(
        "select from Foo where (a=2 and b=3 and (a = 4 and (b=5 and d lucene 'foo')))) or select"
            + " from Foo where (a=2 and b=3 and (a = 4 and (b=5 and d lucene 'foo'))))");

    checkWrongSyntax("select from collection:foo where a lucene 'b'");
    checkWrongSyntax("select from #12:0 where a lucene 'b'");
    checkWrongSyntax("select from [#12:0, #12:1] where a lucene 'b'");
  }

  @Test
  public void testBacktick() {
    checkRightSyntax("select `foo` from foo where `foo` = 'bar'");
    checkRightSyntax("select `SELECT` from foo where `SELECT` = 'bar'");
    checkRightSyntax("select `TRAVERSE` from foo where `TRAVERSE` = 'bar'");
    checkRightSyntax("select `INSERT` from foo where `INSERT` = 'bar'");
    checkRightSyntax("select `CREATE` from foo where `CREATE` = 'bar'");
    checkRightSyntax("select `DELETE` from foo where `DELETE` = 'bar'");
    checkRightSyntax("select `VERTEX` from foo where `VERTEX` = 'bar'");
    checkRightSyntax("select `EDGE` from foo where `EDGE` = 'bar'");
    checkRightSyntax("select `UPDATE` from foo where `UPDATE` = 'bar'");
    checkRightSyntax("select `UPSERT` from foo where `UPSERT` = 'bar'");
    checkRightSyntax("select `FROM` from foo where `FROM` = 'bar'");
    checkRightSyntax("select `TO` from foo where `TO` = 'bar'");
    checkRightSyntax("select `WHERE` from foo where `WHERE` = 'bar'");
    checkRightSyntax("select `WHILE` from foo where `WHILE` = 'bar'");
    checkRightSyntax("select `INTO` from foo where `INTO` = 'bar'");
    checkRightSyntax("select `VALUES` from foo where `VALUES` = 'bar'");
    checkRightSyntax("select `SET` from foo where `SET` = 'bar'");
    checkRightSyntax("select `ADD` from foo where `ADD` = 'bar'");
    checkRightSyntax("select `PUT` from foo where `PUT` = 'bar'");
    checkRightSyntax("select `MERGE` from foo where `MERGE` = 'bar'");
    checkRightSyntax("select `CONTENT` from foo where `CONTENT` = 'bar'");
    checkRightSyntax("select `REMOVE` from foo where `REMOVE` = 'bar'");
    checkRightSyntax("select `INCREMENT` from foo where `INCREMENT` = 'bar'");
    checkRightSyntax("select `AND` from foo where `AND` = 'bar'");
    checkRightSyntax("select `OR` from foo where `OR` = 'bar'");
    checkRightSyntax("select `NULL` from foo where `NULL` = 'bar'");
    checkRightSyntax("select `DEFINED` from foo where `DEFINED` = 'bar'");
    checkRightSyntax("select `ORDER` from foo where `ORDER` = 'bar'");
    checkRightSyntax("select `GROUP` from foo where `GROUP` = 'bar'");
    checkRightSyntax("select `BY` from foo where `BY` = 'bar'");
    checkRightSyntax("select `LIMIT` from foo where `LIMIT` = 'bar'");
    checkRightSyntax("select `SKIP2` from foo where `SKIP2` = 'bar'");
    checkRightSyntax("select `OFFSET` from foo where `OFFSET` = 'bar'");
    checkRightSyntax("select `TIMEOUT` from foo where `TIMEOUT` = 'bar'");
    checkRightSyntax("select `ASC` from foo where `ASC` = 'bar'");
    checkRightSyntax("select `AS` from foo where `AS` = 'bar'");
    checkRightSyntax("select `DESC` from foo where `DESC` = 'bar'");
    checkRightSyntax("select `FETCHPLAN` from foo where `FETCHPLAN` = 'bar'");
    checkRightSyntax("select `RETURN` from foo where `RETURN` = 'bar'");
    checkRightSyntax("select `BEFORE` from foo where `BEFORE` = 'bar'");
    checkRightSyntax("select `AFTER` from foo where `AFTER` = 'bar'");
    checkRightSyntax("select `LOCK` from foo where `LOCK` = 'bar'");
    checkRightSyntax("select `RECORD` from foo where `RECORD` = 'bar'");
    checkRightSyntax("select `WAIT` from foo where `WAIT` = 'bar'");

    checkRightSyntax("select `identifier` from foo where `identifier` = 'bar'");
    checkRightSyntax("select `select` from foo where `select` = 'bar'");
    checkRightSyntax("select `traverse` from foo where `traverse` = 'bar'");
    checkRightSyntax("select `insert` from foo where `insert` = 'bar'");
    checkRightSyntax("select `create` from foo where `create` = 'bar'");
    checkRightSyntax("select `delete` from foo where `delete` = 'bar'");
    checkRightSyntax("select `vertex` from foo where `vertex` = 'bar'");
    checkRightSyntax("select `edge` from foo where `edge` = 'bar'");
    checkRightSyntax("select `update` from foo where `update` = 'bar'");
    checkRightSyntax("select `upsert` from foo where `upsert` = 'bar'");
    checkRightSyntax("select `from` from foo where `from` = 'bar'");
    checkRightSyntax("select `to` from foo where `to` = 'bar'");
    checkRightSyntax("select `where` from foo where `where` = 'bar'");
    checkRightSyntax("select `while` from foo where `while` = 'bar'");
    checkRightSyntax("select `into` from foo where `into` = 'bar'");
    checkRightSyntax("select `values` from foo where `values` = 'bar'");
    checkRightSyntax("select `set` from foo where `set` = 'bar'");
    checkRightSyntax("select `add` from foo where `add` = 'bar'");
    checkRightSyntax("select `put` from foo where `put` = 'bar'");
    checkRightSyntax("select `merge` from foo where `merge` = 'bar'");
    checkRightSyntax("select `content` from foo where `content` = 'bar'");
    checkRightSyntax("select `remove` from foo where `remove` = 'bar'");
    checkRightSyntax("select `increment` from foo where `increment` = 'bar'");
    checkRightSyntax("select `and` from foo where `and` = 'bar'");
    checkRightSyntax("select `or` from foo where `or` = 'bar'");
    checkRightSyntax("select `null` from foo where `null` = 'bar'");
    checkRightSyntax("select `defined` from foo where `defined` = 'bar'");
    checkRightSyntax("select `order` from foo where `order` = 'bar'");
    checkRightSyntax("select `group` from foo where `group` = 'bar'");
    checkRightSyntax("select `by` from foo where `by` = 'bar'");
    checkRightSyntax("select `limit` from foo where `limit` = 'bar'");
    checkRightSyntax("select `skip2` from foo where `skip2` = 'bar'");
    checkRightSyntax("select `offset` from foo where `offset` = 'bar'");
    checkRightSyntax("select `timeout` from foo where `timeout` = 'bar'");
    checkRightSyntax("select `asc` from foo where `asc` = 'bar'");
    checkRightSyntax("select `as` from foo where `as` = 'bar'");
    checkRightSyntax("select `desc` from foo where `desc` = 'bar'");
    checkRightSyntax("select `fetchplan` from foo where `fetchplan` = 'bar'");
    checkRightSyntax("select `return` from foo where `return` = 'bar'");
    checkRightSyntax("select `before` from foo where `before` = 'bar'");
    checkRightSyntax("select `after` from foo where `after` = 'bar'");
    checkRightSyntax("select `lock` from foo where `lock` = 'bar'");
    checkRightSyntax("select `record` from foo where `record` = 'bar'");
    checkRightSyntax("select `wait` from foo where `wait` = 'bar'");
    checkRightSyntax("select `retry` from foo where `retry` = 'bar'");
    checkRightSyntax("select `let` from foo where `let` = 'bar'");
    checkRightSyntax("select `nocache` from foo where `nocache` = 'bar'");
    checkRightSyntax("select `unsafe` from foo where `unsafe` = 'bar'");
    checkRightSyntax("select `parallel` from foo where `parallel` = 'bar'");
    checkRightSyntax("select `strategy` from foo where `strategy` = 'bar'");
    checkRightSyntax("select `depth_first` from foo where `depth_first` = 'bar'");
    checkRightSyntax("select `breadth_first` from foo where `breadth_first` = 'bar'");
    checkRightSyntax("select `lucene` from foo where `lucene` = 'bar'");
    checkRightSyntax("select `near` from foo where `near` = 'bar'");
    checkRightSyntax("select `within` from foo where `within` = 'bar'");
    checkRightSyntax("select `unwind` from foo where `unwind` = 'bar'");
    checkRightSyntax("select `maxdepth` from foo where `maxdepth` = 'bar'");
    checkRightSyntax("select `not` from foo where `not` = 'bar'");
    checkRightSyntax("select `in` from foo where `in` = 'bar'");
    checkRightSyntax("select `like` from foo where `like` = 'bar'");
    checkRightSyntax("select `is` from foo where `is` = 'bar'");
    checkRightSyntax("select `between` from foo where `between` = 'bar'");
    checkRightSyntax("select `contains` from foo where `contains` = 'bar'");
    checkRightSyntax("select `containsall` from foo where `containsall` = 'bar'");
    checkRightSyntax("select `containskey` from foo where `containskey` = 'bar'");
    checkRightSyntax("select `containsvalue` from foo where `containsvalue` = 'bar'");
    checkRightSyntax("select `containstext` from foo where `containstext` = 'bar'");
    checkRightSyntax("select `matches` from foo where `matches` = 'bar'");
    checkRightSyntax("select `key` from foo where `key` = 'bar'");
    checkRightSyntax("select `instanceof` from foo where `instanceof` = 'bar'");
    checkRightSyntax("select `collection` from foo where `collection` = 'bar'");

    checkRightSyntax("select `foo-bar` from foo where `collection` = 'bar'");

    checkWrongSyntax("select `collection from foo where `collection` = 'bar'");
    checkWrongSyntax("select `collection from foo where collection` = 'bar'");
  }

  @Test
  public void testReturn() {
    checkRightSyntax("select from ouser timeout 1 exception");
    checkRightSyntax("select from ouser timeout 1 return");
  }

  @Test
  public void testFlatten() {
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(session);
    var stm =
        (SQLSelectStatement) checkRightSyntax("select from ouser where name = 'foo'");

    var flattended = stm.whereClause.flatten(ctx, session.getClassInternal("OUser"));
    assertTrue(
        ((SQLBinaryCondition) flattended.getFirst().subBlocks.getFirst()).left.isBaseIdentifier());
    assertFalse(
        ((SQLBinaryCondition) flattended.getFirst().subBlocks.getFirst()).right.isBaseIdentifier());
    assertFalse(
        ((SQLBinaryCondition) flattended.getFirst().subBlocks.getFirst())
            .left.isEarlyCalculated(ctx));
    assertTrue(
        ((SQLBinaryCondition) flattended.getFirst().subBlocks.getFirst())
            .right.isEarlyCalculated(ctx));
  }

  @Test
  public void testParamWithMatches() {
    // issue #5229
    checkRightSyntax("select from Person where name matches :param1");
  }

  @Test
  public void testInstanceOfE() {
    // issue #5212
    checkRightSyntax("select from Friend where @class instanceof 'E'");
  }

  @Test
  public void testReservedWordsAsNamedParams() {
    // issue #5493
    checkRightSyntax("select from V limit :limit");
  }

  @Test
  public void testFetchPlanBracketStar() {
    // issue #5689
    checkRightSyntax("SELECT FROM Def fetchplan *:2 [*]in_*:-2");
    checkRightSyntax("SELECT FROM Def fetchplan *:2 [1]in_*:-2");
  }

  @Test
  public void testJsonQuoting() {
    // issue #5911
    checkRightSyntax("SELECT '\\/\\/'");
    checkRightSyntax("SELECT \"\\/\\/\"");
  }

  private static void printTree(String s) {
    var osql = getParserFor(s);
    try {
      SimpleNode n = osql.parse();

    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSkipLimitInQueryWithNoTarget() {
    // issue #5589
    checkRightSyntax(
        """
            SELECT eval('$TotalListsQuery[0].Count') AS TotalLists
               LET $TotalListsQuery = ( SELECT Count(1) AS Count FROM ContactList WHERE\
             Account=#20:1 AND EntityInfo.State=0)
             LIMIT 1""");

    checkRightSyntax(
        """
            SELECT eval('$TotalListsQuery[0].Count') AS TotalLists
               LET $TotalListsQuery = ( SELECT Count(1) AS Count FROM ContactList WHERE\
             Account=#20:1 AND EntityInfo.State=0)
             SKIP 10 LIMIT 1""");
  }

  @Test
  public void testQuotedBacktick() {
    checkRightSyntax("SELECT \"\" as `bla\\`bla` from foo");
  }

  @Test
  public void testParamConcat() {
    // issue #6049
    checkRightSyntax("Select * From ACNodeAuthentication where acNodeID like ? ");
    checkRightSyntax("Select * From ACNodeAuthentication where acNodeID like ? + '%'");
    checkRightSyntax("Select * From ACNodeAuthentication where acNodeID like \"%\" + ? + '%'");
  }

  @Test
  public void testAppendParams() {
    checkRightSyntax("select from User where Account.Name like :name + '%'");
  }

  @Test
  public void testLetMatch() {
    checkRightSyntax(
        "select $a let $a = (MATCH {class:Foo, as:bar, where:(name = 'foo')} return $elements)");
  }

  @Test
  public void testDistinct() {
    checkRightSyntax("select distinct(foo) from V");
    checkRightSyntax("select distinct foo, bar, baz from V");
  }

  @Test
  public void testRange() {
    checkRightSyntax("select foo[1..5] from V");
    checkRightSyntax("select foo[1...5] from V");
    checkRightSyntax("select foo[?..?] from V");
    checkRightSyntax("select foo[?...?] from V");
    checkRightSyntax("select foo[:a..:b] from V");
    checkRightSyntax("select foo[:a...:b] from V");
    checkWrongSyntax("select foo[1....5] from V");
  }

  @Test
  public void testRidString() {
    checkRightSyntax("select \"@rid\" as v from V");
    var stm2 = checkRightSyntax("select {\"@rid\": \"#12:0\"} as v from V");
    System.out.println(stm2);
  }

  @Test
  public void testTranslateLucene() {
    var stm =
        (SQLSelectStatement) checkRightSyntax("select from V where name LUCENE 'foo'");
    stm.whereClause.getBaseExpression().translateLuceneOperator();
    Assert.assertTrue(
        stm.whereClause.toString().contains("search_fields([\"name\"], \"foo\") = true"));
    Assert.assertFalse(stm.whereClause.toString().contains("LUCENE"));
  }

  @Test
  public void testFromAsNamedParam() {
    checkRightSyntax("select from V where fromDate = :from");
  }

  @Test
  public void testNestedProjections() {
    checkRightSyntax("select foo:{*} from V");
    checkRightSyntax("select foo:{name, surname, address:{*}} from V");
    checkRightSyntax("select foo:{!name} from V");
    checkRightSyntax("select foo:{!out_*} from V");
    checkRightSyntax("select foo:{!out_*, !in_*} from V");
    checkRightSyntax("select foo:{*, !out_*, !in_*} from V");
  }

  @Test
  public void testCollectionFilteringByValue() {
    checkRightSyntax("select foo[= 'foo'] from V");
    checkRightSyntax("select foo[like '%foo'] from V");
    checkRightSyntax("select foo[> 2] from V");
    checkRightSyntax("select foo[> 2][< 5] from V");

    checkRightSyntax("select foo[ IN ['a', 'b']] from V");
    checkRightSyntax("select bar[IN (select from foo) ] from V");
    checkRightSyntax("select bar[IN $a ] from V LET $a = (SELECT FROM V)");

    checkRightSyntax("select foo[not IN ['a', 'b']] from V");
    checkRightSyntax("select bar[not IN (select from foo) ] from V");
    checkRightSyntax("select bar[not IN $a ] from V LET $a = (SELECT FROM V)");
  }

  @Test
  public void testContainsAny() {
    checkRightSyntax("select from V WHERE foo containsany ['foo', 'bar']");
    checkRightSyntax("select from V WHERE foo CONTAINSANY ['foo', 'bar']");
    checkWrongSyntax("select from V WHERE foo CONTAINSANY ");
  }

  @Test
  public void testOrderByCollate() {
    checkRightSyntax("select from V order by foo asc collate ci");
    checkRightSyntax("select from V order by foo asc collate ci, bar desc collate ci");
    checkRightSyntax("select from V order by foo collate ci, bar collate ci");
    checkWrongSyntax("select from V order by foo collate ");
    checkWrongSyntax("select from V order by foo asc collate ");
  }

  /**
   * FIRST/LAST/NULLS are ORDER BY keywords ({@code NULLS FIRST|LAST}) but must remain usable as
   * identifiers so {@code first()}/{@code last()} and aliases like {@code AS first} still parse.
   * Regression for the CI failures on YTDB-1198 when those tokens were reserved without being
   * added to {@code Identifier()}.
   */
  @Test
  public void testFirstLastRemainIdentifiersAlongsideNullsOrdering() {
    checkRightSyntax("select first(addresses) from V");
    checkRightSyntax("select last(addresses) from V");
    checkRightSyntax("select first(addresses) as first from V order by first");
    checkRightSyntax("select last(addresses) as last from V order by last desc");
    checkRightSyntax("select from V order by last");
    checkRightSyntax("select from V order by foo nulls first");
    checkRightSyntax("select from V order by foo asc nulls last");
    checkRightSyntax("select from V order by foo desc nulls first, bar nulls last");
    // alias named first, then an explicit nulls clause on that item
    checkRightSyntax("select first(addresses) as first from V order by first nulls last");
    checkRightSyntax("select from V order by `nulls` nulls first");
    checkWrongSyntax("select from V order by foo nulls");
    checkWrongSyntax("select from V order by foo nulls asc");

    var nullsFirst =
        (SQLSelectStatement) checkSyntax("select from V order by foo nulls first", true);
    assertEquals(
        SQLOrderByItem.NULLS_FIRST,
        nullsFirst.getOrderBy().getItems().getFirst().getNullOrdering());

    var aliasThenNulls =
        (SQLSelectStatement)
            checkSyntax("select first(addresses) as first from V order by first nulls last", true);
    assertEquals(
        SQLOrderByItem.NULLS_LAST,
        aliasThenNulls.getOrderBy().getItems().getFirst().getNullOrdering());
    assertEquals("first", aliasThenNulls.getOrderBy().getItems().getFirst().getAlias());
  }

  protected static void checkGenericStatement(String query, String expectedGeneric) {
    var result = checkSyntax(query, true);
    Assert.assertEquals(expectedGeneric, result.toGenericStatement());
  }

  @Test
  public void testGenericStatementGeneration() {
    checkGenericStatement(
        "select from Clazz WHERE key = ['foo', 'bar']", "SELECT FROM Clazz WHERE key = [?, ?]");
    checkGenericStatement("select from test WHERE key > 10", "SELECT FROM test WHERE key > ?");
    checkGenericStatement("select from test WHERE key > ? ", "SELECT FROM test WHERE key > ?");
    checkGenericStatement(
        "select from test WHERE key > :name ", "SELECT FROM test WHERE key > :name");
    checkGenericStatement(
        "select from test WHERE key CONTAINS 'foo'", "SELECT FROM test WHERE key CONTAINS ?");
    checkGenericStatement("select funct(#10:20) ", "SELECT funct(?)");
  }

  protected static YouTrackDBSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    return new YouTrackDBSql(is);
  }
}
