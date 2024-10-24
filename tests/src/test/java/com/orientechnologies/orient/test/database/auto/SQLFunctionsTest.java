/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLFunctionsTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLFunctionsTest(@Optional String url) {
    super(url);
  }

  @Test
  public void queryMax() {
    OResultSet result = database.command("select max(id) as max from Account");

    assertNotNull(result.next().getProperty("max"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryMaxInline() {
    List<ODocument> result =
        database.query("select max(1,2,7,0,-2,3) as max").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("max"));

      Assert.assertEquals(((Number) d.field("max")).intValue(), 7);
    }
  }

  @Test
  public void queryMin() {
    OResultSet result = database.command("select min(id) as min from Account");

    OResult d = result.next();
    Assert.assertNotNull(d.getProperty("min"));

    Assert.assertEquals(((Number) d.getProperty("min")).longValue(), 0L);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryMinInline() {
    List<ODocument> result =
        database.query("select min(1,2,7,0,-2,3) as min").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("min"));

      Assert.assertEquals(((Number) d.field("min")).intValue(), -2);
    }
  }

  @Test
  public void querySum() {
    OResultSet result = database.command("select sum(id) as sum from Account");
    OResult d = result.next();
    Assert.assertNotNull(d.getProperty("sum"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryCount() {
    OResultSet result = database.command("select count(*) as total from Account");
    OResult d = result.next();
    Assert.assertNotNull(d.getProperty("total"));
    Assert.assertTrue(((Number) d.getProperty("total")).longValue() > 0);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  public void queryCountExtendsRestricted() {
    OClass restricted = database.getMetadata().getSchema().getClass("ORestricted");
    Assert.assertNotNull(restricted);

    database.getMetadata().getSchema().createClass("QueryCountExtendsRestrictedClass", restricted);

    OUser admin = database.getMetadata().getSecurity().getUser("admin");
    OUser reader = database.getMetadata().getSecurity().getUser("reader");

    @SuppressWarnings("deprecation")
    ORole byPassRestrictedRole =
        database
            .getMetadata()
            .getSecurity()
            .createRole("byPassRestrictedRole", ORole.ALLOW_MODES.DENY_ALL_BUT);
    byPassRestrictedRole.addRule(
        ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ);
    database.begin();
    byPassRestrictedRole.save();
    database.commit();

    database
        .getMetadata()
        .getSecurity()
        .createUser("superReader", "superReader", "reader", "byPassRestrictedRole");

    ODocument docAdmin = new ODocument("QueryCountExtendsRestrictedClass");
    docAdmin.field(
        "_allowRead",
        new HashSet<OIdentifiable>(Collections.singletonList(admin.getIdentity().getIdentity())));
    database.begin();
    docAdmin.save();
    database.commit();

    ODocument docReader = new ODocument("QueryCountExtendsRestrictedClass");
    docReader.field("_allowRead", new HashSet<>(Collections.singletonList(reader.getIdentity())));
    database.begin();
    docReader.save();
    database.commit();

    List<ODocument> result =
        database.query("select count(*) from QueryCountExtendsRestrictedClass").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();
    ODocument count = result.get(0);
    Assert.assertEquals(2L, count.<Object>field("count(*)"));

    database.close();
    //noinspection deprecation
    database.open("admin", "admin");

    result =
        database.query("select count(*) as count from QueryCountExtendsRestrictedClass").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();
    count = result.get(0);
    Assert.assertEquals(2L, count.<Object>field("count"));

    database.close();
    //noinspection deprecation
    //    database.open("reader", "reader");
    //
    //    result =
    //        database.query("select count(*) as count from
    // QueryCountExtendsRestrictedClass").stream()
    //            .map(r -> (ODocument) r.toElement())
    //            .toList();
    //    count = result.get(0);
    //    Assert.assertEquals(1L, count.<Object>field("count"));
    //
    //    database.close();
    //noinspection deprecation
    database.open("superReader", "superReader");

    result =
        database.query("select count(*) as count from QueryCountExtendsRestrictedClass").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();
    count = result.get(0);
    Assert.assertEquals(2L, count.<Object>field("count"));
  }

  @Test
  public void queryCountWithConditions() {
    OClass indexed = database.getMetadata().getSchema().getOrCreateClass("Indexed");
    indexed.createProperty("key", OType.STRING);
    indexed.createIndex("keyed", OClass.INDEX_TYPE.NOTUNIQUE, "key");

    database.begin();
    database.<ODocument>newInstance("Indexed").field("key", "one").save();
    database.<ODocument>newInstance("Indexed").field("key", "two").save();
    database.commit();

    List<ODocument> result =
        database.query("select count(*) as total from Indexed where key > 'one'").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("total"));
      Assert.assertTrue(((Number) d.field("total")).longValue() > 0);
    }
  }

  @Test
  public void queryDistinct() {
    List<ODocument> result =
        database.query("select distinct(name) as name from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertTrue(result.size() > 1);

    Set<String> cities = new HashSet<String>();
    for (ODocument city : result) {
      String cityName = city.field("name");
      Assert.assertFalse(cities.contains(cityName));
      cities.add(cityName);
    }
  }

  @Test
  public void queryFunctionRenamed() {
    List<ODocument> result =
        database.query("select distinct(name) as dist from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertTrue(result.size() > 1);

    for (ODocument city : result) {
      Assert.assertTrue(city.containsField("dist"));
    }
  }

  @Test
  public void queryUnionAllAsAggregationNotRemoveDuplicates() {
    List<ODocument> result =
        database.query("select from City").stream().map(r -> (ODocument) r.toElement()).toList();
    int count = result.size();

    result =
        database.query("select unionAll(name) as name from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();
    Collection<Object> citiesFound = result.get(0).field("name");
    Assert.assertEquals(citiesFound.size(), count);
  }

  @Test
  public void querySetNotDuplicates() {
    List<ODocument> result =
        database.query("select set(name) as name from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 1);

    Collection<Object> citiesFound = result.get(0).field("name");
    Assert.assertTrue(citiesFound.size() > 1);

    Set<String> cities = new HashSet<String>();
    for (Object city : citiesFound) {
      Assert.assertFalse(cities.contains(city.toString()));
      cities.add(city.toString());
    }
  }

  @Test
  public void queryList() {
    List<ODocument> result =
        database.query("select list(name) as names from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      List<Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  public void testSelectMap() {
    List<ODocument> result =
        database
            .query("select list( 1, 4, 5.00, 'john', map( 'kAA', 'vAA' ) ) as myresult")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 1);

    ODocument document = result.get(0);
    @SuppressWarnings("rawtypes")
    List myresult = document.field("myresult");
    Assert.assertNotNull(myresult);

    Assert.assertTrue(myresult.remove(Integer.valueOf(1)));
    Assert.assertTrue(myresult.remove(Integer.valueOf(4)));
    Assert.assertTrue(myresult.remove(Float.valueOf(5)));
    Assert.assertTrue(myresult.remove("john"));

    Assert.assertEquals(myresult.size(), 1);

    Assert.assertTrue(myresult.get(0) instanceof Map, "The object is: " + myresult.getClass());
    @SuppressWarnings("rawtypes")
    Map map = (Map) myresult.get(0);

    String value = (String) map.get("kAA");
    Assert.assertEquals(value, "vAA");

    Assert.assertEquals(map.size(), 1);
  }

  @Test
  public void querySet() {
    List<ODocument> result =
        database.query("select set(name) as names from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Set<Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryMap() {
    List<ODocument> result =
        database.query("select map(name, country.name) as names from City").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Map<Object, Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryUnionAllAsInline() {
    List<ODocument> result =
        database.query("select unionAll(out, in) as edges from V").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertTrue(result.size() > 1);
    for (ODocument d : result) {
      Assert.assertEquals(d.fieldNames().length, 1);
      Assert.assertTrue(d.containsField("edges"));
    }
  }

  @Test
  public void queryComposedAggregates() {
    List<ODocument> result =
        database
            .query(
                "select MIN(id) as min, max(id) as max, AVG(id) as average, sum(id) as total"
                    + " from Account")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("min"));
      Assert.assertNotNull(d.field("max"));
      Assert.assertNotNull(d.field("average"));
      Assert.assertNotNull(d.field("total"));

      Assert.assertTrue(
          ((Number) d.field("max")).longValue() > ((Number) d.field("average")).longValue());
      Assert.assertTrue(
          ((Number) d.field("average")).longValue() >= ((Number) d.field("min")).longValue());
      Assert.assertTrue(
          ((Number) d.field("total")).longValue() >= ((Number) d.field("max")).longValue(),
          "Total " + d.field("total") + " max " + d.field("max"));
    }
  }

  @Test
  public void queryFormat() {
    List<ODocument> result =
        database
            .query(
                "select format('%d - %s (%s)', nr, street, type, dummy ) as output from"
                    + " Account")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertTrue(result.size() > 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("output"));
    }
  }

  @Test
  public void querySysdateNoFormat() {
    OResultSet result = database.command("select sysdate() as date from Account");

    Assert.assertTrue(result.hasNext());
    while (result.hasNext()) {
      OResult d = result.next();
      Assert.assertNotNull(d.getProperty("date"));
    }
  }

  @Test
  public void querySysdateWithFormat() {
    List<ODocument> result =
        database.query("select sysdate('dd-MM-yyyy') as date from Account").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertTrue(result.size() > 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("date"));
    }
  }

  @Test
  public void queryDate() {
    OResultSet result = database.command("select count(*) as tot from Account");

    int tot = ((Number) result.next().getProperty("tot")).intValue();
    assertFalse(result.hasNext());

    database.begin();
    long updated =
        database.command("update Account set created = date()").next().getProperty("count");
    database.commit();

    Assert.assertEquals(updated, tot);

    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

    result =
        database.query(
            "select from Account where created <= date('"
                + dateFormat.format(new Date())
                + "', \""
                + pattern
                + "\")");

    Assert.assertEquals(result.stream().count(), tot);
    result =
        database.query(
            "select from Account where created <= date('"
                + dateFormat.format(new Date())
                + "', \""
                + pattern
                + "\")");
    while (result.hasNext()) {
      OResult d = result.next();
      Assert.assertNotNull(d.getProperty("created"));
    }
  }

  @Test(expectedExceptions = OCommandSQLParsingException.class)
  public void queryUndefinedFunction() {
    //noinspection ResultOfMethodCallIgnored
    database.query("select blaaaa(salary) as max from Account").stream()
        .map(r -> (ODocument) r.toElement())
        .toList();
  }

  @Test
  public void queryCustomFunction() {
    OSQLEngine.getInstance()
        .registerFunction(
            "bigger",
            new OSQLFunctionAbstract("bigger", 2, 2) {
              @Override
              public String getSyntax() {
                return "bigger(<first>, <second>)";
              }

              @Override
              public Object execute(
                  Object iThis,
                  OIdentifiable iCurrentRecord,
                  Object iCurrentResult,
                  final Object[] iParams,
                  OCommandContext iContext) {
                if (iParams[0] == null || iParams[1] == null)
                // CHECK BOTH EXPECTED PARAMETERS
                {
                  return null;
                }

                if (!(iParams[0] instanceof Number) || !(iParams[1] instanceof Number))
                // EXCLUDE IT FROM THE RESULT SET
                {
                  return null;
                }

                // USE DOUBLE TO AVOID LOSS OF PRECISION
                final double v1 = ((Number) iParams[0]).doubleValue();
                final double v2 = ((Number) iParams[1]).doubleValue();

                return Math.max(v1, v2);
              }
            });

    List<ODocument> result =
        database.query("select from Account where bigger(id,1000) = 1000").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());
    for (ODocument d : result) {
      Assert.assertTrue((Integer) d.field("id") <= 1000);
    }

    OSQLEngine.getInstance().unregisterFunction("bigger");
  }

  @Test
  public void queryAsLong() {
    long moreThanInteger = 1 + (long) Integer.MAX_VALUE;
    String sql =
        "select numberString.asLong() as value from ( select '"
            + moreThanInteger
            + "' as numberString from Account ) limit 1";
    List<ODocument> result =
        database.query(sql).stream().map(r -> (ODocument) r.toElement()).toList();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("value"));
      Assert.assertTrue(d.field("value") instanceof Long);
      Assert.assertEquals(moreThanInteger, d.<Object>field("value"));
    }
  }

  @Test
  public void testHashMethod() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    List<ODocument> result =
        database
            .query("select name, name.hash() as n256, name.hash('sha-512') as n512 from OUser")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());
    for (ODocument d : result) {
      final String name = d.field("name");

      Assert.assertEquals(OSecurityManager.createHash(name, "SHA-256"), d.field("n256"));
      Assert.assertEquals(OSecurityManager.createHash(name, "SHA-512"), d.field("n512"));
    }
  }

  @Test
  public void testFirstFunction() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    List<Long> sequence = new ArrayList<Long>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    database.begin();
    new ODocument("V").field("sequence", sequence).save();
    sequence.remove(0);
    new ODocument("V").field("sequence", sequence).save();
    database.commit();

    List<ODocument> result =
        database.query("select first(sequence) as first from V where sequence is not null").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).<Object>field("first"), 0L);
    Assert.assertEquals(result.get(1).<Object>field("first"), 1L);
  }

  @Test
  public void testLastFunction() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    List<Long> sequence = new ArrayList<Long>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    database.begin();
    new ODocument("V").field("sequence2", sequence).save();
    sequence.remove(sequence.size() - 1);
    new ODocument("V").field("sequence2", sequence).save();
    database.commit();

    List<ODocument> result =
        database.query("select last(sequence2) as last from V where sequence2 is not null").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).<Object>field("last"), 99L);
    Assert.assertEquals(result.get(1).<Object>field("last"), 98L);
  }

  @Test
  public void querySplit() {
    String sql = "select v.split('-') as value from ( select '1-2-3' as v ) limit 1";

    List<ODocument> result =
        database.query(sql).stream().map(r -> (ODocument) r.toElement()).toList();

    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertNotNull(d.field("value"));
      Assert.assertTrue(d.field("value").getClass().isArray());

      Object[] array = d.field("value");

      Assert.assertEquals(array.length, 3);
      Assert.assertEquals(array[0], "1");
      Assert.assertEquals(array[1], "2");
      Assert.assertEquals(array[2], "3");
    }
  }
}
