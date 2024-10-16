package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.util.Date;
import org.junit.Test;

public class DefaultValueTest extends BaseMemoryDatabase {

  @Test
  public void testKeepValueSerialization() {
    // create example schema
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassC");

    OProperty prop = classA.createProperty("name", OType.STRING);
    prop.setDefaultValue("uuid()");

    ODocument doc = new ODocument("ClassC");

    byte[] val = doc.toStream();
    ODocument doc1 = new ODocument();
    ORecordInternal.unsetDirty(doc1);
    doc1.fromStream(val);
    doc1.deserializeFields();
    assertEquals((String) doc.field("name"), (String) doc1.field("name"));
  }

  @Test
  public void testDefaultValueDate() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATE);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));
    OProperty some = classA.createProperty("id", OType.STRING);
    some.setDefaultValue("uuid()");

    db.begin();
    ODocument doc = new ODocument(classA);
    ODocument saved = db.save(doc);
    db.commit();

    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("id"));

    db.begin();
    OResult inserted = db.command("insert into ClassA content {}").next();
    db.commit();

    ODocument seved1 = db.load(inserted.getIdentity().get());
    assertNotNull(seved1.field("date"));
    assertNotNull(seved1.field("id"));
    assertTrue(seved1.field("date") instanceof Date);
  }

  @Test
  public void testDefaultValueDateFromContent() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATE);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));
    OProperty some = classA.createProperty("id", OType.STRING);
    some.setDefaultValue("uuid()");

    String value = "2000-01-01 00:00:00";

    db.begin();
    ODocument doc = new ODocument(classA);
    ODocument saved = db.save(doc);
    db.commit();

    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("id"));

    db.begin();
    OResult inserted = db.command("insert into ClassA content {\"date\":\"" + value + "\"}").next();
    db.commit();

    ODocument seved1 = db.load(inserted.getIdentity().get());
    assertNotNull(seved1.field("date"));
    assertNotNull(seved1.field("id"));
    assertTrue(seved1.field("date") instanceof Date);
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(seved1.field("date")), value);
  }

  @Test
  public void testDefaultValueFromJson() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATE);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));

    db.begin();
    ODocument doc = new ODocument().fromJSON("{'@class':'ClassA','other':'other'}");
    ODocument saved = db.save(doc);
    db.commit();

    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueProvidedFromJson() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATETIME);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));

    String value1 = ODateHelper.getDateTimeFormatInstance().format(new Date());
    db.begin();
    ODocument doc =
        new ODocument().fromJSON("{'@class':'ClassA','date':'" + value1 + "','other':'other'}");
    ODocument saved = db.save(doc);
    db.commit();

    assertNotNull(saved.field("date"));
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(saved.field("date")), value1);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueMandatoryReadonlyFromJson() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATE);
    prop.setMandatory(true);
    prop.setReadonly(true);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));

    db.begin();
    ODocument doc = new ODocument().fromJSON("{'@class':'ClassA','other':'other'}");
    ODocument saved = db.save(doc);
    db.commit();

    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueProvidedMandatoryReadonlyFromJson() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATETIME);
    prop.setMandatory(true);
    prop.setReadonly(true);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));

    String value1 = ODateHelper.getDateTimeFormatInstance().format(new Date());
    ODocument doc =
        new ODocument().fromJSON("{'@class':'ClassA','date':'" + value1 + "','other':'other'}");
    db.begin();
    ODocument saved = db.save(doc);
    db.commit();
    assertNotNull(saved.field("date"));
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(saved.field("date")), value1);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueUpdateMandatoryReadonlyFromJson() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ClassA");

    OProperty prop = classA.createProperty("date", OType.DATETIME);
    prop.setMandatory(true);
    prop.setReadonly(true);
    prop.setDefaultValue(ODateHelper.getDateTimeFormatInstance().format(new Date()));

    db.begin();
    ODocument doc = new ODocument().fromJSON("{'@class':'ClassA','other':'other'}");
    ODocument saved = db.save(doc);
    db.commit();

    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("other"));
    String val = ODateHelper.getDateTimeFormatInstance().format(doc.field("date"));
    ODocument doc1 =
        new ODocument().fromJSON("{'@class':'ClassA','date':'" + val + "','other':'other1'}");
    saved.merge(doc1, true, true);

    db.begin();
    saved = db.save(saved);
    db.commit();

    assertNotNull(saved.field("date"));
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(saved.field("date")), val);
    assertEquals(saved.field("other"), "other1");
  }
}
