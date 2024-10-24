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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SecurityTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SecurityTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    database.close();
  }

  public void testWrongPassword() throws IOException {
    try {
      database.open("reader", "swdsds");
    } catch (OException e) {
      Assert.assertTrue(
          e instanceof OSecurityAccessException
              || e.getCause() != null
                  && e.getCause()
                          .toString()
                          .indexOf(
                              "com.orientechnologies.orient.core.exception.OSecurityAccessException")
                      > -1);
    }
  }

  public void testSecurityAccessWriter() throws IOException {
    database.open("writer", "writer");

    try {
      database.begin();
      new ODocument().save("internal");
      database.commit();

      Assert.assertTrue(false);
    } catch (OSecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      database.close();
    }
  }

  @Test
  public void testSecurityAccessReader() throws IOException {
    database.open("reader", "reader");

    try {
      database.createClassIfNotExist("Profile");

      database.begin();
      new ODocument("Profile")
          .fields(
              "nick",
              "error",
              "password",
              "I don't know",
              "lastAccessOn",
              new Date(),
              "registeredOn",
              new Date())
          .save();
      database.commit();
    } catch (OSecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      database.close();
    }
  }

  @Test
  public void testEncryptPassword() throws IOException {
    database.open("admin", "admin");

    database.begin();
    Long updated =
        database
            .command("update ouser set password = 'test' where name = 'reader'")
            .next()
            .getProperty("count");
    database.commit();

    Assert.assertEquals(updated.intValue(), 1);

    OResultSet result = database.query("select from ouser where name = 'reader'");
    Assert.assertFalse(result.next().getProperty("password").equals("test"));

    // RESET OLD PASSWORD
    database.begin();
    updated =
        database
            .command("update ouser set password = 'reader' where name = 'reader'")
            .next()
            .getProperty("count");
    database.commit();
    Assert.assertEquals(updated.intValue(), 1);

    result = database.query("select from ouser where name = 'reader'");
    Assert.assertFalse(result.next().getProperty("password").equals("reader"));

    database.close();
  }

  public void testParentRole() {
    database.open("admin", "admin");

    database.begin();
    OSecurity security = database.getMetadata().getSecurity();
    ORole writer = security.getRole("writer");

    ORole writerChild =
        security.createRole("writerChild", writer, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
    writerChild.save();
    database.commit();

    try {
      database.begin();
      ORole writerGrandChild =
          security.createRole(
              "writerGrandChild", writerChild, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
      writerGrandChild.save();
      database.commit();

      try {
        database.begin();
        OUser child = security.createUser("writerChild", "writerChild", writerGrandChild);
        child.save();
        database.commit();

        try {
          Assert.assertTrue(child.hasRole("writer", true));
          Assert.assertFalse(child.hasRole("wrter", true));

          database.close();
          if (!database.isRemote()) {
            database.open("writerChild", "writerChild");

            OSecurityUser user = database.getUser();
            Assert.assertTrue(user.hasRole("writer", true));
            Assert.assertFalse(user.hasRole("wrter", true));

            database.close();
          }
          database.open("admin", "admin");
          security = database.getMetadata().getSecurity();
        } finally {
          security.dropUser("writerChild");
        }
      } finally {
        security.dropRole("writerGrandChild");
      }
    } finally {
      security.dropRole("writerChild");
    }
  }

  @Test
  public void testQuotedUserName() {
    database.open("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();

    ORole adminRole = security.getRole("admin");
    security.createUser("user'quoted", "foobar", adminRole);

    database.close();

    database.open("user'quoted", "foobar");
    database.close();

    database.open("admin", "admin");
    security = database.getMetadata().getSecurity();
    OUser user = security.getUser("user'quoted");
    Assert.assertNotNull(user);
    security.dropUser(user.getName());

    database.close();

    try {
      database.open("user'quoted", "foobar");
      Assert.fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testUserNoRole() {
    database.open("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();

    OUser newUser = security.createUser("noRole", "noRole", (String[]) null);

    database.close();

    try {
      database.open("noRole", "noRole");
      Assert.fail();
    } catch (OSecurityAccessException e) {
      database.open("admin", "admin");
      security = database.getMetadata().getSecurity();
      security.dropUser("noRole");
    }
  }

  @Test
  public void testAdminCanSeeSystemClusters() {
    database.open("admin", "admin");

    List<OResult> result =
        database.command("select from ouser").stream().collect(Collectors.toList());
    Assert.assertFalse(result.isEmpty());

    Assert.assertTrue(database.browseClass("OUser").hasNext());

    Assert.assertTrue(database.browseCluster("OUser").hasNext());
  }

  @Test
  @Ignore
  public void testOnlyAdminCanSeeSystemClusters() {
    database.open("reader", "reader");

    try {
      database.command(new OCommandSQL("select from ouser")).execute();
    } catch (OSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseClass("OUser").hasNext());
      Assert.fail();
    } catch (OSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseCluster("OUser").hasNext());
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testCannotExtendClassWithNoUpdateProvileges() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().createClass("Protected");
    database.close();

    database.open("writer", "writer");

    try {
      database.command(new OCommandSQL("alter class Protected superclass OUser")).execute();
      Assert.fail();
    } catch (OSecurityException e) {
    } finally {
      database.close();

      database.open("admin", "admin");
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testSuperUserCanExtendClassWithNoUpdateProvileges() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().createClass("Protected");

    try {
      database.command("alter class Protected superclass OUser").close();
    } finally {
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testEmptyUserName() {
    database.open("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      String userName = "";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithAllSpaces() {
    database.open("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = "  ";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesOne() {
    database.open("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = " sas";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesTwo() {
    database.open("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = "sas ";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesThree() {
    database.open("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = " sas ";
      try {
        security.createUser(userName, "foobar", reader);
        Assert.assertTrue(false);
      } catch (OValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSpacesInTheMiddle() {
    database.open("admin", "admin");
    try {
      OSecurity security = database.getMetadata().getSecurity();

      ORole reader = security.getRole("reader");
      final String userName = "s a s";
      security.createUser(userName, "foobar", reader);
      Assert.assertNotNull(security.getUser(userName));
      security.dropUser(userName);
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }
}
