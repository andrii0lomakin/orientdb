/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.db;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@Deprecated
public class ODatabaseHelper {
  @Deprecated
  public static void createDatabase(ODatabase database, final String url) throws IOException {
    createDatabase(database, url, "server", "plocal");
  }

  @Deprecated
  public static void createDatabase(ODatabase database, final String url, String type)
      throws IOException {
    createDatabase(database, url, "server", type);
  }

  @Deprecated
  public static void openDatabase(ODatabase database) {
    ((ODatabaseInternal) database).open("admin", "admin");
  }

  @Deprecated
  public static void createDatabase(
      ODatabase database, final String url, String directory, String type) throws IOException {
    if (url.startsWith(OEngineRemote.NAME)) {
      new OServerAdmin(url)
          .connect("root", getServerRootPassword(directory))
          .createDatabase("document", type)
          .close();
    } else {
      ((ODatabaseInternal) database).create();
      database.close();
    }
  }

  @Deprecated
  public static void deleteDatabase(final ODatabase database, String storageType)
      throws IOException {
    deleteDatabase(database, "server", storageType);
  }

  @Deprecated
  public static void deleteDatabase(
      final ODatabase database, final String directory, String storageType) throws IOException {
    dropDatabase(database, directory, storageType);
  }

  @Deprecated
  public static void dropDatabase(final ODatabase database, String storageType) throws IOException {
    dropDatabase(database, "server", storageType);
  }

  @Deprecated
  public static void dropDatabase(
      final ODatabase database, final String directory, String storageType) throws IOException {
    if (existsDatabase(database, storageType)) {
      if (database.getURL().startsWith("remote:")) {
        database.activateOnCurrentThread();
        database.close();
        OServerAdmin admin =
            new OServerAdmin(database.getURL()).connect("root", getServerRootPassword(directory));
        admin.dropDatabase(storageType);
        admin.close();
      } else {
        if (database.isClosed()) openDatabase(database);
        else database.activateOnCurrentThread();
        ((ODatabaseInternal) database).drop();
      }
    }
  }

  @Deprecated
  public static boolean existsDatabase(final ODatabase database, String storageType)
      throws IOException {
    database.activateOnCurrentThread();
    if (database.getURL().startsWith("remote")) {
      OServerAdmin admin =
          new OServerAdmin(database.getURL()).connect("root", getServerRootPassword());
      boolean exist = admin.existsDatabase(storageType);
      admin.close();
      return exist;
    }

    return ((ODatabaseInternal) database).exists();
  }

  @Deprecated
  public static boolean existsDatabase(final String url) throws IOException {
    if (url.startsWith("remote")) {
      OServerAdmin admin = new OServerAdmin(url).connect("root", getServerRootPassword());
      boolean exist = admin.existsDatabase();
      admin.close();
      return exist;
    }
    return new ODatabaseDocumentTx(url).exists();
  }

  @Deprecated
  public static void freezeDatabase(final ODatabase database) throws IOException {
    database.activateOnCurrentThread();
    if (database.getURL().startsWith("remote")) {
      final OServerAdmin serverAdmin = new OServerAdmin(database.getURL());
      serverAdmin.connect("root", getServerRootPassword()).freezeDatabase("plocal");
      serverAdmin.close();
    } else {
      database.freeze();
    }
  }

  @Deprecated
  public static void releaseDatabase(final ODatabase database) throws IOException {
    database.activateOnCurrentThread();
    if (database.getURL().startsWith("remote")) {
      final OServerAdmin serverAdmin = new OServerAdmin(database.getURL());
      serverAdmin.connect("root", getServerRootPassword()).releaseDatabase("plocal");
      serverAdmin.close();
    } else {
      database.release();
    }
  }

  @Deprecated
  public static File getConfigurationFile() {
    return getConfigurationFile(null);
  }

  @Deprecated
  public static String getServerRootPassword() throws IOException {
    return getServerRootPassword("server");
  }

  @Deprecated
  protected static String getServerRootPassword(final String iDirectory) throws IOException {
    String passwd = System.getProperty("ORIENTDB_ROOT_PASSWORD");
    if (passwd != null) return passwd;

    final File file = getConfigurationFile(iDirectory);

    final FileReader f = new FileReader(file);
    final char[] buffer = new char[(int) file.length()];
    f.read(buffer);
    f.close();

    final String fileContent = new String(buffer);
    // TODO search is wrong because if first user is not root tests will fail
    int pos = fileContent.indexOf("password=\"");
    pos += "password=\"".length();
    return fileContent.substring(pos, fileContent.indexOf('"', pos));
  }

  @Deprecated
  protected static File getConfigurationFile(final String iDirectory) {
    // LOAD SERVER CONFIG FILE TO EXTRACT THE ROOT'S PASSWORD
    String sysProperty = System.getProperty("orientdb.config.file");
    File file = new File(sysProperty != null ? sysProperty : "");
    if (!file.exists()) {
      sysProperty = System.getenv("CONFIG_FILE");
      file = new File(sysProperty != null ? sysProperty : "");
    }
    if (!file.exists())
      file =
          new File(
              "../releases/orientdb-"
                  + OConstants.getRawVersion()
                  + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file =
          new File(
              "../releases/orientdb-community-"
                  + OConstants.getRawVersion()
                  + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file =
          new File(
              "../../releases/orientdb-"
                  + OConstants.getRawVersion()
                  + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file =
          new File(
              "../../releases/orientdb-community-"
                  + OConstants.getRawVersion()
                  + "/config/orientdb-server-config.xml");
    if (!file.exists() && iDirectory != null) {
      file = new File(iDirectory + "/config/orientdb-server-config.xml");
      if (!file.exists())
        file = new File("../" + iDirectory + "/config/orientdb-server-config.xml");
    }
    if (!file.exists())
      file =
          new File(
              OSystemVariableResolver.resolveSystemVariables(
                  "${" + Orient.ORIENTDB_HOME + "}/config/orientdb-server-config.xml"));
    if (!file.exists())
      throw new OConfigurationException(
          "Cannot load file orientdb-server-config.xml to execute remote tests. Current directory"
              + " is "
              + new File(".").getAbsolutePath());
    return file;
  }
}
