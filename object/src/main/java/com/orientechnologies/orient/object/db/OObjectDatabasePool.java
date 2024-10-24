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
package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabasePoolBase;

/** @deprecated Please use com.orientechnologies.orient.core.db.OPartitionedDatabasePool instead. */
@Deprecated
public class OObjectDatabasePool extends ODatabasePoolBase<OObjectDatabaseTxInternal> {
  private static OObjectDatabasePool globalInstance = new OObjectDatabasePool();

  public OObjectDatabasePool() {
    super();
  }

  public OObjectDatabasePool(
      final String iURL, final String iUserName, final String iUserPassword) {
    super(iURL, iUserName, iUserPassword);
  }

  public static OObjectDatabasePool global() {
    globalInstance.setup();
    return globalInstance;
  }

  public static OObjectDatabasePool global(final int iPoolMin, final int iPoolMax) {
    globalInstance.setup(iPoolMin, iPoolMax);
    return globalInstance;
  }

  @Override
  protected OObjectDatabaseTxInternal createResource(
      final Object owner, final String iDatabaseName, final Object... iAdditionalArgs) {
    return new OObjectDatabaseTxInternalPooled(
        (OObjectDatabasePool) owner,
        iDatabaseName,
        (String) iAdditionalArgs[0],
        (String) iAdditionalArgs[1]);
  }
}
