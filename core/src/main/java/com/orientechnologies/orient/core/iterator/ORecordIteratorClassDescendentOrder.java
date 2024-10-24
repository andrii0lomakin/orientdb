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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Record iterator to browse records in inverse order: from last to the first.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordIteratorClassDescendentOrder<REC extends ORecord>
    extends ORecordIteratorClass<REC> {

  public ORecordIteratorClassDescendentOrder(
      ODatabaseDocumentInternal iDatabase,
      ODatabaseDocumentInternal iLowLevelDatabase,
      String iClassName,
      boolean iPolymorphic) {
    super(iDatabase, iClassName, iPolymorphic);

    currentClusterIdx = clusterIds.length - 1; // START FROM THE LAST CLUSTER
    updateClusterRange();
  }

  @Override
  public ORecordIteratorClusters<REC> begin() {
    return super.last();
  }

  @Override
  public REC next() {
    return super.previous();
  }

  @Override
  public boolean hasNext() {
    return super.hasPrevious();
  }
}
