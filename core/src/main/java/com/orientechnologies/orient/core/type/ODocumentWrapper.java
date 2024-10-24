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
package com.orientechnologies.orient.core.type;

import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.Serializable;

/**
 * Base abstract class to wrap a document.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class ODocumentWrapper implements Serializable {
  @ODocumentInstance private ODocument document;

  public ODocumentWrapper() {}

  public ODocumentWrapper(final ORID iRID) {
    this((ODocument) iRID.getRecord());
  }

  public ODocumentWrapper(final String iClassName) {
    this(new ODocument(iClassName));
  }

  public ODocumentWrapper(final ODocument iDocument) {
    document = iDocument;
  }

  public void fromStream(final ODocument iDocument) {
    document = iDocument;
  }

  public ODocument toStream() {
    return document;
  }

  void load() {
    document = (ODocument) document.load();
  }

  public <RET extends ODocumentWrapper> RET load(
      final String iFetchPlan, final boolean iIgnoreCache) {
    checkProxy();
    document = document.load(iFetchPlan, iIgnoreCache);
    return (RET) this;
  }

  public <RET extends ODocumentWrapper> RET reload() {
    checkProxy();
    if (document.isUnloaded()) {
      document = ODatabaseSession.getActiveSession().load(document.getIdentity());
    } else {
      document.reload();
    }
    return (RET) this;
  }

  public <RET extends ODocumentWrapper> RET reload(final String iFetchPlan) {
    checkProxy();
    if (document.isUnloaded()) {
      document =
          ((ODatabaseDocumentInternal) ODatabaseSession.getActiveSession())
              .load(document.getIdentity(), iFetchPlan);
    } else {
      document.reload(iFetchPlan, true);
    }
    return (RET) this;
  }

  public <RET extends ODocumentWrapper> RET reload(
      final String iFetchPlan, final boolean iIgnoreCache) {
    checkProxy();
    if (document.isUnloaded()) {
      document =
          ((ODatabaseDocumentInternal) ODatabaseSession.getActiveSession())
              .load(document.getIdentity(), iFetchPlan, iIgnoreCache);
    } else {
      document.reload(iFetchPlan, iIgnoreCache);
    }
    return (RET) this;
  }

  public <RET extends ODocumentWrapper> RET save() {
    document.save();
    return (RET) this;
  }

  public <RET extends ODocumentWrapper> RET save(final String iClusterName) {
    checkProxy();
    document.save(iClusterName);
    return (RET) this;
  }

  public ODocument getDocument() {
    checkProxy();
    return document;
  }

  public void setDocument(ODocument document) {
    this.document = document;
  }

  private void checkProxy() {
    if (document != null) {
      document = (ODocument) document.getRecord();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((document == null) ? 0 : document.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    final ODocumentWrapper other = (ODocumentWrapper) obj;
    if (document == null) {
      return other.document == null;
    } else return document.equals(other.document);
  }

  @Override
  public String toString() {
    return document != null ? document.toString() : "?";
  }
}
