package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;

/**
 * Created by tglman on 07/07/16.
 */
public class ODatabaseDocumentRemotePooled extends ODatabaseDocumentRemote {

  private ODatabasePoolInternal pool;

  public ODatabaseDocumentRemotePooled(
      ODatabasePoolInternal pool, OStorageRemote storage, OSharedContext sharedContext) {
    super(storage, sharedContext);
    this.pool = pool;
  }

  @Override
  public void close() {
    if (isClosed()) {
      return;
    }

    internalClose(true);
    pool.release(this);
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    return (ODatabaseDocumentInternal) pool.acquire();
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(ODatabase.STATUS.OPEN);
  }

  public void realClose() {
    ODatabaseDocumentInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      activateOnCurrentThread();
      super.close();
    } finally {
      if (old == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(old);
      }
    }
  }
}
