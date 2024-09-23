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
package com.orientechnologies.orient.server.network.protocol.http.command.patch;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.OEmptyRecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPatchDocument extends OServerCommandDocumentAbstract {

  private static final String[] NAMES = {"PATCH|document/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: document/<database>[/<record-id>]");

    iRequest.getData().commandInfo = "Edit Document";

    ODatabaseSession db = null;
    com.orientechnologies.orient.core.id.ORecordId recordId;
    final com.orientechnologies.orient.core.record.impl.ODocument doc;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      if (urlParts.length > 2) {
        // EXTRACT RID
        final int parametersPos = urlParts[2].indexOf('?');
        final String rid =
            parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
        recordId = new com.orientechnologies.orient.core.id.ORecordId(rid);

        if (!recordId.isValid()) {
          throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
        }
      } else {
        recordId = new OEmptyRecordId();
      }

      // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
      doc = new com.orientechnologies.orient.core.record.impl.ODocument();
      doc.fromJSON(iRequest.getContent());

      if (iRequest.getIfMatch() != null)
      // USE THE IF-MATCH HTTP HEADER AS VERSION
      {
        ORecordInternal.setVersion(doc, Integer.parseInt(iRequest.getIfMatch()));
      }

      if (!recordId.isValid()) {
        recordId = (com.orientechnologies.orient.core.id.ORecordId) doc.getIdentity();
      } else {
        ORecordInternal.setIdentity(doc, recordId);
      }

      if (!recordId.isValid()) {
        throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
      }

      final com.orientechnologies.orient.core.record.impl.ODocument currentDocument =
          db.load(recordId);

      if (currentDocument == null) {
        iResponse.send(
            OHttpUtils.STATUS_NOTFOUND_CODE,
            OHttpUtils.STATUS_NOTFOUND_DESCRIPTION,
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Record " + recordId + " was not found.",
            null);
        return false;
      }

      db.executeInTx(
          () -> {
            boolean partialUpdateMode = true;
            currentDocument.merge(doc, partialUpdateMode, false);
            ORecordInternal.setVersion(currentDocument, doc.getVersion());

            currentDocument.save();
          });

      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          OHttpUtils.STATUS_OK_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          currentDocument.toJSON(),
          OHttpUtils.HEADER_ETAG + doc.getVersion());

    } finally {
      if (db != null) {
        db.close();
      }
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
