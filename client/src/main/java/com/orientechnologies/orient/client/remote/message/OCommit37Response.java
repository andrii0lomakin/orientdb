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
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class OCommit37Response implements OBinaryResponse {

  private Map<UUID, OBonsaiCollectionPointer> collectionChanges;
  private List<ObjectObjectImmutablePair<ORID, ORID>> updatedRids;

  public OCommit37Response(
      Map<ORID, ORID> updatedRids, Map<UUID, OBonsaiCollectionPointer> collectionChanges) {
    super();
    this.updatedRids = new ArrayList<>(updatedRids.size());

    for (Map.Entry<ORID, ORID> entry : updatedRids.entrySet()) {
      this.updatedRids.add(new ObjectObjectImmutablePair<>(entry.getValue(), entry.getKey()));
    }

    this.collectionChanges = collectionChanges;
  }

  public OCommit37Response() {}

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {

    channel.writeInt(updatedRids.size());
    for (var pair : updatedRids) {
      channel.writeRID(pair.first());
      channel.writeRID(pair.second());
    }

    OMessageHelper.writeCollectionChanges(channel, collectionChanges);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {

    int updatedRidsSize = network.readInt();
    updatedRids = new ArrayList<>(updatedRidsSize);
    for (int i = 0; i < updatedRidsSize; i++) {
      ORID first = network.readRID();
      ORID second = network.readRID();

      updatedRids.add(new ObjectObjectImmutablePair<>(first, second));
    }

    collectionChanges = OMessageHelper.readCollectionChanges(network);
  }

  public List<ObjectObjectImmutablePair<ORID, ORID>> getUpdatedRids() {
    return updatedRids;
  }

  public Map<UUID, OBonsaiCollectionPointer> getCollectionChanges() {
    return collectionChanges;
  }
}
