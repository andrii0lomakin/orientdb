package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;

public interface OCoordinatedExecutor {

  void executeOperationRequest(ONodeIdentity sender, String database, OLogId id, ONodeRequest request);

  void executeOperationResponse(ONodeIdentity sender, String database, OLogId id, ONodeResponse response);

  void executeSubmitResponse(ONodeIdentity sender, String database, OSessionOperationId operationId, OSubmitResponse response);

  void executeSubmitRequest(ONodeIdentity sender, String database, OSessionOperationId operationId, OSubmitRequest request);

  void executeStructuralSubmitRequest(ONodeIdentity sender, OSessionOperationId id, OStructuralSubmitRequest request);

  void executeStructuralSubmitResponse(ONodeIdentity sender, OSessionOperationId id, OStructuralSubmitResponse response);

  void executePropagate(ONodeIdentity sender, OLogId id, ORaftOperation operation);

  void executeConfirm(ONodeIdentity sender, OLogId id);

  void executeAck(ONodeIdentity sender, OLogId id);

  void nodeConnected(ONodeIdentity identity);

  void nodeDisconnected(ONodeIdentity identity);

  void setLeader(ONodeIdentity leader, OLogId leaderLastValid);

  void setDatabaseLeader(ONodeIdentity leader, String database, OLogId leaderLastValid);

  void ping(ONodeIdentity leader, OLogId leaderLastValid);

  void databasePing(ONodeIdentity leader, String database, OLogId leaderLastValid);

}