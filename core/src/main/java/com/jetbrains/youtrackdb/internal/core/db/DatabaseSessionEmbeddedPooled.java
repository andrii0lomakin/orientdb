/*
 *
 *
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
 *
 *
 */
package com.jetbrains.youtrackdb.internal.core.db;

import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;

public class DatabaseSessionEmbeddedPooled extends DatabaseSessionEmbedded implements
    PooledSession {

  private final DatabasePoolInternal pool;

  public DatabaseSessionEmbeddedPooled(DatabasePoolInternal pool,
      AbstractStorage storage, boolean serverMode) {
    super(storage, serverMode);
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
  public void reuse() {
    activateOnCurrentThread();
    // Clear the Dekker teardown-intent mark for this fresh borrow. For a RECYCLED session this
    // clear is the ONLY belt: the recycle teardown (internalClose(true)) legitimately marks — it
    // tears — so every returned session arrives here marked, and without the clear the borrower's
    // first mutex engage would self-abort on a healthy session. (The no-mark-on-no-op-close rule
    // guards a different path: a guard-returning close that tears nothing.) The teardown claim is
    // reset for the same reason — the recycle teardown consumed it, and the next close of this
    // borrow must be able to claim its own teardown.
    clearTeardownIntent();
    resetTeardownClaim();
    getPlanNullPlacements().reset();
    setStatus(STATUS.OPEN);
  }

  @Override
  public DatabaseSessionEmbedded copy() {
    assertIfNotActive();
    return pool.acquire();
  }

  @Override
  public void realClose() {
    // Pool-side Dekker completer handshake: write the teardown-intent mark FIRST (volatile
    // store), then re-validate the skip condition. Pairs with the owner side, which publishes its
    // commit-completion state (the volatile tx status write) first and then reads the mark in its
    // completer — so at least one side always runs the full teardown: if our re-check below still
    // sees the commit in flight, the owner's later mark-read sees this mark and completes; if the
    // commit already finished, we fall through to the full teardown ourselves. Both acting is the
    // benign overlap case, contained by the atomic teardown claim inside internalClose (exactly
    // one full teardown proceeds) and the atomic release claim (exactly one permit release).
    markTeardownIntent();
    if (hasInFlightForeignCommit()) {
      // The pool-teardown skip: this session's transaction is COMMITTING on its owner's thread
      // right now.
      // Tearing it down here would mutate the live commit's transaction object (clear() of the
      // record operations mid-apply, cache shutdown under the promotion reads) and could flip
      // checkOpenness under the committing owner — corrupting or falsely failing a durable
      // commit. Perform ONLY the whitelist: the mark is set (above) and we log; deliberately NO
      // rollback/clear, NO mutex release (the owner's own close releases the live commit's
      // permit), NO status flip, NO session-count decrement, NO cache/sharedContext teardown, and
      // the internalClose teardown claim is NOT consumed — the owner's completer runs the full
      // internalClose on the owning thread after its tx closes. Nothing pool-thread-private was
      // planted before this check (activation happens only on the fall-through below), so there
      // is nothing to remove here.
      //
      // The message is pre-formatted and passed through the (requester, message, Throwable)
      // overload: a (requester, String, String) call shape would resolve to the
      // (requester, dbName, message) overload and log only the database name, losing the
      // diagnostic entirely.
      LogManager.instance()
          .warn(this,
              String.format(
                  "Pool close found session %08X of database '%s' mid-commit on its owner thread"
                      + " (tx status %s); deferring the session teardown to the committing owner",
                  System.identityHashCode(this), getDatabaseName(),
                  getTransactionStatusForDiagnostics()),
              (Throwable) null);
      return;
    }
    activateOnCurrentThread();
    super.close();
  }

  @Override
  public boolean isBackendClosed() {
    return getStorage().isClosed(this);
  }
}
