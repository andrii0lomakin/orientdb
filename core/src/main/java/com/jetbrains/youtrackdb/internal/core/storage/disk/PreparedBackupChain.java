package com.jetbrains.youtrackdb.internal.core.storage.disk;

import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageException;
import it.unimi.dsi.fastutil.objects.ObjectBooleanImmutablePair;
import it.unimi.dsi.fastutil.objects.ObjectBooleanPair;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.file.PathUtils;

/**
 * Owns the validated temporary copies of one backup chain of one restore request.
 *
 * <p>A backup chain is one full backup followed by ordered incremental backups of one database.
 * The preparation of a restore copies every selected unit of that chain into one temporary
 * directory and validates every copy. The replay then reads those copies alone.
 *
 * <p>One request owns one instance. The owner keeps the instance across the final target
 * admission and across the deletion of an interrupted target. The owner also keeps it across the
 * recreation, the replay, and the final checks. The owner closes the instance afterwards, which
 * removes every copy.
 *
 * <p>The close covers a successful replay, a refused chain, an ordinary failure, and an observed
 * cancellation. A sudden process stop or host stop can leave the temporary directory behind for
 * manual removal, because no automatic reclamation of that residue exists.
 *
 * <p>An instance is confined to its owning request and needs no thread safety of its own.
 */
public final class PreparedBackupChain implements AutoCloseable {

  private final String storageName;
  private final Path temporaryDirectory;
  private final List<ObjectBooleanPair<Path>> units = new ArrayList<>();

  private long lastTxId = -1;
  private boolean complete;
  private boolean closed;

  PreparedBackupChain(final String storageName, final Path temporaryDirectory) {
    this.storageName = storageName;
    this.temporaryDirectory = temporaryDirectory;
  }

  String storageName() {
    return storageName;
  }

  /** Returns the temporary directory that holds every copy of this chain. */
  public Path temporaryDirectory() {
    return temporaryDirectory;
  }

  /** Returns every prepared copy in chain order, with the full-backup flag of each copy. */
  List<ObjectBooleanPair<Path>> units() {
    return units;
  }

  /** Returns the highest transaction identifier of the prepared chain. */
  long lastTxId() {
    return lastTxId;
  }

  /** Adds one validated copy to the end of this chain. */
  void addUnit(final Path preparedFile, final boolean fullBackup) {
    units.add(new ObjectBooleanImmutablePair<>(preparedFile, fullBackup));
  }

  /** Marks this chain as completely prepared and validated. */
  void completePreparation(final long chainLastTxId) {
    this.lastTxId = chainLastTxId;
    this.complete = true;
  }

  /**
   * Refuses a replay of one chain that no complete preparation produced.
   *
   * <p>This check is defense in depth. Every production caller replays a chain of one finished
   * preparation, because a failed preparation throws instead of returning a chain.
   */
  void requireComplete() {
    if (closed) {
      throw new IllegalStateException(
          "The prepared backup chain of database '" + storageName + "' is closed already");
    }
    if (!complete) {
      throw new IllegalStateException(
          "The backup chain of database '" + storageName + "' passed no complete validation");
    }
  }

  /**
   * Removes every prepared copy and the temporary directory of this request.
   *
   * <p>A second close does nothing. A failed removal reports one warning that names the
   * temporary path, and the close itself reports no failure. A finished restore therefore stays
   * successful, because leftover temporary files change no database.
   *
   * <p>The same rule protects every failed request. This close runs inside the cleanup of a
   * failed request as well, and it never replaces the failure of that request.
   */
  @Override
  public void close() {
    var cleanupFailure = removeTemporaryCopies();
    if (cleanupFailure != null) {
      LogManager.instance()
          .warn(PreparedBackupChain.class,
              "Cannot delete the temporary directory of the backup restore of database '%s': %s."
                  + " Remove that directory manually.",
              cleanupFailure, storageName, temporaryDirectory.toAbsolutePath());
    }
  }

  /**
   * Closes this chain and keeps one primary failure primary.
   *
   * <p>A cleanup failure joins the suppressed failures of the primary failure. The caller
   * therefore keeps the diagnostics of the cleanup without any change of the reported cause.
   *
   * @param primaryFailure the failure that ends the owning request
   */
  void closeSuppressing(final Throwable primaryFailure) {
    var cleanupFailure = removeTemporaryCopies();
    if (cleanupFailure != null) {
      primaryFailure.addSuppressed(
          BaseException.wrapException(
              new StorageException(storageName,
                  "Cannot delete the temporary directory of the backup restore "
                      + temporaryDirectory.toAbsolutePath()),
              cleanupFailure,
              storageName));
    }
  }

  /**
   * Removes the temporary directory of this request once, and reports one failed removal.
   *
   * @return the failure of the removal, and null for a removed directory or a second close
   */
  @Nullable private IOException removeTemporaryCopies() {
    if (closed) {
      return null;
    }
    closed = true;
    try {
      PathUtils.deleteDirectory(temporaryDirectory);
      LogManager.instance()
          .info(PreparedBackupChain.class,
              "Temporary directory for the backup restore of database '%s' deleted: %s.",
              storageName, temporaryDirectory.toAbsolutePath());
      return null;
    } catch (IOException deletionFailure) {
      return deletionFailure;
    }
  }
}
