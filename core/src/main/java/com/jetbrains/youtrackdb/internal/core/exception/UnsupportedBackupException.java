package com.jetbrains.youtrackdb.internal.core.exception;

import com.jetbrains.youtrackdb.api.exception.HighLevelException;

/**
 * Refuses one backup unit or one backup chain that this build does not support.
 *
 * <p>A backup header is the metadata record at the tail of one backup unit file. Every supported
 * header carries the semantic database format of the backed-up database. Every supported header
 * also carries evidence of a finished creation of that database. A header without both values is
 * unsupported, even when the content of the unit would otherwise fit this build.
 *
 * <p>An incremental backup refuses to extend a chain that holds one unsupported unit. A restore
 * refuses an unsupported chain before the restore changes the target.
 */
public class UnsupportedBackupException extends DatabaseException implements HighLevelException {

  public UnsupportedBackupException(String dbName, String message) {
    super(dbName, message);
  }
}
