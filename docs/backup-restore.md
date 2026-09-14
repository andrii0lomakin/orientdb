# Backup and Restore (Operator Guide)

This page describes the binary backup files of YouTrackDB. It covers supported backups,
older-backup migration, restore admission, and temporary disk space.

Export and import of a JavaScript Object Notation (JSON) dump is a different procedure.
See [Database Migration Procedure](operator-migration-procedure.md) for that procedure.

## Backup units and backup chains

One backup writes one **backup unit** file with the `.ibu` extension. A **full backup**
starts a chain. Each later **incremental backup** appends one unit to that chain. A restore
replays the full backup and each following increment from the same database.

Each unit carries a small **backup header** at its tail. The header records the database
identifier as a universally unique identifier (UUID). It also records the unit position,
covered log range, and content hash code.

## Run a backup or restore

Java applications use these public entry points:

- Call `YTDBGraphTraversalSource.fullBackup(Path)` to create a full backup.
- Call `YTDBGraphTraversalSource.backup(Path)` to create or extend an incremental chain.
- Call `YouTrackDB.restore(databaseName, path)` to create a database from a backup chain.
- Call `YouTrackDB.restartInterruptedRestore(...)` to restart an interrupted restore.
  This last entry point supports embedded connections only.

Server clients can call the `ytdbFullBackup` and `ytdbIncrementalBackup` Gremlin services.
Each service accepts a string `path` parameter. These service paths refer to the server host.

The `YouTrackDB.restore(...)` overloads also support remote connections. A remote restore path
refers to the server host.

## The header records the database format and creation completion evidence

A backup header of this release also records two semantic values:

- The **database format** identifies the storage features and on-disk storage layout.
- The **creation completion evidence** states that creation of the backed-up database finished.

Both values are admission conditions. Every selected unit must carry the supported database
format and accepted creation completion evidence. A restore refuses a different database format,
even when the chain content would otherwise fit this release.

The header evidence does not replace the final restore checks. After replay, the restore checks
that the restored database finished its creation. The restore writes all restored data to disk
before the database becomes available.

## Older backups are unsupported

A backup unit from an earlier release lacks the database format and creation completion evidence.
This release refuses such a unit in three situations:

- A restore of that chain fails.
- An incremental backup refuses to extend that chain.
- A restart of an interrupted restore from that chain fails.

**Migration:** create a full backup in a new, empty directory. Use
`YTDBGraphTraversalSource.fullBackup(Path)` or the `ytdbFullBackup` service. Keep the old
directory unchanged until the new chain is complete.

A full backup still replaces every unit for its database in the target directory. The new,
empty directory protects the old chain from that overwrite behavior.

## Unreadable backup output needs an operator decision

A crash during a backup write can leave output without a readable header. YouTrackDB cannot
classify that output. It preserves the file and refuses every later incremental extension of
that chain.

Inspect the file before changing it. After confirming that it is interrupted output, move it
outside the backup directory or remove it. Then call `YTDBGraphTraversalSource.backup(Path)` or
the `ytdbIncrementalBackup` service again.

You can instead create a full backup in a new, empty directory. This option leaves the existing
chain unchanged.

An incremental backup automatically removes trailing output only when its header identifies
incomplete output from this release. YouTrackDB never automatically removes an unclassifiable
file. It also performs no automatic cleanup of files left by a process or host crash.

## Restore admission runs before every target change

A restore performs its checks in this order:

1. It checks the database name, target directory, target existence, reserved prefixes, and
   expected database identifier. These checks do not read the backup source or change a file.
2. It copies every selected unit into temporary files and validates the complete chain.
3. It creates the target, replays only the temporary copies, performs final checks, and makes
   the database available.

A fresh restore refuses a database name that already exists. YouTrackDB does not support an
in-place restore. Use a new name, or explicitly drop an unwanted target before a fresh restore.

An unsupported chain therefore leaves no fresh restore target. Source changes after preparation
do not affect the prepared copies or replay.

Preparation does not hold a snapshot lock on the source directory. A concurrent backup can
rotate or remove a unit that preparation already listed. The restore then fails before changing
the target. Retry the restore after that backup finishes.

A concurrent unfinished unit can also fail selection or validation. Retry the restore after the
backup finishes or after resolving the unfinished file.

## Restarting an interrupted restore

An interrupted restore leaves a target in the restore-in-progress state. That target accepts no
session. Two actions resolve such a target:

- Drop the target and restore again.
- Call `YouTrackDB.restartInterruptedRestore(...)` for that target through an embedded connection.

A restart deletes the interrupted target and restores the named backup into the same name. It
validates the complete chain before deletion. It also repeats target eligibility immediately
before deletion. A restart therefore refuses a healthy database without deleting it.

## Temporary disk space and cleanup

A restore needs temporary space for the complete selected chain. An interrupted-restore restart
needs that space beside the interrupted target because preparation happens before deletion.

The temporary files use the host temporary directory. Insufficient temporary space fails the
restore and leaves the target unchanged.

YouTrackDB attempts cleanup when each restore request ends. A preparation-phase failure uses
suppressing cleanup. A cleanup failure is attached to that preparation failure as a suppressed
failure.

After preparation returns, ordinary cleanup applies to target refusal, replay failure, and
successful activation. A cleanup failure only logs a warning that names the temporary path. It
never changes the restore result. The operator must remove that directory manually.

A process or host crash can also leave a temporary directory. YouTrackDB performs no automatic
reclamation of that directory. The operator must remove it manually.
