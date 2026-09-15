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
package com.jetbrains.youtrackdb.api.config;

import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrackdb.internal.core.config.ConfigurationChangeCallback;
import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import javax.annotation.Nullable;

/// Keeps all configuration settings. At startup assigns the configuration values by reading system
/// properties.
@SuppressWarnings("ImmutableEnumChecker")
public enum GlobalConfiguration {
  ENVIRONMENT_DUMP_CFG_AT_STARTUP(
      "youtrackdb.environment.dumpCfgAtStartup",
      "Dumps the configuration during application startup",
      Boolean.class,
      false),

  ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL(
      "youtrackdb.environment.lock.concurrency.level",
      "Concurrency level of lock manager",
      Integer.class,
      Runtime.getRuntime().availableProcessors() << 3,
      false),

  // SCRIPT
  SCRIPT_POOL(
      "youtrackdb.script.pool.maxSize",
      "Maximum number of instances in the pool of script engines",
      Integer.class,
      20),

  SCRIPT_POLYGLOT_USE_GRAAL(
      "youtrackdb.script.polyglot.useGraal", "Use GraalVM as polyglot engine", Boolean.class, true),

  // MEMORY
  MEMORY_USE_UNSAFE(
      "youtrackdb.memory.useUnsafe",
      "Indicates whether Unsafe will be used, if it is present",
      Boolean.class,
      true),

  MEMORY_PROFILING(
      "youtrackdb.memory.profiling",
      "Switches on profiling of allocations of direct memory inside of YouTrackDB.",
      Boolean.class,
      false),

  MEMORY_PROFILING_REPORT_INTERVAL(
      "youtrackdb.memory.profiling.reportInterval",
      "Interval of printing of memory profiling results in minutes",
      Integer.class,
      15),

  MEMORY_LEFT_TO_OS(
      "youtrackdb.memory.leftToOS",
      "Amount of free memory which should be left unallocated in case of YouTrackDB is started"
          + " outside of container. Value can be set as % of total memory provided to YouTrackDB or"
          + " as absolute value in bytes, kilobytes, megabytes or gigabytes. If you set value as"
          + " 10% it means that 10% of memory will not be allocated by YouTrackDB and will be left to"
          + " use by the rest of applications, if 2g value is provided it means that 2 gigabytes of"
          + " memory will be left to use by the rest of applications. Default value is 2g",
      String.class,
      "2g"),

  MEMORY_LEFT_TO_CONTAINER(
      "youtrackdb.memory.leftToContainer",
      "Amount of free memory which should be left unallocated in case of YouTrackDB is started inside"
          + " of container. Value can be set as % of total memory provided to YouTrackDB or as"
          + " absolute value in bytes, kilobytes, megabytes or gigabytes. If you set value as 10%"
          + " it means that 10% of memory will not be allocated by YouTrackDB and will be left to use"
          + " by the rest of applications, if 2g value is provided it means that 2 gigabytes of"
          + " memory will be left to use by the rest of applications. Default value is 256m",
      String.class,
      "256m"),

  DIRECT_MEMORY_POOL_LIMIT(
      "youtrackdb.memory.pool.limit",
      "Limit of the pages cached inside of direct memory pool to avoid frequent reallocation of"
          + " memory in OS",
      Integer.class,
      Integer.MAX_VALUE),

  PAGE_FRAME_POOL_LIMIT(
      "youtrackdb.memory.pageFramePool.limit",
      "Maximum number of PageFrame objects retained in the pool. Default -1 means"
          + " auto-size to 2x the disk cache page count (DISK_CACHE_SIZE / PAGE_SIZE)."
          + " Set explicitly to override. Excessively large values cause heap pressure"
          + " from the allocatedFrames tracking set.",
      Integer.class,
      -1),

  DIRECT_MEMORY_PREALLOCATE(
      "youtrackdb.memory.directMemory.preallocate",
      "Preallocate amount of direct memory which is needed for the disk cache",
      Boolean.class,
      false),

  DIRECT_MEMORY_TRACK_MODE(
      "youtrackdb.memory.directMemory.trackMode",
      "Activates the direct memory pool [leak detector](Leak-Detector.md). This detector causes a"
          + " large overhead and should be used for debugging purposes only. It's also a good idea"
          + " to pass the"
          + " -Djava.util.logging.manager=com.jetbrains.youtrackdb.internal.common.log.ShutdownLogManager"
          + " switch to the JVM, if you use this mode, this will enable the logging from JVM"
          + " shutdown hooks.",
      Boolean.class,
      false),

  DIRECT_MEMORY_ONLY_ALIGNED_ACCESS(
      "youtrackdb.memory.directMemory.onlyAlignedMemoryAccess",
      "Some architectures do not allow unaligned memory access or may suffer from speed"
          + " degradation. For such platforms, this flag should be set to true",
      Boolean.class,
      true),

  // STORAGE
  /**
   * Limit of amount of files which may be open simultaneously
   */
  OPEN_FILES_LIMIT(
      "youtrackdb.storage.openFiles.limit",
      "Limit of amount of files which may be open simultaneously, -1 (default) means automatic"
          + " detection",
      Integer.class,
      -1),

  /**
   * Amount of cached locks is used for component lock in atomic operation to avoid constant
   * creation of new lock instances, default value is 10000.
   */
  COMPONENTS_LOCK_CACHE(
      "youtrackdb.storage.componentsLock.cache",
      "Amount of cached locks is used for component lock to avoid constant creation of new lock"
          + " instances",
      Integer.class,
      10000),

  DISK_CACHE_SIZE(
      "youtrackdb.storage.diskCache.bufferSize", "Size of disk buffer in megabytes", Integer.class,
      4 << 10),

  DISK_WRITE_CACHE_PART(
      "youtrackdb.storage.diskCache.writeCachePart",
      "Percentage of disk cache, which is used as write cache",
      Integer.class,
      5),

  DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL(
      "youtrackdb.storage.diskCache.writeCachePageFlushInterval",
      "Interval between flushing of pages from write cache (in ms)",
      Integer.class,
      25),

  STORAGE_CHECKSUM_MODE(
      "youtrackdb.storage.diskCache.checksumMode",
      "Controls the per-page checksum storage and verification done by the file cache. Possible"
          + " modes: 'off' – checksums are completely off; 'store' – checksums are calculated and"
          + " stored on page flushes, no verification is done on page loads, stored checksums are"
          + " verified only during user-initiated health checks; 'StoreAndVerify' – checksums are"
          + " calculated and stored on page flushes, verification is performed on each page load,"
          + " errors are reported in the log; 'StoreAndThrow' – same as `storeAndVerify` with"
          + " addition of exceptions thrown on errors, this mode is useful for debugging and"
          + " testing, but should be avoided in a production environment;"
          + " 'StoreAndSwitchReadOnlyMode' (default) - Same as 'StoreAndVerify' with addition that"
          + " storage will be switched in read only mode till it will not be repaired.",
      String.class,
      "StoreAndSwitchReadOnlyMode",
      false),

  STORAGE_EXCLUSIVE_FILE_ACCESS(
      "youtrackdb.storage.exclusiveFileAccess",
      "Limit access to the datafiles to the single API user, set to "
          + "true to prevent concurrent modification files by different instances of storage",
      Boolean.class,
      true),

  STORAGE_ENCRYPTION_KEY(
      "youtrackdb.storage.encryptionKey",
      "Contains the storage encryption key. This setting is hidden",
      String.class,
      null,
      false,
      true),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE(
      "youtrackdb.storage.makeFullCheckpointAfterCreate",
      "Indicates whether a full checkpoint should be performed, if storage was created",
      Boolean.class,
      true),

  STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_LIMIT(
      "youtrackdb.storage.atomicOperationsTable.compactionLimit",
      "Limit of size of atomic operations table after which compaction will be triggered on",
      Integer.class,
      10_000),

  STORAGE_CALL_FSYNC(
      "youtrackdb.storage.callFsync",
      "Call the file synchronization system call in the live storage engine, true by default. "
          + "A false value removes the durability barriers on the storage hot path. "
          + "The crash marker and the encryption initialization vector file stay synchronous. "
          + "After a power loss the database can lose recent data. "
          + "The file registry can also lose entries. "
          + "A truncated registry can leave the database unopenable. "
          + "Use false only for test runs.",
      Boolean.class,
      true),

  STORAGE_USE_DOUBLE_WRITE_LOG(
      "youtrackdb.storage.useDoubleWriteLog",
      "Allows usage of double write log in storage. "
          + "This log prevents pages to be teared apart so it is not recommended to switch it off.",
      Boolean.class,
      true),

  STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE(
      "youtrackdb.storage.doubleWriteLog.maxSegSize",
      "Maximum size of double write log segment in megabytes, -1 means that size will be calculated"
          + " automatically",
      Integer.class,
      -1),

  STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT(
      "youtrackdb.storage.doubleWriteLog.maxSegSizePercent",
      "Maximum size of segment of double write log in percents, should be set to value bigger than"
          + " 0",
      Integer.class,
      5),

  STORAGE_DOUBLE_WRITE_LOG_MIN_SEG_SIZE(
      "youtrackdb.storage.doubleWriteLog.minSegSize",
      "Minimum size of segment of double write log in megabytes, should be set to value bigger than"
          + " 0. If both set maximum and minimum size of segments. Minimum size always will have"
          + " priority over maximum size.",
      Integer.class,
      256),

  STORAGE_COLLECTION_VERSION(
      "youtrackdb.storage.collection.version",
      "Binary version of collection which will be used inside of storage",
      Integer.class,
      3),

  STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS(
      "youtrackdb.storage.printWALPerformanceStatistics",
      "Periodically prints statistics about WAL performance",
      Boolean.class,
      false),

  STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL(
      "youtrackdb.storage.walPerformanceStatisticsInterval",
      "Interval in seconds between consequent reports of WAL performance statistics",
      Integer.class,
      10),

  STORAGE_BLOB_COLLECTIONS_COUNT("youtrackdb.storage.blob.collections.count",
      "Amount storage collections allocated for storing blobs", Integer.class,
      8),

  EXPORT_RECORD_SPILL_THRESHOLD("youtrackdb.export.recordSpillThreshold",
      "Size in bytes above which a single record's JSON rendering spills from memory to a"
          + " transient file during database export. Bounds the export's memory use; an"
          + " oversized-but-healthy record is still exported whole, never shed. Operational"
          + " tuning, not correctness.",
      Integer.class,
      32 * 1024 * 1024),

  STORAGE_SNAPSHOT_INDEX_CLEANUP_THRESHOLD(
      "youtrackdb.storage.snapshotIndex.cleanupThreshold",
      "Number of entries in the shared snapshot index that triggers GC of stale entries",
      Integer.class,
      10_000),

  STORAGE_TX_MONITOR_ENABLED(
      "youtrackdb.storage.tx.monitor.enabled",
      "Enable periodic monitoring of long-running transactions that prevent snapshot GC",
      Boolean.class,
      true),

  STORAGE_TX_MONITOR_INTERVAL_SECS(
      "youtrackdb.storage.tx.monitor.intervalSecs",
      "How often (in seconds) the stale transaction monitor runs",
      Integer.class,
      30),

  STORAGE_TX_WARN_TIMEOUT_SECS(
      "youtrackdb.storage.tx.warnTimeoutSecs",
      "Log a WARN after a transaction has been open for this many seconds",
      Integer.class,
      60),

  STORAGE_TX_CRITICAL_TIMEOUT_SECS(
      "youtrackdb.storage.tx.criticalTimeoutSecs",
      "Log an ERROR after a transaction has been open for this many seconds",
      Integer.class,
      300),

  STORAGE_TX_CAPTURE_STACK_TRACE(
      "youtrackdb.storage.tx.captureStackTrace",
      "Capture stack trace at transaction begin for diagnostics (adds overhead per tx begin)",
      Boolean.class,
      false),

  STORAGE_OPTIMISTIC_READ_NULL_RECHECK_REPORT_INTERVAL_SECS(
      "youtrackdb.storage.optimisticRead.nullRecheckReportIntervalSecs",
      "Minimum interval in seconds between reports of the optimistic-read null-recheck"
          + " anomaly detector. On a disagreement (the pinned re-check finds an entry that a"
          + " validated optimistic lookup missed) at most one ERROR is logged per interval"
          + " per storage component.",
      Integer.class,
      60),

  STORAGE_COLLECTION_GC_MIN_THRESHOLD(
      "youtrackdb.storage.collection.gc.minThreshold",
      "Minimum number of dead records in a collection before the records GC is considered."
          + " Avoids thrashing on small collections.",
      Integer.class,
      1_000),

  STORAGE_COLLECTION_GC_SCALE_FACTOR(
      "youtrackdb.storage.collection.gc.scaleFactor",
      "Fraction of collection size (approximate record count) added to the minimum threshold"
          + " to compute the records GC trigger. For example, 0.1 means 10% of live records"
          + " must be dead before GC triggers.",
      Float.class,
      0.1f),

  STORAGE_COLLECTION_GC_PAUSE_INTERVAL(
      "youtrackdb.storage.collection.gc.pauseInterval",
      "Interval in seconds between periodic records GC cycles."
          + " The GC task runs with fixed delay: the next cycle starts this many seconds"
          + " after the previous one completes.",
      Integer.class,
      60),

  WAL_CACHE_SIZE(
      "youtrackdb.storage.wal.cacheSize",
      "Maximum size of WAL cache (in amount of WAL pages, each page is 4k) If set to 0, caching"
          + " will be disabled",
      Integer.class,
      65536),

  WAL_BUFFER_SIZE(
      "youtrackdb.storage.wal.bufferSize",
      "Size of the direct memory WAL buffer which is used inside of "
          + "the background write thread (in MB)",
      Integer.class,
      64),

  WAL_SEGMENTS_INTERVAL(
      "youtrackdb.storage.wal.segmentsInterval",
      "Maximum interval in time in min. after which new WAL segment will be added",
      Integer.class,
      10),

  WAL_MAX_SEGMENT_SIZE(
      "youtrackdb.storage.wal.maxSegmentSize",
      "Maximum size of single WAL segment (in megabytes)",
      Integer.class,
      -1),

  WAL_MAX_SEGMENT_SIZE_PERCENT(
      "youtrackdb.storage.wal.maxSegmentSizePercent",
      "Maximum size of single WAL segment in percent of initial free space",
      Integer.class,
      5),

  WAL_MIN_SEG_SIZE(
      "youtrackdb.storage.wal.minSegSize",
      "Minimal value of maximum WAL segment size in MB",
      Integer.class,
      6 << 10),

  WAL_MIN_COMPRESSED_RECORD_SIZE(
      "youtrackdb.storage.wal.minCompressedRecordSize",
      "Minimum size of record which is needed to be compressed before stored on disk",
      Integer.class,
      8 << 10),

  WAL_MAX_SIZE(
      "youtrackdb.storage.wal.maxSize", "Maximum size of WAL on disk (in megabytes)", Integer.class,
      -1),

  WAL_KEEP_SINGLE_SEGMENT(
      "youtrackdb.storage.wal.keepSingleSegment",
      "Database will provide the best efforts to keep only single WAL inside the storage",
      Boolean.class,
      true),

  WAL_COMMIT_TIMEOUT(
      "youtrackdb.storage.wal.commitTimeout",
      "Maximum interval between WAL commits (in ms.)",
      Integer.class,
      1000),

  WAL_SHUTDOWN_TIMEOUT(
      "youtrackdb.storage.wal.shutdownTimeout",
      "Maximum wait interval between events, when the background flush thread"
          + "receives a shutdown command and when the background flush will be stopped (in ms.)",
      Integer.class,
      10000),

  WAL_FUZZY_CHECKPOINT_INTERVAL(
      "youtrackdb.storage.wal.fuzzyCheckpointInterval",
      "Interval between fuzzy checkpoints (in seconds)",
      Integer.class,
      300),

  WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE(
      "youtrackdb.storage.wal.reportAfterOperationsDuringRestore",
      "Amount of processed log operations, after which status of data restore procedure will be"
          + " printed (0 or a negative value, disables the logging)",
      Integer.class,
      10000),

  WAL_RESTORE_BATCH_SIZE(
      "youtrackdb.storage.wal.restore.batchSize",
      "Amount of WAL records, which are read at once in a single batch during a restore procedure",
      Integer.class,
      1000),

  WAL_LOCATION(
      "youtrackdb.storage.wal.path",
      "Path to the WAL file on the disk. By default, it is placed in the DB directory, but"
          + " it is highly recommended to use a separate disk to store log operations",
      String.class,
      null),

  DISK_CACHE_PAGE_SIZE(
      "youtrackdb.storage.diskCache.pageSize",
      "Size of page of disk buffer (in kilobytes). !!! NEVER CHANGE THIS VALUE !!!",
      Integer.class,
      8),

  DISK_CACHE_FREE_SPACE_LIMIT(
      "youtrackdb.storage.diskCache.diskFreeSpaceLimit",
      "Minimum amount of space on disk, which, when exceeded, "
          + "will cause the database to switch to read-only mode (in megabytes)",
      Long.class,
      8 * WAL_MAX_SEGMENT_SIZE.getValueAsLong()),

  STORAGE_LOCK_TIMEOUT(
      "youtrackdb.storage.lockTimeout",
      "Maximum amount of time (in ms) to lock the storage",
      Integer.class,
      0),

  STORAGE_BACKUP_MINIMUM_TIMEOUT_INTERVAL("youtrackdb.storage.minimumTimeOutInterval",
      "Minimum interval between backups (in seconds). If users performs backup too often, "
          + "storage will wait for this interval,"
          + " as file system metadata update can be performed with delay in some filesystems. "
          + "Default is 5 seconds.",
      Integer.class, 5),

  // DATABASE
  DB_POOL_MIN("youtrackdb.db.pool.min", "Default database pool minimum size", Integer.class, 1),

  DB_POOL_MAX("youtrackdb.db.pool.max", "Default database pool maximum size", Integer.class, 500),

  DB_CACHED_POOL_CAPACITY(
      "youtrackdb.db.cached.pool.capacity", "Default database cached pools capacity", Integer.class,
      100),

  DB_STRING_CAHCE_SIZE(
      "youtrackdb.db.string.cache.size",
      "Number of common string to keep in memory cache",
      Integer.class,
      5000),

  DB_CACHED_POOL_CLEAN_UP_TIMEOUT(
      "youtrackdb.db.cached.pool.cleanUpTimeout",
      "Default timeout for clean up cache from unused or closed database pools, value in"
          + " milliseconds",
      Long.class,
      600_000),

  DB_POOL_ACQUIRE_TIMEOUT(
      "youtrackdb.db.pool.acquireTimeout",
      "Default database pool timeout in milliseconds",
      Integer.class,
      60000),

  DB_VALIDATION(
      "youtrackdb.db.validation", "Enables or disables validation of records", Boolean.class, true,
      true),

  DB_SYSTEM_DATABASE_ENABLED(
      "youtrackdb.systemDatabase.enabled",
      "Enables usage of system database. If disabled, it will turn off the initialization "
          + "of system database and system users in server mode and will initiate an error on "
          + "all attempts to access the system database.",
      Boolean.class, true, true),

  // INDEX
  INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD(
      "youtrackdb.index.embeddedToSbtreeBonsaiThreshold",
      "Amount of values, after which the index implementation will use an sbtree as a values"
          + " container. Set to -1, to disable and force using an sbtree",
      Integer.class,
      40,
      true),

  INDEX_SYNCHRONOUS_AUTO_REBUILD(
      "youtrackdb.index.auto.synchronousAutoRebuild",
      "Synchronous execution of auto rebuilding of indexes, in case of a DB crash",
      Boolean.class,
      true),

  INDEX_IGNORE_NULL_VALUES_DEFAULT(
      "youtrackdb.index.ignoreNullValuesDefault",
      "Controls whether null values will be ignored by default "
          + "by newly created indexes or not (false by default)",
      Boolean.class,
      false),

  INDEX_CURSOR_PREFETCH_SIZE(
      "youtrackdb.index.stream.prefetchSize", "Default prefetch size of index stream",
      Integer.class, 10),

  // SBTREE
  BTREE_MAX_DEPTH(
      "youtrackdb.btree.maxDepth",
      "Maximum depth of sbtree, which will be traversed during key look up until it will be treated"
          + " as broken (64 by default)",
      Integer.class,
      64),

  BTREE_MAX_KEY_SIZE(
      "youtrackdb.btree.maxKeySize",
      "Maximum size of a key, which can be put in the SBTree in bytes (-1 by default, calculated"
          + " automatically from the page size)",
      Integer.class,
      -1),

  BTREE_MAX_EMBEDDED_VALUE_SIZE(
      "youtrackdb.btree.maxEmbeddedValueSize",
      "Maximum size of value which can be put in an SBTree without creation link to a standalone"
          + " page in bytes (40960 by default)",
      Integer.class,
      40960),

  // LINK COLLECTION
  LINK_COLLECTION_EMBEDDED_DEFAULT_SIZE(
      "youtrackdb.linkcollection.embeddedDefaultSize",
      "Size of embedded collection, when created (empty)",
      Integer.class,
      4),

  LINK_COLLECTION_EMBEDDED_TO_BTREE_THRESHOLD(
      "youtrackdb.linkcollection.embeddedToBtreeThreshold",
      "Amount of values after which a link-based collection implementation will use btree as values container."
          + " Set to -1 to always use an btree",
      Integer.class,
      40,
      true),

  LINK_COLLECTION_BTREE_TO_EMBEDDED_THRESHOLD(
      "youtrackdb.linkcollection.btreeToEmbeddedToThreshold",
      "Amount of values, after which a link-based collection implementation will use an embedded values container"
          + " (disabled by default)",
      Integer.class,
      -1,
      true),

  FILE_LOCK("youtrackdb.file.lock", "Locks files when used. Default is true", boolean.class, true),

  FILE_LOG_DELETION(
      "youtrackdb.file.log.deletion", "Log file deletion (true by default)", boolean.class, true),

  FILE_DELETE_DELAY(
      "youtrackdb.file.deleteDelay",
      "Delay time (in ms) to wait for another attempt to delete a locked file",
      Integer.class,
      10),

  FILE_DELETE_RETRY(
      "youtrackdb.file.deleteRetry", "Number of retries to delete a locked file", Integer.class,
      50),

  // SECURITY
  SECURITY_USER_PASSWORD_SALT_ITERATIONS(
      "youtrackdb.security.userPasswordSaltIterations",
      "Number of iterations to generate the salt or user password. Changing this setting does not"
          + " affect stored passwords",
      Integer.class,
      65536),

  SECURITY_USER_PASSWORD_SALT_CACHE_SIZE(
      "youtrackdb.security.userPasswordSaltCacheSize",
      "Cache size of hashed salt passwords. The cache works as LRU. Use 0 to disable the cache",
      Integer.class,
      500),

  SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM(
      "youtrackdb.security.userPasswordDefaultAlgorithm",
      "Default encryption algorithm used for passwords hashing",
      String.class,
      "PBKDF2WithHmacSHA256"),

  // NETWORK
  NETWORK_MAX_CONCURRENT_SESSIONS(
      "youtrackdb.network.maxConcurrentSessions",
      "Maximum number of concurrent sessions",
      Integer.class,
      1000,
      true),

  NETWORK_SOCKET_BUFFER_SIZE(
      "youtrackdb.network.socketBufferSize",
      "TCP/IP Socket buffer size, if 0 use the OS default",
      Integer.class,
      0,
      true),

  NETWORK_LOCK_TIMEOUT(
      "youtrackdb.network.lockTimeout",
      "Timeout (in ms) to acquire a lock against a channel",
      Integer.class,
      15000,
      true),

  NETWORK_SOCKET_TIMEOUT(
      "youtrackdb.network.socketTimeout", "TCP/IP Socket timeout (in ms)", Integer.class, 15000,
      true),

  NETWORK_REQUEST_TIMEOUT(
      "youtrackdb.network.requestTimeout",
      "Request completion timeout (in ms)",
      Integer.class,
      3600000 /* one hour */,
      true),

  NETWORK_SOCKET_RETRY(
      "youtrackdb.network.retry",
      "Number of attempts to connect to the server on failure",
      Integer.class,
      5,
      true),

  NETWORK_SOCKET_RETRY_DELAY(
      "youtrackdb.network.retryDelay",
      "The time (in ms) the client must wait, before reconnecting to the server on failure",
      Integer.class,
      500,
      true),

  NETWORK_BINARY_DNS_LOADBALANCING_ENABLED(
      "youtrackdb.network.binary.loadBalancing.enabled",
      "Asks for DNS TXT record, to determine if load balancing is supported",
      Boolean.class,
      false,
      true),

  NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT(
      "youtrackdb.network.binary.loadBalancing.timeout",
      "Maximum time (in ms) to wait for the answer from DNS about the TXT record for load"
          + " balancing",
      Integer.class,
      2000,
      true),

  NETWORK_BINARY_MAX_CONTENT_LENGTH(
      "youtrackdb.network.binary.maxLength",
      "TCP/IP max content length (in KB) of BINARY requests",
      Integer.class,
      16384,
      true),

  NETWORK_BINARY_MIN_PROTOCOL_VERSION(
      "youtrackdb.network.binary.minProtocolVersion",
      "Set the minimum enabled binary protocol version and disable all backward compatible"
          + " behaviour for version previous the one specified",
      Integer.class,
      1,
      false),

  NETWORK_BINARY_DEBUG(
      "youtrackdb.network.binary.debug",
      "Debug mode: print all data incoming on the binary channel",
      Boolean.class,
      false,
      true),

  NETWORK_BINARY_ALLOW_NO_TOKEN(
      "youtrackdb.network.binary.allowNoToken",
      "Backward compatibility option to allow binary connections without tokens (STRONGLY"
          + " DISCOURAGED, FOR SECURITY REASONS)",
      Boolean.class,
      false,
      true),

  // HTTP
  NETWORK_HTTP_INSTALL_DEFAULT_COMMANDS(
      "youtrackdb.network.http.installDefaultCommands",
      "Installs the default HTTP commands",
      Boolean.class,
      true,
      true),

  NETWORK_HTTP_SERVER_INFO(
      "youtrackdb.network.http.serverInfo",
      "Server info to send in HTTP responses. Change the default if you want to hide it is a"
          + " YouTrackDB Server",
      String.class,
      "YouTrackDB Server v." + YouTrackDBConstants.getVersion(),
      true),

  NETWORK_HTTP_MAX_CONTENT_LENGTH(
      "youtrackdb.network.http.maxLength",
      "TCP/IP max content length (in bytes) for HTTP requests",
      Integer.class,
      1000000,
      true),

  NETWORK_HTTP_STREAMING(
      "youtrackdb.network.http.streaming",
      "Enable Http chunked streaming for json responses",
      Boolean.class,
      false,
      true),

  NETWORK_HTTP_CONTENT_CHARSET(
      "youtrackdb.network.http.charset", "Http response charset", String.class, "utf-8", true),

  NETWORK_HTTP_JSON_RESPONSE_ERROR(
      "youtrackdb.network.http.jsonResponseError", "Http response error in json", Boolean.class,
      true, true),

  NETWORK_HTTP_JSONP_ENABLED(
      "youtrackdb.network.http.jsonp",
      "Enable the usage of JSONP, if requested by the client. The parameter name to use is"
          + " 'callback'",
      Boolean.class,
      false,
      true),

  NETWORK_HTTP_SESSION_EXPIRE_TIMEOUT(
      "youtrackdb.network.http.sessionExpireTimeout",
      "Timeout, after which an http session is considered to have expired (in seconds)",
      Integer.class,
      900),

  NETWORK_HTTP_SESSION_COOKIE_SAME_SITE(
      "youtrackdb.network.http.session.cookie.sameSite",
      "Activate the same site cookie session",
      Boolean.class,
      true),

  NETWORK_HTTP_USE_TOKEN(
      "youtrackdb.network.http.useToken", "Enable Token based sessions for http", Boolean.class,
      false),

  NETWORK_TOKEN_SECRETKEY(
      "youtrackdb.network.token.secretKey", "Network token sercret key", String.class, "", false,
      true),

  NETWORK_TOKEN_ENCRYPTION_ALGORITHM(
      "youtrackdb.network.token.encryptionAlgorithm", "Network token algorithm", String.class,
      "HmacSHA256"),

  NETWORK_TOKEN_EXPIRE_TIMEOUT(
      "youtrackdb.network.token.expireTimeout",
      "Timeout, after which a binary session is considered to have expired (in minutes)",
      Integer.class,
      60),

  INIT_IN_SERVLET_CONTEXT_LISTENER(
      "youtrackdb.initInServletContextListener",
      "If this value set to ture (default) YouTrackDB engine "
          + "will be initialzed using embedded ServletContextListener",
      Boolean.class,
      true),

  PROFILER_MEMORYCHECK_INTERVAL(
      "youtrackdb.profiler.memoryCheckInterval",
      "Checks the memory usage every configured milliseconds. Use 0 to disable it",
      Long.class,
      120000),

  PROFILER_TICKER_GRANULARITY(
      "youtrackdb.profiler.tickerGranularity",
      "Granularity (in nanoseconds) of the profiler ticker. Controls the precision of metrics"
          + " collection and long-running queries detection.",
      Long.class,
      10_000_000L),

  PROFILER_TICKER_ADJUSTMENT_RATE(
      "youtrackdb.profiler.tickerAdjustmentRate",
      "Adjustment rate (in nanoseconds) of the ticker's absolute time. Internally, ticker "
          + "approximates absolute time based on system time and nanotime difference. This parameter "
          + "controls how often the ticker adjusts this difference. Default value is 10 seconds.",
      Long.class,
      10_000_000_000L),

  // SEQUENCES

  SEQUENCE_MAX_RETRY(
      "youtrackdb.sequence.maxRetry",
      "Maximum number of retries between attempt to change a sequence in concurrent mode",
      Integer.class,
      1_000),

  SEQUENCE_RETRY_DELAY(
      "youtrackdb.sequence.retryDelay",
      "Maximum number of ms to wait between concurrent modification exceptions. The value is"
          + " computed as random between 1 and this number",
      Integer.class,
      200),

  // CLASS
  CLASS_COLLECTIONS_COUNT(
      "youtrackdb.class.collectionsCount",
      "Minimum collections to create when a new class is created. 0 means Automatic",
      Integer.class,
      8),

  // LOG
  LOG_SUPPORTS_ANSI(
      "youtrackdb.log.console.ansi",
      "ANSI Console support. 'auto' means automatic check if it is supported, 'true' to force using"
          + " ANSI, 'false' to avoid using ANSI",
      String.class,
      "auto"),

  // CACHE
  CACHE_LOCAL_IMPL(
      "youtrackdb.cache.local.impl",
      "Local Record cache implementation",
      String.class,
      "com.jetbrains.youtrackdb.internal.core.cache.RecordCacheWeakRefs"),

  // COMMAND
  COMMAND_TIMEOUT("youtrackdb.command.timeout",
      "Default timeout for commands (in ms)", Long.class, 0, true),

  // QUERY
  QUERY_REMOTE_RESULTSET_PAGE_SIZE(
      "youtrackdb.query.remoteResultSet.pageSize",
      "The size of a remote ResultSet page, ie. the number of recordsthat are fetched together"
          + " during remote query execution. This has to be set on the client.",
      Integer.class,
      1000),

  QUERY_LET_MATERIALIZATION_MAX_SIZE(
      "youtrackdb.query.letMaterialization.maxSize",
      "Maximum number of records to materialize when sharing a common inner subquery"
          + " across multiple correlated LET clauses. If the inner subquery returns more"
          + " records than this limit, each LET clause executes independently.",
      Integer.class,
      10000,
      true),

  QUERY_MATCH_HASH_JOIN_THRESHOLD(
      "youtrackdb.query.match.hashJoinThreshold",
      "Maximum estimated build-side cardinality for the MATCH hash join"
          + " optimization. When the planner estimates that a NOT sub-pattern or"
          + " secondary branch produces more rows than this threshold, it falls"
          + " back to nested-loop evaluation. Set to 0 to disable hash join.",
      Long.class,
      10000L,
      true),

  QUERY_MATCH_HASH_JOIN_UPSTREAM_MIN(
      "youtrackdb.query.match.hashJoinUpstreamMin",
      "Minimum upstream (probe-side) cardinality for the MATCH hash join"
          + " optimization. When the probe side has fewer rows than this value,"
          + " hash join is skipped because nested-loop is already fast."
          + " Set to 0 to bypass upstream and cost-based guards"
          + " (only the build-side threshold applies).",
      Long.class,
      5L,
      true),

  QUERY_MATCH_CORRELATED_CACHE_SIZE(
      "youtrackdb.query.match.correlatedCacheSize",
      "Maximum number of entries in the LRU neighbor cache used by the"
          + " correlated optional hash join step. Higher values reduce rebuilds"
          + " when upstream rows interleave many distinct correlated vertices,"
          + " at the cost of memory.",
      Integer.class,
      16,
      true),

  QUERY_PARALLEL_AUTO(
      "youtrackdb.query.parallelAuto",
      "Auto enable parallel query, if requirements are met",
      Boolean.class,
      false),

  QUERY_PARALLEL_MINIMUM_RECORDS(
      "youtrackdb.query.parallelMinimumRecords",
      "Minimum number of records to activate parallel query automatically",
      Long.class,
      300000),

  QUERY_PARALLEL_RESULT_QUEUE_SIZE(
      "youtrackdb.query.parallelResultQueueSize",
      "Size of the queue that holds results on parallel execution. The queue is blocking, so in"
          + " case the queue is full, the query threads will be in a wait state",
      Integer.class,
      20000),

  QUERY_SCAN_BATCH_SIZE(
      "youtrackdb.query.scanBatchSize",
      "Scan collections in blocks of records. This setting reduces the lock time on the collection during"
          + " scans. A high value mean a faster execution, but also a lower concurrency level. Set"
          + " to 0 to disable batch scanning. Disabling batch scanning is suggested for read-only"
          + " databases only",
      Long.class,
      1000),

  QUERY_ONLY_GREMLIN_DDL("youtrackdb.query.onlyGremlinDDL",
      "Only Gremlin can be used for DDL queries (true by default)", Boolean.class, true),

  QUERY_LIMIT_THRESHOLD_TIP(
      "youtrackdb.query.limitThresholdTip",
      "If the total number of returned records exceeds this value, then a warning is given. (Use 0"
          + " to disable)",
      Long.class,
      10000),

  QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP(
      "youtrackdb.query.maxHeapElementsAllowedPerOp",
      "Maximum number of entities (records) allowed in a single query for memory-intensive"
          + " operations (eg. ORDER BY in heap). If exceeded, the query fails with an"
          + " CommandExecutionException. Negative number means no limit.This setting is intended"
          + " as a safety measure against excessive resource consumption from a single query (eg."
          + " prevent OutOfMemory)",
      Long.class,
      500_000),

  QUERY_LIVE_SUPPORT(
      "youtrackdb.query.live.support",
      "Enable/Disable the support of live query. (Use false to disable)",
      Boolean.class,
      true),

  QUERY_RESULT_SET_OPEN_WARNING_THRESHOLD(
      "youtrackdb.query.resultSetOpenThresholdWarning",
      "Number of simultaneous open result sets to warn about. Negative number means no warning.",
      Integer.class,
      10),

  QUERY_GREMLIN_POLYMORPHIC_BY_DEFAULT(
      "youtrackdb.query.gremlin.polymorphicByDefault",
      "Controls the default behavior of hasLabel step in Gremlin queries. True means that"
          + " queries are polymorphic, unless configured otherwise using GraphTraversalSource#with. True by default.",
      Boolean.class,
      true),

  QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY(
      "youtrackdb.query.gremlin.orderIncludesMissingKey",
      "Controls whether a global-scope Gremlin order() step keeps a record that does not carry"
          + " the ordered property. True, the default, orders such a record as a null key exactly"
          + " as YQL ORDER BY does. False restores standard order semantics, where the"
          + " by-modulator produces nothing and the record is dropped. Local-scope order and the"
          + " select, values, group and dedup modulators are unaffected either way. A single"
          + " traversal overrides the default with"
          + " GraphTraversalSource#with(YTDBQueryConfigParam.orderIncludesMissingKey, value).",
      Boolean.class,
      true,
      true),

  QUERY_TX_RESULT_CACHE_ENABLED(
      "youtrackdb.query.txResultCache.enabled",
      "Enable the per-transaction query result cache. When true, repeated idempotent SELECT/MATCH"
          + " queries within a transaction return cached results merged with in-transaction"
          + " mutations, identical to a fresh uncached execution. Off by default; enabling it never"
          + " changes result cardinality.",
      Boolean.class,
      false,
      true),

  QUERY_TX_RESULT_CACHE_MAX_ENTRIES(
      "youtrackdb.query.txResultCache.maxEntries",
      "Maximum number of cached query results retained per transaction (LRU eviction). Entries with"
          + " live result-set views are exempt from eviction, so the map may grow transiently above"
          + " this bound while a view iterates.",
      Integer.class,
      200,
      true),

  QUERY_TX_RESULT_CACHE_MAX_RECORDS_PER_ENTRY(
      "youtrackdb.query.txResultCache.maxRecordsPerEntry",
      "Per-entry cap on the number of records a cached query result may hold (and, for a cached"
          + " aggregate, the size of its per-contributor collections). When populating crosses this"
          + " cap the entry overflows: its key is marked non-cacheable for the rest of the"
          + " transaction (and an already-stored entry is removed), while the consumer still"
          + " receives every result from the live stream.",
      Integer.class,
      10000,
      true),

  QUERY_TX_RESULT_CACHE_K0_NONE_INVALIDATION_THRESHOLD(
      "youtrackdb.query.txResultCache.deltaUnreconcilableInvalidationThreshold",
      "Number of times a delta-unreconcilable cached entry may be invalidated by an"
          + " intervening mutation before its key is routed to the per-transaction non-cacheable"
          + " set, bypassing the cache for the remainder of the transaction. Bounds repopulate"
          + " churn for write-heavy fragments without penalising pure-read repeats.",
      Integer.class,
      3,
      true),

  QUERY_TX_RESULT_CACHE_MULTI_INVALIDATION_THRESHOLD(
      "youtrackdb.query.txResultCache.matchMultiInvalidationThreshold",
      "Number of times a multi alias match cached entry may be invalidated by an"
          + " intervening mutation before its key is routed to the per-transaction non-cacheable"
          + " set, bypassing the cache for the remainder of the transaction. Bounds repopulate"
          + " churn for write-heavy fragments without penalising pure-read repeats.",
      Integer.class,
      3,
      true),

  QUERY_GREMLIN_TO_MATCH_TRANSLATOR_ENABLED(
      "youtrackdb.query.gremlin.toMatchTranslator.enabled",
      "Enables the Gremlin-to-MATCH translator strategy. When true, a Gremlin traversal"
          + " whose every step is in the recognized set is rewritten to a MATCH execution"
          + " plan so the existing cost-based planner can pick join order and use index"
          + " statistics; any unrecognized step declines the whole traversal to native"
          + " TinkerPop execution unchanged (all-or-nothing). Set to false to bypass"
          + " translation entirely (runtime kill-switch). True by default.",
      Boolean.class,
      true),

  STATEMENT_CACHE_SIZE(
      "youtrackdb.statement.cacheSize",
      "Number of parsed SQL statements kept in cache. Zero means cache disabled",
      Integer.class,
      100),

  /**
   * Maximum size of pool of network channels between client and server. A channel is a TCP/IP
   * connection.
   */
  CLIENT_CHANNEL_MAX_POOL(
      "youtrackdb.client.channel.maxPool",
      "Maximum size of pool of network channels between client and server. A channel is a TCP/IP"
          + " connection",
      Integer.class,
      100),

  /**
   * Maximum time, where the client should wait for a connection from the pool, when all connections
   * busy.
   */
  CLIENT_CONNECT_POOL_WAIT_TIMEOUT(
      "youtrackdb.client.connectionPool.waitTimeout",
      "Maximum time, where the client should wait for a connection from the pool, when all"
          + " connections busy",
      Integer.class,
      5000,
      true),

  CLIENT_DB_RELEASE_WAIT_TIMEOUT(
      "youtrackdb.client.channel.dbReleaseWaitTimeout",
      "Delay (in ms), after which a data modification command will be resent, if the DB was frozen",
      Integer.class,
      10000,
      true),

  CLIENT_USE_SSL("youtrackdb.client.ssl.enabled", "Use SSL for client connections", Boolean.class,
      false),

  CLIENT_SSL_KEYSTORE("youtrackdb.client.ssl.keyStore", "Use SSL for client connections",
      String.class, null),

  CLIENT_SSL_KEYSTORE_PASSWORD(
      "youtrackdb.client.ssl.keyStorePass", "Use SSL for client connections", String.class, null,
      false, true),

  CLIENT_SSL_TRUSTSTORE(
      "youtrackdb.client.ssl.trustStore", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_TRUSTSTORE_PASSWORD(
      "youtrackdb.client.ssl.trustStorePass",
      "Use SSL for client connections",
      String.class,
      null,
      false,
      true),

  // SERVER
  SERVER_OPEN_ALL_DATABASES_AT_STARTUP(
      "youtrackdb.server.openAllDatabasesAtStartup",
      "If true, the server opens all the available databases at startup. Available since 2.2",
      Boolean.class,
      false),

  SERVER_CHANNEL_CLEAN_DELAY(
      "youtrackdb.server.channel.cleanDelay",
      "Time in ms of delay to check pending closed connections",
      Integer.class,
      5000),

  SERVER_CACHE_FILE_STATIC(
      "youtrackdb.server.cache.staticFile", "Cache static resources upon loading", Boolean.class,
      false),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL(
      "youtrackdb.server.log.dumpClientExceptionLevel",
      "Logs client exceptions. Use any level supported by Java java.util.logging.Level class: OFF,"
          + " FINE, CONFIG, INFO, WARNING, SEVERE",
      String.class,
      Level.FINE.getName()),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE(
      "youtrackdb.server.log.dumpClientExceptionFullStackTrace",
      "Dumps the full stack trace of the exception sent to the client",
      Boolean.class,
      false,
      true),

  CLIENT_KRB5_CONFIG(
      "youtrackdb.client.krb5.config", "Location of the Kerberos configuration file", String.class,
      null),

  CLIENT_KRB5_CCNAME(
      "youtrackdb.client.krb5.ccname", "Location of the Kerberos client ticketcache", String.class,
      null),

  CLIENT_KRB5_KTNAME(
      "youtrackdb.client.krb5.ktname", "Location of the Kerberos client keytab", String.class,
      null),

  CLIENT_CONNECTION_STRATEGY(
      "youtrackdb.client.connection.strategy",
      "Strategy used for open connections from a client in case of multiple servers, possible"
          + " options:STICKY, ROUND_ROBIN_CONNECT, ROUND_ROBIN_REQUEST",
      String.class,
      null),

  CLIENT_CONNECTION_FETCH_HOST_LIST(
      "youtrackdb.client.connection.fetchHostList",
      "If set true fetch the list of other possible hosts from the distributed environment ",
      Boolean.class,
      true),

  CLIENT_CREDENTIAL_INTERCEPTOR(
      "youtrackdb.client.credentialinterceptor",
      "The name of the CredentialInterceptor class",
      String.class,
      null),

  CLIENT_CI_KEYALGORITHM(
      "youtrackdb.client.ci.keyalgorithm",
      "The key algorithm used by the symmetric key credential interceptor",
      String.class,
      "AES"),

  CLIENT_CI_CIPHERTRANSFORM(
      "youtrackdb.client.ci.ciphertransform",
      "The cipher transformation used by the symmetric key credential interceptor",
      String.class,
      "AES/CBC/PKCS5Padding"),

  CLIENT_CI_KEYSTORE_FILE(
      "youtrackdb.client.ci.keystore.file",
      "The file path of the keystore used by the symmetric key credential interceptor",
      String.class,
      null),

  CLIENT_CI_KEYSTORE_PASSWORD(
      "youtrackdb.client.ci.keystore.password",
      "The password of the keystore used by the symmetric key credential interceptor",
      String.class,
      null,
      false,
      true),

  CREATE_DEFAULT_USERS(
      "youtrackdb.security.createDefaultUsers",
      "Indicates whether default database users should be created",
      Boolean.class,
      false), WARNING_DEFAULT_USERS(
          "youtrackdb.security.warningDefaultUsers",
          "Indicates whether access with default users should show a warning",
          Boolean.class,
          true),

  SERVER_SECURITY_FILE(
      "youtrackdb.server.security.file",
      "Location of the YouTrackDB security.json configuration file",
      String.class,
      null),

  SPATIAL_ENABLE_DIRECT_WKT_READER(
      "youtrackdb.spatial.enableDirectWktReader",
      "Enable direct usage of WKTReader for additional dimension info",
      Boolean.class,
      false),

  AUTO_CLOSE_AFTER_DELAY(
      "youtrackdb.storage.autoCloseAfterDelay",
      "Enable auto close of storage after a specified delay if no session are active",
      Boolean.class,
      false),

  AUTO_CLOSE_DELAY(
      "youtrackdb.storage.autoCloseDelay",
      "Storage auto close delay time in minutes", Integer.class, 20),

  CLIENT_CHANNEL_IDLE_CLOSE(
      "youtrackdb.client.channel.idleAutoClose",
      "Enable the automatic close of idle sockets after a specific timeout",
      Boolean.class,
      false),

  TX_BATCH_SIZE(
      "youtrackdb.tx.batchSize",
      "Size of the single batch by default in case of call to the"
          + " DatabaseSessionEmbedded#executeInTxBatches call. 1_000 by default.",
      Integer.class,
      1_000),

  CLIENT_CHANNEL_IDLE_TIMEOUT(
      "youtrackdb.client.channel.idleTimeout", "sockets maximum time idle in seconds",
      Integer.class, 900),

  EXECUTOR_DEBUG_TRACE_SOURCE(
      "youtrackdb.executor.debug.traceSource",
      "Enable tracing of the source that submit a task in database executor in case of exception",
      Boolean.class,
      false),

  EXECUTOR_POOL_MAX_SIZE(
      "youtrackdb.executor.pool.maxSize",
      "Maximum number of threads in the executor pool"
          + " (-1 will base the size on the number CPUs)",
      Integer.class,
      -1),

  EXECUTOR_POOL_IO_MAX_SIZE(
      "youtrackdb.executor.pool.io.maxSize",
      "Maximum number of threads in the IO executor pool"
          + " (-1 will base the size on the number CPUs)",
      Integer.class,
      -1),

  EXECUTOR_POOL_IO_ENABLED(
      "youtrackdb.executor.pool.io.enabled",
      "Flag to use the executor pool for IO, default enabled",
      Boolean.class,
      true),

  // ---- Histogram / query statistics configuration ----

  QUERY_STATS_DEFAULT_SELECTIVITY(
      "youtrackdb.query.stats.defaultSelectivity",
      "Default selectivity fraction for non-indexed or unknown predicates",
      Double.class,
      0.1,
      true),

  QUERY_STATS_DEFAULT_FAN_OUT(
      "youtrackdb.query.stats.defaultFanOut",
      "Default edge fan-out when schema metadata is unavailable",
      Double.class,
      10.0,
      true),

  QUERY_STATS_HISTOGRAM_BUCKETS(
      "youtrackdb.query.stats.histogramBuckets",
      "Maximum number of equi-depth histogram buckets per index."
          + " Takes effect on the next histogram build or rebalance",
      Integer.class,
      128,
      true),

  QUERY_STATS_HISTOGRAM_MIN_SIZE(
      "youtrackdb.query.stats.histogramMinSize",
      "Minimum number of non-null index entries required to build a histogram",
      Integer.class,
      1000,
      true),

  QUERY_STATS_REBALANCE_MUTATION_FRACTION(
      "youtrackdb.query.stats.rebalanceMutationFraction",
      "Fraction of totalCountAtLastBuild that triggers histogram rebalance",
      Double.class,
      0.3,
      true),

  QUERY_STATS_MIN_REBALANCE_MUTATIONS(
      "youtrackdb.query.stats.minRebalanceMutations",
      "Minimum absolute mutation count before histogram rebalance is triggered",
      Long.class,
      1000L,
      true),

  QUERY_STATS_MAX_REBALANCE_MUTATIONS(
      "youtrackdb.query.stats.maxRebalanceMutations",
      "Maximum rebalance threshold (caps the fraction-based computation)",
      Long.class,
      10_000_000L,
      true),

  QUERY_STATS_MAX_BOUNDARY_BYTES(
      "youtrackdb.query.stats.maxBoundaryBytes",
      "Maximum serialized size in bytes for a single histogram boundary key."
          + " Takes effect on the next histogram build or rebalance",
      Integer.class,
      256,
      true),

  QUERY_STATS_PERSIST_BATCH_SIZE(
      "youtrackdb.query.stats.persistBatchSize",
      "Number of committed mutations before flushing histogram stats to disk",
      Integer.class,
      500,
      true),

  QUERY_STATS_REBALANCE_FAILURE_COOLDOWN(
      "youtrackdb.query.stats.rebalanceFailureCooldown",
      "Cooldown period in milliseconds after a rebalance failure before retrying",
      Long.class,
      60_000L,
      true),

  QUERY_STATS_MAX_CONCURRENT_REBALANCES(
      "youtrackdb.query.stats.maxConcurrentRebalances",
      "Maximum concurrent histogram rebalance tasks per storage."
          + " -1 = auto: max(2, availableProcessors / 4)."
          + " Requires database restart to take effect",
      Integer.class,
      -1),

  QUERY_STATS_COST_SEQ_PAGE_READ(
      "youtrackdb.query.stats.costSeqPageRead",
      "Cost of a sequential page read (baseline unit for cost model). "
          + "Internal tuning knob — not intended for end-user configuration.",
      Double.class,
      1.0,
      true,
      true),

  QUERY_STATS_COST_RANDOM_PAGE_READ(
      "youtrackdb.query.stats.costRandomPageRead",
      "Cost of a random page read relative to sequential. "
          + "Internal tuning knob — not intended for end-user configuration.",
      Double.class,
      4.0,
      true,
      true),

  QUERY_STATS_COST_PER_ROW_CPU(
      "youtrackdb.query.stats.costPerRowCpu",
      "Per-row CPU cost for filtering and comparison. "
          + "Internal tuning knob — not intended for end-user configuration.",
      Double.class,
      0.01,
      true,
      true),

  QUERY_STATS_DEFAULT_INDEX_TREE_DEPTH(
      "youtrackdb.query.stats.defaultIndexTreeDepth",
      "Assumed B-tree depth for index seek cost estimation. "
          + "Internal tuning knob — not intended for end-user configuration.",
      Integer.class,
      4,
      true,
      true),

  // ---- Pre-filter (adaptive RidSet intersection) configuration ----

  QUERY_PREFILTER_MAX_RIDSET_SIZE(
      "youtrackdb.query.prefilter.maxRidSetSize",
      "Maximum number of RIDs collected from an index or reverse edge lookup"
          + " before aborting the pre-filter build (memory protection cap)."
          + " Auto-scaled to 0.5% of max heap, clamped to [100K, 10M]."
          + " Set explicitly to override auto-scaling",
      Integer.class,
      (int) Math.min(10_000_000L,
          Math.max(100_000L, Runtime.getRuntime().maxMemory() / 200)),
      true),

  QUERY_PREFILTER_EDGE_LOOKUP_MAX_RATIO(
      "youtrackdb.query.prefilter.edgeLookupMaxRatio",
      "Maximum ratio of ridSetSize/linkBagSize for EdgeRidLookup pre-filters."
          + " Above this the overlap is too high to be useful.",
      Double.class,
      0.8,
      true),

  QUERY_PREFILTER_INDEX_LOOKUP_MAX_SELECTIVITY(
      "youtrackdb.query.prefilter.indexLookupMaxSelectivity",
      "Maximum selectivity (estimateHits/totalCount) for IndexLookup"
          + " pre-filters. Above this the condition matches too many records."
          + " Independent of edgeLookupMaxRatio (different semantics).",
      Double.class,
      0.95,
      true),

  QUERY_PREFILTER_MIN_LINKBAG_SIZE(
      "youtrackdb.query.prefilter.minLinkBagSize",
      "Minimum link bag size below which pre-filtering is skipped entirely."
          + " Loading a few records is cheaper than building a RidSet",
      Integer.class,
      50,
      true),

  QUERY_PREFILTER_LOAD_TO_SCAN_RATIO(
      "youtrackdb.query.prefilter.loadToScanRatio",
      "Cost ratio of random record load vs. RidSet scan entry, used in"
          + " the IndexLookup build amortization formula. Calibrated for"
          + " cold SSD storage; override with a positive value to tune for"
          + " specific hardware",
      Double.class,
      100.0,
      true),

  // ---- MATCH chain-fold cost-model configuration ----

  QUERY_MATCH_CHAIN_FOLD_MAX_HOPS(
      "youtrackdb.query.match.chainFold.maxHops",
      "Maximum chain depth for the cost-fold during MATCH plan optimization."
          + " The fold propagates downstream WHERE selectivities and edge"
          + " fan-outs into the first edge's cost so that the planner sorts"
          + " competing branches by their full chain selectivity, not just"
          + " the single-hop leading edge. A linear-chain walk terminates"
          + " naturally at branch points or visited nodes, so this knob is"
          + " a defense-in-depth cap rather than the typical bound. The"
          + " default (10) matches the planner's WHILE-depth default and"
          + " covers the chain depths real queries reach; raise it for unusually"
          + " deep linear MATCH patterns. Set to 1 to restrict the fold to"
          + " the immediate downstream vertex; note that 1 still folds that"
          + " vertex, so only 0 restores the schedule the planner produced"
          + " before the fold existed. Set to 0"
          + " to disable the fold entirely (rollback-only safety valve)."
          + " Read once per plan construction, so it takes effect on"
          + " restart like every other setting here.",
      Integer.class,
      10,
      true),

  /**
   * Null placement for ascending {@code ORDER BY} items and Gremlin {@code Order.asc} when no
   * explicit placement is given. Per-storage values override the runtime global.
   */
  QUERY_ORDER_BY_NULLS_PLACEMENT_ASC(
      "youtrackdb.query.orderBy.nullsPlacementAsc",
      "Null placement for ascending ORDER BY items and Gremlin Order.asc. Ships as FIRST. "
          + "Per-storage settings override the runtime global. "
          + "An explicit NULLS FIRST/LAST clause overrides this setting.",
      OrderByNullsPlacement.class,
      OrderByNullsPlacement.FIRST,
      true),

  /**
   * Null placement for descending {@code ORDER BY} items and Gremlin {@code Order.desc} when no
   * explicit placement is given. Per-storage values override the runtime global.
   */
  QUERY_ORDER_BY_NULLS_PLACEMENT_DESC(
      "youtrackdb.query.orderBy.nullsPlacementDesc",
      "Null placement for descending ORDER BY items and Gremlin Order.desc. Ships as LAST. "
          + "Per-storage settings override the runtime global. "
          + "An explicit NULLS FIRST/LAST clause overrides this setting.",
      OrderByNullsPlacement.class,
      OrderByNullsPlacement.LAST,
      true),

  QUERY_INDEX_ORDERED_MIN_LINKBAG(
      "youtrackdb.query.indexOrdered.minLinkBag",
      "Minimum LinkBag size below which the index-ordered MATCH optimization"
          + " always falls back to load-all-and-sort. Loading a few records is"
          + " cheaper than RidSet construction + index scan setup",
      Integer.class,
      10,
      true),

  QUERY_INDEX_ORDERED_MAX_SCAN(
      "youtrackdb.query.indexOrdered.maxScan",
      "Safety-valve cap on expected index scan length for index-ordered MATCH."
          + " If the estimated number of index entries to scan exceeds this,"
          + " fall back to load-all-and-sort",
      Long.class,
      5_000_000L,
      true),

  QUERY_INDEX_ORDERED_COST_BIAS(
      "youtrackdb.query.indexOrdered.costBias",
      "Multiplicative bias applied to the index scan cost estimate in the"
          + " index-ordered MATCH heuristic. Values > 1.0 make the optimizer"
          + " more conservative (prefer load-all-and-sort). The default 1.2"
          + " adds a 20%% safety margin for estimation error",
      Double.class,
      1.2,
      true),

  QUERY_INDEX_ORDERED_MAX_SOURCES(
      "youtrackdb.query.indexOrdered.maxSources",
      "Maximum number of upstream source rows to materialize in multi-source"
          + " index-ordered MATCH. Beyond this, the optimization falls back to"
          + " load-all-and-sort to limit memory usage",
      Integer.class,
      10_000,
      true),

  QUERY_INDEX_ORDERED_ENTRIES_PER_PAGE(
      "youtrackdb.query.indexOrdered.entriesPerPage",
      "Estimated B-tree index entries per leaf page for amortizing sequential"
          + " read costs in the index-ordered MATCH cost model",
      Integer.class,
      200,
      true),
      ;

  static {
    readConfiguration();
  }

  /**
   * Place holder for the "undefined" value of setting.
   */
  private final Object nullValue = new Object();

  private final String key;
  private final Object defValue;
  private final Class<?> type;
  private final String description;
  private final ConfigurationChangeCallback changeCallback;
  private final Boolean canChangeAtRuntime;
  private final boolean hidden;
  private final boolean env;

  private volatile Object value = nullValue;

  GlobalConfiguration(
      final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    this(iKey, iDescription, iType, iDefValue, false);
  }

  GlobalConfiguration(
      final String iKey,
      final String iDescription,
      final Class<?> iType,
      final Object iDefValue,
      final Boolean iCanChange) {
    this(iKey, iDescription, iType, iDefValue, iCanChange, false);
  }

  GlobalConfiguration(
      final String iKey,
      final String iDescription,
      final Class<?> iType,
      final Object iDefValue,
      final boolean iCanChange,
      final boolean iHidden) {
    this(iKey, iDescription, iType, iDefValue, iCanChange, iHidden, false);
  }

  GlobalConfiguration(
      final String iKey,
      final String iDescription,
      final Class<?> iType,
      final Object iDefValue,
      final boolean iCanChange,
      final boolean iHidden,
      final boolean iEnv) {
    key = iKey;
    description = iDescription;
    defValue = iDefValue;
    type = iType;
    canChangeAtRuntime = iCanChange;
    hidden = iHidden;
    env = iEnv;
    changeCallback = null;
  }

  public static void dumpConfiguration(final PrintStream out) {
    out.print("YouTrackDB ");
    out.print(YouTrackDBConstants.getVersion());
    out.println(" configuration dump:");

    var lastSection = "";
    for (var value : values()) {
      final var index = value.key.indexOf('.');

      var section = value.key;
      if (index >= 0) {
        section = value.key.substring(0, index);
      }

      if (!lastSection.equals(section)) {
        out.print("- ");
        out.println(section.toUpperCase(Locale.ENGLISH));
        lastSection = section;
      }
      out.print("  + ");
      out.print(value.key);
      out.print(" = ");
      out.println(value.hidden ? "<hidden>" : String.valueOf((Object) value.getValue()));
    }
  }

  /**
   * Find the GlobalConfiguration instance by the key. Key is case insensitive.
   *
   * @param iKey Key to find. It's case insensitive.
   * @return GlobalConfiguration instance if found, otherwise null
   */
  @Nullable public static GlobalConfiguration findByKey(final String iKey) {
    for (var v : values()) {
      if (v.key.equalsIgnoreCase(iKey)) {
        return v;
      }
    }
    return null;
  }

  /**
   * Changes the configuration values in one shot by passing a Map of values. Keys can be the Java
   * ENUM names or the string representation of configuration values
   */
  public static void setConfiguration(final Map<String, Object> iConfig) {
    for (var config : iConfig.entrySet()) {
      for (var v : values()) {
        if (v.key.equals(config.getKey())) {
          v.setValue(config.getValue());
          break;
        } else if (v.name().equals(config.getKey())) {
          v.setValue(config.getValue());
          break;
        }
      }
    }
  }

  /**
   * Assign configuration values by reading system properties.
   */
  private static void readConfiguration() {
    String prop;
    for (var config : values()) {
      prop = System.getProperty(config.key);
      if (prop != null) {
        config.setValue(prop);
      }
    }

    for (var config : values()) {

      var key = getEnvKey(config);
      if (key != null) {
        prop = System.getenv(key);
        if (prop != null) {
          config.setValue(prop);
        }
      }
    }
  }

  @Nullable public static String getEnvKey(GlobalConfiguration config) {

    if (!config.env) {
      return null;
    }
    return "YOUTRACKDB_" + config.name();
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> T getValue() {
    //noinspection unchecked
    return (T) (value != null && value != nullValue ? value : defValue);
  }

  /**
   * Returns whether this configuration value has been changed from its default.
   *
   * @return <code>true</code> if configuration was changed from default value and <code>false
   *     </code> otherwise.
   */
  public boolean isChanged() {
    return value != nullValue;
  }

  /**
   * Resets this configuration entry to its default state, as if it was
   * never explicitly set. After calling this, {@link #isChanged()} returns
   * {@code false} and {@link #getValue()} returns the declared default.
   *
   * <p>Fires the change callback (if any) for consistency with
   * {@link #setValue(Object)}.
   *
   * <p>Intended for test teardown only — production code should use
   * {@link #setValue(Object)} instead.
   */
  public void resetToDefault() {
    var oldValue = value;
    value = nullValue;

    if (changeCallback != null) {
      try {
        changeCallback.change(
            oldValue == nullValue ? null : oldValue, null);
      } catch (Exception e) {
        LogManager.instance()
            .error(this, "Error during call of 'change callback'", e);
      }
    }
  }

  public void setValue(final Object iValue) {
    var oldValue = value;

    if (iValue != null) {
      if (type == Boolean.class) {
        value = Boolean.parseBoolean(iValue.toString());
      } else if (type == Integer.class) {
        value = Integer.parseInt(iValue.toString());
      } else if (type == Float.class) {
        value = Float.parseFloat(iValue.toString());
      } else if (type == Double.class) {
        value = Double.parseDouble(iValue.toString());
      } else if (type == String.class) {
        value = iValue.toString();
      } else if (type.isEnum()) {
        var accepted = false;

        if (type.isInstance(iValue)) {
          value = iValue;
          accepted = true;
        } else if (iValue instanceof String string) {

          for (var constant : type.getEnumConstants()) {
            final var enumConstant = (Enum<?>) constant;

            if (enumConstant.name().equalsIgnoreCase(string)) {
              value = enumConstant;
              accepted = true;
              break;
            }
          }
        }

        if (!accepted) {
          throw new IllegalArgumentException("Invalid value of `" + key + "` option.");
        }
      } else {
        value = iValue;
      }
    }

    if (changeCallback != null) {
      try {
        changeCallback.change(
            oldValue == nullValue ? null : oldValue, value == nullValue ? null : value);
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during call of 'change callback'", e);
      }
    }
  }

  public boolean getValueAsBoolean() {
    final var v = value != null && value != nullValue ? value : defValue;
    return v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
  }

  @Nullable public String getValueAsString() {
    return value != null && value != nullValue
        ? value.toString()
        : defValue != null ? defValue.toString() : null;
  }

  public int getValueAsInteger() {
    final var v = value != null && value != nullValue ? value : defValue;
    return (int) (v instanceof Number n ? n.intValue() : FileUtils.getSizeAsNumber(v.toString()));
  }

  public long getValueAsLong() {
    final var v = value != null && value != nullValue ? value : defValue;
    return v instanceof Number n
        ? n.longValue()
        : FileUtils.getSizeAsNumber(v.toString());
  }

  public float getValueAsFloat() {
    final var v = value != null && value != nullValue ? value : defValue;
    return v instanceof Float f ? f : Float.parseFloat(v.toString());
  }

  public double getValueAsDouble() {
    final var v = value != null && value != nullValue ? value : defValue;
    return v instanceof Number n ? n.doubleValue()
        : Double.parseDouble(v.toString());
  }

  public String getKey() {
    return key;
  }

  public Boolean isChangeableAtRuntime() {
    return canChangeAtRuntime;
  }

  public boolean isHidden() {
    return hidden;
  }

  public Object getDefValue() {
    return defValue;
  }

  public Class<?> getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }
}
