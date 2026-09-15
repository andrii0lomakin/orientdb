package com.jetbrains.youtrackdb.internal.core.storage.memory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB.LocalUserCredential;
import com.jetbrains.youtrackdb.api.YouTrackDB.PredefinedLocalRole;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.engine.memory.EngineMemory;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Tests for {@link DirectMemoryStorage} paths that are accessible via the engine lifecycle
 * (DbTestBase memory mode) and via reflection for the protected stub methods.
 *
 * <p>The {@link DirectMemoryStorage} class cannot be constructed directly (the constructor
 * requires a fully-built {@link com.jetbrains.youtrackdb.internal.core.db.YouTrackDBInternalEmbedded});
 * instead these tests obtain an instance via the normal engine-create path and then exercise the
 * remaining coverage gaps through the public API and reflection.
 *
 * <p><b>Why per-test lifecycle (not class-static)?</b> See Step-1 episode: holding a
 * {@code MEMORY} storage instance until JVM shutdown causes the direct-memory page tracker to
 * detect unreleased pages and abort the surefire JVM with {@code System.exit(1)}.  A fresh
 * {@link YouTrackDBImpl} created and closed within each test avoids this problem because the
 * storage is properly torn down before the JVM exits.
 */
public class DirectMemoryStorageTest {

  // ---------------------------------------------------------------------------
  // Helper — obtain the DirectMemoryStorage from a live YouTrackDBImpl
  // ---------------------------------------------------------------------------

  /**
   * Creates a fresh in-memory database, retrieves the underlying {@link DirectMemoryStorage},
   * executes the given action, and then closes everything.  The caller is responsible for
   * re-throwing any exception from the action.
   */
  private interface StorageAction {
    void accept(DirectMemoryStorage storage) throws Exception;
  }

  private static void withMemoryStorage(StorageAction action) throws Exception {
    try (var ytdb = (YouTrackDBImpl) YourTracks.instance(
        DbTestBase.getBaseDirectoryPathStr(DirectMemoryStorageTest.class))) {
      var dbName = "dmsTest_" + Thread.currentThread().getId() + "_" + System.nanoTime();
      ytdb.create(dbName, DatabaseType.MEMORY,
          new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD,
              PredefinedLocalRole.ADMIN));

      // Access the internal storage map via reflection — see StorageDeleteErrorHandlingTest
      // for the reflected-field documentation pattern.
      var storagesField = ytdb.internal.getClass().getDeclaredField("storages");
      storagesField.setAccessible(true);
      @SuppressWarnings("unchecked")
      var storages = (java.util.Map<String, AbstractStorage>) storagesField.get(ytdb.internal);
      var storage = (DirectMemoryStorage) storages.get(dbName);

      try {
        action.accept(storage);
      } finally {
        // Drop must run even when the action throws — six tests use
        // @Test(expected = UnsupportedOperationException.class) and explicitly throw out of
        // the action. Without this finally block, drop() never runs for those, the
        // try-with-resources ytdb.close() takes the slow path that does not release
        // direct-memory pages incrementally, and the page tracker (enabled via
        // -Dyoutrackdb.memory.directMemory.trackMode=true in core/pom.xml) calls
        // System.exit(1) at JVM shutdown — aborting the surefire JVM and masking the
        // real failure as "Tests run: 0".
        try {
          ytdb.drop(dbName);
        } catch (Exception ignored) {
          // Drop failure during cleanup must not mask the original action exception.
        }
      }
    }
  }

  /** Each memory storage instance receives a distinct volatile identity pair. */
  @Test
  public void volatileIdentityIsStableAndUniquePerStorageInstance() throws Exception {
    var firstStorage = new AtomicReference<Object>();
    var firstLineage = new AtomicReference<Object>();
    withMemoryStorage(
        storage -> {
          firstStorage.set(storage.getStorageIdentity());
          firstLineage.set(storage.getStorageLineageIdentity());
          assertEquals(firstStorage.get(), storage.getStorageIdentity());
          assertEquals(firstLineage.get(), storage.getStorageLineageIdentity());
        });

    withMemoryStorage(
        storage -> {
          assertNotEquals(firstStorage.get(), storage.getStorageIdentity());
          assertNotEquals(firstLineage.get(), storage.getStorageLineageIdentity());
        });
  }

  /** The default identity-read hook leaves volatile memory identities unchanged. */
  @Test
  public void defaultIdentityReadHookPreservesMemoryIdentities() throws Exception {
    withMemoryStorage(storage -> {
      var storageIdentity = storage.getStorageIdentity();
      var lineageIdentity = storage.getStorageLineageIdentity();
      var method = AbstractStorage.class.getDeclaredMethod("readStorageIdentity");
      method.setAccessible(true);

      method.invoke(storage);

      assertEquals(storageIdentity, storage.getStorageIdentity());
      assertEquals(lineageIdentity, storage.getStorageLineageIdentity());
    });
  }

  /** The memory-storage creation path can complete an operation with the ranged table. */
  @Test
  public void memoryStorageCompletesAtomicOperationWithRangedTable() throws Exception {
    withMemoryStorage(
        storage -> storage.getAtomicOperationsManager().executeInsideAtomicOperation(operation -> {
          // An empty operation exercises identifier registration and the complete table lifecycle.
        }));
  }

  // ---------------------------------------------------------------------------
  // getType / getURL
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code getType()} returns {@link EngineMemory#NAME}.
   *
   * <p>The engine name is used by the YouTrackDB registry to route storage creation requests;
   * returning the wrong value would silently break engine lookup.
   */
  @Test
  public void testGetType() throws Exception {
    withMemoryStorage(storage -> assertEquals(EngineMemory.NAME, storage.getType()));
  }

  /**
   * Verifies that {@code getURL()} returns a URL of the form {@code "memory:<path>"}.
   */
  @Test
  public void testGetUrl() throws Exception {
    withMemoryStorage(storage -> {
      var url = storage.getURL();
      assertTrue("URL must start with the engine name", url.startsWith(EngineMemory.NAME + ":"));
    });
  }

  // ---------------------------------------------------------------------------
  // Backup methods — all throw UnsupportedOperationException
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code fullBackup(Path)} throws {@link UnsupportedOperationException}.
   *
   * <p>Memory storage has no filesystem representation and cannot be backed up.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testFullBackupPathThrows() throws Exception {
    withMemoryStorage(storage -> storage.fullBackup(Paths.get("/tmp")));
  }

  /**
   * Verifies that {@code fullBackup(Supplier, Function, Consumer)} throws
   * {@link UnsupportedOperationException}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testFullBackupStreamingThrows() throws Exception {
    withMemoryStorage(storage -> storage.fullBackup(
        () -> java.util.Collections.emptyIterator(),
        name -> null,
        name -> {
        }));
  }

  /**
   * Verifies that {@code backup(Path)} throws {@link UnsupportedOperationException}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testBackupPathThrows() throws Exception {
    withMemoryStorage(storage -> storage.backup(Paths.get("/tmp")));
  }

  /**
   * Verifies that {@code backup(Supplier, Function, Function, Consumer)} throws
   * {@link UnsupportedOperationException}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testBackupStreamingThrows() throws Exception {
    withMemoryStorage(storage -> storage.backup(
        () -> java.util.Collections.emptyIterator(),
        name -> null,
        name -> null,
        name -> {
        }));
  }

  /**
   * Verifies that {@code restoreFromBackup(Path, String)} throws
   * {@link UnsupportedOperationException}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testRestoreFromBackupPathThrows() throws Exception {
    withMemoryStorage(storage -> storage.restoreFromBackup(Paths.get("/tmp"), null));
  }

  /**
   * Verifies that {@code restoreFromBackup(Supplier, Function, String)} throws
   * {@link UnsupportedOperationException}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testRestoreFromBackupStreamingThrows() throws Exception {
    withMemoryStorage(storage -> storage.restoreFromBackup(
        () -> java.util.Collections.emptyIterator(),
        name -> null,
        null));
  }

  /**
   * Verifies that {@code restoreFromPreparedBackupChain(PreparedBackupChain)} throws
   * {@link UnsupportedOperationException}.
   *
   * <p>A memory storage supports no backup at all, so a memory storage replays no prepared backup
   * chain either.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testRestoreFromPreparedBackupChainThrows() throws Exception {
    withMemoryStorage(storage -> storage.restoreFromPreparedBackupChain(null));
  }

  // ---------------------------------------------------------------------------
  // Protected stub methods (readIv / getIv / initIv / copyWALToBackup /
  // createWalTempDirectory / createWalFromIBUFiles) — accessed via reflection
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code readIv()} completes without throwing.
   *
   * <p>The method is a no-op in memory storage (memory storage has no IV file to read).
   */
  @Test
  public void testReadIvNoOp() throws Exception {
    withMemoryStorage(storage -> {
      var m = DirectMemoryStorage.class.getDeclaredMethod("readIv");
      m.setAccessible(true);
      m.invoke(storage); // must not throw
    });
  }

  /**
   * Verifies that {@code getIv()} returns an empty byte array.
   *
   * <p>Memory storage has no encryption key material; it always returns {@code new byte[0]}.
   */
  @Test
  public void testGetIvReturnsEmptyArray() throws Exception {
    withMemoryStorage(storage -> {
      var m = DirectMemoryStorage.class.getDeclaredMethod("getIv");
      m.setAccessible(true);
      var iv = (byte[]) m.invoke(storage);
      assertArrayEquals("getIv must return an empty byte array", new byte[0], iv);
    });
  }

  /**
   * Verifies that {@code initIv()} completes without throwing.
   */
  @Test
  public void testInitIvNoOp() throws Exception {
    withMemoryStorage(storage -> {
      var m = DirectMemoryStorage.class.getDeclaredMethod("initIv");
      m.setAccessible(true);
      m.invoke(storage); // must not throw
    });
  }

  /**
   * Verifies that {@code copyWALToBackup(ZipOutputStream, long)} returns {@code null} —
   * memory storage has no WAL segments to copy.
   */
  @Test
  public void testCopyWALToBackupReturnsNull() throws Exception {
    withMemoryStorage(storage -> {
      var m = DirectMemoryStorage.class.getDeclaredMethod(
          "copyWALToBackup",
          java.util.zip.ZipOutputStream.class,
          long.class);
      m.setAccessible(true);
      var result = m.invoke(storage, (java.util.zip.ZipOutputStream) null, 0L);
      assertNull("copyWALToBackup must return null for memory storage", result);
    });
  }

  /**
   * Verifies that {@code createWalTempDirectory()} returns {@code null} — memory storage
   * needs no temporary WAL directory during incremental-backup restore.
   */
  @Test
  public void testCreateWalTempDirectoryReturnsNull() throws Exception {
    withMemoryStorage(storage -> {
      var m = DirectMemoryStorage.class.getDeclaredMethod("createWalTempDirectory");
      m.setAccessible(true);
      var result = m.invoke(storage);
      assertNull("createWalTempDirectory must return null for memory storage", result);
    });
  }

  /**
   * Verifies that {@code createWalFromIBUFiles(File, ContextConfiguration, Locale, byte[])}
   * returns {@code null} — memory storage does not reconstruct a WAL from backup files.
   */
  @Test
  public void testCreateWalFromIBUFilesReturnsNull() throws Exception {
    withMemoryStorage(storage -> {
      var m = DirectMemoryStorage.class.getDeclaredMethod(
          "createWalFromIBUFiles",
          java.io.File.class,
          com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration.class,
          java.util.Locale.class,
          byte[].class);
      m.setAccessible(true);
      var result = m.invoke(storage, null, null, java.util.Locale.ROOT, new byte[0]);
      assertNull("createWalFromIBUFiles must return null for memory storage", result);
    });
  }

  // ---------------------------------------------------------------------------
  // flushAllData / postCloseSteps
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code flushAllData()} completes without throwing — it is a no-op in
   * memory storage.
   */
  @Test
  public void testFlushAllDataNoOp() throws Exception {
    withMemoryStorage(storage -> storage.flushAllData());
  }
}
