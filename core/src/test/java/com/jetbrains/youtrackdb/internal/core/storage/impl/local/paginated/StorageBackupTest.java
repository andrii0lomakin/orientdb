package com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB.LocalUserCredential;
import com.jetbrains.youtrackdb.api.YouTrackDB.PredefinedLocalRole;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.common.io.IOUtils;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Schema;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.record.impl.EntityImpl;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageBackupTest {

  private String testDirectory;

  @Before
  public void before() {
    testDirectory = DbTestBase.getBaseDirectoryPathStr(getClass());
  }

  @Test
  public void testBrokenFullBackup() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    String backupFileName;
    final var random = new Random();

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);

        //full backup
        backupFileName = traversal.backup(backupDir.toPath());
        //lock file and backup file
        Assert.assertEquals(2, backupDir.listFiles().length);

        try (var backupChannel = FileChannel.open(backupDir.toPath().resolve(backupFileName),
            StandardOpenOption.WRITE, StandardOpenOption.READ)) {
          // The first byte belongs to backup content, not to the identity header at the tail.
          // Corrupting it keeps the unit recognizable and makes cleanup deterministic.
          var position = 0L;
          var data = ByteBuffer.allocate(1);

          IOUtils.readByteBuffer(data, backupChannel, position, true);
          data.flip();
          var readByte = data.get();

          data.rewind();
          data.put((byte) (readByte + 1));
          data.flip();

          IOUtils.writeByteBuffer(data, backupChannel, position);
        }

        generateChunkOfData(traversal, random);

        //one more full backup
        traversal.backup(backupDir.toPath());
        //lock file and backup file
        Assert.assertEquals(2, backupDir.listFiles().length);
      }
    }

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

      final var compare =
          new DatabaseCompare(
              youTrackDB.open(dbName, "admin", "admin"),
              youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare.compare());
    }
  }

  @Test
  public void testBrokenIncrementalBackup() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    String backupFileName;
    final var random = new Random();

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);

        //full backup
        traversal.backup(backupDir.toPath());
        Assert.assertEquals(2, backupDir.listFiles().length);

        generateChunkOfData(traversal, random);

        //incremental backup
        backupFileName = traversal.backup(backupDir.toPath());
        Assert.assertEquals(3, backupDir.listFiles().length);

        try (var backupChannel = FileChannel.open(backupDir.toPath().resolve(backupFileName),
            StandardOpenOption.WRITE, StandardOpenOption.READ)) {
          // The first byte belongs to backup content, not to the identity header at the tail.
          // Corrupting it keeps the unit recognizable and makes cleanup deterministic.
          var position = 0L;
          var data = ByteBuffer.allocate(1);

          IOUtils.readByteBuffer(data, backupChannel, position, true);
          data.flip();
          var readByte = data.get();

          data.rewind();
          data.put((byte) (readByte + 1));
          data.flip();

          IOUtils.writeByteBuffer(data, backupChannel, position);
        }

        generateChunkOfData(traversal, random);

        //as incremental backup broken we create new one starting from changes from full backup.
        traversal.backup(backupDir.toPath());
        Assert.assertEquals(3, backupDir.listFiles().length);
      }
    }

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

      final var compare =
          new DatabaseCompare(
              youTrackDB.open(dbName, "admin", "admin"),
              youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare.compare());
    }
  }

  @Test
  public void testRemoveFullBackupAndLeaveTwoIncrementalBackups() throws Exception {
    //Create three backups and remove full backup, ensure that the error is thrown during restore.
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    var random = new Random();
    String backupFileName;
    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);

        backupFileName = traversal.backup(backupDir.toPath());
        Assert.assertEquals(2, backupDir.listFiles().length);

        generateChunkOfData(traversal, random);
        traversal.backup(backupDir.toPath());
        Assert.assertEquals(3, backupDir.listFiles().length);

        generateChunkOfData(traversal, random);
        traversal.backup(backupDir.toPath());
        Assert.assertEquals(4, backupDir.listFiles().length);
      }
    }

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    Files.deleteIfExists(backupDir.toPath().resolve(backupFileName));
    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      try {
        youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());
        Assert.fail("Exception expected");
      } catch (Exception e) {
        //we have removed a full backup and left two incremental backups, so the exception is expected.
      }
    }
  }

  @Test
  public void testRemoveIncrementalBackupAndFullAndIncrementalBackups() throws Exception {
    //Create three backups and remove the first incremental backup, ensure that the error is thrown during restore.
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    var random = new Random();
    String backupFileName;
    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);

        traversal.backup(backupDir.toPath());
        Assert.assertEquals(2, backupDir.listFiles().length);

        generateChunkOfData(traversal, random);
        backupFileName = traversal.backup(backupDir.toPath());
        Assert.assertEquals(3, backupDir.listFiles().length);

        generateChunkOfData(traversal, random);
        traversal.backup(backupDir.toPath());
        Assert.assertEquals(4, backupDir.listFiles().length);
      }
    }

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    Files.deleteIfExists(backupDir.toPath().resolve(backupFileName));
    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      try {
        youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());
        Assert.fail("Exception expected");
      } catch (Exception e) {
        //we have removed a middle incremental backup and left full and last incremental backups,
        //so the exception is expected.
      }
    }
  }

  @Test
  public void testBackupWithoutSequenceIndex() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    var random = new Random();
    String backupFileName;

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);

        backupFileName = traversal.backup(backupDir.toPath());
      }
    }

    var sequenceNumberStartIndex = 57;
    var sequenceNumberEndIndex = backupFileName.indexOf('-', sequenceNumberStartIndex);
    var sequenceNumber = backupFileName.substring(sequenceNumberStartIndex, sequenceNumberEndIndex);
    Assert.assertEquals("0", sequenceNumber);

    var newFileName =
        backupFileName.substring(0, sequenceNumberStartIndex) + backupFileName.substring(
            sequenceNumberEndIndex + 1);
    Files.move(backupDir.toPath().resolve(backupFileName), backupDir.toPath().resolve(newFileName));

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    Files.deleteIfExists(backupDir.toPath().resolve(backupFileName));
    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      try {
        youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());
        Assert.fail("Exception expected");
      } catch (Exception e) {
        //we have removed sequence index, so the exception is expected.
      }
    }
  }

  @Test
  public void testTwoDBsMakeBackupToTheSameDir() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));

    final var dbName1 = StorageBackupTest.class.getSimpleName() + "1";
    final var dbName2 = StorageBackupTest.class.getSimpleName() + "2";

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    var random = new Random();
    UUID firstDbUUId;
    UUID secondDbUUId;

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName1, DatabaseType.DISK, "admin", "admin", "admin");
      youTrackDB.create(dbName2, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal1 = youTrackDB.openTraversal(dbName1, "admin", "admin")) {
        try (var traversal2 = youTrackDB.openTraversal(dbName2, "admin", "admin")) {

          firstDbUUId = traversal1.uuid();
          secondDbUUId = traversal2.uuid();

          generateChunkOfData(traversal1, random);
          generateChunkOfData(traversal2, random);

          traversal1.backup(backupDir.toPath());
          traversal2.backup(backupDir.toPath());

          generateChunkOfData(traversal1, random);
          generateChunkOfData(traversal2, random);

          traversal1.backup(backupDir.toPath());
          traversal2.backup(backupDir.toPath());

          generateChunkOfData(traversal1, random);
          generateChunkOfData(traversal2, random);

          traversal1.backup(backupDir.toPath());
          traversal2.backup(backupDir.toPath());
        }
      }

    }
    final var backupDbName1 = StorageBackupTest.class.getSimpleName() + "BackUp1";
    final var backupDbName2 = StorageBackupTest.class.getSimpleName() + "BackUp2";

    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName1, backupDir.getAbsolutePath(), firstDbUUId.toString(),
          new BaseConfiguration());
      youTrackDB.restore(backupDbName2, backupDir.getAbsolutePath(), secondDbUUId.toString(),
          new BaseConfiguration());

      final var compare1 =
          new DatabaseCompare(
              youTrackDB.open(dbName1, "admin", "admin"),
              youTrackDB.open(backupDbName1, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare1.compare());

      final var compare2 =
          new DatabaseCompare(
              youTrackDB.open(dbName2, "admin", "admin"),
              youTrackDB.open(backupDbName2, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare2.compare());
    }
  }

  @Test
  public void testCreateBackupRenamedDatabaseCreateTwoMoreBackups() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    var random = new Random();

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversalSource = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversalSource, random);
        traversalSource.backup(backupDir.toPath());
      }
    }

    var newDbName = "new" + dbName;
    var newDBDirectory = new File(testDirectory, newDbName);
    FileUtils.moveDirectory(new File(new File(testDirectory), dbName), newDBDirectory);

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      try (var graph = youTrackDB.openTraversal(newDbName, "admin", "admin")) {
        generateChunkOfData(graph, random);
        graph.backup(backupDir.toPath());

        generateChunkOfData(graph, random);
        graph.backup(backupDir.toPath());
      }
    }

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

      final var compare =
          new DatabaseCompare(
              youTrackDB.open(newDbName, "admin", "admin"),
              youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare.compare());
    }
  }

  @Test
  public void testSingeThreadFullBackup() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory);
    youTrackDB.create(dbName, DatabaseType.DISK,
        new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD, PredefinedLocalRole.ADMIN));
    var db = youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD);

    final Schema schema = db.getMetadata().getSchema();
    final var backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", PropertyType.INTEGER);
    backupClass.createProperty("data", PropertyType.BINARY);

    backupClass.createIndex("backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

    final var random = new Random();
    for (var i = 0; i < 1000; i++) {
      db.begin();
      final var data = new byte[16];
      random.nextBytes(data);

      final var num = random.nextInt();

      final var document = ((EntityImpl) db.newEntity("BackupClass"));
      document.setProperty("num", num);
      document.setProperty("data", data);

      db.commit();
    }

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.backup(backupDir.toPath());
    youTrackDB.close();

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory);
    youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

    final var compare =
        new DatabaseCompare(
            youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD),
            youTrackDB.open(backupDbName, "admin", DbTestBase.ADMIN_PASSWORD),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory);
    if (youTrackDB.exists(dbName)) {
      youTrackDB.drop(dbName);
    }
    if (youTrackDB.exists(backupDbName)) {
      youTrackDB.drop(backupDbName);
    }

    youTrackDB.close();

    FileUtils.deleteDirectory(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackup() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));

    var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory);

    final var dbName = StorageBackupTest.class.getSimpleName();
    youTrackDB.create(dbName, DatabaseType.DISK,
        new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD, PredefinedLocalRole.ADMIN));
    var db = youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD);

    final Schema schema = db.getMetadata().getSchema();
    final var backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", PropertyType.INTEGER);
    backupClass.createProperty("data", PropertyType.BINARY);

    backupClass.createIndex("backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

    final var random = new Random();
    for (var i = 0; i < 1000; i++) {
      db.begin();
      final var data = new byte[16];
      random.nextBytes(data);

      final var num = random.nextInt();

      final var document = ((EntityImpl) db.newEntity("BackupClass"));
      document.setProperty("num", num);
      document.setProperty("data", data);

      db.commit();
    }

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.backup(backupDir.toPath());

    for (var n = 0; n < 10; n++) {
      for (var i = 0; i < 10_000; i++) {
        db.begin();
        final var data = new byte[16];
        random.nextBytes(data);

        final var num = random.nextInt();

        final var document = ((EntityImpl) db.newEntity("BackupClass"));
        document.setProperty("num", num);
        document.setProperty("data", data);

        db.commit();
      }

      db.backup(backupDir.toPath());
    }

    db.backup(backupDir.toPath());
    db.close();

    youTrackDB.close();

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory);
    youTrackDB.restore(
        backupDbName,
        backupDir.getAbsolutePath());

    final var compare =
        new DatabaseCompare(
            youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD),
            youTrackDB.open(backupDbName, "admin", DbTestBase.ADMIN_PASSWORD),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory);
    youTrackDB.drop(dbName);
    youTrackDB.drop(backupDbName);

    youTrackDB.close();

    FileUtils.deleteDirectory(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackupEncryption() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var config = new BaseConfiguration();
    config.setProperty(GlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey(),
        "T1JJRU5UREJfSVNfQ09PTA==");

    var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory, config);

    final var dbName = StorageBackupTest.class.getSimpleName();
    youTrackDB.create(dbName, DatabaseType.DISK,
        new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD, PredefinedLocalRole.ADMIN));
    var db = youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD);

    final Schema schema = db.getMetadata().getSchema();
    final var backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", PropertyType.INTEGER);
    backupClass.createProperty("data", PropertyType.BINARY);

    backupClass.createIndex("backupIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "num");

    final var random = new Random();
    for (var i = 0; i < 1000; i++) {
      db.begin();
      final var data = new byte[16];
      random.nextBytes(data);

      final var num = random.nextInt();

      final var document = ((EntityImpl) db.newEntity("BackupClass"));
      document.setProperty("num", num);
      document.setProperty("data", data);

      db.commit();
    }

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);

    if (!backupDir.exists()) {
      Assert.assertTrue(backupDir.mkdirs());
    }

    db.backup(backupDir.toPath());

    for (var n = 0; n < 10; n++) {
      for (var i = 0; i < 10_000; i++) {
        db.begin();
        final var data = new byte[16];
        random.nextBytes(data);

        final var num = random.nextInt();

        final var document = ((EntityImpl) db.newEntity("BackupClass"));
        document.setProperty("num", num);
        document.setProperty("data", data);

        db.commit();
      }

      db.backup(backupDir.toPath());
    }

    db.backup(backupDir.toPath());
    db.close();

    youTrackDB.close();

    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";

    youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory, config);
    youTrackDB.restore(
        backupDbName,
        backupDir.getAbsolutePath(),
        null,
        config);

    final var compare =
        new DatabaseCompare(
            youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD),
            youTrackDB.open(backupDbName, "admin", DbTestBase.ADMIN_PASSWORD),
            System.out::println);

    Assert.assertTrue(compare.compare());

    if (youTrackDB.isOpen()) {
      youTrackDB.close();
    }

    youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory, config);
    if (youTrackDB.exists(dbName)) {
      youTrackDB.drop(dbName);
    }
    if (youTrackDB.exists(backupDbName)) {
      youTrackDB.drop(backupDbName);
    }

    youTrackDB.close();

    FileUtils.deleteDirectory(backupDir);
  }

  @Test
  public void testFullBackupViaTraversalSource() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "fullBackupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    final var random = new Random();

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);

        var backupFileName = traversal.fullBackup(backupDir.toPath());

        Assert.assertNotNull("Backup file name should not be null", backupFileName);
        Assert.assertTrue("Backup file should exist",
            Files.exists(backupDir.toPath().resolve(backupFileName)));
      }
    }

    // Verify restore works
    final var backupDbName = StorageBackupTest.class.getSimpleName() + "FullBackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

      final var compare =
          new DatabaseCompare(
              youTrackDB.open(dbName, "admin", "admin"),
              youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare.compare());
    }
    FileUtils.deleteDirectory(backupDir);
  }

  // Verifies that the 3-arg restore(name, path, config) overload on
  // YouTrackDBInternalEmbedded correctly delegates to the 4-arg version with null expectedUUID.
  @Test
  public void testRestoreWithConfig() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));
    final var dbName = StorageBackupTest.class.getSimpleName();

    final var backupDir = new File(testDirectory, "backupDir");
    FileUtils.deleteDirectory(backupDir);
    Assert.assertTrue(backupDir.mkdirs());

    var random = new Random();

    try (var youTrackDB = YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK, "admin", "admin", "admin");

      try (var traversal = youTrackDB.openTraversal(dbName, "admin", "admin")) {
        generateChunkOfData(traversal, random);
        traversal.backup(backupDir.toPath());
      }
    }

    // Restore using the 3-arg internal restore(name, path, config) overload
    final var backupDbName = StorageBackupTest.class.getSimpleName() + "BackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.internal.restore(backupDbName, backupDir.getAbsolutePath(), null);

      final var compare =
          new DatabaseCompare(
              youTrackDB.open(dbName, "admin", "admin"),
              youTrackDB.open(backupDbName, "admin", "admin"),
              System.out::println);

      Assert.assertTrue(compare.compare());
    }
    FileUtils.deleteDirectory(backupDir);
  }

  @Test
  public void testIdGenRestoredFromBackupMetadata() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));

    final var dbName = StorageBackupTest.class.getSimpleName();
    final var backupDir = new File(testDirectory, "backupDir");
    Assert.assertTrue(backupDir.mkdirs());

    long originalIdGen;

    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK,
          new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD, PredefinedLocalRole.ADMIN));

      try (var db = youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD)) {
        final var schema = db.getMetadata().getSchema();
        final var backupClass = schema.createClass("BackupClass");
        backupClass.createProperty("num", PropertyType.INTEGER);
        backupClass.createProperty("data", PropertyType.BINARY);

        final var random = new Random();
        for (var i = 0; i < 100; i++) {
          db.begin();
          final var document = ((EntityImpl) db.newEntity("BackupClass"));
          document.setProperty("num", random.nextInt());
          document.setProperty("data", new byte[16]);
          db.commit();
        }

        originalIdGen = db.getStorage().getIdGen().getLastId();
        Assert.assertTrue("idGen should have advanced after commits", originalIdGen > 0);

        db.backup(backupDir.toPath());
      }
    }

    final var backupDbName = dbName + "IdGenBackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

      try (var db = youTrackDB.open(backupDbName, "admin", DbTestBase.ADMIN_PASSWORD)) {
        var restoredIdGen = db.getStorage().getIdGen().getLastId();

        Assert.assertTrue(
            "Restored idGen (" + restoredIdGen + ") must be >= original ("
                + originalIdGen + "). "
                + "If idGen is not properly restored from backup metadata, "
                + "records will be invisible under snapshot isolation.",
            restoredIdGen >= originalIdGen);
      }
    }

    FileUtils.deleteDirectory(backupDir);
  }

  @Test
  public void testIdGenRestoredFromIncrementalBackupMetadata() throws Exception {
    FileUtils.deleteDirectory(new File(testDirectory));

    final var dbName = StorageBackupTest.class.getSimpleName();
    final var backupDir = new File(testDirectory, "backupDir");
    Assert.assertTrue(backupDir.mkdirs());

    long idGenAfterIncrementalData;

    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.create(dbName, DatabaseType.DISK,
          new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD, PredefinedLocalRole.ADMIN));

      try (var db = youTrackDB.open(dbName, "admin", DbTestBase.ADMIN_PASSWORD)) {
        final var schema = db.getMetadata().getSchema();
        final var backupClass = schema.createClass("BackupClass");
        backupClass.createProperty("num", PropertyType.INTEGER);
        backupClass.createProperty("data", PropertyType.BINARY);

        final var random = new Random();
        for (var i = 0; i < 50; i++) {
          db.begin();
          final var document = ((EntityImpl) db.newEntity("BackupClass"));
          document.setProperty("num", random.nextInt());
          document.setProperty("data", new byte[16]);
          db.commit();
        }

        // Full backup
        db.backup(backupDir.toPath());

        // Add more data after full backup
        for (var i = 0; i < 50; i++) {
          db.begin();
          final var document = ((EntityImpl) db.newEntity("BackupClass"));
          document.setProperty("num", random.nextInt());
          document.setProperty("data", new byte[16]);
          db.commit();
        }

        idGenAfterIncrementalData = db.getStorage().getIdGen().getLastId();

        // Incremental backup
        db.backup(backupDir.toPath());
      }
    }

    final var backupDbName = dbName + "IdGenIncrBackUp";
    try (var youTrackDB = (YouTrackDBImpl) YourTracks.instance(testDirectory)) {
      youTrackDB.restore(backupDbName, backupDir.getAbsolutePath());

      try (var db = youTrackDB.open(backupDbName, "admin", DbTestBase.ADMIN_PASSWORD)) {
        var restoredIdGen = db.getStorage().getIdGen().getLastId();

        Assert.assertTrue(
            "Restored idGen (" + restoredIdGen + ") must be >= idGen after "
                + "incremental data (" + idGenAfterIncrementalData + ").",
            restoredIdGen >= idGenAfterIncrementalData);
      }
    }

    FileUtils.deleteDirectory(backupDir);
  }

  private static void generateChunkOfData(YTDBGraphTraversalSource traversalSource, Random random) {
    for (var i = 0; i < 1000; i++) {
      traversalSource.executeInTx(g -> {
        final var data = new byte[16];
        random.nextBytes(data);
        final var num = random.nextInt();

        g.addV("BackupClass").property("num", num, "data", data).iterate();
      });
    }
  }
}
