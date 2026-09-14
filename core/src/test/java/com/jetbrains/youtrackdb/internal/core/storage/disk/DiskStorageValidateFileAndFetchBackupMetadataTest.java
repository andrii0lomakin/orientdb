package com.jetbrains.youtrackdb.internal.core.storage.disk;

import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.LogSequenceNumber;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import net.jpountz.xxhash.XXHashFactory;
import org.junit.Assert;
import org.junit.Test;

public class DiskStorageValidateFileAndFetchBackupMetadataTest {

  @Test
  public void testValidateFileAndFetchBackupMetadata() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataWithContent() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance()
            .newStreamingHash64(DiskStorage.XX_HASH_SEED)) {

      final var content = new byte[1024];
      for (var i = 0; i < content.length; i++) {
        content[i] = (byte) i;
      }

      outputStream.write(content);
      xxHash64.update(content, 0, content.length);

      try (var dataOutputStream = new DataOutputStream(outputStream)) {
        dataOutputStream.writeShort(backupFormatVersion);
        dataOutputStream.writeLong(uuid.getLeastSignificantBits());
        dataOutputStream.writeLong(uuid.getMostSignificantBits());
        dataOutputStream.writeInt(sequenceNumber);
        dataOutputStream.writeLong(startLsn.getSegment());
        dataOutputStream.writeInt(startLsn.getPosition());
        dataOutputStream.writeLong(endLsn.getSegment());
        dataOutputStream.writeInt(endLsn.getPosition());
        dataOutputStream.writeLong(42L);
        writeSupportedSemanticIdentity(dataOutputStream);

        dataOutputStream.flush();

        final var metadata = outputStream.toByteArray();
        xxHash64.update(metadata, content.length, metadata.length - content.length);

        final var hashCode = xxHash64.getValue();
        dataOutputStream.writeLong(hashCode);
        dataOutputStream.flush();

        try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            var copyStream = new ByteArrayOutputStream()) {
          final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

          final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
              inputStream, copyStream);

          Assert.assertNotNull(result);
          Assert.assertEquals(backupMetadata, result);
          Assert.assertArrayEquals(outputStream.toByteArray(), copyStream.toByteArray());
        }
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataBrokenHash() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      dataOutputStream.writeLong(123);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataBrokenUUID() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits() + 1);
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataBrokenSequenceNumber() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber + 1);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataFileTooShort() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

    try (var outputStream = new ByteArrayOutputStream()) {
      outputStream.write(new byte[10]);

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  /**
   * An old backup header carries backup format version 2 and is unsupported.
   *
   * <p>Version 2 holds no semantic database format and no creation completion evidence. The
   * validation therefore rejects the unit, even when the rest of the header is well formed.
   */
  @Test
  public void testValidateFileAndFetchBackupMetadataVersionMismatch() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 2;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataFileNameTooShort() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = "short.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataInvalidFileNameUUID() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = "invalid-uuid-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataInvalidLastLsn() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(-1, -1);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataInvalidSequenceNumberInFileName()
      throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-invalid-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataEmptyInputStream() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

    try (var inputStream = new ByteArrayInputStream(new byte[0])) {
      final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
          inputStream, null);

      Assert.assertNull(result);
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataWithNullDbUUID() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        // Pass null for dbUUID - should skip UUID validation
        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", null,
            inputStream, null);

        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataWithNullStartLsn() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    // When startLsn is (-1, -1), the returned metadata should have null startLsn
    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, null, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(-1); // startLsn segment
      dataOutputStream.writeInt(-1); // startLsn position
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
        Assert.assertNull(result.startLsn());
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataWithLargeContent() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance()
            .newStreamingHash64(DiskStorage.XX_HASH_SEED)) {

      // Content larger than 64KB buffer to exercise multiple read iterations
      // and the shifting metadata logic
      final var content = new byte[(100 << 10)]; // 100KB
      for (var i = 0; i < content.length; i++) {
        content[i] = (byte) i;
      }

      outputStream.write(content);
      xxHash64.update(content, 0, content.length);

      try (var dataOutputStream = new DataOutputStream(outputStream)) {
        dataOutputStream.writeShort(backupFormatVersion);
        dataOutputStream.writeLong(uuid.getLeastSignificantBits());
        dataOutputStream.writeLong(uuid.getMostSignificantBits());
        dataOutputStream.writeInt(sequenceNumber);
        dataOutputStream.writeLong(startLsn.getSegment());
        dataOutputStream.writeInt(startLsn.getPosition());
        dataOutputStream.writeLong(endLsn.getSegment());
        dataOutputStream.writeInt(endLsn.getPosition());
        dataOutputStream.writeLong(42L);
        writeSupportedSemanticIdentity(dataOutputStream);

        dataOutputStream.flush();

        final var metadataBytes = outputStream.toByteArray();
        xxHash64.update(metadataBytes, content.length, metadataBytes.length - content.length);

        final var hashCode = xxHash64.getValue();
        dataOutputStream.writeLong(hashCode);
        dataOutputStream.flush();

        try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            var copyStream = new ByteArrayOutputStream()) {
          final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

          final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
              inputStream, copyStream);

          Assert.assertNotNull(result);
          Assert.assertEquals(backupMetadata, result);
          Assert.assertArrayEquals(outputStream.toByteArray(), copyStream.toByteArray());
        }
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataInvalidLastLsnSegmentOnly() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(-1); // Invalid segment
      dataOutputStream.writeInt(2); // Valid position
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataInvalidLastLsnPositionOnly()
      throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(2); // Valid segment
      dataOutputStream.writeInt(-1); // Invalid position
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataFileNameUUIDMismatch() throws IOException {
    final var metadataUuid = UUID.randomUUID();
    final var fileNameUuid = UUID.randomUUID(); // Different UUID in filename
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    // The result should still be returned (with warning logged) using the filename UUID
    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, fileNameUuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(metadataUuid.getLeastSignificantBits());
      dataOutputStream.writeLong(metadataUuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        // Use fileNameUuid in filename but metadataUuid in content
        final var fileName = fileNameUuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        // Pass null dbUUID to skip the first UUID check, testing only filename vs metadata UUID mismatch
        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", null,
            inputStream, null);

        // Should return metadata with filename UUID (warning only, no rejection)
        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataBrokenUUIDHigherBits() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits() + 1); // Different higher bits
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull(result);
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataPartialStartLsnSegmentOnly()
      throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    // When only segment is -1, startLsn should be null
    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, null, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(-1); // startLsn segment is -1
      dataOutputStream.writeInt(1); // startLsn position is valid
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
        Assert.assertNull(result.startLsn());
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataPartialStartLsnPositionOnly()
      throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    // When only position is -1, startLsn should be null
    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, null, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(1); // startLsn segment is valid
      dataOutputStream.writeInt(-1); // startLsn position is -1
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
        Assert.assertNull(result.startLsn());
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataMissingDashAfterSequenceNumber()
      throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        // The file name carries no dash after the sequence number, because the database name
        // part is missing.
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + ".ibu";

        // The inspection refuses such a name in a controlled way. The unit stays
        // unclassifiable, so no automatic removal ever covers it.
        final var inspection = DiskStorage.inspectBackupUnit(fileName, "db", uuid,
            inputStream, null);

        Assert.assertNull("a name without the sequence dash must reject the unit",
            inspection.metadata());
        Assert.assertEquals(
            "a name without the sequence dash must stay unclassifiable",
            DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
            inspection.classification());
        Assert.assertTrue(
            "the detail must name the missing sequence number, saw: " + inspection.detail(),
            inspection.detail().contains("no backup sequence number"));
      }
    }
  }

  @Test
  public void testValidateFileAndFetchBackupMetadataWithSmallChunkReading() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance()
            .newStreamingHash64(DiskStorage.XX_HASH_SEED)) {

      // Content larger than metadata size to establish metadataCandidate
      final var content = new byte[100];
      for (var i = 0; i < content.length; i++) {
        content[i] = (byte) i;
      }

      outputStream.write(content);
      xxHash64.update(content, 0, content.length);

      try (var dataOutputStream = new DataOutputStream(outputStream)) {
        dataOutputStream.writeShort(backupFormatVersion);
        dataOutputStream.writeLong(uuid.getLeastSignificantBits());
        dataOutputStream.writeLong(uuid.getMostSignificantBits());
        dataOutputStream.writeInt(sequenceNumber);
        dataOutputStream.writeLong(startLsn.getSegment());
        dataOutputStream.writeInt(startLsn.getPosition());
        dataOutputStream.writeLong(endLsn.getSegment());
        dataOutputStream.writeInt(endLsn.getPosition());
        dataOutputStream.writeLong(42L);
        writeSupportedSemanticIdentity(dataOutputStream);

        dataOutputStream.flush();

        final var metadataBytes = outputStream.toByteArray();
        xxHash64.update(metadataBytes, content.length, metadataBytes.length - content.length);

        final var hashCode = xxHash64.getValue();
        dataOutputStream.writeLong(hashCode);
        dataOutputStream.flush();

        final var fullData = outputStream.toByteArray();

        // Custom InputStream that returns data in small chunks
        try (var smallChunkInputStream = new InputStream() {
          private int position = 0;
          private static final int chunkSize = 10; // Small chunk size to exercise shifting logic

          @Override
          public int read() throws IOException {
            if (position >= fullData.length) {
              return -1;
            }
            return fullData[position++] & 0xFF;
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            if (position >= fullData.length) {
              return -1;
            }
            var bytesToRead = Math.min(chunkSize, Math.min(len, fullData.length - position));
            System.arraycopy(fullData, position, b, off, bytesToRead);
            position += bytesToRead;
            return bytesToRead;
          }
        }) {
          final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

          final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
              smallChunkInputStream, null);

          Assert.assertNotNull(result);
          Assert.assertEquals(backupMetadata, result);
        }
      }
    }
  }

  /**
   * Test to cover lines 1037-1038: the case when inputStream.read() returns 0. This is a defensive
   * code path that handles streams that may return 0 bytes read. The test returns 0 on the first
   * read, then returns actual data on subsequent reads.
   */
  @Test
  public void testValidateFileAndFetchBackupMetadataWithZeroReadInputStream() throws IOException {
    final var uuid = UUID.randomUUID();
    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, uuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      final var fullData = outputStream.toByteArray();

      // Custom InputStream that returns 0 on the first read to trigger the continue branch
      // at lines 1037-1038, then returns actual data on subsequent reads
      try (var zeroReadInputStream = new InputStream() {
        private int position = 0;
        private boolean returnedZero = false;

        @Override
        public int read() throws IOException {
          if (position >= fullData.length) {
            return -1;
          }
          return fullData[position++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          // Return 0 on the first read to exercise the continue branch (lines 1037-1038)
          if (!returnedZero) {
            returnedZero = true;
            return 0;
          }
          if (position >= fullData.length) {
            return -1;
          }
          var bytesToRead = Math.min(len, fullData.length - position);
          System.arraycopy(fullData, position, b, off, bytesToRead);
          position += bytesToRead;
          return bytesToRead;
        }
      }) {
        final var fileName = uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", uuid,
            zeroReadInputStream, null);

        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
      }
    }
  }

  /**
   * Test to cover line 1167: the most significant bits comparison in the filename UUID vs metadata
   * UUID check. This test creates UUIDs where least significant bits match but most significant
   * bits differ, forcing evaluation of the second part of the OR condition at line 1167.
   */
  @Test
  public void testValidateFileAndFetchBackupMetadataFileNameUUIDMostSignificantBitsMismatch()
      throws IOException {
    // Create two UUIDs with same least significant bits but different most significant bits
    final var leastSignificantBits = 0x123456789ABCDEF0L;
    final var metadataMostSignificantBits = 0xFEDCBA9876543210L;
    final var fileNameMostSignificantBits = 0xFEDCBA9876543211L; // Different by 1

    final var metadataUuid = new UUID(metadataMostSignificantBits, leastSignificantBits);
    final var fileNameUuid = new UUID(fileNameMostSignificantBits, leastSignificantBits);

    final var sequenceNumber = 1;
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);
    final var backupFormatVersion = 3;

    // The result should still be returned (with warning logged) using the filename UUID
    final var backupMetadata = new DiskStorage.BackupMetadata(backupFormatVersion, fileNameUuid,
        sequenceNumber, startLsn, endLsn, 42L,
        DiskStorage.supportedBackupSemanticIdentity());

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {

      dataOutputStream.writeShort(backupFormatVersion);
      // Write metadata UUID (different most significant bits than filename)
      dataOutputStream.writeLong(metadataUuid.getLeastSignificantBits());
      dataOutputStream.writeLong(metadataUuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      writeSupportedSemanticIdentity(dataOutputStream);

      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);

      final var hashCode = xxHash64.getValue();
      dataOutputStream.writeLong(hashCode);
      dataOutputStream.flush();

      try (var inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
        // Use fileNameUuid in filename (same least significant bits, different most significant bits)
        final var fileName = fileNameUuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";

        // Pass null dbUUID to skip the first UUID check at lines 1131-1140
        // This test specifically covers line 1167 where:
        // - fileNameUUID.getLeastSignificantBits() == metadataUUIDLowerBits (first condition is FALSE)
        // - fileNameUUID.getMostSignificantBits() != metadataUUIDHigherBits (second condition at line 1167 is TRUE)
        final var result = DiskStorage.validateFileAndFetchBackupMetadata(fileName, "db", null,
            inputStream, null);

        // Should return metadata with filename UUID (warning only, no rejection)
        Assert.assertNotNull(result);
        Assert.assertEquals(backupMetadata, result);
      }
    }
  }

  /**
   * Writes the three semantic identity fields that this build accepts in one backup header.
   *
   * <p>The fields are the database feature format, the storage layout version, and the creation
   * completion evidence. Every valid header of this build carries these accepted values.
   */
  private static void writeSupportedSemanticIdentity(DataOutputStream dataOutputStream)
      throws IOException {
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    dataOutputStream.writeInt(identity.featureFormatVersion());
    dataOutputStream.writeInt(identity.storageLayoutVersion());
    dataOutputStream.writeInt(identity.creationEvidence());
  }

  /**
   * Builds one complete backup unit with a chosen header.
   *
   * <p>The unit holds the header alone, which is enough for every header check. A caller chooses
   * the backup format version, the three semantic identity fields, and the validity of the hash
   * code.
   *
   * @param validHash true for a hash code that matches the content
   */
  private static byte[] backupUnitWithHeader(UUID uuid, int sequenceNumber,
      int backupFormatVersion, int featureFormat, int layoutVersion, int creationEvidence,
      boolean validHash) throws IOException {
    final var startLsn = new LogSequenceNumber(1, 1);
    final var endLsn = new LogSequenceNumber(2, 2);

    try (var outputStream = new ByteArrayOutputStream();
        var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(DiskStorage.XX_HASH_SEED);
        var dataOutputStream = new DataOutputStream(outputStream)) {
      dataOutputStream.writeShort(backupFormatVersion);
      dataOutputStream.writeLong(uuid.getLeastSignificantBits());
      dataOutputStream.writeLong(uuid.getMostSignificantBits());
      dataOutputStream.writeInt(sequenceNumber);
      dataOutputStream.writeLong(startLsn.getSegment());
      dataOutputStream.writeInt(startLsn.getPosition());
      dataOutputStream.writeLong(endLsn.getSegment());
      dataOutputStream.writeInt(endLsn.getPosition());
      dataOutputStream.writeLong(42L);
      dataOutputStream.writeInt(featureFormat);
      dataOutputStream.writeInt(layoutVersion);
      dataOutputStream.writeInt(creationEvidence);
      dataOutputStream.flush();

      final var metadata = outputStream.toByteArray();
      xxHash64.update(metadata, 0, metadata.length);
      dataOutputStream.writeLong(validHash ? xxHash64.getValue() : xxHash64.getValue() + 1);
      dataOutputStream.flush();

      return outputStream.toByteArray();
    }
  }

  /** Returns the unit file name of one database identifier and one sequence number. */
  private static String unitFileName(UUID uuid, int sequenceNumber) {
    return uuid + "-2021-01-01-00-00-00-" + sequenceNumber + "-db.ibu";
  }

  /** Inspects one prepared backup unit without any copy of its bytes. */
  private static DiskStorage.BackupUnitInspection inspect(byte[] unit, String fileName, UUID dbUUID)
      throws IOException {
    try (var inputStream = new ByteArrayInputStream(unit)) {
      return DiskStorage.inspectBackupUnit(fileName, "db", dbUUID, inputStream, null);
    }
  }

  /** Admits the header of one prepared backup unit without any content check. */
  private static DiskStorage.BackupUnitInspection inspectHeader(byte[] unit, String fileName,
      UUID dbUUID) throws IOException {
    try (var inputStream = new ByteArrayInputStream(unit)) {
      return DiskStorage.inspectBackupUnitHeader(fileName, "db", dbUUID, inputStream);
    }
  }

  /**
   * A header of another database feature format is unsupported.
   *
   * <p>The scenario writes an otherwise valid header whose feature format differs from the feature
   * format of this build. The expected outcome has two parts. The validation rejects the unit. The
   * classification refuses every automatic removal of the unit.
   */
  @Test
  public void featureFormatMismatchIsUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion() + 1, identity.storageLayoutVersion(),
            identity.creationEvidence(), true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertNull("a foreign feature format must reject the unit", inspection.metadata());
    Assert.assertEquals(
        "a foreign feature format must stay unclassifiable",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * A header of another storage layout version is unsupported.
   *
   * <p>The scenario writes an otherwise valid header whose storage layout version differs from the
   * layout version of this build. The expected outcome has two parts. The validation rejects the
   * unit. The classification refuses every automatic removal of the unit.
   */
  @Test
  public void storageLayoutMismatchIsUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion() + 1,
            identity.creationEvidence(), true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertNull("a foreign layout version must reject the unit", inspection.metadata());
    Assert.assertEquals(
        "a foreign layout version must stay unclassifiable",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * A header without creation completion evidence is unsupported.
   *
   * <p>The scenario writes a header of the supported format, of this feature format, and of this
   * layout version. That header carries no creation completion evidence. Such a unit is complete
   * output of a database without a finished creation.
   *
   * <p>The expected outcome has three parts. The validation rejects the unit. The failure names
   * the missing evidence. The classification refuses every automatic removal of the unit.
   */
  @Test
  public void absentCreationEvidenceIsUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            DiskStorage.CREATION_EVIDENCE_ABSENT, true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertNull(
        "absent creation completion evidence must reject the unit", inspection.metadata());
    Assert.assertTrue(
        "the detail must name the missing creation completion evidence, saw: "
            + inspection.detail(),
        inspection.detail().contains("creation completion evidence"));
    Assert.assertEquals(
        "absent creation completion evidence must stay unclassifiable",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * A header with an unknown creation completion evidence value is unsupported.
   *
   * <p>The accepted evidence is one fixed marker. The scenario writes another value in that field.
   * The expected outcome is one rejected unit, so a random or truncated value never passes as
   * accepted evidence.
   */
  @Test
  public void foreignCreationEvidenceIsRejected() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            DiskStorage.CREATION_COMPLETED_EVIDENCE + 1, true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertNull(
        "an unknown evidence value must reject the unit", inspection.metadata());
    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE, inspection.classification());
  }

  /**
   * A complete supported header of this database with a broken hash is removable output.
   *
   * <p>The scenario writes the complete accepted header of one database and breaks the stored hash
   * code. The expected outcome has two parts. The validation rejects the unit. The classification
   * names recognized incomplete output, which an incremental backup removes before it extends the
   * chain.
   */
  @Test
  public void brokenHashOfRecognizedHeaderIsRemovableOutput() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), false);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertNull("a broken hash code must reject the unit", inspection.metadata());
    Assert.assertEquals(
        "a recognized header with a broken hash code must stay removable",
        DiskStorage.BackupUnitClassification.RECOGNIZED_INCOMPLETE,
        inspection.classification());
  }

  /**
   * An old header with a broken hash stays unclassifiable and therefore protected.
   *
   * <p>The scenario writes a header of the old backup format version and breaks the stored hash
   * code. The expected outcome is the unclassifiable classification, so no automatic removal ever
   * covers a unit of an earlier build.
   */
  @Test
  public void brokenHashOfOldHeaderStaysUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION - 1,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), false);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        "an old header must never become removable output",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * A recognized header of another database stays unclassifiable.
   *
   * <p>The scenario writes the complete accepted header of one database and inspects that unit
   * against another database identifier. The expected outcome is the unclassifiable
   * classification, so one database never removes the backup units of another database.
   */
  @Test
  public void recognizedHeaderOfAnotherDatabaseStaysUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), false);

    final var inspection = inspect(unit, unitFileName(uuid, 1), UUID.randomUUID());

    Assert.assertEquals(
        "a unit of another database must never become removable output",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * A supported unit reports its semantic identity to the caller.
   *
   * <p>The scenario writes the complete accepted header of one database. The expected outcome has
   * two parts. The inspection accepts the unit. The returned metadata carries the accepted
   * semantic identity of this build.
   */
  @Test
  public void supportedUnitCarriesTheAcceptedSemanticIdentity() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.SUPPORTED, inspection.classification());
    Assert.assertNotNull(inspection.metadata());
    Assert.assertEquals(identity, inspection.metadata().semanticIdentity());
  }

  /**
   * A file name that disagrees with the header stays unclassifiable and therefore protected.
   *
   * <p>An operator can rename complete output of this build. The content check of such a unit
   * passes, so the disagreement alone proves nothing about the content. The scenario writes the
   * complete accepted header of one database under a file name of another sequence number. The
   * expected outcome has two parts. The validation rejects the unit. The classification stays
   * unclassifiable, so no automatic removal ever covers the renamed unit.
   */
  @Test
  public void namingDisagreementOfARecognizedHeaderStaysUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), true);

    final var inspection = inspect(unit, unitFileName(uuid, 7), uuid);

    Assert.assertNull("a renamed unit must reject the restore", inspection.metadata());
    Assert.assertEquals(
        "a naming disagreement must never become removable output",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * A file name without a valid sequence number stays unclassifiable and therefore protected.
   *
   * <p>The scenario writes the complete accepted header of one database under a file name whose
   * sequence part holds no number. The expected outcome is the unclassifiable classification, so
   * a foreign name never turns complete output into removable output.
   */
  @Test
  public void unreadableSequenceNumberOfARecognizedHeaderStaysUnclassifiable() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), true);

    final var inspection =
        inspect(unit, uuid + "-2021-01-01-00-00-00-invalid-db.ibu", uuid);

    Assert.assertEquals(
        "an unreadable sequence number must never become removable output",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * An authentic unit of the earlier version 2 format stays unclassifiable and protected.
   *
   * <p>A version 2 tail is twelve bytes shorter than the tail of this build, and its hash code
   * covers every byte before that hash code. This build therefore reads the last bytes of the
   * content as header fields. The scenario inspects such a unit against the database identifier
   * (UUID) of its own database. The expected outcome has two parts. The validation rejects the
   * unit. The classification stays unclassifiable, so an incremental backup never deletes a real
   * legacy chain.
   */
  @Test
  public void authenticVersion2UnitStaysUnclassifiableForItsOwnDatabase() throws IOException {
    final var uuid = UUID.randomUUID();
    final var unit = BackupUnitFiles.legacyVersion2UnitBytes(uuid, "db", 1, true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), uuid);

    Assert.assertNull("a version 2 unit must reject the restore", inspection.metadata());
    Assert.assertEquals(
        "a version 2 unit must never become removable output",
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE,
        inspection.classification());
  }

  /**
   * An authentic version 2 unit stays unclassifiable without any expected database identifier.
   *
   * <p>A restore of one backup directory expects any database identifier (UUID) when the caller
   * names none. The scenario inspects one authentic version 2 unit in that form. The expected
   * outcome has two parts. The validation rejects the unit. The detail names the unsupported
   * backup format version.
   */
  @Test
  public void authenticVersion2UnitStaysUnclassifiableWithoutAnExpectedIdentifier()
      throws IOException {
    final var uuid = UUID.randomUUID();
    final var unit = BackupUnitFiles.legacyVersion2UnitBytes(uuid, "db", 1, true);

    final var inspection = inspect(unit, unitFileName(uuid, 1), null);

    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE, inspection.classification());
    Assert.assertTrue(
        "the detail must name the unsupported backup format version, saw: " + inspection.detail(),
        inspection.detail().contains("backup format version"));
  }

  /**
   * The header-only admission accepts one supported unit below the head of a chain.
   *
   * <p>An incremental backup admits every unit below the head of the chain from its header alone.
   * The scenario inspects one supported unit in that form. The expected outcome has two parts.
   * The admission accepts the unit. The returned metadata carries the sequence number of the
   * header.
   */
  @Test
  public void headerOnlyAdmissionAcceptsASupportedUnit() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), true);

    final var inspection = inspectHeader(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.SUPPORTED, inspection.classification());
    Assert.assertNotNull(inspection.metadata());
    Assert.assertEquals(1, inspection.metadata().sequenceNumber());
  }

  /**
   * The header-only admission runs no content check and therefore removes nothing.
   *
   * <p>The head of one chain proves the content of every older unit. The scenario breaks the
   * stored hash code of one otherwise supported unit and admits that unit from its header alone.
   * The expected outcome is the supported classification, which proves that this admission reads
   * no content hash and never reports removable output.
   */
  @Test
  public void headerOnlyAdmissionSkipsTheContentCheck() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), false);

    final var inspection = inspectHeader(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        "the header-only admission must report no content failure",
        DiskStorage.BackupUnitClassification.SUPPORTED,
        inspection.classification());
  }

  /**
   * The header-only admission refuses an old header below the head of a chain.
   *
   * <p>The scenario admits one unit of the earlier backup format version from its header alone.
   * The expected outcome is the unclassifiable classification, so one unsupported older unit
   * refuses the extension of its chain.
   */
  @Test
  public void headerOnlyAdmissionRefusesAnOldHeader() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION - 1,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            identity.creationEvidence(), true);

    final var inspection = inspectHeader(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE, inspection.classification());
  }

  /**
   * The header-only admission refuses a header without creation completion evidence.
   *
   * <p>The scenario admits one unit of the accepted database format without creation completion
   * evidence from its header alone. The expected outcome has two parts. The admission refuses the
   * unit. The detail names the missing creation completion evidence.
   */
  @Test
  public void headerOnlyAdmissionRefusesAbsentCreationCompletionEvidence() throws IOException {
    final var uuid = UUID.randomUUID();
    final var identity = DiskStorage.supportedBackupSemanticIdentity();
    final var unit =
        backupUnitWithHeader(uuid, 1, DiskStorage.CURRENT_BACKUP_FORMAT_VERSION,
            identity.featureFormatVersion(), identity.storageLayoutVersion(),
            DiskStorage.CREATION_EVIDENCE_ABSENT, true);

    final var inspection = inspectHeader(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE, inspection.classification());
    Assert.assertTrue(
        "the detail must name the missing creation completion evidence, saw: "
            + inspection.detail(),
        inspection.detail().contains("creation completion evidence"));
  }

  /**
   * The header-only admission refuses an authentic unit of the earlier version 2 format.
   *
   * <p>The scenario admits one authentic version 2 unit from its header alone. The expected
   * outcome is the unclassifiable classification. One real legacy unit below the head of a chain
   * therefore refuses the extension of that chain, and no deletion follows.
   */
  @Test
  public void headerOnlyAdmissionRefusesAnAuthenticVersion2Unit() throws IOException {
    final var uuid = UUID.randomUUID();
    final var unit = BackupUnitFiles.legacyVersion2UnitBytes(uuid, "db", 1, true);

    final var inspection = inspectHeader(unit, unitFileName(uuid, 1), uuid);

    Assert.assertEquals(
        DiskStorage.BackupUnitClassification.UNCLASSIFIABLE, inspection.classification());
  }
}
