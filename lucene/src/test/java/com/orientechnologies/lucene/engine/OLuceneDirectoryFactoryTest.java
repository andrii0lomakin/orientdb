package com.orientechnologies.lucene.engine;

import static com.orientechnologies.lucene.engine.OLuceneDirectoryFactory.DIRECTORY_MMAP;
import static com.orientechnologies.lucene.engine.OLuceneDirectoryFactory.DIRECTORY_NIO;
import static com.orientechnologies.lucene.engine.OLuceneDirectoryFactory.DIRECTORY_RAM;
import static com.orientechnologies.lucene.engine.OLuceneDirectoryFactory.DIRECTORY_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.util.Collections;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Created by frank on 03/03/2016. */
public class OLuceneDirectoryFactoryTest extends BaseLuceneTest {
  private OLuceneDirectoryFactory fc;
  private ODocument meta;
  private OIndexDefinition indexDef;

  @Before
  public void setUp() throws Exception {
    meta = new ODocument();
    indexDef = Mockito.mock(OIndexDefinition.class);
    when(indexDef.getFields()).thenReturn(Collections.<String>emptyList());
    when(indexDef.getClassName()).thenReturn("Song");
    fc = new OLuceneDirectoryFactory();
  }

  @Test
  public void shouldCreateNioFsDirectory() throws Exception {
    meta.field(DIRECTORY_TYPE, DIRECTORY_NIO);
    try (OrientDB ctx =
        new OrientDB("embedded:./target/testDatabase/", OrientDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + dbName
              + " plocal users (admin identified by 'adminpwd' role admin)");
      ODatabaseDocumentInternal db =
          (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");
      Directory directory = fc.createDirectory(db.getStorage(), "index.name", meta).getDirectory();
      assertThat(directory).isInstanceOf(NIOFSDirectory.class);
      assertThat(new File("./target/testDatabase/" + dbName + "/luceneIndexes/index.name"))
          .exists();
      ctx.drop(dbName);
    }
  }

  @Test
  public void shouldCreateMMapFsDirectory() throws Exception {
    meta.field(DIRECTORY_TYPE, DIRECTORY_MMAP);
    try (OrientDB ctx =
        new OrientDB("embedded:./target/testDatabase/", OrientDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + dbName
              + " plocal users (admin identified by 'adminpwd' role admin)");
      ODatabaseDocumentInternal db =
          (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");
      Directory directory = fc.createDirectory(db.getStorage(), "index.name", meta).getDirectory();
      assertThat(directory).isInstanceOf(MMapDirectory.class);
      assertThat(new File("./target/testDatabase/" + dbName + "/luceneIndexes/index.name"))
          .exists();
      ctx.drop(dbName);
    }
  }

  @Test
  public void shouldCreateRamDirectory() throws Exception {
    meta.field(DIRECTORY_TYPE, DIRECTORY_RAM);
    try (OrientDB ctx =
        new OrientDB("embedded:./target/testDatabase/", OrientDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + dbName
              + " plocal users (admin identified by 'adminpwd' role admin)");
      ODatabaseDocumentInternal db =
          (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");
      Directory directory = fc.createDirectory(db.getStorage(), "index.name", meta).getDirectory();
      assertThat(directory).isInstanceOf(RAMDirectory.class);
      ctx.drop(dbName);
    }
  }

  @Test
  public void shouldCreateRamDirectoryOnMemoryDatabase() {
    meta.field(DIRECTORY_TYPE, DIRECTORY_RAM);
    try (OrientDB ctx =
        new OrientDB("embedded:./target/testDatabase/", OrientDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + dbName
              + " memory users (admin identified by 'adminpwd' role admin)");
      ODatabaseDocumentInternal db =
          (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");
      final Directory directory =
          fc.createDirectory(db.getStorage(), "index.name", meta).getDirectory();
      // 'ODatabaseType.MEMORY' and 'DIRECTORY_RAM' determines the RAMDirectory.
      assertThat(directory).isInstanceOf(RAMDirectory.class);
      ctx.drop(dbName);
    }
  }

  @Test
  public void shouldCreateRamDirectoryOnMemoryFromMmapDatabase() {
    meta.field(DIRECTORY_TYPE, DIRECTORY_MMAP);
    try (OrientDB ctx =
        new OrientDB("embedded:./target/testDatabase/", OrientDBConfig.defaultConfig())) {
      ctx.execute(
          "create database "
              + dbName
              + " memory users (admin identified by 'adminpwd' role admin)");
      ODatabaseDocumentInternal db =
          (ODatabaseDocumentInternal) ctx.open(dbName, "admin", "adminpwd");
      final Directory directory =
          fc.createDirectory(db.getStorage(), "index.name", meta).getDirectory();
      // 'ODatabaseType.MEMORY' plus 'DIRECTORY_MMAP' leads to the same result as just
      // 'DIRECTORY_RAM'.
      assertThat(directory).isInstanceOf(RAMDirectory.class);
      ctx.drop(dbName);
    }
  }
}
