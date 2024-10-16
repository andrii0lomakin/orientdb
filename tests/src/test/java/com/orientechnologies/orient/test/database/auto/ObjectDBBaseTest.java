package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.object.db.OObjectDatabaseTxInternal;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public class ObjectDBBaseTest extends BaseTest<OObjectDatabaseTxInternal> {
  public ObjectDBBaseTest() {}

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  @Override
  protected OObjectDatabaseTxInternal createDatabaseInstance(String url) {
    return new OObjectDatabaseTxInternal(url);
  }
}
