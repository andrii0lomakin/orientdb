/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.ConcurrentTestHelper;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ConcurrentQueriesTest extends DocumentDBBaseTest {

  private static final int THREADS = 10;
  private static final int CYCLES = 50;
  private static final int MAX_RETRIES = 50;

  private final AtomicLong counter = new AtomicLong();
  private final AtomicLong totalRetries = new AtomicLong();

  @Parameters(value = "url")
  public ConcurrentQueriesTest(@Optional String url) {
    super(url);
  }

  class CommandExecutor implements Callable<Void> {

    String url;

    public CommandExecutor(String url) {
      this.url = url;
    }

    @Override
    public Void call() {
      for (int i = 0; i < CYCLES; i++) {
        ODatabaseDocument db = new ODatabaseDocumentTx(url).open("admin", "admin");
        try {
          for (int retry = 0; retry < MAX_RETRIES; ++retry) {
            try {
              db.command("select from Concurrent").close();

              counter.incrementAndGet();
              totalRetries.addAndGet(retry);
              break;
            } catch (ONeedRetryException e) {
              try {
                Thread.sleep(retry * 10);
              } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
              }
            }
          }
        } finally {
          db.close();
        }
      }
      return null;
    }
  }

  @BeforeClass
  public void init() {
    if (database.getMetadata().getSchema().existsClass("Concurrent")) {
      database.getMetadata().getSchema().dropClass("Concurrent");
    }

    database.getMetadata().getSchema().createClass("Concurrent");

    for (int i = 0; i < 1000; ++i) {
      database.begin();
      database.<ODocument>newInstance("Concurrent").field("test", i).save();
      database.commit();
    }
  }

  @Test
  public void concurrentCommands() throws Exception {

    ConcurrentTestHelper.test(THREADS, () -> new CommandExecutor(url));
    Assert.assertEquals(counter.get(), CYCLES * THREADS);
  }
}
