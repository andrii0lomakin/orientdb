package com.orientechnologies.orient.test.server.network.http;

import java.io.IOException;
import java.net.URLEncoder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.junit.Assert;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpAuthenticationTest extends BaseHttpDatabaseTest {

  public void testChangeOfUserOnSameConnectionIsAllowed() throws IOException {
    ClassicHttpResponse response =
        get("query/"
                + getDatabaseName()
                + "/sql/"
                + URLEncoder.encode("select from OUSer", "UTF8")
                + "/10")
            .setUserName("root")
            .setUserPassword("root")
            .getResponse();
    Assert.assertEquals(response.getReasonPhrase(), response.getCode(), 200);

    ClassicHttpResponse response1 =
        get("query/"
                + getDatabaseName()
                + "/sql/"
                + URLEncoder.encode("select from OUSer", "UTF8")
                + "/10")
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse();
    Assert.assertEquals(response1.getReasonPhrase(), response1.getCode(), 200);
  }

  @Override
  public String getDatabaseName() {
    return "httpauth";
  }
}
