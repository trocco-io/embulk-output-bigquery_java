package org.embulk.output.bigquery_java.util;

import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.requestBodyJson;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.runWithMockServer;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.uploadedLines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

// Tests of the test harness itself.
public class TestBigqueryMockWebServerTestUtil {
  private static int get(MockWebServer server, String path) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) server.url(path).url().openConnection();
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  // A request beyond the enqueued responses is answered right away with a 599 (instead of
  // blocking until the client's read timeout), and the run is still reported as having made
  // more requests than expected. The JUnit timeout is the safety net should it block after all.
  @Test(timeout = 10_000)
  public void runWithMockServerAnswersUnexpectedRequestsRightAway() {
    int[] codes = new int[2];

    AssertionError thrown =
        assertThrows(
            AssertionError.class,
            () ->
                runWithMockServer(
                    server -> {
                      codes[0] = get(server, "/one");
                      codes[1] = get(server, "/two");
                    },
                    new MockResponse().setResponseCode(200)));

    assertEquals(200, codes[0]);
    assertEquals(599, codes[1]);
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("unexpected request count"));
  }

  // The body helpers read a copy of the recorded body, so the same request can be asserted more
  // than once (e.g. jobIdOf() then jobConfig() on the same POST).
  @Test(timeout = 10_000)
  public void requestBodyCanBeReadMoreThanOnce() throws Exception {
    RecordedRequest request =
        runWithMockServer(
                server -> {
                  HttpURLConnection connection =
                      (HttpURLConnection) server.url("/json").url().openConnection();
                  connection.setRequestMethod("POST");
                  connection.setDoOutput(true);
                  try (OutputStream out = connection.getOutputStream()) {
                    out.write("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
                  }
                  try {
                    connection.getResponseCode();
                  } finally {
                    connection.disconnect();
                  }
                },
                new MockResponse().setResponseCode(200))
            .get(0);

    for (int i = 0; i < 2; i++) {
      assertEquals(1, requestBodyJson(request).getInt("a"));
      assertEquals(Collections.singletonList("{\"a\":1}"), uploadedLines(request, false));
    }
  }
}
