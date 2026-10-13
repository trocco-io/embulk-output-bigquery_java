package org.embulk.output.bigquery_java.testhost;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.Job;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.spi.v2.HttpBigQueryRpc;
import com.google.cloud.http.HttpTransportOptions;
import java.io.IOException;

// Test-only seam (installed by TestHostBigQueryOptions): google-cloud-bigquery 2.14.0's
// HttpBigQueryRpc#open() starts the resumable upload session at the hardcoded googleapis.com
// host regardless of BigQueryOptions#setHost(), so the upload could never reach a MockWebServer.
// This override builds that one URL from the configured host instead, which is what upstream
// does since googleapis/java-bigquery#2696. Everything else, including the chunk uploads
// (write() follows the Location returned here), is the library's own code.
final class TestHostBigQueryRpc extends HttpBigQueryRpc {
  private static final String RESUMABLE_PATH = "upload/bigquery/v2/projects/";

  private final BigQueryOptions options;
  private final HttpRequestFactory requestFactory;

  // Mirrors how the library's own constructor builds its request factory:
  // https://github.com/googleapis/java-bigquery/blob/v2.14.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/spi/v2/HttpBigQueryRpc.java#L102-L117
  TestHostBigQueryRpc(BigQueryOptions options) {
    super(options);
    this.options = options;
    // HttpBigQueryRpc keeps its own request factory private; build an equivalent one from the
    // same transport options (credentials, timeouts).
    HttpTransportOptions transportOptions = (HttpTransportOptions) options.getTransportOptions();
    this.requestFactory =
        transportOptions
            .getHttpTransportFactory()
            .create()
            .createRequestFactory(transportOptions.getHttpRequestInitializer(options));
  }

  // Overrides (hardcoded host):
  // https://github.com/googleapis/java-bigquery/blob/v2.14.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/spi/v2/HttpBigQueryRpc.java#L723-L739
  // Same as the fixed version (googleapis/java-bigquery#2696,
  // 2b4eff1fed8b1ac9bf24bd69440377e904bc66e1):
  // https://github.com/googleapis/java-bigquery/blob/v2.40.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/spi/v2/HttpBigQueryRpc.java#L776-L796
  @Override
  public String open(Job loadJob) {
    try {
      String host = options.getHost();
      String base = host.endsWith("/") ? host : host + "/";
      GenericUrl url = new GenericUrl(base + RESUMABLE_PATH + options.getProjectId() + "/jobs");
      url.set("uploadType", "resumable");
      HttpRequest request =
          requestFactory.buildPostRequest(
              url, new JsonHttpContent(GsonFactory.getDefaultInstance(), loadJob));
      request.getHeaders().set("X-Upload-Content-Value", "application/octet-stream");
      return request.execute().getHeaders().getLocation();
    } catch (IOException ex) {
      throw new BigQueryException(ex);
    }
  }
}
