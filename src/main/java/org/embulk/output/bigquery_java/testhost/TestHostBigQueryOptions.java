package org.embulk.output.bigquery_java.testhost;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import org.embulk.output.bigquery_java.config.PluginTask;

// Test-only seam: points the BigQuery client at test_host (a MockWebServer in this plugin's own
// test suite) with no credentials. Retry backoff and timeouts come from the plugin config as in
// production (test_host.yml keeps them near zero). See TestHostBigQueryRpc for why the resumable
// upload needs its own hook on top of BigQueryOptions#setHost().
public final class TestHostBigQueryOptions {
  private TestHostBigQueryOptions() {}

  // Also requires the TEST_HOST_ENABLED environment variable (set by the `test` task in
  // build.gradle) so a plugin config alone can't redirect a real embulk run at an arbitrary host.
  public static boolean isEnabled(PluginTask task) {
    return task.getTestHost().isPresent() && "true".equals(System.getenv("TEST_HOST_ENABLED"));
  }

  public static BigQueryOptions.Builder builder(PluginTask task) {
    return BigQueryOptions.newBuilder()
        .setHost(task.getTestHost().get())
        .setCredentials(NoCredentials.getInstance())
        .setServiceRpcFactory(TestHostBigQueryRpc::new);
  }
}
