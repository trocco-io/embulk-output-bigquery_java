package org.embulk.output.bigquery_java;

import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import java.util.Collections;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.threeten.bp.Duration;

// Test-only seam: TableDataWriteChannel's resumable upload session (used by BigqueryClient#load()
// in the normal path) hardcodes the real BigQuery host regardless of test_host (a
// google-cloud-bigquery:2.14.0 limitation:
// https://github.com/googleapis/java-bigquery/blob/v2.14.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/spi/v2/HttpBigQueryRpc.java#L726),
// so it can't be exercised against a MockWebServer. BigqueryClient routes through here instead
// whenever task_host is present.
final class BigqueryTestHostSupport {
  private BigqueryTestHostSupport() {}

  // Also requires the TEST_HOST_ENABLED environment variable (set by the `test` task in
  // build.gradle) so a plugin config alone can't redirect a real embulk run at an arbitrary host.
  static boolean isEnabled(PluginTask task) {
    return task.getTestHost().isPresent() && "true".equals(System.getenv("TEST_HOST_ENABLED"));
  }

  static BigQueryOptions.Builder getBigQueryOptionsBuilder(PluginTask task) {
    return BigQueryOptions.newBuilder()
        .setHost(task.getTestHost().get())
        .setCredentials(NoCredentials.getInstance())
        .setRetrySettings(
            ServiceOptions.getDefaultRetrySettings()
                .toBuilder()
                .setInitialRetryDelay(Duration.ofMillis(1))
                .setMaxRetryDelay(Duration.ofMillis(1))
                .setRetryDelayMultiplier(1.0)
                .setTotalTimeout(Duration.ofSeconds(1))
                .build());
  }

  // Submits the load job configuration directly via jobs.insert instead of streaming the file
  // through a resumable upload session, so the destination table/schema is still observable in
  // tests.
  static Job createLoadJob(
      BigQuery bigquery,
      PluginTask task,
      TableId tableId,
      String jobId,
      JobInfo.WriteDisposition writeDisposition,
      Schema schema) {
    LoadJobConfiguration loadJobConfiguration =
        LoadJobConfiguration.newBuilder(tableId, Collections.emptyList(), FormatOptions.json())
            .setWriteDisposition(writeDisposition)
            .setMaxBadRecords(task.getMaxBadRecords())
            .setIgnoreUnknownValues(task.getIgnoreUnknownValues())
            .setSchema(schema)
            .build();
    return bigquery.create(
        JobInfo.newBuilder(loadJobConfiguration).setJobId(JobId.of(jobId)).build());
  }
}
