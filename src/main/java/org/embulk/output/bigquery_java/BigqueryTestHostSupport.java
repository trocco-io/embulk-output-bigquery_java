package org.embulk.output.bigquery_java;

import com.google.cloud.NoCredentials;
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

// Test-only seam: TableDataWriteChannel's resumable upload session (used by BigqueryClient#load()
// in the normal path) hardcodes the real BigQuery host regardless of test_host (a
// google-cloud-bigquery:2.14.0 limitation), so it can't be exercised against a MockWebServer.
// BigqueryClient routes through here instead whenever task_host is present.
final class BigqueryTestHostSupport {
  private BigqueryTestHostSupport() {}

  static boolean isEnabled(PluginTask task) {
    return task.getTestHost().isPresent();
  }

  static BigQuery getBigQueryService(PluginTask task, String project) {
    return BigQueryOptions.newBuilder()
        .setProjectId(project)
        .setHost(task.getTestHost().get())
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
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
