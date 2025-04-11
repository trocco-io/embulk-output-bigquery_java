package org.embulk.output.bigquery_java;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Schema;

public class BigqueryJobRunner implements Callable<JobStatistics> {
  private BigqueryClient client;
  private Path path;
  private PluginTask task;
  private Schema schema;

  public BigqueryJobRunner(PluginTask task, Schema schema, Path path) {
    this.task = task;
    this.path = path;
    this.schema = schema;
  }

  @Override
  public JobStatistics call() throws Exception {
    client = new BigqueryClient(this.task, this.schema);
    String tableName;
    // append_direct use table name
    if (task.getMode().equals("append_direct")) {
      tableName = task.getTable();
    } else {
      tableName = task.getTempTable().get();
    }

    return client.load(this.path, tableName, JobInfo.WriteDisposition.WRITE_APPEND);
  }
}
