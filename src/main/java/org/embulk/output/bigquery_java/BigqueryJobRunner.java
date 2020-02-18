package org.embulk.output.bigquery_java;


import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Schema;

import java.nio.file.Path;
import java.util.concurrent.Callable;

public class BigqueryJobRunner implements Callable<JobStatistics.LoadStatistics> {
    private BigqueryClient client;
    private Path path;
    private PluginTask task;
    private Schema schema;

    public BigqueryJobRunner(PluginTask task, Schema schema, Path path){
        this.task = task;
        this.path = path;
        this.schema = schema;
    }

    public JobStatistics.LoadStatistics call(){
        client = new BigqueryClient(this.task, this.schema);

        return client.load(this.path,
                this.task.getTempTable().get(),
                JobInfo.WriteDisposition.WRITE_APPEND);
    }
}
