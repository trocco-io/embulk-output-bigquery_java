package org.embulk.output.bigquery_java;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.HTMLDocument;

public class BigqueryJavaOutputPlugin
        implements OutputPlugin
{
    private final Logger logger = LoggerFactory.getLogger(BigqueryJavaOutputPlugin.class);
    private List<Path> paths;

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryConfigBuilder configBuilder = new BigqueryConfigBuilder(task);
        configBuilder.build();

        System.out.println(task.getTemplateTable().or("NOT SET!!!!!!!!!!!!!!!!"));
        BigqueryClient client = new BigqueryClient(task, schema);

        client.createTableIfNotExist(task.getTempTable().get(), task.getDataset());

        logger.info("write to files");
        control.run(task.dump());
        logger.info("embulk-output-bigquery: finish to create intermediate files");

        try {
            paths = BigqueryUtil.getIntermediateFiles(task);
        } catch (Exception e){
            logger.info(e.getMessage());
        }
        logger.debug(String.format("embulk-output-bigquery: LOAD IN PARALLEL %s",
                paths.stream().map(Path::toString).collect(Collectors.joining("\n"))));

        // transfer data to BQ from files
        ExecutorService executor = Executors.newFixedThreadPool(paths.size());
        List<Future<JobStatistics.LoadStatistics>> statisticFutures;
        List<JobStatistics.LoadStatistics> statistics = new ArrayList<>();

        statisticFutures = paths.stream()
                .map(path -> executor.submit(new BigqueryJobRunner(task, schema, path)))
                .collect(Collectors.toList());

        for (Future<JobStatistics.LoadStatistics> statisticFuture : statisticFutures) {
            try {
                statistics.add(statisticFuture.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        getTransactionReport(statistics);

        if (task.getTempTable().isPresent()){
            client.copy(task.getTempTable().get(), task.getTable(), task.getDataset(), JobInfo.WriteDisposition.WRITE_TRUNCATE);
            client.deleteTable(task.getTempTable().get());
        }

        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("bigquery_java output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new BigqueryPageOutput(task, schema);
    }


    private BigqueryTransactionReport getTransactionReport(List<JobStatistics.LoadStatistics> statistics) {
        //
        long badRecord = statistics.stream().map(JobStatistics.LoadStatistics::getBadRecords).reduce(Long::sum).orElse(0L);
        long outputRow = statistics.stream().map(JobStatistics.LoadStatistics::getOutputRows).reduce(Long::sum).orElse(0L);

        // TODO:
        return new BigqueryTransactionReport(0L, 0L, outputRow, 0L);
    }
}
