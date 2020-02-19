package org.embulk.output.bigquery_java;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.bigquery_java.config.BigqueryTaskBuilder;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryJavaOutputPlugin
        implements OutputPlugin
{
    private final Logger logger = LoggerFactory.getLogger(BigqueryJavaOutputPlugin.class);
    private List<Path> paths;
    private HashMap<Long, BigqueryFileWriter> writers = new HashMap<>();

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        BigqueryTaskBuilder.build(task);
        BigqueryClient client = new BigqueryClient(task, schema);
        client.createTableIfNotExist(task.getTempTable().get(), task.getDataset());

        control.run(task.dump());

        this.writers.values().forEach(BigqueryFileWriter::close);
        logger.info("embulk-output-bigquery: finish to create intermediate files");

        try {
            paths = BigqueryUtil.getIntermediateFiles(task);
        } catch (Exception e){
            logger.info(e.getMessage());
            throw new RuntimeException(e);
        }
        if (paths.isEmpty()){
            logger.info("embulk-output-bigquery: Nothing for transfer");
            return Exec.newConfigDiff();
        }

        logger.debug("embulk-output-bigquery: LOAD IN PARALLEL {}",
                paths.stream().map(Path::toString).collect(Collectors.joining("\n")));

        // transfer data to BQ from files
        ExecutorService executor = Executors.newFixedThreadPool(paths.size());
        List<Future<JobStatistics.LoadStatistics>> statisticFutures = new ArrayList<>();
        List<JobStatistics.LoadStatistics> statistics = new ArrayList<>();

        for (Path path: paths){
            Future<JobStatistics.LoadStatistics> loadStatisticsFuture = executor.submit(new BigqueryJobRunner(task, schema, path));
            statisticFutures.add(loadStatisticsFuture);
        }

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

        if (task.getDeleteFromLocalWhenJobEnd()){
            paths.forEach(p -> new File(p.toString()).delete());
        }else{
            paths.forEach(p->{
                File intermediateFile = new File(p.toString());
                if (intermediateFile.exists()){
                    logger.info("embulk-output-bigquery: keep {}", p.toString());
                }
            });
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


        long badRecord = statistics.stream().map(JobStatistics.LoadStatistics::getBadRecords).reduce(0L, Long::sum);

        // TODO:
        return new BigqueryTransactionReport(inputRows, responseRows, outputRows, rejectedRows);
    }
}
