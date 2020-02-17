package org.embulk.output.bigquery_java;

import com.google.api.services.bigquery.Bigquery;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.*;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.threeten.bp.Duration;

import static com.google.cloud.bigquery.JobStatus.State.DONE;

public class BigqueryClient {
    private final Logger logger = LoggerFactory.getLogger(BigqueryClient.class);
    private BigQuery bigquery;
    private String dataset;
    private PluginTask task;
    private Schema schema;
    private List<BigqueryColumnOption> columnOptions;


    public BigqueryClient(PluginTask task, Schema schema){
        this.task = task;
        this.schema = schema;
        this.dataset = task.getDataset();
        this.columnOptions = this.task.getColumnOptions().orElse(Collections.emptyList());
        try{
            this.bigquery = getClientWithJsonKey(this.task.getJsonKeyfile());
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private static BigQuery getClientWithJsonKey(String key) throws IOException {
        return BigQueryOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }

    // def load_in_parallel(paths, table)
    public void loadInParallel(String[] paths, String table){

    }

    public JobStatistics.LoadStatistics load(String path, String table, JobInfo.WriteDisposition writeDestination){
        Path loadFile = Paths.get(path);
        UUID uuid = UUID.randomUUID();
        String jobId = String.format("embulk_load_job_%s", uuid.toString());

        if (Files.exists(loadFile)) {
            // TODO:  "embulk-output-bigquery: Load job starting... job_id:[#{job_id}] #{path} => #{@project}:#{@dataset}.#{table} in #{@location_for_log}"
            String msg = String.format( "embulk-output-bigquery: Load job starting... job_id:[%s] %s => %s.%s",
                    jobId, loadFile.toString(), this.dataset, table);
            logger.info(msg);
        } else {
            logger.info(String.format("embulk-output-bigquery: Load job starting... %s does not exist, skipped", loadFile.toString()));
            // TODO: should throw error?
            return null;
        }


        TableId tableId = TableId.of(this.dataset, table);
        WriteChannelConfiguration writeChannelConfiguration =
                // Encoding ??
                // allow_quoted_newlines ??
                // jobreference ??
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.json())
                        .setWriteDisposition(writeDestination)
                        .setMaxBadRecords(this.task.getMaxBadRecords())
                        .setIgnoreUnknownValues(this.task.getIgnoreUnknownValues())
                        .setSchema(buildSchema(this.schema, this.columnOptions))
                        .build();
        TableDataWriteChannel writer = bigquery.writer(JobId.of(jobId), writeChannelConfiguration);

        try (OutputStream stream = Channels.newOutputStream(writer)) {
            Files.copy(Paths.get(path), stream);
        } catch (IOException e){
            this.logger.info(e.getMessage());
        }

        Job job = writer.getJob();
        return waitLoad(job);
    }

    // def copy(source_table, destination_table, destination_dataset = nil, write_disposition: 'WRITE_TRUNCATE')
    public void copy(String sourceTable, String destinationTable, String destinationDataset, String writeDestination){

    }

    private JobStatistics.LoadStatistics waitLoad(Job job){
        Job completedJob;
        Date started = new Date();

        while (true) {
            completedJob = this.bigquery.getJob(job.getJobId(), BigQuery.JobOption.fields(BigQuery.JobField.STATUS));
            Date now = new Date();
            long elapsed = (now.getTime() - started.getTime()) / 1000;
            if (completedJob.getStatus().getState().equals(DONE)) {
                // logger.info("embulk-output-bigquery: #{kind} job completed... ");
                logger.info("embulk-output-bigquery: #{kind} job completed... ");
                logger.info(String.format("job_id:%s elapsed_time %d sec status[DONE]", completedJob.getJobId(), elapsed));
                break;
            } else if (elapsed > this.task.getJobStatusMaxPollingTime()) {
                // logger.info("embulk-output-bigquery: #{kind} job checking... ");
                logger.warn("embulk-output-bigquery: job checking... ");
                logger.warn(String.format("job_id[%s] elapsed_time %d sec status[TIMEOUT]", completedJob.getJobId(), elapsed));
                break;
            } else {
                // logger.info("embulk-output-bigquery: #{kind} job checking... ");
                logger.info("embulk-output-bigquery: job checking... ");
                logger.info(String.format("job_id[%s] elapsed_time %d sec status[%s]",
                        completedJob.getJobId(), elapsed, completedJob.getStatus().toString()));
                try {
                    Thread.sleep(this.task.getJobStatusPollingInterval() * 1000);
                }catch (InterruptedException e){
                    logger.info(e.getLocalizedMessage());
                    throw new RuntimeException(e);
                }
                break;
            }
        }

        if (completedJob.getStatus().getError() != null){
            logger.warn(String.format("job_id[%s] elapsed_time %d sec status[%s]"));
            //  Embulk.logger.warn { "embulk-output-bigquery: #{kind} job errors... job_id:[#{job_id}] errors:#{_errors.map(&:to_h)}" }
            logger.warn(String.format("embulk-output-bigquery: job errors... job_id:[%s] errors:%s",
                    completedJob.getJobId(), completedJob.getStatus().getError().getMessage()));

        }

        // TODO: "embulk-output-bigquery: #{kind} job response... job_id:[#{job_id}] response.statistics:#{_response.statistics.to_h}" }
        logger.info(String.format("embulk-output-bigquery: job response... job_id:[%s] response.statistics:%s",
                completedJob.getJobId().toString(), completedJob.getStatus().toString()), completedJob.getStatistics().toString());

        return completedJob.getStatistics();
    }

    @VisibleForTesting
    protected com.google.cloud.bigquery.Schema buildSchema(Schema schema, List<BigqueryColumnOption> columnOptions){
        // TODO: support schema file

        if (!this.task.getTemplateTable().isEmpty()){
            TableId tableId = TableId.of(this.dataset, this.task.getTemplateTable());
            Table table = this.bigquery.getTable(tableId);
            return table.getDefinition().getSchema();
        }

        List<Field> fields =  new ArrayList<>();

        for (Column col : schema.getColumns()){
            StandardSQLTypeName typeName = getStandardSQLTypeNameByEmbulkType(col.getType());
            Field.Mode fieldMode = Field.Mode.NULLABLE;
            Optional<BigqueryColumnOption> columnOption =  BigqueryUtil.findColumnOption(col.getName(), columnOptions);

            if (columnOption.isPresent()){
                BigqueryColumnOption colOpt = columnOption.get();

                if (!colOpt.getMode().isEmpty()) {
                    fieldMode = Field.Mode.valueOf(colOpt.getMode());
                }

                if (!colOpt.getType().isEmpty()){
                    typeName = StandardSQLTypeName.valueOf(colOpt.getType());
                }
            }

            //  TODO:: support field for JSON type
            Field field = Field.of(col.getName(), typeName)
                    .toBuilder()
                    .setMode(fieldMode)
                    .build();
            fields.add(field);
        }
        return com.google.cloud.bigquery.Schema.of(fields);
    }

    @VisibleForTesting
    protected StandardSQLTypeName getStandardSQLTypeNameByEmbulkType(Type type){
        if (type instanceof BooleanType) {
            return StandardSQLTypeName.BOOL;
        } else if (type instanceof LongType) {
            return StandardSQLTypeName.INT64;
        } else if (type instanceof DoubleType) {
            return StandardSQLTypeName.FLOAT64;
        } else if (type instanceof StringType) {
            return StandardSQLTypeName.STRING;
        } else if (type instanceof TimestampType) {
            return StandardSQLTypeName.TIMESTAMP;
        } else if (type instanceof JsonType) {
            return StandardSQLTypeName.STRING;
        } else {
            throw new RuntimeException("never reach here");
        }
    }
}
