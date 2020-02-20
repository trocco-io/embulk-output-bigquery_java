package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.annotations.VisibleForTesting;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Optional;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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

    public Job getJob(JobId jobId){
        return this.bigquery.getJob(jobId);
    }

    public Table getTable(String name){
        return getTable(TableId.of(this.dataset, name));
    }

    public Table getTable(TableId tableId){
        return this.bigquery.getTable(tableId);
    }

    public Table createTableIfNotExist(String table, String dataset){
        com.google.cloud.bigquery.Schema schema = buildSchema(this.schema,  this.columnOptions);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        return bigquery.create(TableInfo.newBuilder(TableId.of(dataset, table), tableDefinition).build());
    }

    public JobStatistics.LoadStatistics load(Path loadFile, String table, JobInfo.WriteDisposition writeDestination){
        UUID uuid = UUID.randomUUID();
        String jobId = String.format("embulk_load_job_%s", uuid.toString());

        if (Files.exists(loadFile)) {
            // TODO:  "embulk-output-bigquery: Load job starting... job_id:[#{job_id}] #{path} => #{@project}:#{@dataset}.#{table} in #{@location_for_log}"
            logger.info("embulk-output-bigquery: Load job starting... job_id:[{}] {} => {}.{}",
                    jobId, loadFile.toString(), this.dataset, table);
        } else {
            logger.info("embulk-output-bigquery: Load job starting... {} does not exist, skipped", loadFile.toString());
            // TODO: should throw error?
            return null;
        }

        TableId tableId = TableId.of(this.dataset, table);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.json())
                        .setWriteDisposition(writeDestination)
                        .setMaxBadRecords(this.task.getMaxBadRecords())
                        .setIgnoreUnknownValues(this.task.getIgnoreUnknownValues())
                        .setSchema(buildSchema(this.schema, this.columnOptions))
                        .build();
        TableDataWriteChannel writer = bigquery.writer(JobId.of(jobId), writeChannelConfiguration);

        try (OutputStream stream = Channels.newOutputStream(writer)) {
            Files.copy(loadFile, stream);
        } catch (IOException e){
            this.logger.info(e.getMessage());
        }

        Job job = writer.getJob();
        return (JobStatistics.LoadStatistics) waitForLoad(job);
    }

    public JobStatistics.CopyStatistics copy(String sourceTable, String destinationTable, String destinationDataset, JobInfo.WriteDisposition writeDestination){
        UUID uuid = UUID.randomUUID();
        String jobId = String.format("embulk_load_job_%s", uuid.toString());
        TableId destTableId = TableId.of(destinationDataset, destinationTable);
        TableId srcTableId = TableId.of(this.dataset, sourceTable);

        CopyJobConfiguration copyJobConfiguration = CopyJobConfiguration.newBuilder(destTableId, srcTableId)
                .setWriteDisposition(writeDestination)
                .build();

        Job job = bigquery.create(JobInfo.newBuilder(copyJobConfiguration).setJobId(JobId.of(jobId)).build());
        return (JobStatistics.CopyStatistics) waitForCopy(job);
    }

    public boolean deleteTable(String table){
        return this.bigquery.delete(TableId.of(this.dataset, table));
    }

    public boolean deleteTable(String table, String dataset){
        return this.bigquery.delete(TableId.of(dataset, table));
    }

    private JobStatistics waitForLoad(Job job){
        return new BigqueryJobWaiter(this.task, this, job).waitFor("Load");
    }

    private JobStatistics waitForCopy(Job job){
        return new BigqueryJobWaiter(this.task, this, job).waitFor("Copy");
    }

    @VisibleForTesting
    protected com.google.cloud.bigquery.Schema buildSchema(Schema schema, List<BigqueryColumnOption> columnOptions){
        // TODO: support schema file

        if (this.task.getTemplateTable().isPresent()){
            TableId tableId = TableId.of(this.dataset, this.task.getTemplateTable().get());
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
                if (colOpt.getType().isPresent()){
                    typeName = StandardSQLTypeName.valueOf(colOpt.getType().get());
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
