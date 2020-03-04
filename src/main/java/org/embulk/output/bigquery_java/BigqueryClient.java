package org.embulk.output.bigquery_java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.base.Throwables;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryBackendException;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.embulk.output.bigquery_java.exception.BigqueryInternalException;
import org.embulk.output.bigquery_java.exception.BigqueryRateLimitExceededException;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.RetryExecutor;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigqueryClient {
    private final Logger logger = LoggerFactory.getLogger(BigqueryClient.class);
    private BigQuery bigquery;
    private String dataset;
    private PluginTask task;
    private Schema schema;
    private List<BigqueryColumnOption> columnOptions;

    public BigqueryClient(PluginTask task, Schema schema) {
        this.task = task;
        this.schema = schema;
        this.dataset = task.getDataset();
        this.columnOptions = this.task.getColumnOptions().orElse(Collections.emptyList());
        try {
            this.bigquery = getClientWithJsonKey(this.task.getJsonKeyfile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static BigQuery getClientWithJsonKey(String key) throws IOException {
        return BigQueryOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }

    public Job getJob(JobId jobId) {
        return this.bigquery.getJob(jobId);
    }

    public Table getTable(String name) {
        return getTable(TableId.of(this.dataset, name));
    }

    public Table getTable(TableId tableId) {
        return this.bigquery.getTable(tableId);
    }

    public Table createTableIfNotExist(String table, String dataset) {
        com.google.cloud.bigquery.Schema schema = buildSchema(this.schema, this.columnOptions);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        return bigquery.create(TableInfo.newBuilder(TableId.of(dataset, table), tableDefinition).build());
    }

    public JobStatistics.LoadStatistics load(Path loadFile, String table, JobInfo.WriteDisposition writeDestination) throws BigqueryException {
        String dataset = this.dataset;
        int retries = this.task.getRetries();
        PluginTask task = this.task;
        Schema schema = this.schema;
        List<BigqueryColumnOption> columnOptions = this.columnOptions;

        try {
            return retryExecutor()
                    .withRetryLimit(retries)
                    .withInitialRetryWait(2 * 1000)
                    .withMaxRetryWait(10 * 1000)
                    .runInterruptible(new RetryExecutor.Retryable<JobStatistics.LoadStatistics>() {
                        @Override
                        public JobStatistics.LoadStatistics call() {
                            UUID uuid = UUID.randomUUID();
                            String jobId = String.format("embulk_load_job_%s", uuid.toString());

                            if (Files.exists(loadFile)) {
                                // TODO:  "embulk-output-bigquery: Load job starting... job_id:[#{job_id}] #{path} => #{@project}:#{@dataset}.#{table} in #{@location_for_log}"
                                logger.info("embulk-output-bigquery: Load job starting... job_id:[{}] {} => {}.{}",
                                        jobId, loadFile.toString(), dataset, table);
                            } else {
                                logger.info("embulk-output-bigquery: Load job starting... {} does not exist, skipped", loadFile.toString());
                                // TODO: should throw error?
                                return null;
                            }

                            TableId tableId = TableId.of(dataset, table);
                            WriteChannelConfiguration writeChannelConfiguration =
                                    WriteChannelConfiguration.newBuilder(tableId)
                                            .setFormatOptions(FormatOptions.json())
                                            .setWriteDisposition(writeDestination)
                                            .setMaxBadRecords(task.getMaxBadRecords())
                                            .setIgnoreUnknownValues(task.getIgnoreUnknownValues())
                                            .setSchema(buildSchema(schema, columnOptions))
                                            .build();
                            TableDataWriteChannel writer = bigquery.writer(JobId.of(jobId), writeChannelConfiguration);

                            try (OutputStream stream = Channels.newOutputStream(writer)) {
                                Files.copy(loadFile, stream);
                            } catch (IOException e) {
                                logger.info(e.getMessage());
                            }

                            Job job = writer.getJob();
                            return (JobStatistics.LoadStatistics) waitForLoad(job);
                        }

                        @Override
                        public boolean isRetryableException(Exception exception) {
                            return exception instanceof BigqueryBackendException
                                    || exception instanceof BigqueryRateLimitExceededException
                                    || exception instanceof BigqueryInternalException;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryExecutor.RetryGiveupException {
                            String message = String.format("embulk-output-bigquery: Load job failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % retries == 0) {
                                logger.warn(message, exception);
                            } else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryExecutor.RetryGiveupException {
                            logger.error("embulk-output-bigquery: Give up retrying for Load job");
                        }
                    });

        } catch (RetryExecutor.RetryGiveupException ex) {
            Throwables.throwIfInstanceOf(ex.getCause(), BigqueryException.class);
            // TODO:
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            throw new BigqueryException("interrupted");
        }
    }

    public JobStatistics.CopyStatistics copy(String sourceTable,
                                             String destinationTable,
                                             String destinationDataset,
                                             JobInfo.WriteDisposition writeDestination) throws BigqueryException {
        String dataset = this.dataset;
        int retries = this.task.getRetries();

        try {
            return retryExecutor()
                    .withRetryLimit(retries)
                    .withInitialRetryWait(2 * 1000)
                    .withMaxRetryWait(10 * 1000)
                    .runInterruptible(new RetryExecutor.Retryable<JobStatistics.CopyStatistics>() {
                        @Override
                        public JobStatistics.CopyStatistics call() {
                            UUID uuid = UUID.randomUUID();
                            String jobId = String.format("embulk_load_job_%s", uuid.toString());
                            TableId destTableId = TableId.of(destinationDataset, destinationTable);
                            TableId srcTableId = TableId.of(dataset, sourceTable);

                            CopyJobConfiguration copyJobConfiguration = CopyJobConfiguration.newBuilder(destTableId, srcTableId)
                                    .setWriteDisposition(writeDestination)
                                    .build();

                            Job job = bigquery.create(JobInfo.newBuilder(copyJobConfiguration).setJobId(JobId.of(jobId)).build());
                            return (JobStatistics.CopyStatistics) waitForCopy(job);
                        }

                        @Override
                        public boolean isRetryableException(Exception exception) {
                            return exception instanceof BigqueryBackendException
                                    || exception instanceof BigqueryRateLimitExceededException
                                    || exception instanceof BigqueryInternalException;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryExecutor.RetryGiveupException {
                            String message = String.format("embulk-output-bigquery: Copy job failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % retries == 0) {
                                logger.warn(message, exception);
                            } else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryExecutor.RetryGiveupException {
                            logger.error("embulk-output-bigquery: Give up retrying for Copy job");
                        }
                    });

        } catch (RetryExecutor.RetryGiveupException ex) {
            Throwables.throwIfInstanceOf(ex.getCause(), BigqueryException.class);
            // TODO:
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            throw new BigqueryException("interrupted");
        }
    }

    public boolean deleteTable(String table) {
        return this.bigquery.delete(TableId.of(this.dataset, table));
    }

    public boolean deleteTable(String table, String dataset) {
        return this.bigquery.delete(TableId.of(dataset, table));
    }

    private JobStatistics waitForLoad(Job job) throws BigqueryException {
        return new BigqueryJobWaiter(this.task, this, job).waitFor("Load");
    }

    private JobStatistics waitForCopy(Job job) throws BigqueryException {
        return new BigqueryJobWaiter(this.task, this, job).waitFor("Copy");
    }

    @VisibleForTesting
    protected com.google.cloud.bigquery.Schema buildSchema(Schema schema, List<BigqueryColumnOption> columnOptions) {
        // TODO: support schema file

        if (this.task.getTemplateTable().isPresent()) {
            TableId tableId = TableId.of(this.dataset, this.task.getTemplateTable().get());
            Table table = this.bigquery.getTable(tableId);
            return table.getDefinition().getSchema();
        }

        List<Field> fields = new ArrayList<>();

        for (Column col : schema.getColumns()) {
            Field field;
            StandardSQLTypeName sqlTypeName = getStandardSQLTypeNameByEmbulkType(col.getType());
            LegacySQLTypeName legacySQLTypeName = getLegacySQLTypeNameByEmbulkType(col.getType());
            Field.Mode fieldMode = Field.Mode.NULLABLE;
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(col.getName(), columnOptions);

            if (columnOption.isPresent()) {
                BigqueryColumnOption colOpt = columnOption.get();
                if (!colOpt.getMode().isEmpty()) {
                    fieldMode = Field.Mode.valueOf(colOpt.getMode());
                }
                if (colOpt.getType().isPresent()) {
                    if (this.task.getEnableStandardSQL()) {
                        sqlTypeName = StandardSQLTypeName.valueOf(colOpt.getType().get());
                    } else {
                        legacySQLTypeName = LegacySQLTypeName.valueOf(colOpt.getType().get());
                    }
                }
            }

            if (task.getEnableStandardSQL()) {
                field = Field.of(col.getName(), sqlTypeName);
            } else {
                field = Field.of(col.getName(), legacySQLTypeName);
            }

            //  TODO:: support field for JSON type
            field = field.toBuilder()
                    .setMode(fieldMode)
                    .build();
            fields.add(field);
        }
        return com.google.cloud.bigquery.Schema.of(fields);
    }

    @VisibleForTesting
    protected StandardSQLTypeName getStandardSQLTypeNameByEmbulkType(Type type) {
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

    @VisibleForTesting
    protected LegacySQLTypeName getLegacySQLTypeNameByEmbulkType(Type type) {
        if (type instanceof BooleanType) {
            return LegacySQLTypeName.BOOLEAN;
        } else if (type instanceof LongType) {
            return LegacySQLTypeName.INTEGER;
        } else if (type instanceof DoubleType) {
            return LegacySQLTypeName.FLOAT;
        } else if (type instanceof StringType) {
            return LegacySQLTypeName.STRING;
        } else if (type instanceof TimestampType) {
            return LegacySQLTypeName.TIMESTAMP;
        } else if (type instanceof JsonType) {
            return LegacySQLTypeName.STRING;
        } else {
            throw new RuntimeException("never reach here");
        }

    }
}
