package org.embulk.output.bigquery_java;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryTimePartitioning;
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
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryClient {
  private final Logger logger = LoggerFactory.getLogger(BigqueryClient.class);
  private BigQuery bigquery;
  private String dataset;
  private String location;
  private String locationForLog;
  private PluginTask task;
  private Schema schema;
  private List<BigqueryColumnOption> columnOptions;

  public BigqueryClient(PluginTask task, Schema schema) {
    this.task = task;
    this.schema = schema;
    this.dataset = task.getDataset();
    if (task.getLocation().isPresent()) {
      this.location = task.getLocation().get();
      this.locationForLog = task.getLocation().get();
    } else {
      this.locationForLog = "us/eu";
    }
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

  public Dataset createDataset(String datasetId) {
    DatasetInfo.Builder builder = DatasetInfo.newBuilder(datasetId);
    if (this.location != null) {
      builder.setLocation(this.location);
    }
    return bigquery.create(builder.build());
  }

  public Dataset getDataset(String datasetId) {
    return bigquery.getDataset(datasetId);
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

  public void createTableIfNotExist(String table) {
    createTableIfNotExist(table, dataset);
  }

  public void createTableIfNotExist(String table, String dataset) {
    com.google.cloud.bigquery.Schema schema = buildSchema(this.schema, this.columnOptions);
    StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition.newBuilder();
    tableDefinitionBuilder.setSchema(schema);
    if (this.task.getTimePartitioning().isPresent()) {
      tableDefinitionBuilder.setTimePartitioning(
          buildTimePartitioning(this.task.getTimePartitioning().get()));
    }
    if (this.task.getClustering().isPresent()) {
      tableDefinitionBuilder.setClustering(
          Clustering.newBuilder()
              .setFields(this.task.getClustering().get().getFields().get())
              .build());
    }
    TableDefinition tableDefinition = tableDefinitionBuilder.build();

    try {
      bigquery.create(TableInfo.newBuilder(TableId.of(dataset, table), tableDefinition).build());
    } catch (BigQueryException e) {
      if (e.getCode() == 409 && e.getMessage().contains("Already Exists:")) {
        return;
      }
      logger.error(String.format("embulk-out_bigquery: insert_table(%s, %s)", dataset, table));
      throw new BigqueryException(
          String.format("failed to create table %s.%s, response: %s", dataset, table, e));
    }
  }

  public TimePartitioning buildTimePartitioning(BigqueryTimePartitioning bigqueryTimePartitioning) {
    TimePartitioning.Builder timePartitioningBuilder;

    switch (bigqueryTimePartitioning.getType().toUpperCase()) {
      case "HOUR":
        timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.HOUR);
        break;
      case "DAY":
        timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        break;
      case "MONTH":
        timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.MONTH);
        break;
      case "YEAR":
        timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.YEAR);
        break;
      default:
        throw new RuntimeException("time_partitioning.type is not HOUR, DAY, MONTH, or YEAR");
    }

    if (bigqueryTimePartitioning.getExpirationMs().isPresent()) {
      timePartitioningBuilder.setExpirationMs(bigqueryTimePartitioning.getExpirationMs().get());
    }

    if (bigqueryTimePartitioning.getField().isPresent()) {
      timePartitioningBuilder.setField(bigqueryTimePartitioning.getField().get());
    }
    return timePartitioningBuilder.build();
  }

  public JobStatistics.LoadStatistics load(
      Path loadFile, String table, JobInfo.WriteDisposition writeDisposition)
      throws BigqueryException {
    String dataset = this.dataset;
    int retries = this.task.getRetries();
    PluginTask task = this.task;
    Schema schema = this.schema;
    List<BigqueryColumnOption> columnOptions = this.columnOptions;

    try {
      // https://cloud.google.com/bigquery/quotas#standard_tables
      // Maximum rate of table metadata update operations — 5 operations every 10 seconds per table
      return RetryExecutor.builder()
          .withRetryLimit(retries)
          .withInitialRetryWaitMillis(2 * 1000)
          .withMaxRetryWaitMillis(10 * 1000)
          .build()
          .runInterruptible(
              new Retryable<JobStatistics.LoadStatistics>() {
                @Override
                public JobStatistics.LoadStatistics call() {
                  UUID uuid = UUID.randomUUID();
                  String jobId = String.format("embulk_load_job_%s", uuid.toString());

                  if (Files.exists(loadFile)) {
                    logger.info(
                        "embulk-output-bigquery: Load job starting... job_id:[{}] {} => {}.{} in {}",
                        jobId,
                        loadFile.toString(),
                        dataset,
                        table,
                        locationForLog);
                  } else {
                    logger.info(
                        "embulk-output-bigquery: Load job starting... {} does not exist, skipped",
                        loadFile.toString());
                    // TODO: should throw error?
                    return null;
                  }

                  TableId tableId = TableId.of(dataset, table);
                  WriteChannelConfiguration writeChannelConfiguration =
                      WriteChannelConfiguration.newBuilder(tableId)
                          .setFormatOptions(FormatOptions.json())
                          .setWriteDisposition(writeDisposition)
                          .setMaxBadRecords(task.getMaxBadRecords())
                          .setIgnoreUnknownValues(task.getIgnoreUnknownValues())
                          .setSchema(buildSchema(schema, columnOptions))
                          .build();
                  TableDataWriteChannel writer =
                      bigquery.writer(JobId.of(jobId), writeChannelConfiguration);

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
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait)
                    throws RetryGiveupException {
                  String message =
                      String.format(
                          "embulk-output-bigquery: Load job failed. Retrying %d/%d after %d seconds. Message: %s",
                          retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                  if (retryCount % retries == 0) {
                    logger.warn(message, exception);
                  } else {
                    logger.warn(message);
                  }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {
                  logger.error("embulk-output-bigquery: Give up retrying for Load job");
                }
              });

    } catch (RetryGiveupException ex) {
      if (ex.getCause() instanceof BigqueryException) {
        throw (BigqueryException) ex.getCause();
      }
      // TODO:
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new BigqueryException("interrupted");
    }
  }

  public JobStatistics.CopyStatistics copy(
      String sourceTable,
      String destinationTable,
      String destinationDataset,
      JobInfo.WriteDisposition writeDisposition)
      throws BigqueryException {
    String dataset = this.dataset;
    int retries = this.task.getRetries();

    try {
      return RetryExecutor.builder()
          .withRetryLimit(retries)
          .withInitialRetryWaitMillis(2 * 1000)
          .withMaxRetryWaitMillis(10 * 1000)
          .build()
          .runInterruptible(
              new Retryable<JobStatistics.CopyStatistics>() {
                @Override
                public JobStatistics.CopyStatistics call() {
                  UUID uuid = UUID.randomUUID();
                  String jobId = String.format("embulk_load_job_%s", uuid.toString());
                  TableId destTableId = TableId.of(destinationDataset, destinationTable);
                  TableId srcTableId = TableId.of(dataset, sourceTable);

                  CopyJobConfiguration copyJobConfiguration =
                      CopyJobConfiguration.newBuilder(destTableId, srcTableId)
                          .setWriteDisposition(writeDisposition)
                          .build();

                  Job job =
                      bigquery.create(
                          JobInfo.newBuilder(copyJobConfiguration)
                              .setJobId(JobId.of(jobId))
                              .build());
                  return (JobStatistics.CopyStatistics) waitForCopy(job);
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  return exception instanceof BigqueryBackendException
                      || exception instanceof BigqueryRateLimitExceededException
                      || exception instanceof BigqueryInternalException;
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait)
                    throws RetryGiveupException {
                  String message =
                      String.format(
                          "embulk-output-bigquery: Copy job failed. Retrying %d/%d after %d seconds. Message: %s",
                          retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                  if (retryCount % retries == 0) {
                    logger.warn(message, exception);
                  } else {
                    logger.warn(message);
                  }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {
                  logger.error("embulk-output-bigquery: Give up retrying for Copy job");
                }
              });

    } catch (RetryGiveupException ex) {
      if (ex.getCause() instanceof BigqueryException) {
        throw (BigqueryException) ex.getCause();
      }
      // TODO:
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new BigqueryException("interrupted");
    }
  }

  public JobStatistics.QueryStatistics merge(
      String sourceTable, String targetTable, List<String> mergeKeys, List<String> mergeRule) {
    StringBuilder sb = new StringBuilder();
    sb.append("MERGE ");
    sb.append(quoteIdentifier(dataset));
    sb.append(".");
    sb.append(quoteIdentifier(targetTable));
    sb.append(" T");
    sb.append(" USING ");
    sb.append(quoteIdentifier(dataset));
    sb.append(".");
    sb.append(quoteIdentifier(sourceTable));
    sb.append(" S");
    sb.append(" ON ");
    appendMergeKeys(sb, mergeKeys.isEmpty() ? getMergeKeys(targetTable) : mergeKeys);
    sb.append(" WHEN MATCHED THEN");
    sb.append(" UPDATE SET ");
    appendMergeRule(sb, mergeRule, schema);
    sb.append(" WHEN NOT MATCHED THEN");
    sb.append(" INSERT (");
    appendColumns(sb, schema);
    sb.append(") VALUES (");
    appendColumns(sb, schema);
    sb.append(")");
    String query = sb.toString();
    logger.info(String.format("embulk-output-bigquery: Execute query... %s", query));
    return executeQuery(query);
  }

  private List<String> getMergeKeys(String table) {
    String query =
        "SELECT"
            + " KCU.COLUMN_NAME "
            + "FROM "
            + quoteIdentifier(dataset)
            + ".INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU "
            + "JOIN "
            + quoteIdentifier(dataset)
            + ".INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC "
            + "ON"
            + " KCU.CONSTRAINT_CATALOG = TC.CONSTRAINT_CATALOG AND"
            + " KCU.CONSTRAINT_SCHEMA = TC.CONSTRAINT_SCHEMA AND"
            + " KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME AND"
            + " KCU.TABLE_CATALOG = TC.TABLE_CATALOG AND"
            + " KCU.TABLE_SCHEMA = TC.TABLE_SCHEMA AND"
            + " KCU.TABLE_NAME = TC.TABLE_NAME "
            + "WHERE"
            + " TC.TABLE_NAME = '"
            + table
            + "' AND"
            + " TC.CONSTRAINT_TYPE = 'PRIMARY KEY' "
            + "ORDER BY"
            + " KCU.ORDINAL_POSITION";
    return stream(runQuery(query).iterateAll())
        .flatMap(BigqueryClient::stream)
        .map(FieldValue::getStringValue)
        .collect(Collectors.toList());
  }

  public TableResult runQuery(String query) {
    int retries = task.getRetries();
    try {
      return RetryExecutor.builder()
          .withRetryLimit(retries)
          .withInitialRetryWaitMillis(2 * 1000)
          .withMaxRetryWaitMillis(10 * 1000)
          .build()
          .runInterruptible(
              new Retryable<TableResult>() {
                @Override
                public TableResult call() throws Exception {
                  QueryJobConfiguration configuration =
                      QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
                  String job = String.format("embulk_query_job_%s", UUID.randomUUID());
                  JobId.Builder builder = JobId.newBuilder().setJob(job);
                  if (location != null) {
                    builder.setLocation(location);
                  }
                  return bigquery.query(configuration, builder.build());
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  return exception instanceof BigqueryBackendException
                      || exception instanceof BigqueryRateLimitExceededException
                      || exception instanceof BigqueryInternalException;
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait) {
                  String message =
                      String.format(
                          "embulk-output-bigquery: Query job failed. Retrying %d/%d after %d seconds. Message: %s",
                          retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                  if (retryCount % retries == 0) {
                    logger.warn(message, exception);
                  } else {
                    logger.warn(message);
                  }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException) {
                  logger.error("embulk-output-bigquery: Give up retrying for Query job");
                }
              });
    } catch (RetryGiveupException e) {
      if (e.getCause() instanceof BigqueryException) {
        throw (BigqueryException) e.getCause();
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new BigqueryException("interrupted");
    }
  }

  private static <T> Stream<T> stream(Iterable<T> iterable) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterable.iterator(), Spliterator.ORDERED), false);
  }

  private static StringBuilder appendMergeKeys(StringBuilder sb, List<String> mergeKeys) {
    if (mergeKeys.isEmpty()) {
      throw new RuntimeException("merge key or primary key is required");
    }
    for (int i = 0; i < mergeKeys.size(); i++) {
      if (i != 0) {
        sb.append(" AND ");
      }
      String mergeKey = quoteIdentifier(mergeKeys.get(i));
      sb.append("T.");
      sb.append(mergeKey);
      sb.append(" = S.");
      sb.append(mergeKey);
    }
    return sb;
  }

  private static StringBuilder appendMergeRule(
      StringBuilder sb, List<String> mergeRule, Schema schema) {
    return mergeRule.isEmpty() ? appendMergeRule(sb, schema) : appendMergeRule(sb, mergeRule);
  }

  private static StringBuilder appendMergeRule(StringBuilder sb, List<String> mergeRule) {
    for (int i = 0; i < mergeRule.size(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(mergeRule.get(i));
    }
    return sb;
  }

  private static StringBuilder appendMergeRule(StringBuilder sb, Schema schema) {
    for (int i = 0; i < schema.getColumnCount(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      String column = quoteIdentifier(schema.getColumnName(i));
      sb.append("T.");
      sb.append(column);
      sb.append(" = S.");
      sb.append(column);
    }
    return sb;
  }

  private static StringBuilder appendColumns(StringBuilder sb, Schema schema) {
    for (int i = 0; i < schema.getColumnCount(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(quoteIdentifier(schema.getColumnName(i)));
    }
    return sb;
  }

  private static String quoteIdentifier(String identifier) {
    return "`" + identifier + "`";
  }

  public JobStatistics.QueryStatistics executeQuery(String query) {
    int retries = this.task.getRetries();
    String location = this.location;

    try {
      return RetryExecutor.builder()
          .withRetryLimit(retries)
          .withInitialRetryWaitMillis(2 * 1000)
          .withMaxRetryWaitMillis(10 * 1000)
          .build()
          .runInterruptible(
              new Retryable<JobStatistics.QueryStatistics>() {
                @Override
                public JobStatistics.QueryStatistics call() {
                  UUID uuid = UUID.randomUUID();
                  String jobId = String.format("embulk_query_job_%s", uuid.toString());

                  QueryJobConfiguration queryConfig =
                      QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
                  JobId.Builder jobIdBuilder = JobId.newBuilder().setJob(jobId);
                  if (location != null) {
                    jobIdBuilder.setLocation(location);
                  }

                  Job job =
                      bigquery.create(
                          JobInfo.newBuilder(queryConfig).setJobId(jobIdBuilder.build()).build());
                  return (JobStatistics.QueryStatistics) waitForQuery(job);
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  return exception instanceof BigqueryBackendException
                      || exception instanceof BigqueryRateLimitExceededException
                      || exception instanceof BigqueryInternalException;
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait)
                    throws RetryGiveupException {
                  String message =
                      String.format(
                          "embulk-output-bigquery: Query job failed. Retrying %d/%d after %d seconds. Message: %s",
                          retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                  if (retryCount % retries == 0) {
                    logger.warn(message, exception);
                  } else {
                    logger.warn(message);
                  }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {
                  logger.error("embulk-output-bigquery: Give up retrying for Query job");
                }
              });

    } catch (RetryGiveupException ex) {
      if (ex.getCause() instanceof BigqueryException) {
        throw (BigqueryException) ex.getCause();
      }
      // TODO:
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new BigqueryException("interrupted");
    }
  }

  public boolean deleteTable(String table) {
    return deleteTable(table, null);
  }

  public boolean deleteTable(String table, String dataset) {
    if (dataset == null) {
      dataset = this.dataset;
    }
    String chompedTable = BigqueryUtil.chompPartitionDecorator(table);
    return deleteTableOrPartition(chompedTable, dataset);
  }

  public boolean deleteTableOrPartition(String table) {
    return deleteTableOrPartition(table, null);
  }

  //  if `table` with a partition decorator is given, a partition is deleted.
  public boolean deleteTableOrPartition(String table, String dataset) {
    if (dataset == null) {
      dataset = this.dataset;
    }
    logger.info(String.format("embulk-output-bigquery: Delete table... %s.%s", dataset, table));
    return this.bigquery.delete(TableId.of(dataset, table));
  }

  private JobStatistics waitForLoad(Job job) throws BigqueryException {
    return new BigqueryJobWaiter(this.task, this, job).waitFor("Load");
  }

  private JobStatistics waitForCopy(Job job) throws BigqueryException {
    return new BigqueryJobWaiter(this.task, this, job).waitFor("Copy");
  }

  private JobStatistics waitForQuery(Job job) throws BigqueryException {
    return new BigqueryJobWaiter(this.task, this, job).waitFor("Query");
  }

  protected com.google.cloud.bigquery.Schema buildSchema(
      Schema schema, List<BigqueryColumnOption> columnOptions) {
    // TODO: support schema file

    if (this.task.getTemplateTable().isPresent()) {
      TableId tableId = TableId.of(this.dataset, this.task.getTemplateTable().get());
      Table table = this.bigquery.getTable(tableId);
      return table.getDefinition().getSchema();
    }

    List<Field> fields = new ArrayList<>();

    for (Column col : schema.getColumns()) {
      Field.Mode fieldMode = Field.Mode.NULLABLE;
      Optional<BigqueryColumnOption> columnOption =
          BigqueryUtil.findColumnOption(col.getName(), columnOptions);
      Field.Builder fieldBuilder = createFieldBuilder(task, col, columnOption);

      if (columnOption.isPresent()) {
        BigqueryColumnOption colOpt = columnOption.get();
        if (!colOpt.getMode().isEmpty()) {
          fieldMode = Field.Mode.valueOf(colOpt.getMode());
        }
        fieldBuilder.setMode(fieldMode);

        if (colOpt.getDescription().isPresent()) {
          fieldBuilder.setDescription(colOpt.getDescription().get());
        }
      }
      //  TODO:: support field for JSON type
      Field field = fieldBuilder.build();
      fields.add(field);
    }
    return com.google.cloud.bigquery.Schema.of(fields);
  }

  protected Field.Builder createFieldBuilder(
      PluginTask task, Column col, Optional<BigqueryColumnOption> columnOption) {
    StandardSQLTypeName sqlTypeName = getStandardSQLTypeNameByEmbulkType(col.getType());
    LegacySQLTypeName legacySQLTypeName = getLegacySQLTypeNameByEmbulkType(col.getType());

    if (columnOption.isPresent()) {
      BigqueryColumnOption colOpt = columnOption.get();
      if (colOpt.getType().isPresent()) {
        if (task.getEnableStandardSQL()) {
          sqlTypeName = StandardSQLTypeName.valueOf(colOpt.getType().get());
        } else {
          legacySQLTypeName = LegacySQLTypeName.valueOf(colOpt.getType().get());
        }
      }
    }
    if (task.getEnableStandardSQL()) {
      return Field.newBuilder(col.getName(), sqlTypeName);
    } else {
      return Field.newBuilder(col.getName(), legacySQLTypeName);
    }
  }

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
