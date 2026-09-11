package org.embulk.output.bigquery_java;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.cloud.ServiceOptions;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
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
import com.google.cloud.http.HttpTransportOptions;
import java.io.ByteArrayInputStream;
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
import org.embulk.config.ConfigException;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryFieldOption;
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
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class BigqueryClient {
  private final Logger logger = LoggerFactory.getLogger(BigqueryClient.class);
  private final BigQuery bigquery;
  private final String project;
  public final String destinationProject; // FIXME: should be private
  public final String destinationDataset; // FIXME: should be private
  private final String location;
  private final String locationForLog;
  private final PluginTask task;
  private final Schema schema;
  private final List<BigqueryColumnOption> columnOptions;
  private FieldList cachedSrcFields = null;

  public BigqueryClient(PluginTask task, Schema schema) {
    this.task = task;
    this.schema = schema;
    project = task.getProject().orElseGet(this::getProjectIdFromJsonKeyfile);
    destinationProject = task.getDestinationProject().orElse(project);
    destinationDataset = task.getDataset();
    if (task.getLocation().isPresent()) {
      location = task.getLocation().get();
      locationForLog = task.getLocation().get();
    } else {
      location = null;
      locationForLog = "us/eu";
    }
    columnOptions = task.getColumnOptions().orElse(Collections.emptyList());
    try {
      bigquery = getBigQueryService();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isNeedUpdateTable(PluginTask task) {
    return needRetainColumnAttributes(task) || hasColumnOptionDescription(task);
  }

  private static boolean needRetainColumnAttributes(PluginTask task) {
    return task.getMode().equals("replace")
        && (task.getRetainColumnDescriptions() || task.getRetainColumnPolicyTags());
  }

  private static boolean hasColumnOptionDescription(PluginTask task) {
    return task.getColumnOptions()
        .map(
            options ->
                options.stream()
                    .anyMatch(
                        o -> o.getDescription().isPresent() || hasFieldDescription(o.getFields())))
        .orElse(false);
  }

  private static boolean hasFieldDescription(Optional<List<BigqueryFieldOption>> fields) {
    return fields
        .map(
            list ->
                list.stream()
                    .anyMatch(
                        f -> f.getDescription().isPresent() || hasFieldDescription(f.getFields())))
        .orElse(false);
  }

  public FieldList storeCachedSrcFieldsIfNeed() {
    if (!isNeedUpdateTable(task)) {
      return null;
    }
    Table srcTable = getTable(task.getTable());
    if (srcTable == null) {
      return null;
    }
    com.google.cloud.bigquery.Schema srcSchema = srcTable.getDefinition().getSchema();
    if (srcSchema == null) {
      return null;
    }
    cachedSrcFields = srcSchema.getFields();
    return cachedSrcFields;
  }

  private String getProjectIdFromJsonKeyfile() {
    if (!task.getJsonKeyfile().isPresent()) {
      throw new ConfigException("Either project or project_id in json_keyfile is required");
    }
    String projectId =
        new JSONObject(
                new JSONTokener(new ByteArrayInputStream(task.getJsonKeyfile().get().getContent())))
            .getString("project_id");
    if (projectId == null) {
      throw new ConfigException("Either project or project_id in json_keyfile is required");
    }
    return projectId;
  }

  private BigQuery getBigQueryService() throws IOException {
    return (BigqueryTestHostSupport.isEnabled(task)
            ? BigqueryTestHostSupport.getBigQueryOptionsBuilder(task)
            : BigQueryOptions.newBuilder()
                .setCredentials(new Auth(task).getCredentials(BigqueryScopes.BIGQUERY)))
        .setRetrySettings(buildRetrySettings(task))
        .setTransportOptions(buildTransportOptions(task))
        .setProjectId(project)
        .build()
        .getService();
  }

  // Optional env var overrides for retry backoff tuning; see README.md for details.
  private static Optional<Long> envMillis(String name) {
    String value = System.getenv(name);
    return value == null ? Optional.empty() : Optional.of(Long.parseLong(value));
  }

  private static Optional<Double> envDouble(String name) {
    String value = System.getenv(name);
    return value == null ? Optional.empty() : Optional.of(Double.parseDouble(value));
  }

  static RetrySettings buildRetrySettings(PluginTask task) {
    RetrySettings.Builder builder =
        ServiceOptions.getDefaultRetrySettings()
            .toBuilder()
            // +1: task.getRetries() means "number of retries", matching
            // RetryExecutor#withRetryLimit usage elsewhere in this class, while
            // RetrySettings#setMaxAttempts counts total attempts.
            .setMaxAttempts(task.getRetries() + 1);
    envMillis("BIGQUERY_OUTPUT_OPTION_RETRY_INITIAL_DELAY_MS")
        .ifPresent(v -> builder.setInitialRetryDelay(Duration.ofMillis(v)));
    envMillis("BIGQUERY_OUTPUT_OPTION_RETRY_MAX_DELAY_MS")
        .ifPresent(v -> builder.setMaxRetryDelay(Duration.ofMillis(v)));
    envDouble("BIGQUERY_OUTPUT_OPTION_RETRY_DELAY_MULTIPLIER")
        .ifPresent(builder::setRetryDelayMultiplier);
    envMillis("BIGQUERY_OUTPUT_OPTION_RETRY_TOTAL_TIMEOUT_MS")
        .ifPresent(v -> builder.setTotalTimeout(Duration.ofMillis(v)));
    return builder.build();
  }

  static TransportOptions buildTransportOptions(PluginTask task) {
    // See README.md for why send_timeout_sec is folded in here.
    int readTimeoutSec = task.getReadTimeoutSec() + task.getSendTimeoutSec();
    return HttpTransportOptions.newBuilder()
        .setConnectTimeout(task.getOpenTimeoutSec() * 1000)
        .setReadTimeout(readTimeoutSec * 1000)
        .build();
  }

  // Backs load()/copy()/runQuery()/executeQuery()'s job-level retry (a fresh job resubmission
  // after a completed-but-errored job), separate from the gax RetrySettings above.
  private RetryExecutor buildJobRetryExecutor() {
    RetryExecutor.Builder builder =
        RetryExecutor.builder()
            .withRetryLimit(task.getRetries())
            .withInitialRetryWaitMillis(2 * 1000)
            .withMaxRetryWaitMillis(10 * 1000);
    envMillis("BIGQUERY_OUTPUT_OPTION_JOB_RETRY_INITIAL_WAIT_MS")
        .ifPresent(v -> builder.withInitialRetryWaitMillis(v.intValue()));
    envMillis("BIGQUERY_OUTPUT_OPTION_JOB_RETRY_MAX_WAIT_MS")
        .ifPresent(v -> builder.withMaxRetryWaitMillis(v.intValue()));
    return builder.build();
  }

  public Dataset createDataset() {
    return createDataset(destinationDataset);
  }

  public Dataset createDataset(String dataset) {
    return createDataset(destinationProject, dataset);
  }

  private Dataset createDataset(String project, String dataset) {
    DatasetInfo.Builder builder = DatasetInfo.newBuilder(DatasetId.of(project, dataset));
    if (location != null) {
      builder.setLocation(location);
    }
    return bigquery.create(builder.build());
  }

  public Dataset getDataset() {
    return getDataset(destinationDataset);
  }

  public Dataset getDataset(String dataset) {
    return getDataset(destinationProject, dataset);
  }

  private Dataset getDataset(String project, String dataset) {
    return bigquery.getDataset(DatasetId.of(project, dataset));
  }

  public Job getJob(JobId jobId) {
    return bigquery.getJob(jobId);
  }

  public Table getTable(String table) {
    return getTable(table, destinationDataset);
  }

  private Table getTable(String table, String dataset) {
    return getTable(table, dataset, destinationProject);
  }

  private Table getTable(String table, String dataset, String project) {
    return bigquery.getTable(TableId.of(project, dataset, table));
  }

  public void createTableIfNotExist(String table) {
    createTableIfNotExist(table, destinationDataset);
  }

  public void createTableIfNotExist(String table, String dataset) {
    createTableIfNotExist(table, dataset, destinationProject);
  }

  private void createTableIfNotExist(String table, String dataset, String project) {
    StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition.newBuilder();
    tableDefinitionBuilder.setSchema(buildSchema(schema, columnOptions));
    if (task.getTimePartitioning().isPresent()) {
      tableDefinitionBuilder.setTimePartitioning(
          buildTimePartitioning(task.getTimePartitioning().get()));
    }
    if (task.getClustering().isPresent()) {
      tableDefinitionBuilder.setClustering(
          Clustering.newBuilder().setFields(task.getClustering().get().getFields().get()).build());
    }
    TableDefinition tableDefinition = tableDefinitionBuilder.build();

    try {
      bigquery.create(
          TableInfo.newBuilder(TableId.of(project, dataset, table), tableDefinition).build());
    } catch (BigQueryException e) {
      if (e.getCode() == 409 && e.getMessage().contains("Already Exists:")) {
        return;
      }
      logger.error(
          String.format("embulk-out_bigquery: insert_table(%s:%s.%s)", project, dataset, table));
      throw new BigqueryException(
          String.format(
              "failed to create table %s:%s.%s, response: %s", project, dataset, table, e));
    }
  }

  public void updateTableIfNeed() {
    Table table = this.getTable(task.getTable());
    com.google.cloud.bigquery.Schema schema = table.getDefinition().getSchema();
    if (schema == null) {
      return;
    }
    com.google.cloud.bigquery.Schema patchSchema =
        buildPatchSchema(task, schema.getFields(), cachedSrcFields);
    if (patchSchema == null) {
      return;
    }
    try {
      bigquery.update(
          TableInfo.newBuilder(
                  table.getTableId(),
                  StandardTableDefinition.newBuilder().setSchema(patchSchema).build())
              .build());
    } catch (BigQueryException e) {
      logger.error(
          String.format(
              "embulk-output-bigquery: update_table(%s:%s.%s)",
              destinationProject, destinationDataset, task.getTable()));
      throw new BigqueryException(
          String.format(
              "failed to update table %s:%s.%s, response: %s",
              destinationProject, destinationDataset, task.getTable(), e));
    }
  }

  public static com.google.cloud.bigquery.Schema buildPatchSchema(
      PluginTask task, FieldList currentFields, FieldList dstFields) {
    if (!isNeedUpdateTable(task)) {
      return null;
    }

    // dstFields (the previous table's schema) is only needed for the retain-from-previous-table
    // copy, which only ever applies in mode:replace. It can be null here (e.g. the destination
    // table didn't exist yet when storeCachedSrcFieldsIfNeed() ran), in which case
    // column_options[].description can still be applied below.
    List<Field> updatedFields = new ArrayList<>();
    for (Field field : currentFields) {
      Field srcField =
          dstFields == null
              ? null
              : dstFields.stream()
                  .filter(x -> x.getName().equals(field.getName()))
                  .findFirst()
                  .orElse(null);
      Optional<BigqueryColumnOption> columnOption =
          task.getColumnOptions()
              .flatMap(
                  columnOptions -> BigqueryUtil.findColumnOption(field.getName(), columnOptions));
      updatedFields.add(
          patchField(
              field,
              srcField,
              task,
              columnOption.flatMap(BigqueryColumnOption::getDescription).orElse(null),
              columnOption.flatMap(BigqueryColumnOption::getFields).orElse(null)));
    }
    return com.google.cloud.bigquery.Schema.of(updatedFields);
  }

  // Patches a single field, recursing into RECORD sub-fields so that both the
  // retain-from-previous-table copy and column_options[].fields[].description apply at any
  // nesting depth, matching hasColumnOptionDescription()'s recursion (used by
  // isNeedUpdateTable() above). description/nestedFieldOptions are nullable rather than Optional
  // since they're plain inputs here, not return values.
  private static Field patchField(
      Field field,
      Field srcField,
      PluginTask task,
      String description,
      List<BigqueryFieldOption> nestedFieldOptions) {
    Field.Builder fieldBuilder = field.toBuilder();
    if (needRetainColumnAttributes(task) && srcField != null) {
      if (task.getRetainColumnDescriptions()) {
        fieldBuilder.setDescription(srcField.getDescription());
      }
      if (task.getRetainColumnPolicyTags()) {
        fieldBuilder.setPolicyTags(srcField.getPolicyTags());
      }
    }
    // column_options[].description is applied regardless of mode or the retain_column_*
    // flags, unlike the retain-from-previous-table copy above which only makes sense for
    // mode:replace.
    if (description != null) {
      fieldBuilder.setDescription(description);
    }

    if (field.getSubFields() != null) {
      FieldList srcSubFields = srcField == null ? null : srcField.getSubFields();
      List<Field> patchedSubFields = new ArrayList<>();
      for (Field subField : field.getSubFields()) {
        Field matchingSrcSubField =
            srcSubFields == null
                ? null
                : srcSubFields.stream()
                    .filter(x -> x.getName().equals(subField.getName()))
                    .findFirst()
                    .orElse(null);
        BigqueryFieldOption subFieldOption =
            nestedFieldOptions == null
                ? null
                : findFieldOption(subField.getName(), nestedFieldOptions).orElse(null);
        patchedSubFields.add(
            patchField(
                subField,
                matchingSrcSubField,
                task,
                subFieldOption == null ? null : subFieldOption.getDescription().orElse(null),
                subFieldOption == null ? null : subFieldOption.getFields().orElse(null)));
      }
      fieldBuilder.setType(field.getType(), FieldList.of(patchedSubFields));
    }

    return fieldBuilder.build();
  }

  private static Optional<BigqueryFieldOption> findFieldOption(
      String fieldName, List<BigqueryFieldOption> fieldOptions) {
    return fieldOptions.stream().filter(f -> f.getName().equals(fieldName)).findFirst();
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
    try {
      // https://cloud.google.com/bigquery/quotas#standard_tables
      // Maximum rate of table metadata update operations — 5 operations every 10 seconds per table
      return buildJobRetryExecutor()
          .runInterruptible(
              new Retryable<JobStatistics.LoadStatistics>() {
                @Override
                public JobStatistics.LoadStatistics call() {
                  UUID uuid = UUID.randomUUID();
                  String jobId = String.format("embulk_load_job_%s", uuid);

                  if (Files.exists(loadFile)) {
                    logger.info(
                        "embulk-output-bigquery: Load job starting... job_id:[{}] {} => {}:{}.{} in {}",
                        jobId,
                        loadFile,
                        destinationProject,
                        destinationDataset,
                        table,
                        locationForLog);
                  } else {
                    logger.info(
                        "embulk-output-bigquery: Load job starting... {} does not exist, skipped",
                        loadFile);
                    // TODO: should throw error?
                    return null;
                  }

                  TableId tableId = TableId.of(destinationProject, destinationDataset, table);
                  if (BigqueryTestHostSupport.isEnabled(task)) {
                    Job job =
                        BigqueryTestHostSupport.createLoadJob(
                            bigquery,
                            task,
                            tableId,
                            jobId,
                            writeDisposition,
                            buildSchema(schema, columnOptions));
                    return (JobStatistics.LoadStatistics) waitForLoad(job);
                  }
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
                  if (retryCount % task.getRetries() == 0) {
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
      String sourceTable, String destinationTable, JobInfo.WriteDisposition writeDisposition)
      throws BigqueryException {
    return copy(sourceTable, destinationTable, destinationDataset, writeDisposition);
  }

  public JobStatistics.CopyStatistics copy(
      String sourceTable,
      String destinationTable,
      String destinationDataset,
      JobInfo.WriteDisposition writeDisposition)
      throws BigqueryException {
    return copy(
        TableId.of(destinationProject, this.destinationDataset, sourceTable),
        TableId.of(destinationProject, destinationDataset, destinationTable),
        writeDisposition);
  }

  private JobStatistics.CopyStatistics copy(
      TableId sourceTable, TableId destinationTable, JobInfo.WriteDisposition writeDisposition)
      throws BigqueryException {
    try {
      return buildJobRetryExecutor()
          .runInterruptible(
              new Retryable<JobStatistics.CopyStatistics>() {
                @Override
                public JobStatistics.CopyStatistics call() {
                  UUID uuid = UUID.randomUUID();
                  String jobId = String.format("embulk_load_job_%s", uuid);

                  CopyJobConfiguration copyJobConfiguration =
                      CopyJobConfiguration.newBuilder(destinationTable, sourceTable)
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
                  if (retryCount % task.getRetries() == 0) {
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
    sb.append(quoteIdentifier(destinationProject));
    sb.append(".");
    sb.append(quoteIdentifier(destinationDataset));
    sb.append(".");
    sb.append(quoteIdentifier(targetTable));
    sb.append(" T");
    sb.append(" USING ");
    sb.append(quoteIdentifier(destinationProject));
    sb.append(".");
    sb.append(quoteIdentifier(destinationDataset));
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
            + quoteIdentifier(destinationProject)
            + "."
            + quoteIdentifier(destinationDataset)
            + ".INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU "
            + "JOIN "
            + quoteIdentifier(destinationProject)
            + "."
            + quoteIdentifier(destinationDataset)
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
    try {
      return buildJobRetryExecutor()
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
                  if (retryCount % task.getRetries() == 0) {
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
    try {
      return buildJobRetryExecutor()
          .runInterruptible(
              new Retryable<JobStatistics.QueryStatistics>() {
                @Override
                public JobStatistics.QueryStatistics call() {
                  UUID uuid = UUID.randomUUID();
                  String jobId = String.format("embulk_query_job_%s", uuid);

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
                  if (retryCount % task.getRetries() == 0) {
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
    return deleteTable(table, destinationDataset);
  }

  private boolean deleteTable(String table, String dataset) {
    return deleteTable(table, dataset, destinationProject);
  }

  private boolean deleteTable(String table, String dataset, String project) {
    String chompedTable = BigqueryUtil.chompPartitionDecorator(table);
    return deleteTableOrPartition(chompedTable, dataset, project);
  }

  public boolean deleteTableOrPartition(String table) {
    return deleteTableOrPartition(table, destinationDataset);
  }

  private boolean deleteTableOrPartition(String table, String dataset) {
    return deleteTableOrPartition(table, dataset, destinationProject);
  }

  //  if `table` with a partition decorator is given, a partition is deleted.
  private boolean deleteTableOrPartition(String table, String dataset, String project) {
    logger.info(
        String.format("embulk-output-bigquery: Delete table... %s:%s.%s", project, dataset, table));
    return bigquery.delete(TableId.of(project, dataset, table));
  }

  private JobStatistics waitForLoad(Job job) throws BigqueryException {
    return new BigqueryJobWaiter(task, this, job).waitFor("Load");
  }

  private JobStatistics waitForCopy(Job job) throws BigqueryException {
    return new BigqueryJobWaiter(task, this, job).waitFor("Copy");
  }

  private JobStatistics waitForQuery(Job job) throws BigqueryException {
    return new BigqueryJobWaiter(task, this, job).waitFor("Query");
  }

  private com.google.cloud.bigquery.Schema buildSchema(
      Schema schema, List<BigqueryColumnOption> columnOptions) {
    // TODO: support schema file

    if (task.getTemplateTable().isPresent()) {
      Table table = getTable(task.getTemplateTable().get());
      return table.getDefinition().getSchema();
    }

    List<Field> fields = new ArrayList<>();

    for (Column col : schema.getColumns()) {
      Field.Mode fieldMode = Field.Mode.NULLABLE;
      Optional<BigqueryColumnOption> columnOption =
          BigqueryUtil.findColumnOption(col.getName(), columnOptions);

      Field.Builder fieldBuilder;
      if (columnOption.isPresent() && columnOption.get().getFields().isPresent()) {
        BigqueryColumnOption colOpt = columnOption.get();
        FieldList subFields = buildSubFields(colOpt.getFields().get());
        String typeName = colOpt.getType().orElse("RECORD");
        if (task.getEnableStandardSQL()) {
          fieldBuilder =
              Field.newBuilder(col.getName(), StandardSQLTypeName.valueOf(typeName), subFields);
        } else {
          fieldBuilder =
              Field.newBuilder(col.getName(), LegacySQLTypeName.valueOf(typeName), subFields);
        }
      } else {
        fieldBuilder = createFieldBuilder(task, col, columnOption);
      }

      if (columnOption.isPresent()) {
        BigqueryColumnOption colOpt = columnOption.get();
        if (!colOpt.getMode().isEmpty()) {
          fieldMode = Field.Mode.valueOf(colOpt.getMode());
        }
        fieldBuilder.setMode(fieldMode);

        // CAUTION: If isNeedUpdateTable() is true, description may be overwritten by
        // updateTableIfNeed().
        if (colOpt.getDescription().isPresent()) {
          fieldBuilder.setDescription(colOpt.getDescription().get());
        }
      }
      Field field = fieldBuilder.build();
      fields.add(field);
    }
    return com.google.cloud.bigquery.Schema.of(fields);
  }

  private Field.Builder createFieldBuilder(
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

  private FieldList buildSubFields(List<BigqueryFieldOption> fieldOptions) {
    List<Field> subFields = new ArrayList<>();
    for (BigqueryFieldOption fieldOption : fieldOptions) {
      Field.Builder fb;
      if (fieldOption.getFields().isPresent()) {
        FieldList nestedFields = buildSubFields(fieldOption.getFields().get());
        if (task.getEnableStandardSQL()) {
          fb =
              Field.newBuilder(
                  fieldOption.getName(),
                  StandardSQLTypeName.valueOf(fieldOption.getType()),
                  nestedFields);
        } else {
          fb =
              Field.newBuilder(
                  fieldOption.getName(),
                  LegacySQLTypeName.valueOf(fieldOption.getType()),
                  nestedFields);
        }
      } else {
        if (task.getEnableStandardSQL()) {
          fb =
              Field.newBuilder(
                  fieldOption.getName(), StandardSQLTypeName.valueOf(fieldOption.getType()));
        } else {
          fb =
              Field.newBuilder(
                  fieldOption.getName(), LegacySQLTypeName.valueOf(fieldOption.getType()));
        }
      }
      if (!fieldOption.getMode().isEmpty()) {
        fb.setMode(Field.Mode.valueOf(fieldOption.getMode()));
      }
      if (fieldOption.getDescription().isPresent()) {
        fb.setDescription(fieldOption.getDescription().get());
      }
      subFields.add(fb.build());
    }
    return FieldList.of(subFields);
  }

  private StandardSQLTypeName getStandardSQLTypeNameByEmbulkType(Type type) {
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

  private LegacySQLTypeName getLegacySQLTypeNameByEmbulkType(Type type) {
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
