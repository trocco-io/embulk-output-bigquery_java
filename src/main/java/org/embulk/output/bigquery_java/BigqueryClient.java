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
import java.net.SocketException;
import java.net.SocketTimeoutException;
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
import org.embulk.output.bigquery_java.config.BigqueryConfigResolver;
import org.embulk.output.bigquery_java.config.BigqueryFieldOption;
import org.embulk.output.bigquery_java.config.BigqueryTimePartitioning;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryBackendException;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.embulk.output.bigquery_java.exception.BigqueryInternalException;
import org.embulk.output.bigquery_java.exception.BigqueryRateLimitExceededException;
import org.embulk.output.bigquery_java.exception.BigqueryUploadException;
import org.embulk.output.bigquery_java.testhost.TestHostBigQueryOptions;
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
  private String jobLocation; // resolved by jobLocation()
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
    return task.getMode().equals("replace")
        && (task.getRetainColumnDescriptions() || task.getRetainColumnPolicyTags());
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
    return (TestHostBigQueryOptions.isEnabled(task)
            ? TestHostBigQueryOptions.builder(task)
            : BigQueryOptions.newBuilder()
                .setCredentials(new Auth(task).getCredentials(BigqueryScopes.BIGQUERY)))
        .setRetrySettings(buildRetrySettings(task))
        .setTransportOptions(buildTransportOptions(task))
        .setProjectId(project)
        // Filled into every job id the library builds for jobs.get / jobs.insert / query that has
        // no location of its own (null when `location` isn't configured, which leaves them as
        // before). The resumable upload does not get it from here, see jobIdWithLocation().
        .setLocation(location)
        .build()
        .getService();
  }

  static RetrySettings buildRetrySettings(PluginTask task) {
    return ServiceOptions.getDefaultRetrySettings()
        .toBuilder()
        // +1: task.getRetries() means "number of retries", matching
        // RetryExecutor#withRetryLimit usage elsewhere in this class, while
        // RetrySettings#setMaxAttempts counts total attempts.
        .setMaxAttempts(task.getRetries() + 1)
        .setInitialRetryDelay(Duration.ofSeconds(task.getRetryInitialDelaySec()))
        .setMaxRetryDelay(Duration.ofSeconds(task.getRetryMaxDelaySec()))
        .setRetryDelayMultiplier(task.getRetryDelayMultiplier())
        // gax would otherwise draw each delay from [0, delay], halving the expected wait. Ruby's
        // client library randomizes around the nominal value (0.5x to 1.5x) instead, so applying
        // the delays as configured keeps java inside ruby's range.
        .setJittered(false)
        .setTotalTimeout(Duration.ofSeconds(task.getRetryTotalTimeoutSec()))
        .build();
  }

  // Ranges are checked up front by BigqueryConfigValidator#validateTimeouts, so the int
  // millisecond conversions here can't overflow.
  static TransportOptions buildTransportOptions(PluginTask task) {
    return HttpTransportOptions.newBuilder()
        .setConnectTimeout(task.getOpenTimeoutSec() * 1000)
        .setReadTimeout((int) (BigqueryConfigResolver.effectiveReadTimeoutSec(task) * 1000))
        .build();
  }

  // Backs load()/copy()/runQuery()/executeQuery()'s job-level retry (a fresh job resubmission
  // after a completed-but-errored job), separate from the gax RetrySettings above.
  private RetryExecutor buildJobRetryExecutor() {
    return RetryExecutor.builder()
        .withRetryLimit(task.getRetries())
        .withInitialRetryWaitMillis(task.getJobRetryInitialWaitSec() * 1000)
        .withMaxRetryWaitMillis(task.getJobRetryMaxWaitSec() * 1000)
        .build();
  }

  // For re-sending a whole upload (upload()). The library retries each request of an upload with
  // the backoff from buildRetrySettings(); this sits outside that and only catches what it won't
  // retry at all (a socket error under TLS, see shouldResendUpload()), by starting a new session.
  // No wait between attempts, as ruby's with_network_retry retries at once as well.
  private RetryExecutor buildUploadRetryExecutor() {
    return RetryExecutor.builder()
        .withRetryLimit(task.getRetries())
        .withInitialRetryWaitMillis(0)
        .withMaxRetryWaitMillis(0)
        .build();
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

  // The id of the load job behind a resumable upload, with the configured location, as ruby puts
  // `location` into the job_reference of every job it inserts. The library fills the options-level
  // location into jobs.get / jobs.insert / query itself (getBigQueryService()), but not into the
  // upload's job reference, so this is the one place that still needs it.
  private JobId jobIdWithLocation(String jobId) {
    JobId.Builder builder = JobId.newBuilder().setJob(jobId);
    if (location != null) {
      builder.setLocation(location);
    }
    return builder.build();
  }

  // Where this client's jobs run: `location` if configured, else the destination dataset's (jobs
  // run where their dataset is), fetched once on first use. For jobs.get on a job id this plugin
  // made up, which needs the location outside the US and EU multi-regions or answers 404 for a
  // job that exists; ruby's wait_load likewise falls back to the job reference's location.
  private String jobLocation() {
    if (jobLocation == null) {
      if (location != null) {
        jobLocation = location;
      } else {
        Dataset dataset = getDataset();
        jobLocation = dataset == null ? null : dataset.getLocation();
      }
    }
    return jobLocation;
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
    if (!isNeedUpdateTable(task) || dstFields == null) {
      return null;
    }

    List<Field> updatedFields = new ArrayList<>();
    for (Field field : currentFields) {
      Field.Builder fieldBuilder = field.toBuilder();
      dstFields.stream()
          .filter(x -> x.getName().equals(field.getName()))
          .findFirst()
          .ifPresent(
              srcField -> {
                if (task.getRetainColumnDescriptions()) {
                  fieldBuilder.setDescription(srcField.getDescription());
                }
                if (task.getRetainColumnPolicyTags()) {
                  fieldBuilder.setPolicyTags(srcField.getPolicyTags());
                }
              });
      task.getColumnOptions()
          .flatMap(columnOptions -> BigqueryUtil.findColumnOption(field.getName(), columnOptions))
          .flatMap(BigqueryColumnOption::getDescription)
          .ifPresent(fieldBuilder::setDescription);
      updatedFields.add(fieldBuilder.build());
    }
    return com.google.cloud.bigquery.Schema.of(updatedFields);
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
                  WriteChannelConfiguration writeChannelConfiguration =
                      WriteChannelConfiguration.newBuilder(tableId)
                          .setFormatOptions(FormatOptions.json())
                          .setWriteDisposition(writeDisposition)
                          .setMaxBadRecords(task.getMaxBadRecords())
                          .setIgnoreUnknownValues(task.getIgnoreUnknownValues())
                          .setSchema(buildSchema(schema, columnOptions))
                          .build();
                  Job job = upload(loadFile, table, jobId, writeChannelConfiguration);
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

  // Mirrors ruby's with_network_retry, which sits inside with_job_retry: the job id is fixed for
  // the whole upload, so a retry re-sends the file under the same id (as a new resumable session)
  // instead of creating a second job, and retries are immediate. A job that ran and failed is
  // retried by the caller with a new job id, like ruby's with_job_retry.
  private Job upload(
      Path loadFile,
      String table,
      String jobId,
      WriteChannelConfiguration writeChannelConfiguration) {
    try {
      return buildUploadRetryExecutor()
          .runInterruptible(
              new Retryable<Job>() {
                @Override
                public Job call() {
                  return uploadOnce(loadFile, table, jobId, writeChannelConfiguration);
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  return exception instanceof BigqueryUploadException;
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait) {
                  logger.warn(
                      "embulk-output-bigquery: retry #{}, {}", retryCount, exception.getMessage());
                }

                // Reached both when the retries are used up and for a failure that is not
                // retryable at all; only the former gets the "exhausted" line.
                @Override
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {
                  if (lastException instanceof BigqueryUploadException) {
                    logger.error(
                        "embulk-output-bigquery: retry exhausted #{}, {}",
                        task.getRetries(),
                        lastException.getMessage());
                  }
                  // RetryExecutor would wrap the first failure; the last one is the one to report
                  // (e.g. a 4xx after a 5xx).
                  throw new RetryGiveupException(lastException);
                }
              });
    } catch (RetryGiveupException ex) {
      if (ex.getCause() instanceof BigqueryException) {
        throw (BigqueryException) ex.getCause();
      }
      throw new RuntimeException(ex);
    } catch (InterruptedException ex) {
      throw new BigqueryException("interrupted");
    }
  }

  private Job uploadOnce(
      Path loadFile,
      String table,
      String jobId,
      WriteChannelConfiguration writeChannelConfiguration) {
    TableDataWriteChannel writer;
    // The channel reports network failures as BigQueryException (a RuntimeException), not
    // IOException: BigQueryRpc#write translates the IOException and flushBuffer() rethrows it.
    // IOException here only comes from reading the local file.
    try {
      writer = bigquery.writer(jobIdWithLocation(jobId), writeChannelConfiguration);
      // Deliberately not try-with-resources: closing the channel after a failed upload finalizes
      // the resumable session with the bytes sent so far, and BigQuery then starts a load job for
      // that partial file. The session is only closed (and the load job started) when the whole
      // file has been written.
      OutputStream stream = Channels.newOutputStream(writer);
      Files.copy(loadFile, stream);
      stream.close();
    } catch (IOException e) {
      // A file that can't be read won't become readable by retrying; ruby's with_network_retry
      // only retries socket errors too.
      throw new BigqueryException(uploadFailureMessage(loadFile, table, e), e);
    } catch (BigQueryException e) {
      // A 409 on the request that creates the job means a previous attempt did create it (it is
      // the final chunk that creates the job) but its response was lost, so the retry under the
      // same job id is refused. The job id is a fresh UUID per load(), so nothing else can
      // conflict; and rather than trusting the error text (the library doesn't parse the error
      // body on this path, so there is no structured reason either), the job is looked up: if
      // it exists, wait for it instead of failing, or of starting a second one. If it ran and
      // failed, the caller's job-level retry resubmits under a new id as usual.
      // ruby (verified): with_network_retry re-sends insert_job under the same job_id, the retry
      // gets 409 "duplicate: Already Exists: Job", load raises Error, and since the first job did
      // run its rows are in the table, so the user's rerun appends them a second time. This goes
      // one step further on purpose.
      if (e.getCode() == 409) {
        Job existing = lookUpRefusedJob(jobId, loadFile, table, e);
        if (existing != null) {
          return existing;
        }
        logger.warn(
            "embulk-output-bigquery: the upload of {} was refused as a duplicate of job_id:[{}], "
                + "but that job was not found in {}; failing the upload. If the job exists, its rows "
                + "may already be in {}:{}.{}",
            loadFile,
            jobId,
            jobLocation() == null ? "the default location" : jobLocation(),
            destinationProject,
            destinationDataset,
            table);
      }
      throw uploadFailure(loadFile, table, e);
    }
    return writer.getJob();
  }

  // Fetches the job a same-id upload retry was refused for (uploadOnce), or null if there is no
  // such job. Only the lookup is retried here, with the job-retry waits and for the failures the
  // upload would retry: re-sending the file instead would just reach the same 409 and the same
  // lookup again. A lookup that still fails ends the load, as the job's state is unknown and a
  // new job id could load the rows twice.
  private Job lookUpRefusedJob(
      String jobId, Path loadFile, String table, BigQueryException refusal) {
    JobId lookupId = JobId.newBuilder().setJob(jobId).setLocation(jobLocation()).build();
    try {
      return buildJobRetryExecutor()
          .runInterruptible(
              new Retryable<Job>() {
                @Override
                public Job call() {
                  return bigquery.getJob(lookupId);
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                  return exception instanceof BigQueryException
                      && shouldRetryJobLookup((BigQueryException) exception);
                }

                @Override
                public void onRetry(
                    Exception exception, int retryCount, int retryLimit, int retryWait) {
                  logger.warn(
                      "embulk-output-bigquery: looking up job_id:[{}] failed. Retrying {}/{} after {} seconds. Message: {}",
                      jobId,
                      retryCount,
                      retryLimit,
                      retryWait / 1000,
                      exception.getMessage());
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryGiveupException {
                  throw new RetryGiveupException(lastException);
                }
              });
    } catch (RetryGiveupException ex) {
      BigqueryException failure =
          new BigqueryException(
              String.format(
                  "embulk-output-bigquery: failed to look up job_id:[%s] after the upload of %s to %s:%s.%s was refused as a duplicate of it, message: %s",
                  jobId,
                  loadFile,
                  destinationProject,
                  destinationDataset,
                  table,
                  ex.getCause().getMessage()),
              ex.getCause());
      failure.addSuppressed(refusal);
      throw failure;
    } catch (InterruptedException ex) {
      throw new BigqueryException("interrupted");
    }
  }

  // The exception for an upload attempt that failed with `e`: a BigqueryUploadException, which
  // upload() re-sends the file for, only when shouldResendUpload() says so.
  private BigqueryException uploadFailure(Path loadFile, String table, BigQueryException e) {
    String msg = uploadFailureMessage(loadFile, table, e);
    return shouldResendUpload(e)
        ? new BigqueryUploadException(msg, e)
        : new BigqueryException(msg, e);
  }

  // Whether upload() should re-send the whole file in a new session. Only for a failure the
  // library won't retry itself; what it does consider retryable (a 5xx, a plain connection reset)
  // it has already re-sent `retries` times inside the session, and re-sending the file on top of
  // that would multiply both the attempts and the bytes by `retries` again. ruby retries less
  // still: its client library is configured with no retries, with_network_retry re-sends only the
  // socket errors below, and the Error that a 5xx becomes is not one of the three classes
  // with_job_retry rescues.
  // Package-private for the unit test: a TLS-level failure can't be staged on the plain-HTTP
  // MockWebServer.
  static boolean shouldResendUpload(BigQueryException e) {
    return !e.isRetryable() && hasSocketErrorCause(e);
  }

  // Whether the jobs.get after a 409 (lookUpRefusedJob) is worth another attempt. Unlike the
  // upload above, this does retry what the library retries too: the request is a few hundred
  // bytes, and giving up fails a load whose job may have run, which the user's rerun would then
  // load a second time.
  private static boolean shouldRetryJobLookup(BigQueryException e) {
    return e.isRetryable() || hasSocketErrorCause(e);
  }

  // google-cloud-core 2.8.0 considers a SocketException / SocketTimeoutException retryable, but
  // only when it is the exception itself: on a TLS connection the JDK reports a reset or a broken
  // pipe as javax.net.ssl.SSLException with the socket error attached as its cause, and that
  // outer type is not retryable. Looking down the cause chain finds those, which is what ruby's
  // with_network_retry (google_client.rb) retries as well (they reach it wrapped in SSLError by
  // httpclient).
  private static boolean hasSocketErrorCause(BigQueryException e) {
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof SocketException || cause instanceof SocketTimeoutException) {
        return true;
      }
    }
    return false;
  }

  private String uploadFailureMessage(Path loadFile, String table, Exception cause) {
    return String.format(
        "embulk-output-bigquery: failed to upload %s to %s:%s.%s, message: %s",
        loadFile, destinationProject, destinationDataset, table, cause.getMessage());
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
                  return bigquery.query(configuration, JobId.of(job));
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
                  Job job =
                      bigquery.create(
                          JobInfo.newBuilder(queryConfig).setJobId(JobId.of(jobId)).build());
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
