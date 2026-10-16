package org.embulk.output.bigquery_java;

import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.COPY_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.QUERY_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.UPLOAD_CHUNK_SIZE;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertFieldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetDataset;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetJob;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetJobStatus;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertLoadAttempt;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPatchTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostJobs;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostTables;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertStartResumableUpload;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertUploadChunk;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertUploadLoadFile;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createCopyJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createQueryJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.datasetResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.datasetResponseWithLocation;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.disconnectDuringUploadResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.errorResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.firstSchemaField;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.jobIdOf;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.jobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.loadAttemptResponses;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.loadTestHostConfig;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.requestBodyJson;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.runWithMockServer;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.runWithMockServerExpectingFailure;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.startResumableUploadResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableIdOf;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithNullableC0;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithOldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithSchemaFields;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.testHostOf;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.unansweredResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.uploadChunkAcceptedResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.uploadLoadFileResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForCopyJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForLoadJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForQueryJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.writeLoadFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryException;
import org.embulk.output.bigquery_java.exception.BigqueryInternalException;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.MockResponses;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.MockServerAction;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.RequestCursor;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.type.Types;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// MockWebServer-based tests: exercise BigqueryClient's HTTP-facing behavior directly, without
// going through the plugin's transaction/output flow.
public class TestBigqueryClientWithMockServer {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .build();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  // Builds a BigqueryClient wired to `server` via test_host, from test_host.yml customized by
  // `customizeConfig` (e.g. to flip a flag or switch mode).
  private BigqueryClient buildTestHostClient(
      MockWebServer server,
      Function<ConfigSource, ConfigSource> customizeConfig,
      org.embulk.spi.Schema schema) {
    ConfigSource config =
        loadTestHostConfig(embulk, customizeConfig).set("test_host", testHostOf(server));
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    return new BigqueryClient(task, schema);
  }

  private BigqueryClient buildTestHostClient(
      MockWebServer server, Function<ConfigSource, ConfigSource> customizeConfig) {
    return buildTestHostClient(
        server,
        customizeConfig,
        new org.embulk.spi.Schema(Arrays.asList(new Column(0, "c0", Types.STRING))));
  }

  private BigqueryClient buildTestHostClient(MockWebServer server) {
    return buildTestHostClient(server, c -> c);
  }

  @Test
  public void testCreateTableIfNotExistIgnoresAlreadyExists() throws Exception {
    try (RequestCursor requests =
        runWithMockServer(
            // Should swallow the 409 and return normally.
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            errorResponse(409, "Already Exists: Table project:dataset.table", "duplicate"))) {
      assertPostTables(requests.next());
    }
  }

  @Test
  public void testCreateTableIfNotExistThrowsOnOtherError() throws Exception {
    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            // 400 (not 500/503/429) so the BigQuery client library's own transport-level retry
            // doesn't kick in before this reaches BigqueryClient's error handling.
            errorResponse(400, "Invalid table definition", "invalid"))) {
      assertPostTables(requests.next());
    }
  }

  @Test
  public void testCreateTableIfNotExistPassesThroughPartitionDecoratorInTableName()
      throws Exception {
    // Ruby strips the partition decorator (`$YYYYMMDD`) from the table name before creating a
    // temp table for it, so decorated destination tables work there. Java passes the table name
    // through as-is, so BigQuery rejects the decorator as part of a table ID and this fails.
    // TODO: strip the partition decorator from the table name before creating the table, like
    // ruby does, instead of passing it through and letting BigQuery reject it.
    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table$20160929"),
            errorResponse(400, "Invalid table ID", "invalid"))) {
      RecordedRequest request = requests.next();
      assertPostTables(request);
      assertEquals("table$20160929", tableIdOf(requestBodyJson(request), "tableReference"));
    }
  }

  @Test
  public void testCreateTableIfNotExistRetriesTransientErrorsThenSucceeds() throws Exception {
    // test_host.yml sets retries: 1, so 1 failure then a success should still go through (1
    // initial attempt + 1 retry = 2 total), driven by the task's `retries` config (see
    // buildRetrySettings() in BigqueryClient).
    List<MockResponse> responses = new ArrayList<>();
    responses.add(errorResponse(500, "boom", "internalError"));
    responses.add(tableResponse());

    try (RequestCursor requests =
        runWithMockServer(
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            responses.toArray(new MockResponse[0]))) {
      assertPostTables(requests.next());
      assertPostTables(requests.next());
    }
  }

  @Test
  public void testCreateTableIfNotExistGivesUpAfterMaxTransientErrors() throws Exception {
    // test_host.yml sets retries: 1, so 2 consecutive failures (1 initial attempt + 1 retry)
    // should exhaust the budget and give up, driven by the task's `retries` config.
    List<MockResponse> responses = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      responses.add(errorResponse(500, "boom", "internalError"));
    }

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            responses.toArray(new MockResponse[0]))) {
      assertPostTables(requests.next());
      assertPostTables(requests.next());
    }
  }

  private static final String COLUMN_OPTION_DESCRIPTION = "column-option-description";

  private static List<ConfigSource> columnOptionsWithDescription() {
    return Arrays.asList(
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("name", "c0")
            .set("description", COLUMN_OPTION_DESCRIPTION));
  }

  // mode: replace is the only mode isNeedUpdateTable() ever allows through, so this is the only
  // one where retainDescriptions/retainPolicyTags can affect the outcome. When
  // `columnOptionDescription` is true, c0 gets a COLUMN_OPTION_DESCRIPTION column_options
  // description, letting callers check how it interacts with the retained fields.
  private void updateTableInReplaceMode(
      MockWebServer server,
      boolean retainDescriptions,
      boolean retainPolicyTags,
      boolean columnOptionDescription) {
    BigqueryClient client =
        buildTestHostClient(
            server,
            c -> {
              c.set("mode", "replace")
                  .set("retain_column_descriptions", retainDescriptions)
                  .set("retain_column_policy_tags", retainPolicyTags);
              if (columnOptionDescription) {
                c.set("column_options", columnOptionsWithDescription());
              }
              return c;
            });
    client.storeCachedSrcFieldsIfNeed();
    client.updateTableIfNeed();
  }

  // For any other mode, isNeedUpdateTable() is false regardless of the retain flags, so only
  // `mode` (and, to double-check, `columnOptionDescription`) is worth varying here; the retain
  // flags are left at their (false) config default.
  private void updateTableInMode(
      MockWebServer server, String mode, boolean columnOptionDescription) {
    BigqueryClient client =
        buildTestHostClient(
            server,
            c -> {
              c.set("mode", mode);
              if (columnOptionDescription) {
                c.set("column_options", columnOptionsWithDescription());
              }
              return c;
            });
    client.storeCachedSrcFieldsIfNeed();
    client.updateTableIfNeed();
  }

  // Runs `action` (storeCachedSrcFieldsIfNeed() + updateTableIfNeed()) against a src table with
  // both a description and a policy tag, and a post-replace destination schema with neither, and
  // asserts the resulting PATCH restores exactly `expectedDescription`/`expectedPolicyTag` (null
  // meaning the field should be absent from the patched schema).
  private void assertPatchedField(
      MockServerAction action, String expectedDescription, String expectedPolicyTag)
      throws Exception {
    try (RequestCursor requests =
        runWithMockServer(
            action,
            // storeCachedSrcFieldsIfNeed(): GET table returns the old schema, with both a
            // description and a policy tag.
            tableResponseWithOldDescriptionAndPolicyTag(),
            // updateTableIfNeed(): GET table returns the new (post-replace) schema, with neither;
            // the PATCH request should restore whichever cached fields are retained.
            tableResponseWithNullableC0(),
            tableResponseWithSchemaFields("[{\"name\":\"c0\",\"type\":\"STRING\"}]"))) {
      assertGetTable(requests.next(), "table"); // storeCachedSrcFieldsIfNeed()

      assertGetTable(requests.next(), "table"); // updateTableIfNeed()

      RecordedRequest patchRequest = requests.next();
      assertPatchTable(patchRequest, "table");
      assertFieldDescriptionAndPolicyTag(
          firstSchemaField(requestBodyJson(patchRequest)), expectedDescription, expectedPolicyTag);
    }
  }

  // Runs `action` (storeCachedSrcFieldsIfNeed() + updateTableIfNeed()) and asserts it makes no
  // PATCH: just a single GET table request from updateTableIfNeed() (storeCachedSrcFieldsIfNeed()
  // makes no request at all when isNeedUpdateTable() is false).
  private void assertNoPatch(MockServerAction action) throws Exception {
    try (RequestCursor requests = runWithMockServer(action, tableResponseWithNullableC0())) {
      assertGetTable(requests.next(), "table"); // updateTableIfNeed()
    }
  }

  // With the retain flags left at their false default, isNeedUpdateTable() is false regardless of
  // mode or column_options[].description, so updateTableIfNeed() never PATCHes in any valid mode.

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInAppendMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "append", false));
    assertNoPatch(server -> updateTableInMode(server, "append", true));
  }

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInAppendDirectMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "append_direct", false));
    assertNoPatch(server -> updateTableInMode(server, "append_direct", true));
  }

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInDeleteInAdvanceMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "delete_in_advance", false));
    assertNoPatch(server -> updateTableInMode(server, "delete_in_advance", true));
  }

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInMergeMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "merge", false));
    assertNoPatch(server -> updateTableInMode(server, "merge", true));
  }

  // isNeedUpdateTable() (mode: replace with a retain flag on) gates whether updateTableIfNeed()
  // PATCHes at all; retainDescriptions/retainPolicyTags separately control whether each retained
  // field is restored; column_options[].description, when present, is applied after the retained
  // description and so wins over it.

  @Test
  public void testUpdateTableIfNeedDoesNotPatchWhenRetainFlagsFalse() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "replace", false));
    assertNoPatch(server -> updateTableInMode(server, "replace", true));
    assertNoPatch(server -> updateTableInReplaceMode(server, false, false, false));
    assertNoPatch(server -> updateTableInReplaceMode(server, false, false, true));
  }

  @Test
  public void testUpdateTableIfNeedPatchesInReplaceMode() throws Exception {
    assertPatchedField(
        server -> updateTableInReplaceMode(server, false, true, false), null, "old-policy-tag");
    assertPatchedField(
        server -> updateTableInReplaceMode(server, false, true, true),
        "column-option-description",
        "old-policy-tag");
    assertPatchedField(
        server -> updateTableInReplaceMode(server, true, false, false), "old-description", null);
    assertPatchedField(
        server -> updateTableInReplaceMode(server, true, false, true),
        "column-option-description",
        null);
    assertPatchedField(
        server -> updateTableInReplaceMode(server, true, true, false),
        "old-description",
        "old-policy-tag");
    assertPatchedField(
        server -> updateTableInReplaceMode(server, true, true, true),
        "column-option-description",
        "old-policy-tag");
  }

  // Builds the RUNNING-then-DONE response pair for two successive job attempts (jobId1 then
  // jobId2), where the first fails with internalError and the second succeeds.
  private static MockResponse[] jobRetrySucceedsResponses(
      String configKey, String configBody, String jobId1, String jobId2) {
    return new MockResponse[] {
      jobResponse(jobId1, configKey, configBody, "RUNNING", null),
      jobResponse(jobId1, configKey, configBody, "DONE", "internalError"),
      jobResponse(jobId2, configKey, configBody, "RUNNING", null),
      jobResponse(jobId2, configKey, configBody, "DONE", null)
    };
  }

  // As above, but both attempts fail with internalError.
  private static MockResponse[] jobRetryGivesUpResponses(
      String configKey, String configBody, String jobId1, String jobId2) {
    return new MockResponse[] {
      jobResponse(jobId1, configKey, configBody, "RUNNING", null),
      jobResponse(jobId1, configKey, configBody, "DONE", "internalError"),
      jobResponse(jobId2, configKey, configBody, "RUNNING", null),
      jobResponse(jobId2, configKey, configBody, "DONE", "internalError")
    };
  }

  // Asserts the POST job + GET status pair recorded for each of the two attempts built by
  // jobRetrySucceedsResponses()/jobRetryGivesUpResponses() above.
  private static void assertJobRetryRequests(RequestCursor requests, String jobId1, String jobId2) {
    assertPostJobs(requests.next());
    assertGetJobStatus(requests.next(), jobId1);
    assertPostJobs(requests.next());
    assertGetJobStatus(requests.next(), jobId2);
  }

  // A one-row JSONL file for load() to upload.
  private Path newLoadFile(String name) throws Exception {
    Path loadFile = testFolder.newFile(name).toPath();
    Files.write(loadFile, Collections.singletonList("{\"c0\":\"hello\"}"));
    return loadFile;
  }

  // A load differs from copy/query in how the job is created: the file goes through a resumable
  // upload session, so each attempt is the 3 requests of loadAttemptResponses(), and every retry
  // re-uploads the whole file as a new load job.
  @Test
  public void testLoadRetriesOnInternalErrorThenSucceeds() throws Exception {
    Path loadFile = newLoadFile("load-retry-test.json");

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            MockResponses.of(loadAttemptResponses("load-job-1", "internalError"))
                .add(loadAttemptResponses("load-job-2", null))
                .toArray())) {
      // A job that ran and failed is retried as a new job (new job id), like ruby's with_job_retry.
      RecordedRequest firstOpen = assertLoadAttempt(requests, "load-job-1").openUpload;
      RecordedRequest secondOpen = assertLoadAttempt(requests, "load-job-2").openUpload;
      assertNotEquals(jobIdOf(firstOpen), jobIdOf(secondOpen));
    }
  }

  @Test
  public void testLoadGivesUpAfterMaxRetries() throws Exception {
    Path loadFile = newLoadFile("load-giveup-test.json");

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Load job get_job\\(load-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1))
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            MockResponses.of(loadAttemptResponses("load-job-1", "internalError"))
                .add(loadAttemptResponses("load-job-2", "internalError"))
                .toArray())) {
      assertLoadAttempt(requests, "load-job-1");
      assertLoadAttempt(requests, "load-job-2");
    }
  }

  // The library itself retries a chunk PUT that fails with a network error (here a connection
  // reset while sending): the same chunk is re-sent on the same upload session, up to `retries`
  // more times (test_host.yml: 1), and load() never notices.
  @Test
  public void testLoadResendsChunkOnConnectionReset() throws Exception {
    Path loadFile = newLoadFile("load-reset-test.json");
    long size = Files.size(loadFile);

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            disconnectDuringUploadResponse(),
            uploadLoadFileResponse("testjob"),
            waitForLoadJobResponse("testjob", null))) {
      assertStartResumableUpload(requests.next());
      assertUploadChunk(requests.next(), 0, size - 1, size); // reset while sending
      assertUploadLoadFile(requests.next()); // the library's retry, same session
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // A file over the library's chunk size goes up as an intermediate chunk (answered with 308)
  // followed by the final chunk, which creates the job.
  @Test
  public void testLoadUploadsLargeFileInChunks() throws Exception {
    Path loadFile = testFolder.newFile("load-large-test.json").toPath();
    long size = writeLoadFile(loadFile, UPLOAD_CHUNK_SIZE + 1);

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            uploadChunkAcceptedResponse(),
            uploadLoadFileResponse("testjob"),
            waitForLoadJobResponse("testjob", null))) {
      assertStartResumableUpload(requests.next());
      assertUploadChunk(requests.next(), 0, UPLOAD_CHUNK_SIZE - 1, -1);
      assertUploadChunk(requests.next(), UPLOAD_CHUNK_SIZE, size - 1, size);
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // A chunk PUT answered with a 5xx is re-sent by the library on the same session, `retries`
  // more times (test_host.yml: 1). Once the library gives up, the upload fails: load() does not
  // re-send the file in a new session on top of the library's attempts, which would multiply
  // both the attempts and the bytes by `retries` again. ruby doesn't retry a 5xx here at all.
  @Test
  public void testLoadDoesNotReopenUploadSessionAfterServerErrors() throws Exception {
    Path loadFile = newLoadFile("load-5xx-test.json");

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to upload .*503.*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(503, "boom", "backendError"),
            errorResponse(503, "boom", "backendError"))) {
      assertStartResumableUpload(requests.next());
      assertUploadLoadFile(requests.next());
      assertUploadLoadFile(requests.next()); // the library's retry, same session
      // No second session: the cursor's close() would report one.
    }
  }

  // A chunk PUT answered with a 4xx is not re-sent, neither by the library nor by load(), and
  // the failure is not a BigqueryUploadException (which would be retried).
  @Test
  public void testLoadDoesNotResendChunkOnClientError() throws Exception {
    Path loadFile = newLoadFile("load-4xx-test.json");

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to upload .*400.*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(400, "Invalid", "invalid"))) {
      assertStartResumableUpload(requests.next());
      assertUploadLoadFile(requests.next());
    }
  }

  // The configured read timeout applies to the chunk PUT as well (#113): a server that never
  // answers makes the client give up after read_timeout_sec (1 s in test_host.yml), and load()
  // reports it as an upload failure. A read timeout is retryable to the library, so it is the
  // library that re-sends it, `retries` times inside the session; retries: 0 keeps this to a
  // single 1 s wait.
  @Test
  public void testLoadFailsOnReadTimeoutDuringUpload() throws Exception {
    Path loadFile = newLoadFile("load-timeout-test.json");

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to upload .*Read timed out.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 0))
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            unansweredResponse())) {
      assertStartResumableUpload(requests.next());
      assertUploadLoadFile(requests.next());
    }
  }

  // The counterpart of the test above: a 5xx the library's own retry gets past costs one extra
  // PUT on the same session, and the load succeeds.
  @Test
  public void testLoadSucceedsWhenTheLibraryRetryGetsPastAServerError() throws Exception {
    Path loadFile = newLoadFile("load-5xx-recovery-test.json");

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            errorResponse(503, "boom", "backendError"),
            uploadLoadFileResponse("testjob"),
            waitForLoadJobResponse("testjob", null))) {
      assertStartResumableUpload(requests.next());
      assertUploadLoadFile(requests.next());
      assertUploadLoadFile(requests.next()); // the library's retry, same session
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // A file that can't be read fails the load right away, like ruby, whose with_network_retry
  // only retries socket errors: the session is opened, nothing is PUT, no second session.
  @Test
  public void testLoadFailsAtOnceWhenLoadFileCannotBeRead() throws Exception {
    Path notAFile = testFolder.newFolder().toPath();

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to upload .*",
            server ->
                buildTestHostClient(server)
                    .load(notAFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse())) {
      assertStartResumableUpload(requests.next());
    }
  }

  // Once an upload has failed, its session must not be finalized: closing the channel would PUT
  // the bytes buffered so far as the last chunk, and BigQuery would start a load job for that
  // partial file. Visible only with more than one chunk: the first chunk fails for good, and the
  // only PUTs on that session are the two attempts at it, each with an unknown total ("bytes
  // 0-N/*"), with no "bytes 0-N/N+1" finalizing PUT after them.
  @Test
  public void testLoadDoesNotFinalizeFailedUploadSession() throws Exception {
    Path loadFile = testFolder.newFile("load-chunked-fail-test.json").toPath();
    writeLoadFile(loadFile, UPLOAD_CHUNK_SIZE + 1);

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to upload .*503.*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(503, "boom", "backendError"),
            errorResponse(503, "boom", "backendError"))) {
      assertStartResumableUpload(requests.next());
      assertUploadChunk(requests.next(), 0, UPLOAD_CHUNK_SIZE - 1, -1);
      assertUploadChunk(requests.next(), 0, UPLOAD_CHUNK_SIZE - 1, -1); // the library's retry
      // No finalizing PUT: the cursor's close() would report one.
    }
  }

  // The final chunk creates the job; if its response is lost, the retry under the same job id is
  // refused with 409 "Already Exists: Job" although the job is running. load() then fetches
  // that job and waits for it instead of failing (or starting a second one). ruby would fail
  // here, so this is one step beyond ruby on purpose.
  // jobs.get needs the job's location outside the US and EU multi-regions (or it answers 404 for
  // a job that exists, and the rescue would silently turn into the plain failure); with no
  // `location` configured, the lookup asks the dataset where its jobs run.
  @Test
  public void testLoadWaitsForTheJobWhenTheRetryIsRefusedAsDuplicate() throws Exception {
    Path loadFile = newLoadFile("load-409-test.json");
    long size = Files.size(loadFile);

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            disconnectDuringUploadResponse(), // the response that created the job is lost
            // Deliberately not BigQuery's "Already Exists: Job ..." wording: the status code and
            // the jobs.get below decide, not the error text.
            errorResponse(409, "boom", "duplicate"),
            datasetResponseWithLocation("asia-northeast1"), // where do the jobs run?
            uploadLoadFileResponse("testjob"), // jobs.get of the existing job
            waitForLoadJobResponse("testjob", null))) {
      RecordedRequest open = requests.next();
      assertStartResumableUpload(open);
      assertUploadChunk(requests.next(), 0, size - 1, size); // disconnected while sending
      assertUploadLoadFile(requests.next()); // the library's retry, refused with 409
      assertGetDataset(requests.next());
      assertGetJob(requests.next(), jobIdOf(open), "asia-northeast1");
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // With `location` configured there is nothing to ask the dataset: it goes into the job
  // reference of the upload (as ruby does) and into the lookup.
  @Test
  public void testLoadLooksUpTheDuplicateJobInTheConfiguredLocation() throws Exception {
    Path loadFile = newLoadFile("load-409-location-test.json");

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server, c -> c.set("location", "asia-northeast1"))
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            errorResponse(409, "boom", "duplicate"),
            uploadLoadFileResponse("testjob"), // jobs.get of the existing job
            waitForLoadJobResponse("testjob", null))) {
      RecordedRequest open = requests.next();
      assertStartResumableUpload(open);
      assertEquals(
          "asia-northeast1",
          requestBodyJson(open).getJSONObject("jobReference").getString("location"));
      assertUploadLoadFile(requests.next()); // refused with 409
      assertGetJob(requests.next(), jobIdOf(open), "asia-northeast1");
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // A dataset in the US or EU multi-region reports no location that jobs.get would need; the
  // lookup then goes without one, as every jobs.get did before.
  @Test
  public void testLoadLooksUpTheDuplicateJobWithoutLocationWhenNeitherIsKnown() throws Exception {
    Path loadFile = newLoadFile("load-409-no-location-test.json");

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            errorResponse(409, "boom", "duplicate"),
            datasetResponse(), // no location
            uploadLoadFileResponse("testjob"),
            waitForLoadJobResponse("testjob", null))) {
      RecordedRequest open = requests.next();
      assertStartResumableUpload(open);
      assertUploadLoadFile(requests.next());
      assertGetDataset(requests.next());
      assertGetJob(requests.next(), jobIdOf(open), null);
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // The lookup after a 409 can fail on its own. A retryable failure (here 503, twice: the library
  // retries jobs.get once itself) retries only the lookup, not the whole upload, which would just
  // be refused with 409 again.
  @Test
  public void testLoadRetriesOnlyTheLookupWhenTheDuplicateJobLookupFails() throws Exception {
    Path loadFile = newLoadFile("load-409-lookup-retry-test.json");

    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server, c -> c.set("location", "asia-northeast1"))
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            startResumableUploadResponse(),
            errorResponse(409, "boom", "duplicate"),
            errorResponse(503, "boom", "backendError"), // jobs.get
            errorResponse(503, "boom", "backendError"), // jobs.get, the library's retry
            uploadLoadFileResponse("testjob"), // jobs.get, load()'s own retry of the lookup
            waitForLoadJobResponse("testjob", null))) {
      RecordedRequest open = requests.next();
      assertStartResumableUpload(open);
      assertUploadLoadFile(requests.next());
      assertGetJob(requests.next(), jobIdOf(open), "asia-northeast1");
      assertGetJob(requests.next(), jobIdOf(open), "asia-northeast1");
      assertGetJob(requests.next(), jobIdOf(open), "asia-northeast1");
      assertGetJobStatus(requests.next(), "testjob");
    }
  }

  // Asking the dataset where its jobs run is part of the lookup, so a failure there is reported
  // the same way, with the refused upload named, rather than escaping as a bare dataset error.
  @Test
  public void testLoadFailsWithTheDuplicateContextWhenTheDatasetLookupIsRefused() throws Exception {
    Path loadFile = newLoadFile("load-409-dataset-refused-test.json");

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to look up job_id:\\[embulk_load_job_.*\\].*was refused as a duplicate"
                + ".*boom-dataset.*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(409, "boom", "duplicate"),
            errorResponse(400, "boom-dataset", "invalid"))) { // GET dataset, for the location
      assertStartResumableUpload(requests.next());
      assertUploadLoadFile(requests.next());
      assertGetDataset(requests.next());
    }
  }

  // A lookup failure that isn't retryable ends the load right away (the job's state is unknown,
  // and a new job id could load the rows twice), with a message that says what happened rather
  // than the raw library error.
  @Test
  public void testLoadFailsAtOnceWhenTheDuplicateJobLookupIsRefused() throws Exception {
    Path loadFile = newLoadFile("load-409-lookup-refused-test.json");

    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to look up job_id:\\[embulk_load_job_.*\\] after the upload of "
                + ".*load-409-lookup-refused-test\\.json .* was refused as a duplicate.*boom-lookup.*",
            server ->
                buildTestHostClient(server, c -> c.set("location", "asia-northeast1"))
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(409, "boom", "duplicate"),
            errorResponse(400, "boom-lookup", "invalid"))) {
      RecordedRequest open = requests.next();
      assertStartResumableUpload(open);
      assertUploadLoadFile(requests.next());
      assertGetJob(requests.next(), jobIdOf(open), "asia-northeast1");
      // No retry of the lookup, and no second upload: the cursor's close() would report either.
    }
  }

  // Every job this plugin creates carries the configured `location` in its job reference, as
  // ruby puts it into every job_reference it inserts. BigQueryOptions.setLocation() does not do
  // it: google-cloud-bigquery 2.14.0 applies that one to jobs.get, jobs.cancel and
  // getQueryResults only, never to the jobs.insert underneath create() and query().
  @Test
  public void testCreatedJobsCarryTheConfiguredLocation() throws Exception {
    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              BigqueryClient client =
                  buildTestHostClient(server, c -> c.set("location", "asia-northeast1"));
              assertNotNull(client.copy("temp", "table", JobInfo.WriteDisposition.WRITE_TRUNCATE));
              assertNotNull(client.executeQuery("SELECT 1"));
            },
            createCopyJobResponse("copy-job"),
            waitForCopyJobResponse("copy-job"),
            createQueryJobResponse("query-job"),
            waitForQueryJobResponse("query-job"))) {
      RecordedRequest copyJob = requests.next();
      assertPostJobs(copyJob);
      assertEquals(
          "asia-northeast1",
          requestBodyJson(copyJob).getJSONObject("jobReference").getString("location"));
      // The poll that follows goes where the job response said the job is (US in the canned
      // response), not where the request asked for it.
      assertGetJobStatus(requests.next(), "copy-job");

      RecordedRequest queryJob = requests.next();
      assertPostJobs(queryJob);
      assertEquals(
          "asia-northeast1",
          requestBodyJson(queryJob).getJSONObject("jobReference").getString("location"));
      assertGetJobStatus(requests.next(), "query-job");
    }
  }

  @Test
  public void testCopyRetriesOnInternalErrorThenSucceeds() throws Exception {
    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.CopyStatistics stats =
                  buildTestHostClient(server)
                      .copy("temp", "table", JobInfo.WriteDisposition.WRITE_TRUNCATE);
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("copy", COPY_CONFIG_BODY, "copy-job-1", "copy-job-2"))) {
      assertJobRetryRequests(requests, "copy-job-1", "copy-job-2");
    }
  }

  @Test
  public void testCopyGivesUpAfterMaxRetries() throws Exception {
    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Copy job get_job\\(copy-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1))
                    .copy("temp", "table", JobInfo.WriteDisposition.WRITE_TRUNCATE),
            jobRetryGivesUpResponses("copy", COPY_CONFIG_BODY, "copy-job-1", "copy-job-2"))) {
      assertJobRetryRequests(requests, "copy-job-1", "copy-job-2");
    }
  }

  @Test
  public void testExecuteQueryRetriesOnInternalErrorThenSucceeds() throws Exception {
    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.QueryStatistics stats =
                  buildTestHostClient(server).executeQuery("SELECT 1");
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("query", QUERY_CONFIG_BODY, "query-job-1", "query-job-2"))) {
      assertJobRetryRequests(requests, "query-job-1", "query-job-2");
    }
  }

  @Test
  public void testExecuteQueryGivesUpAfterMaxRetries() throws Exception {
    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Query job get_job\\(query-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1)).executeQuery("SELECT 1"),
            jobRetryGivesUpResponses("query", QUERY_CONFIG_BODY, "query-job-1", "query-job-2"))) {
      assertJobRetryRequests(requests, "query-job-1", "query-job-2");
    }
  }

  // With merge keys supplied directly, merge() skips the INFORMATION_SCHEMA lookup entirely and
  // just runs the MERGE statement as a query job, so its retry behavior is exactly
  // executeQuery()'s.
  @Test
  public void testMergeWithMergeKeysRetriesOnInternalErrorThenSucceeds() throws Exception {
    try (RequestCursor requests =
        runWithMockServer(
            server -> {
              JobStatistics.QueryStatistics stats =
                  buildTestHostClient(server, c -> c.set("mode", "merge"))
                      .merge("temp", "table", Arrays.asList("c0"), Collections.emptyList());
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("query", QUERY_CONFIG_BODY, "merge-job-1", "merge-job-2"))) {
      assertJobRetryRequests(requests, "merge-job-1", "merge-job-2");
    }
  }

  @Test
  public void testMergeWithMergeKeysGivesUpAfterMaxRetries() throws Exception {
    try (RequestCursor requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Query job get_job\\(merge-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("mode", "merge").set("retries", 1))
                    .merge("temp", "table", Arrays.asList("c0"), Collections.emptyList()),
            jobRetryGivesUpResponses("query", QUERY_CONFIG_BODY, "merge-job-1", "merge-job-2"))) {
      assertJobRetryRequests(requests, "merge-job-1", "merge-job-2");
    }
  }
}
