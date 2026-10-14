package org.embulk.output.bigquery_java;

import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.COPY_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.QUERY_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.UPLOAD_CHUNK_SIZE;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertFieldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetJobStatus;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertLoadAttempt;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPatchTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostJobs;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostTables;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertStartResumableUpload;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertUploadChunk;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertUploadLoadFile;
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
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForLoadJobResponse;
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
    List<RecordedRequest> requests =
        runWithMockServer(
            // Should swallow the 409 and return normally.
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            errorResponse(409, "Already Exists: Table project:dataset.table", "duplicate"));

    assertEquals(1, requests.size());

    assertPostTables(requests.get(0));
  }

  @Test
  public void testCreateTableIfNotExistThrowsOnOtherError() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            // 400 (not 500/503/429) so the BigQuery client library's own transport-level retry
            // doesn't kick in before this reaches BigqueryClient's error handling.
            errorResponse(400, "Invalid table definition", "invalid"));

    assertEquals(1, requests.size());

    assertPostTables(requests.get(0));
  }

  @Test
  public void testCreateTableIfNotExistPassesThroughPartitionDecoratorInTableName()
      throws Exception {
    // Ruby strips the partition decorator (`$YYYYMMDD`) from the table name before creating a
    // temp table for it, so decorated destination tables work there. Java passes the table name
    // through as-is, so BigQuery rejects the decorator as part of a table ID and this fails.
    // TODO: strip the partition decorator from the table name before creating the table, like
    // ruby does, instead of passing it through and letting BigQuery reject it.
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table$20160929"),
            errorResponse(400, "Invalid table ID", "invalid"));

    assertEquals(1, requests.size());

    RecordedRequest request = requests.get(0);
    assertPostTables(request);
    assertEquals("table$20160929", tableIdOf(requestBodyJson(request), "tableReference"));
  }

  @Test
  public void testCreateTableIfNotExistRetriesTransientErrorsThenSucceeds() throws Exception {
    // test_host.yml sets retries: 1, so 1 failure then a success should still go through (1
    // initial attempt + 1 retry = 2 total), driven by the task's `retries` config (see
    // buildRetrySettings() in BigqueryClient).
    List<MockResponse> responses = new ArrayList<>();
    responses.add(errorResponse(500, "boom", "internalError"));
    responses.add(tableResponse());

    List<RecordedRequest> requests =
        runWithMockServer(
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            responses.toArray(new MockResponse[0]));

    assertEquals(2, requests.size());
    for (RecordedRequest request : requests) {
      assertPostTables(request);
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

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            responses.toArray(new MockResponse[0]));

    assertEquals(2, requests.size());
    for (RecordedRequest request : requests) {
      assertPostTables(request);
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
    List<RecordedRequest> requests =
        runWithMockServer(
            action,
            // storeCachedSrcFieldsIfNeed(): GET table returns the old schema, with both a
            // description and a policy tag.
            tableResponseWithOldDescriptionAndPolicyTag(),
            // updateTableIfNeed(): GET table returns the new (post-replace) schema, with neither;
            // the PATCH request should restore whichever cached fields are retained.
            tableResponseWithNullableC0(),
            tableResponseWithSchemaFields("[{\"name\":\"c0\",\"type\":\"STRING\"}]"));

    assertEquals(3, requests.size());

    assertGetTable(requests.get(0), "table"); // storeCachedSrcFieldsIfNeed()

    assertGetTable(requests.get(1), "table"); // updateTableIfNeed()

    RecordedRequest patchRequest = requests.get(2);
    assertPatchTable(patchRequest, "table");
    assertFieldDescriptionAndPolicyTag(
        firstSchemaField(requestBodyJson(patchRequest)), expectedDescription, expectedPolicyTag);
  }

  // Runs `action` (storeCachedSrcFieldsIfNeed() + updateTableIfNeed()) and asserts it makes no
  // PATCH: just a single GET table request from updateTableIfNeed() (storeCachedSrcFieldsIfNeed()
  // makes no request at all when isNeedUpdateTable() is false).
  private void assertNoPatch(MockServerAction action) throws Exception {
    List<RecordedRequest> requests = runWithMockServer(action, tableResponseWithNullableC0());

    assertEquals(1, requests.size());

    assertGetTable(requests.get(0), "table"); // updateTableIfNeed()
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
  private static void assertJobRetryRequests(
      List<RecordedRequest> requests, String jobId1, String jobId2) {
    assertEquals(4, requests.size());

    assertPostJobs(requests.get(0));

    assertGetJobStatus(requests.get(1), jobId1);

    assertPostJobs(requests.get(2));

    assertGetJobStatus(requests.get(3), jobId2);
  }

  // A load differs from copy/query in how the job is created: the file goes through a resumable
  // upload session, so each attempt is the 3 requests of loadAttemptResponses(), and every retry
  // re-uploads the whole file as a new load job.
  private Path newLoadFile(String name) throws Exception {
    Path loadFile = testFolder.newFile(name).toPath();
    Files.write(loadFile, Collections.singletonList("{\"c0\":\"hello\"}"));
    return loadFile;
  }

  @Test
  public void testLoadRetriesOnInternalErrorThenSucceeds() throws Exception {
    Path loadFile = newLoadFile("load-retry-test.json");

    List<RecordedRequest> requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            MockResponses.of(loadAttemptResponses("load-job-1", "internalError"))
                .add(loadAttemptResponses("load-job-2", null))
                .toArray());

    // A job that ran and failed is retried as a new job (new job id), like ruby's with_job_retry.
    assertEquals(6, requests.size());
    assertLoadAttempt(requests, 0, "load-job-1");
    assertLoadAttempt(requests, 3, "load-job-2");
    assertNotEquals(jobIdOf(requests.get(0)), jobIdOf(requests.get(3)));
  }

  @Test
  public void testLoadGivesUpAfterMaxRetries() throws Exception {
    Path loadFile = newLoadFile("load-giveup-test.json");

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Load job get_job\\(load-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1))
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            MockResponses.of(loadAttemptResponses("load-job-1", "internalError"))
                .add(loadAttemptResponses("load-job-2", "internalError"))
                .toArray());

    assertEquals(6, requests.size());
    assertLoadAttempt(requests, 0, "load-job-1");
    assertLoadAttempt(requests, 3, "load-job-2");
  }

  // The library itself retries a chunk PUT that fails with a network error (here a connection
  // reset while sending): the same chunk is re-sent on the same upload session, up to `retries`
  // more times (test_host.yml: 1), and load() never notices.
  @Test
  public void testLoadResendsChunkOnConnectionReset() throws Exception {
    Path loadFile = newLoadFile("load-reset-test.json");
    long size = Files.size(loadFile);

    List<RecordedRequest> requests =
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
            waitForLoadJobResponse("testjob", null));

    assertEquals(4, requests.size());
    assertStartResumableUpload(requests.get(0));
    assertUploadChunk(requests.get(1), 0, size - 1, size); // reset while sending
    assertUploadLoadFile(requests.get(2)); // the library's retry, same session
    assertGetJobStatus(requests.get(3), "testjob");
  }

  // A file over the library's chunk size goes up as an intermediate chunk (answered with 308)
  // followed by the final chunk, which creates the job.
  @Test
  public void testLoadUploadsLargeFileInChunks() throws Exception {
    Path loadFile = testFolder.newFile("load-large-test.json").toPath();
    long size = writeLoadFile(loadFile, UPLOAD_CHUNK_SIZE + 1);

    List<RecordedRequest> requests =
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
            waitForLoadJobResponse("testjob", null));

    assertEquals(4, requests.size());
    assertStartResumableUpload(requests.get(0));
    assertUploadChunk(requests.get(1), 0, UPLOAD_CHUNK_SIZE - 1, -1);
    assertUploadChunk(requests.get(2), UPLOAD_CHUNK_SIZE, size - 1, size);
    assertGetJobStatus(requests.get(3), "testjob");
  }

  // A chunk PUT answered with a 5xx is re-sent by the library on the same session, `retries`
  // more times (test_host.yml: 1); when they are all 5xx the upload fails.
  @Test
  public void testLoadResendsChunkOnServerErrorThenGivesUp() throws Exception {
    Path loadFile = newLoadFile("load-5xx-test.json");

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            RuntimeException.class,
            "(?s).*BigQueryException: 503 .*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(503, "boom", "backendError"),
            errorResponse(503, "boom", "backendError"));

    assertEquals(3, requests.size());
    assertStartResumableUpload(requests.get(0));
    assertUploadLoadFile(requests.get(1));
    assertUploadLoadFile(requests.get(2)); // the library's retry, same session
  }

  // A chunk PUT answered with a 4xx is not re-sent.
  @Test
  public void testLoadDoesNotResendChunkOnClientError() throws Exception {
    Path loadFile = newLoadFile("load-4xx-test.json");

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            RuntimeException.class,
            "(?s).*BigQueryException: 400 .*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            errorResponse(400, "Invalid", "invalid"));

    assertEquals(2, requests.size());
    assertStartResumableUpload(requests.get(0));
    assertUploadLoadFile(requests.get(1));
  }

  // The configured read timeout applies to the chunk PUT as well (#113): a server that never
  // answers makes the client give up after read_timeout_sec (1 s in test_host.yml), the library
  // retries that like any network failure, and the upload fails once retries run out. Costs
  // about 2 s of real waiting.
  @Test
  public void testLoadFailsOnReadTimeoutDuringUpload() throws Exception {
    Path loadFile = newLoadFile("load-timeout-test.json");

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            RuntimeException.class,
            "(?s).*Read timed out.*",
            server ->
                buildTestHostClient(server)
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            startResumableUploadResponse(),
            unansweredResponse(),
            unansweredResponse());

    assertEquals(3, requests.size());
    assertStartResumableUpload(requests.get(0));
    assertUploadLoadFile(requests.get(1));
    assertUploadLoadFile(requests.get(2)); // the library's retry, same session
  }

  @Test
  public void testCopyRetriesOnInternalErrorThenSucceeds() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServer(
            server -> {
              JobStatistics.CopyStatistics stats =
                  buildTestHostClient(server)
                      .copy("temp", "table", JobInfo.WriteDisposition.WRITE_TRUNCATE);
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("copy", COPY_CONFIG_BODY, "copy-job-1", "copy-job-2"));

    assertJobRetryRequests(requests, "copy-job-1", "copy-job-2");
  }

  @Test
  public void testCopyGivesUpAfterMaxRetries() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Copy job get_job\\(copy-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1))
                    .copy("temp", "table", JobInfo.WriteDisposition.WRITE_TRUNCATE),
            jobRetryGivesUpResponses("copy", COPY_CONFIG_BODY, "copy-job-1", "copy-job-2"));

    assertJobRetryRequests(requests, "copy-job-1", "copy-job-2");
  }

  @Test
  public void testExecuteQueryRetriesOnInternalErrorThenSucceeds() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServer(
            server -> {
              JobStatistics.QueryStatistics stats =
                  buildTestHostClient(server).executeQuery("SELECT 1");
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("query", QUERY_CONFIG_BODY, "query-job-1", "query-job-2"));

    assertJobRetryRequests(requests, "query-job-1", "query-job-2");
  }

  @Test
  public void testExecuteQueryGivesUpAfterMaxRetries() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Query job get_job\\(query-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1)).executeQuery("SELECT 1"),
            jobRetryGivesUpResponses("query", QUERY_CONFIG_BODY, "query-job-1", "query-job-2"));

    assertJobRetryRequests(requests, "query-job-1", "query-job-2");
  }

  // With merge keys supplied directly, merge() skips the INFORMATION_SCHEMA lookup entirely and
  // just runs the MERGE statement as a query job, so its retry behavior is exactly
  // executeQuery()'s.
  @Test
  public void testMergeWithMergeKeysRetriesOnInternalErrorThenSucceeds() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServer(
            server -> {
              JobStatistics.QueryStatistics stats =
                  buildTestHostClient(server, c -> c.set("mode", "merge"))
                      .merge("temp", "table", Arrays.asList("c0"), Collections.emptyList());
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("query", QUERY_CONFIG_BODY, "merge-job-1", "merge-job-2"));

    assertJobRetryRequests(requests, "merge-job-1", "merge-job-2");
  }

  @Test
  public void testMergeWithMergeKeysGivesUpAfterMaxRetries() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Query job get_job\\(merge-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("mode", "merge").set("retries", 1))
                    .merge("temp", "table", Arrays.asList("c0"), Collections.emptyList()),
            jobRetryGivesUpResponses("query", QUERY_CONFIG_BODY, "merge-job-1", "merge-job-2"));

    assertJobRetryRequests(requests, "merge-job-1", "merge-job-2");
  }
}
