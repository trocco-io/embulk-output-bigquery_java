package org.embulk.output.bigquery_java;

import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.COPY_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.LOAD_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.QUERY_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertFieldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetJobStatus;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPatchTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostJobs;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostTables;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.errorResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.firstSchemaField;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.jobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.loadTestHostConfig;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.requestBodyJson;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.runWithMockServer;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.runWithMockServerExpectingFailure;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableIdOf;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithNullableC0;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithOldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithSchemaFields;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
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
        loadTestHostConfig(embulk, customizeConfig).set("test_host", server.url("/").toString());
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

  // retainDescriptions/retainPolicyTags only ever take effect in mode:replace, so this helper is
  // the only place that can exercise them. When `columnOptionDescription` is true, c0 gets a
  // COLUMN_OPTION_DESCRIPTION column_options description, letting callers check how it interacts
  // with the retained fields.
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

  // column_options[].description applies regardless of mode, so isNeedUpdateTable() is true here
  // whenever `columnOptionDescription` is set, even outside mode:replace. The retain flags are
  // left at their (false) config default since they only take effect in mode:replace.
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

  // With no column_options[].description and the retain flags left at their false default,
  // isNeedUpdateTable() is false regardless of mode, so updateTableIfNeed() never PATCHes.
  // When column_options[].description is set, it applies regardless of mode (retain flags never
  // take effect outside mode:replace, so neither field ends up with a policy tag here).

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInAppendMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "append", false));
  }

  @Test
  public void testUpdateTableIfNeedPatchesColumnOptionDescriptionInAppendMode() throws Exception {
    assertPatchedField(
        server -> updateTableInMode(server, "append", true), COLUMN_OPTION_DESCRIPTION, null);
  }

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInAppendDirectMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "append_direct", false));
  }

  @Test
  public void testUpdateTableIfNeedPatchesColumnOptionDescriptionInAppendDirectMode()
      throws Exception {
    assertPatchedField(
        server -> updateTableInMode(server, "append_direct", true),
        COLUMN_OPTION_DESCRIPTION,
        null);
  }

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInDeleteInAdvanceMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "delete_in_advance", false));
  }

  @Test
  public void testUpdateTableIfNeedPatchesColumnOptionDescriptionInDeleteInAdvanceMode()
      throws Exception {
    assertPatchedField(
        server -> updateTableInMode(server, "delete_in_advance", true),
        COLUMN_OPTION_DESCRIPTION,
        null);
  }

  @Test
  public void testUpdateTableIfNeedDoesNotPatchInMergeMode() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "merge", false));
  }

  @Test
  public void testUpdateTableIfNeedPatchesColumnOptionDescriptionInMergeMode() throws Exception {
    assertPatchedField(
        server -> updateTableInMode(server, "merge", true), COLUMN_OPTION_DESCRIPTION, null);
  }

  // isNeedUpdateTable() (mode: replace with a retain flag on, or column_options[].description set
  // regardless of mode) gates whether updateTableIfNeed() PATCHes at all; retainDescriptions/
  // retainPolicyTags separately control whether each retained field is restored, but only in
  // mode:replace; column_options[].description, when present, is applied after the retained
  // description and so wins over it.

  @Test
  public void testUpdateTableIfNeedDoesNotPatchWhenRetainFlagsFalse() throws Exception {
    assertNoPatch(server -> updateTableInMode(server, "replace", false));
    assertNoPatch(server -> updateTableInReplaceMode(server, false, false, false));
  }

  @Test
  public void testUpdateTableIfNeedPatchesColumnOptionDescriptionInReplaceModeWithRetainFlagsFalse()
      throws Exception {
    assertPatchedField(
        server -> updateTableInMode(server, "replace", true), COLUMN_OPTION_DESCRIPTION, null);
    assertPatchedField(
        server -> updateTableInReplaceMode(server, false, false, true),
        COLUMN_OPTION_DESCRIPTION,
        null);
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

  @Test
  public void testLoadRetriesOnInternalErrorThenSucceeds() throws Exception {
    Path loadFile = testFolder.newFile("load-retry-test.json").toPath();

    List<RecordedRequest> requests =
        runWithMockServer(
            server -> {
              JobStatistics.LoadStatistics stats =
                  buildTestHostClient(server)
                      .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);
              assertNotNull(stats);
            },
            jobRetrySucceedsResponses("load", LOAD_CONFIG_BODY, "load-job-1", "load-job-2"));

    assertJobRetryRequests(requests, "load-job-1", "load-job-2");
  }

  @Test
  public void testLoadGivesUpAfterMaxRetries() throws Exception {
    Path loadFile = testFolder.newFile("load-giveup-test.json").toPath();

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryInternalException.class,
            "(?s).*failed during waiting a Load job get_job\\(load-job-1.*",
            server ->
                buildTestHostClient(server, c -> c.set("retries", 1))
                    .load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND),
            jobRetryGivesUpResponses("load", LOAD_CONFIG_BODY, "load-job-1", "load-job-2"));

    assertJobRetryRequests(requests, "load-job-1", "load-job-2");
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
