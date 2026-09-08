package org.embulk.output.bigquery_java;

import static org.embulk.output.bigquery_java.util.AssertUtil.assertMatches;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.LOAD_CONFIG_BODY;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertDeleteTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertFieldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetDataset;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetJobStatus;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPatchTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostJobs;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostTables;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createCopyJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createLoadJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createQueryJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.datasetResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.deleteResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.errorResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.firstSchemaField;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.firstSourceTableId;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.jobConfig;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.jobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.loadTestHostConfig;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.requestBodyJson;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableIdOf;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithNullableC0;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithNumRows;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithOldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForCopyJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForLoadJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForQueryJobResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// MockWebServer-based tests: run the plugin end-to-end against a mocked BigQuery API, one mode at
// a time.
public class TestBigqueryJavaOutputPluginWithMockServer {
  private static final List<String> SINGLE_INPUT_LINES = Arrays.asList("c0:string", "hello");

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .build();

  // Test-only cleanup: BigqueryUtil.FileWriterHolder is a static, JVM-wide map keyed by thread
  // id, and BigqueryJavaOutputPlugin#transaction() never removes its entries. A real embulk run
  // is a fresh JVM per invocation, so this never accumulates in production; but JUnit reuses the
  // same JVM (and often the same threads) across test methods, so stale writers from a previous
  // test would otherwise inflate getTransactionReport()'s num_input_rows here.
  @Before
  public void clearFileWriters() {
    BigqueryUtil.getFileWriters().clear();
  }

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  // Runs the plugin against a MockWebServer, from test_host.yml customized by `customizeConfig`,
  // and returns the requests it recorded, in order.
  private List<RecordedRequest> runWithMockServer(
      Function<ConfigSource, ConfigSource> customizeConfig, MockResponse... responses)
      throws Exception {
    return BigqueryMockWebServerTestUtil.runWithMockServer(
        embulk,
        testFolder,
        loadTestHostConfig(embulk, customizeConfig),
        SINGLE_INPUT_LINES,
        responses);
  }

  // Like runWithMockServer() above, but for a run expected to fail.
  private List<RecordedRequest> runWithMockServerExpectingFailure(
      Function<ConfigSource, ConfigSource> customizeConfig,
      Class<? extends Exception> expectedFailureType,
      String errorMessagePattern,
      MockResponse... responses)
      throws Exception {
    return BigqueryMockWebServerTestUtil.runWithMockServerExpectingFailure(
        embulk,
        testFolder,
        loadTestHostConfig(embulk, customizeConfig),
        SINGLE_INPUT_LINES,
        expectedFailureType,
        errorMessagePattern,
        responses);
  }

  @Test
  public void testRunAppendDirectMode() throws Exception {
    // append_direct has no temp table: autoCreate() creates the destination table directly, load
    // uploads straight into it, and there is no copy/merge step or temp table to delete afterward.
    List<RecordedRequest> requests =
        runWithMockServer(
            c -> c.set("mode", "append_direct"),
            datasetResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponse());

    assertEquals(5, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    assertEquals("table", tableIdOf(requestBodyJson(requests.get(1)), "tableReference"));

    assertPostJobs(requests.get(2));
    JSONObject loadConfig = jobConfig(requests.get(2), "load");
    assertEquals("table", tableIdOf(loadConfig, "destinationTable"));
    assertEquals("WRITE_APPEND", loadConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(3), "testjob");

    assertGetTable(requests.get(4), "table");
  }

  @Test
  public void testRunAppendMode() throws Exception {
    // GET dataset, POST temp table, load into temp table (POST job + GET status), GET temp table
    // row count (getTransactionReport), copy temp to final table (POST job + GET status), GET
    // table (updateTableIfNeed), DELETE temp table.
    List<RecordedRequest> requests =
        runWithMockServer(
            c -> c.set("mode", "append"),
            datasetResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponseWithNumRows(1),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            tableResponse(),
            deleteResponse());

    assertEquals(9, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostJobs(requests.get(2));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(2), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(3), "testjob");

    assertGetTable(requests.get(4), tempTableId); // getTransactionReport()

    assertPostJobs(requests.get(5));
    JSONObject copyConfig = jobConfig(requests.get(5), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_APPEND", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(6), "testjob");

    assertGetTable(requests.get(7), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.get(8), tempTableId);
  }

  @Test
  public void testRunReplaceMode() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServer(
            c -> c.set("mode", "replace"),
            datasetResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponseWithNumRows(1),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            tableResponse(),
            deleteResponse());

    assertEquals(9, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostJobs(requests.get(2));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(2), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(3), "testjob");

    assertGetTable(requests.get(4), tempTableId); // getTransactionReport()

    assertPostJobs(requests.get(5));
    JSONObject copyConfig = jobConfig(requests.get(5), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(6), "testjob");

    assertGetTable(requests.get(7), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.get(8), tempTableId);
  }

  @Test
  public void testRunReplaceModeRestoresRetainedDescriptionAndPolicyTags() throws Exception {
    // With retain_column_descriptions/retain_column_policy_tags on, isNeedUpdateTable() is true:
    // storeCachedSrcFieldsIfNeed() GETs the (pre-existing) destination table right after autoCreate
    // creates the temp table, and updateTableIfNeed() PATCHes the destination afterward (before the
    // temp table delete) to restore the cached description/policy tag onto its post-replace schema.
    List<RecordedRequest> requests =
        runWithMockServer(
            c ->
                c.set("mode", "replace")
                    .set("retain_column_descriptions", true)
                    .set("retain_column_policy_tags", true),
            datasetResponse(),
            tableResponse(),
            tableResponseWithOldDescriptionAndPolicyTag(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponseWithNumRows(1),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            tableResponseWithNullableC0(),
            tableResponse(),
            deleteResponse());

    assertEquals(11, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertGetTable(requests.get(2), "table"); // storeCachedSrcFieldsIfNeed()

    assertPostJobs(requests.get(3));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(3), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(4), "testjob");

    assertGetTable(requests.get(5), tempTableId); // getTransactionReport()

    assertPostJobs(requests.get(6));
    JSONObject copyConfig = jobConfig(requests.get(6), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(7), "testjob");

    assertGetTable(requests.get(8), "table"); // updateTableIfNeed()

    RecordedRequest patchRequest = requests.get(9);
    assertPatchTable(patchRequest, "table");
    assertFieldDescriptionAndPolicyTag(
        firstSchemaField(requestBodyJson(patchRequest)), "old-description", "old-policy-tag");

    assertDeleteTable(requests.get(10), tempTableId);
  }

  @Test
  public void testRunReplaceModeGivesUpAfterMaxLoadRetries() throws Exception {
    // With retries=1, load()'s own RetryExecutor allows only 2 attempts (the initial attempt plus
    // one retry) before giving up; two consecutive internalError job failures exhaust that budget
    // and the whole plugin run fails. The temp table delete is guaranteed via finally, so it still
    // runs even though the load itself failed.
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            c -> c.set("mode", "replace").set("retries", 1),
            RuntimeException.class,
            "(?s).*failed during waiting a Load job get_job\\(load-job-1.*",
            datasetResponse(),
            tableResponse(),
            jobResponse("load-job-1", "load", LOAD_CONFIG_BODY, "RUNNING", null),
            jobResponse("load-job-1", "load", LOAD_CONFIG_BODY, "DONE", "internalError"),
            jobResponse("load-job-2", "load", LOAD_CONFIG_BODY, "RUNNING", null),
            jobResponse("load-job-2", "load", LOAD_CONFIG_BODY, "DONE", "internalError"),
            deleteResponse());

    assertEquals(7, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");

    assertPostJobs(requests.get(2));

    assertGetJobStatus(requests.get(3), "load-job-1");

    assertPostJobs(requests.get(4));

    assertGetJobStatus(requests.get(5), "load-job-2");

    assertDeleteTable(requests.get(6), tempTableId);
  }

  @Test
  public void testRunReplaceModeStillUpdatesSchemaWhenTempTableDeleteFails() throws Exception {
    // Like ruby, the schema update now runs before the temp table delete, and the delete is
    // guaranteed via finally, so a delete failure no longer blocks the schema update from being
    // attempted (it still surfaces the delete failure as the run's exception, though).
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            c -> c.set("mode", "replace").set("retain_column_descriptions", true),
            RuntimeException.class,
            "(?s).*boom.*",
            datasetResponse(),
            tableResponse(),
            tableResponse(), // storeCachedSrcFieldsIfNeed() (retain_column_descriptions is on)
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponseWithNumRows(1),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            tableResponse(), // updateTableIfNeed(), no schema so it returns before patching
            errorResponse(400, "boom", "invalid")); // delete temp table (finally)

    assertEquals(10, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");

    assertGetTable(requests.get(2), "table"); // storeCachedSrcFieldsIfNeed()

    assertPostJobs(requests.get(3));

    assertGetJobStatus(requests.get(4), "testjob");

    assertGetTable(requests.get(5), tempTableId); // getTransactionReport()

    assertPostJobs(requests.get(6));

    assertGetJobStatus(requests.get(7), "testjob");

    assertGetTable(requests.get(8), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.get(9), tempTableId);
  }

  @Test
  public void testRunReplaceModeStillDeletesTempTableWhenStoreCachedSrcFieldsIfNeedFails()
      throws Exception {
    // storeCachedSrcFieldsIfNeed() runs first inside the (now widened) try block, right after
    // autoCreate() creates the temp table. Even when it fails outright, the temp table delete in
    // finally still runs, guaranteeing cleanup.
    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            c -> c.set("mode", "replace").set("retain_column_descriptions", true),
            RuntimeException.class,
            "(?s).*boom.*",
            datasetResponse(),
            tableResponse(),
            errorResponse(400, "boom", "invalid"), // storeCachedSrcFieldsIfNeed()
            deleteResponse());

    assertEquals(4, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");

    assertGetTable(requests.get(2), "table"); // storeCachedSrcFieldsIfNeed(), fails with "boom"

    assertDeleteTable(requests.get(3), tempTableId);
  }

  @Test
  public void testRunReplaceModeDeletesTempTableWhenPathsAreEmpty() throws Exception {
    // With zero input records, BigqueryPageOutput#add() is never called, so no writer is ever
    // registered and no intermediate file gets created: paths.isEmpty() is true, and transaction()
    // returns right after creating the destination table, before ever reaching the load/copy
    // logic. The temp table (already created by autoCreate()) must still be deleted via the
    // guaranteed finally block.
    List<RecordedRequest> requests =
        BigqueryMockWebServerTestUtil.runWithMockServer(
            embulk,
            testFolder,
            loadTestHostConfig(embulk, c -> c.set("mode", "replace")),
            Collections.singletonList("c0:string"),
            datasetResponse(),
            tableResponse(),
            tableResponse(),
            deleteResponse());

    assertEquals(4, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostTables(requests.get(2));
    assertEquals("table", tableIdOf(requestBodyJson(requests.get(2)), "tableReference"));

    assertDeleteTable(requests.get(3), tempTableId);
  }

  @Test
  public void testRunDeleteInAdvanceMode() throws Exception {
    // autoCreate() deletes the destination table (or partition) before creating the temp table;
    // load into the temp table, copy (WRITE_TRUNCATE) to the destination, delete the temp table.
    // TODO: avoid going through a temp table.
    List<RecordedRequest> requests =
        runWithMockServer(
            c -> c.set("mode", "delete_in_advance"),
            datasetResponse(),
            deleteResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponseWithNumRows(1),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            tableResponse(),
            deleteResponse());

    assertEquals(10, requests.size());

    assertGetDataset(requests.get(0));

    assertDeleteTable(requests.get(1), "table");

    assertPostTables(requests.get(2));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(2)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostJobs(requests.get(3));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(3), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(4), "testjob");

    assertGetTable(requests.get(5), tempTableId); // getTransactionReport()

    assertPostJobs(requests.get(6));
    JSONObject copyConfig = jobConfig(requests.get(6), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(7), "testjob");

    assertGetTable(requests.get(8), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.get(9), tempTableId);
  }

  @Test
  public void testRunMergeMode() throws Exception {
    // autoCreate() creates both the temp table and the final table (merge needs the final table
    // to exist for its MERGE statement); load into the temp table, then MERGE it into the
    // destination via a query job, then delete the temp table.
    List<RecordedRequest> requests =
        runWithMockServer(
            // merge_keys is set explicitly to avoid merge mode issuing an extra
            // INFORMATION_SCHEMA query job to discover merge keys when none are configured.
            c -> c.set("mode", "merge").set("merge_keys", Arrays.asList("c0")),
            datasetResponse(),
            tableResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            tableResponseWithNumRows(1),
            createQueryJobResponse("testjob"),
            waitForQueryJobResponse("testjob"),
            tableResponse(),
            deleteResponse());

    assertEquals(10, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostTables(requests.get(2));
    assertEquals("table", tableIdOf(requestBodyJson(requests.get(2)), "tableReference"));

    assertPostJobs(requests.get(3));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(3), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(4), "testjob");

    assertGetTable(requests.get(5), tempTableId); // getTransactionReport()

    assertPostJobs(requests.get(6));
    String mergeQuery = jobConfig(requests.get(6), "query").getString("query");
    assertTrue(
        mergeQuery.matches(
            "(?s).*MERGE.*`table`.*" + java.util.regex.Pattern.quote(tempTableId) + ".*"));

    assertGetJobStatus(requests.get(7), "testjob");

    assertGetTable(requests.get(8), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.get(9), tempTableId);
  }
}
