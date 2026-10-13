package org.embulk.output.bigquery_java;

import static org.embulk.output.bigquery_java.util.AssertUtil.assertMatches;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertDeleteTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertFieldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetDataset;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetJobStatus;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertGetTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertLoadAttempt;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPatchTable;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostJobs;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertPostTables;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertStartResumableUpload;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.assertUploadLoadFile;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createCopyJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.createQueryJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.datasetResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.deleteResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.errorResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.firstSchemaField;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.firstSourceTableId;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.jobConfig;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.loadAttemptResponses;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.loadTestHostConfig;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.requestBodyJson;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableIdOf;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithNullableC0;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithNumRows;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithOldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.uploadedLines;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForCopyJobResponse;
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
import org.embulk.exec.PartialExecutionException;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.MockResponses;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.RequestCursor;
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
  // and returns a cursor over the requests it recorded, in order.
  private RequestCursor runWithMockServer(
      Function<ConfigSource, ConfigSource> customizeConfig, MockResponse... responses)
      throws Exception {
    return new RequestCursor(
        BigqueryMockWebServerTestUtil.runWithMockServer(
            embulk,
            testFolder,
            loadTestHostConfig(embulk, customizeConfig),
            SINGLE_INPUT_LINES,
            responses));
  }

  // Like runWithMockServer() above, but for a run expected to fail.
  private RequestCursor runWithMockServerExpectingFailure(
      Function<ConfigSource, ConfigSource> customizeConfig,
      Class<? extends Exception> expectedFailureType,
      String errorMessagePattern,
      MockResponse... responses)
      throws Exception {
    return new RequestCursor(
        BigqueryMockWebServerTestUtil.runWithMockServerExpectingFailure(
            embulk,
            testFolder,
            loadTestHostConfig(embulk, customizeConfig),
            SINGLE_INPUT_LINES,
            expectedFailureType,
            errorMessagePattern,
            responses));
  }

  @Test
  public void testRunAppendDirectMode() throws Exception {
    // append_direct has no temp table: autoCreate() creates the destination table directly, load
    // uploads straight into it, and there is no copy/merge step or temp table to delete afterward.
    RequestCursor requests =
        runWithMockServer(
            c -> c.set("mode", "append_direct"),
            MockResponses.of(datasetResponse(), tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(tableResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTable = requests.next();
    assertPostTables(createTable);
    assertEquals("table", tableIdOf(requestBodyJson(createTable), "tableReference"));

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    RecordedRequest uploadFile = requests.next();
    assertUploadLoadFile(uploadFile);
    assertGetJobStatus(requests.next(), "testjob");
    JSONObject loadConfig = jobConfig(openUpload, "load");
    assertEquals("table", tableIdOf(loadConfig, "destinationTable"));
    assertEquals("WRITE_APPEND", loadConfig.getString("writeDisposition"));
    // The uploaded bytes are the intermediate file itself: the input row as JSONL.
    assertEquals(Collections.singletonList("{\"c0\":\"hello\"}"), uploadedLines(uploadFile, true));

    assertGetTable(requests.next(), "table");

    requests.assertNoMore();
  }

  @Test
  public void testRunFailsImmediatelyOnRecordConversionError() throws Exception {
    RequestCursor requests =
        runWithMockServerExpectingFailure(
            c ->
                c.set("mode", "append_direct")
                    .set(
                        "column_options",
                        Collections.singletonList(
                            embulk.newConfig().set("name", "c0").set("type", "INTEGER"))),
            PartialExecutionException.class,
            "(?s).*hello cannot be converted to INTEGER.*",
            datasetResponse(),
            tableResponse());

    assertGetDataset(requests.next());
    assertPostTables(requests.next());

    requests.assertNoMore();
  }

  @Test
  public void testRunAppendMode() throws Exception {
    // GET dataset, POST temp table, load into temp table (POST to open the upload session + PUT
    // file + GET status), GET temp table row count (getTransactionReport), copy temp to final
    // table (POST job + GET status), GET table (updateTableIfNeed), DELETE temp table.
    RequestCursor requests =
        runWithMockServer(
            c -> c.set("mode", "append"),
            MockResponses.of(datasetResponse(), tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    JSONObject copyConfig = jobConfig(createJob, "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_APPEND", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunReplaceMode() throws Exception {
    RequestCursor requests =
        runWithMockServer(
            c -> c.set("mode", "replace"),
            MockResponses.of(datasetResponse(), tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    JSONObject copyConfig = jobConfig(createJob, "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunExpandsStrftimeInTableName() throws Exception {
    // table: table_%Y%m%d is expanded to the current date before any BigQuery request is made
    // (BigqueryTaskBuilder.build()), so every reference to the destination table in the recorded
    // requests already reflects the expanded name, not the literal "table_%Y%m%d" pattern.
    RequestCursor requests =
        runWithMockServer(
            c -> c.set("mode", "replace").set("table", "table_%Y%m%d"),
            MockResponses.of(datasetResponse(), tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table_\\d{8}");

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    JSONObject copyConfig = jobConfig(createJob, "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    String destinationTable = tableIdOf(copyConfig, "destinationTable");
    assertMatches(destinationTable, "table_\\d{8}");

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), destinationTable); // updateTableIfNeed()
    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunReplaceModeRestoresRetainedDescriptionAndPolicyTags() throws Exception {
    // With retain_column_descriptions/retain_column_policy_tags on, isNeedUpdateTable() is true:
    // storeCachedSrcFieldsIfNeed() GETs the (pre-existing) destination table right after autoCreate
    // creates the temp table, and updateTableIfNeed() PATCHes the destination afterward (before the
    // temp table delete) to restore the cached description/policy tag onto its post-replace schema.
    RequestCursor requests =
        runWithMockServer(
            c ->
                c.set("mode", "replace")
                    .set("retain_column_descriptions", true)
                    .set("retain_column_policy_tags", true),
            MockResponses.of(
                    datasetResponse(),
                    tableResponse(),
                    tableResponseWithOldDescriptionAndPolicyTag())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponseWithNullableC0(),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertGetTable(requests.next(), "table"); // storeCachedSrcFieldsIfNeed()

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    JSONObject copyConfig = jobConfig(createJob, "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    RecordedRequest patchRequest = requests.next();
    assertPatchTable(patchRequest, "table");
    assertFieldDescriptionAndPolicyTag(
        firstSchemaField(requestBodyJson(patchRequest)), "old-description", "old-policy-tag");

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunReplaceModeGivesUpAfterMaxLoadRetries() throws Exception {
    // With retries=1, load()'s own RetryExecutor allows only 2 attempts (the initial attempt plus
    // one retry) before giving up; two consecutive internalError job failures exhaust that budget
    // and the whole plugin run fails. The temp table delete is guaranteed via finally, so it still
    // runs even though the load itself failed.
    RequestCursor requests =
        runWithMockServerExpectingFailure(
            c -> c.set("mode", "replace").set("retries", 1),
            RuntimeException.class,
            "(?s).*failed during waiting a Load job get_job\\(load-job-1.*",
            MockResponses.of(datasetResponse(), tableResponse())
                .add(loadAttemptResponses("load-job-1", "internalError"))
                .add(loadAttemptResponses("load-job-2", "internalError"))
                .add(deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");

    assertLoadAttempt(requests, "load-job-1");

    // The retry re-uploads the whole file as a new load job.
    assertLoadAttempt(requests, "load-job-2");

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunReplaceModeStillUpdatesSchemaWhenTempTableDeleteFails() throws Exception {
    // Like ruby, the schema update now runs before the temp table delete, and the delete is
    // guaranteed via finally, so a delete failure no longer blocks the schema update from being
    // attempted (it still surfaces the delete failure as the run's exception, though).
    RequestCursor requests =
        runWithMockServerExpectingFailure(
            c -> c.set("mode", "replace").set("retain_column_descriptions", true),
            RuntimeException.class,
            "(?s).*boom.*",
            MockResponses.of(
                    datasetResponse(),
                    tableResponse(),
                    tableResponse() // storeCachedSrcFieldsIfNeed() (retain_column_descriptions is
                    // on)
                    )
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponse(), // updateTableIfNeed(), no schema so it returns before patching
                    errorResponse(400, "boom", "invalid"))
                .toArray()); // delete temp table (finally)
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");

    assertGetTable(requests.next(), "table"); // storeCachedSrcFieldsIfNeed()

    assertLoadAttempt(requests, "testjob");

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    assertPostJobs(requests.next());

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunReplaceModeStillDeletesTempTableWhenStoreCachedSrcFieldsIfNeedFails()
      throws Exception {
    // storeCachedSrcFieldsIfNeed() runs first inside the (now widened) try block, right after
    // autoCreate() creates the temp table. Even when it fails outright, the temp table delete in
    // finally still runs, guaranteeing cleanup.
    RequestCursor requests =
        runWithMockServerExpectingFailure(
            c -> c.set("mode", "replace").set("retain_column_descriptions", true),
            RuntimeException.class,
            "(?s).*boom.*",
            datasetResponse(),
            tableResponse(),
            errorResponse(400, "boom", "invalid"), // storeCachedSrcFieldsIfNeed()
            deleteResponse());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");

    assertGetTable(requests.next(), "table"); // storeCachedSrcFieldsIfNeed(), fails with "boom"

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunReplaceModeDeletesTempTableWhenPathsAreEmpty() throws Exception {
    // With zero input records, BigqueryPageOutput#add() is never called, so no writer is ever
    // registered and no intermediate file gets created: paths.isEmpty() is true, and transaction()
    // returns right after creating the destination table, before ever reaching the load/copy
    // logic. The temp table (already created by autoCreate()) must still be deleted via the
    // guaranteed finally block.
    RequestCursor requests =
        new RequestCursor(
            BigqueryMockWebServerTestUtil.runWithMockServer(
                embulk,
                testFolder,
                loadTestHostConfig(embulk, c -> c.set("mode", "replace")),
                Collections.singletonList("c0:string"),
                datasetResponse(),
                tableResponse(),
                tableResponse(),
                deleteResponse()));

    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    RecordedRequest createTable = requests.next();
    assertPostTables(createTable);
    assertEquals("table", tableIdOf(requestBodyJson(createTable), "tableReference"));

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunDeleteInAdvanceMode() throws Exception {
    // autoCreate() deletes the destination table (or partition) before creating the temp table;
    // load into the temp table, copy (WRITE_TRUNCATE) to the destination, delete the temp table.
    // TODO: avoid going through a temp table.
    RequestCursor requests =
        runWithMockServer(
            c -> c.set("mode", "delete_in_advance"),
            MockResponses.of(datasetResponse(), deleteResponse(), tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    assertDeleteTable(requests.next(), "table");

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    JSONObject copyConfig = jobConfig(createJob, "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunDeleteInAdvanceModeWhenDestinationTableDoesNotExist() throws Exception {
    // First run against a fresh dataset: the destination table doesn't exist yet, so the
    // tables.delete in autoCreate() gets a 404. The BigQuery client library maps that to
    // BigQuery#delete() returning false rather than throwing, and the plugin ignores the return
    // value, so the run proceeds exactly as when the delete succeeds.
    RequestCursor requests =
        runWithMockServer(
            c -> c.set("mode", "delete_in_advance"),
            MockResponses.of(
                    datasetResponse(),
                    errorResponse(404, "Not found: Table project:dataset.table", "notFound"),
                    tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createCopyJobResponse("testjob"),
                    waitForCopyJobResponse("testjob"),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    assertDeleteTable(requests.next(), "table");

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    JSONObject copyConfig = jobConfig(createJob, "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }

  @Test
  public void testRunMergeMode() throws Exception {
    // autoCreate() creates both the temp table and the final table (merge needs the final table
    // to exist for its MERGE statement); load into the temp table, then MERGE it into the
    // destination via a query job, then delete the temp table.
    RequestCursor requests =
        runWithMockServer(
            // merge_keys is set explicitly to avoid merge mode issuing an extra
            // INFORMATION_SCHEMA query job to discover merge keys when none are configured.
            c -> c.set("mode", "merge").set("merge_keys", Arrays.asList("c0")),
            MockResponses.of(datasetResponse(), tableResponse(), tableResponse())
                .add(loadAttemptResponses("testjob", null))
                .add(
                    tableResponseWithNumRows(1),
                    createQueryJobResponse("testjob"),
                    waitForQueryJobResponse("testjob"),
                    tableResponse(),
                    deleteResponse())
                .toArray());
    assertGetDataset(requests.next());

    RecordedRequest createTempTable = requests.next();
    assertPostTables(createTempTable);
    String tempTableId = tableIdOf(requestBodyJson(createTempTable), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    RecordedRequest createTable = requests.next();
    assertPostTables(createTable);
    assertEquals("table", tableIdOf(requestBodyJson(createTable), "tableReference"));

    RecordedRequest openUpload = requests.next();
    assertStartResumableUpload(openUpload);
    assertUploadLoadFile(requests.next());
    assertGetJobStatus(requests.next(), "testjob");
    assertEquals(tempTableId, tableIdOf(jobConfig(openUpload, "load"), "destinationTable"));

    assertGetTable(requests.next(), tempTableId); // getTransactionReport()

    RecordedRequest createJob = requests.next();
    assertPostJobs(createJob);
    String mergeQuery = jobConfig(createJob, "query").getString("query");
    assertTrue(
        mergeQuery.matches(
            "(?s).*MERGE.*`table`.*" + java.util.regex.Pattern.quote(tempTableId) + ".*"));

    assertGetJobStatus(requests.next(), "testjob");

    assertGetTable(requests.next(), "table"); // updateTableIfNeed()

    assertDeleteTable(requests.next(), tempTableId);

    requests.assertNoMore();
  }
}
