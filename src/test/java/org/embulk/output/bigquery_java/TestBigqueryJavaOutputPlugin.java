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
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.tableResponseWithOldDescriptionAndPolicyTag;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForCopyJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForLoadJobResponse;
import static org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil.waitForQueryJobResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.BigqueryTimePartitioning;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.util.BigqueryMockWebServerTestUtil;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.LocalFile;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBigqueryJavaOutputPlugin {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private ConfigSource config;
  private static final String BASIC_RESOURCE_PATH = "/java/org/embulk/output/bigquery_java/";
  private static final List<String> SINGLE_INPUT_LINES = Arrays.asList("c0:string", "hello");

  private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName) {
    return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .build();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void testDefaultConfigValues() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    assertEquals("replace", task.getMode());
    assertEquals(5, task.getRetries());
    assertEquals(0, task.getMaxBadRecords());
    assertEquals("dataset", task.getDataset());
    assertEquals("table", task.getTable());
    assertEquals("service_account", task.getAuthMethod());
    assertEquals("UTC", task.getDefaultTimezone());
    assertEquals("UTF-8", task.getEncoding());
    assertTrue(task.getDeleteFromLocalWhenJobEnd());
    assertFalse(task.getAutoCreateDataset());
    assertTrue(task.getAutoCreateTable());
  }

  @Test
  public void testWithTimePartitioning() {
    config = loadYamlResource(embulk, "time_partitioning.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryTimePartitioning bigqueryTimePartitioning;

    assertTrue(task.getTimePartitioning().isPresent());
    bigqueryTimePartitioning = task.getTimePartitioning().get();
    assertEquals("DAY", bigqueryTimePartitioning.getType());
    assertTrue(bigqueryTimePartitioning.getField().isPresent());
    assertEquals("date", bigqueryTimePartitioning.getField().get());
  }

  public interface TestTask extends Task {
    @Config("json_keyfile")
    LocalFile getJsonKeyfile();

    @Config("dataset")
    String getDataset();

    @Config("table")
    String getTable();
  }

  @Test
  public void testRun() throws IOException {
    ConfigSource testConfig = EmbulkTests.config("EMBULK_OUTPUT_BIGQUERY_TEST_CONFIG");
    TestTask testTask = CONFIG_MAPPER.map(testConfig, TestTask.class);
    ConfigSource outConfig = CONFIG_MAPPER_FACTORY.newConfigSource();
    outConfig.set("type", "bigquery_java");
    outConfig.set("mode", "replace");
    outConfig.set("json_keyfile", testTask.getJsonKeyfile());
    outConfig.set("dataset", testTask.getDataset());
    outConfig.set("table", testTask.getTable());
    outConfig.set("source_format", "NEWLINE_DELIMITED_JSON");

    File in = testFolder.newFile("embulk-output-bigquery_java-test.csv");
    Files.write(
        in.toPath(),
        Arrays.asList("c0:string,c1:boolean,index:double", "test0,true,0", "test1,false,1"));
    TestingEmbulk.RunResult runResult = embulk.runOutput(outConfig, in.toPath());

    BigqueryClient bigqueryClient =
        new BigqueryClient(
            CONFIG_MAPPER.map(outConfig, PluginTask.class), runResult.getOutputSchema());
    TableResult tableResult =
        bigqueryClient.runQuery(
            String.format("SELECT * from `%s`.`%s`", testTask.getDataset(), testTask.getTable()));
    List<FieldValueList> results = new ArrayList<>();
    tableResult.iterateAll().forEach(results::add);
    assertEquals(2, results.size());
    assertEquals("test0", results.get(0).get("c0").getStringValue());
    assertTrue(results.get(0).get("c1").getBooleanValue());
    assertEquals("test1", results.get(1).get("c0").getStringValue());
    assertFalse(results.get(1).get("c1").getBooleanValue());
  }

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
  public void testRunAppendDirectModeWithMockWebServer() throws Exception {
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
  public void testRunAppendModeWithMockWebServer() throws Exception {
    // GET dataset, POST temp table, load into temp table (POST job + GET status), copy temp to
    // final table (POST job + GET status), DELETE temp table, GET table (updateTableIfNeed).
    List<RecordedRequest> requests =
        runWithMockServer(
            c -> c.set("mode", "append"),
            datasetResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            deleteResponse(),
            tableResponse());

    assertEquals(8, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostJobs(requests.get(2));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(2), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(3), "testjob");

    assertPostJobs(requests.get(4));
    JSONObject copyConfig = jobConfig(requests.get(4), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_APPEND", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(5), "testjob");

    assertDeleteTable(requests.get(6), tempTableId);

    assertGetTable(requests.get(7), "table");
  }

  @Test
  public void testRunReplaceModeWithMockWebServer() throws Exception {
    List<RecordedRequest> requests =
        runWithMockServer(
            c -> c.set("mode", "replace"),
            datasetResponse(),
            tableResponse(),
            createLoadJobResponse("testjob"),
            waitForLoadJobResponse("testjob"),
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            deleteResponse(),
            tableResponse());

    assertEquals(8, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostJobs(requests.get(2));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(2), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(3), "testjob");

    assertPostJobs(requests.get(4));
    JSONObject copyConfig = jobConfig(requests.get(4), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(5), "testjob");

    assertDeleteTable(requests.get(6), tempTableId);

    assertGetTable(requests.get(7), "table");
  }

  @Test
  public void testRunReplaceModeWithMockWebServerRestoresRetainedDescriptionAndPolicyTags()
      throws Exception {
    // With retain_column_descriptions/retain_column_policy_tags on, isNeedUpdateTable() is true:
    // storeCachedSrcFieldsIfNeed() GETs the (pre-existing) destination table right after autoCreate
    // creates the temp table, and updateTableIfNeed() PATCHes the destination afterward to restore
    // the cached description/policy tag onto its post-replace schema.
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
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            deleteResponse(),
            tableResponseWithNullableC0(),
            tableResponse());

    assertEquals(10, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertGetTable(requests.get(2), "table"); // storeCachedSrcFieldsIfNeed()

    assertPostJobs(requests.get(3));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(3), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(4), "testjob");

    assertPostJobs(requests.get(5));
    JSONObject copyConfig = jobConfig(requests.get(5), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(6), "testjob");

    assertDeleteTable(requests.get(7), tempTableId);

    assertGetTable(requests.get(8), "table"); // updateTableIfNeed()

    RecordedRequest patchRequest = requests.get(9);
    assertPatchTable(patchRequest, "table");
    assertFieldDescriptionAndPolicyTag(
        firstSchemaField(requestBodyJson(patchRequest)), "old-description", "old-policy-tag");
  }

  @Test
  public void testRunReplaceModeWithMockWebServerGivesUpAfterMaxLoadRetries() throws Exception {
    // With retries=1, load()'s own RetryExecutor allows only 2 attempts (the initial attempt plus
    // one retry) before giving up; two consecutive internalError job failures exhaust that budget
    // and the whole plugin run fails, so autoCreate's temp table create is the only other request.
    // TODO: the temp table should always be deleted, even when the run fails here.
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
            jobResponse("load-job-2", "load", LOAD_CONFIG_BODY, "DONE", "internalError"));

    assertEquals(6, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));

    assertPostJobs(requests.get(2));

    assertGetJobStatus(requests.get(3), "load-job-1");

    assertPostJobs(requests.get(4));

    assertGetJobStatus(requests.get(5), "load-job-2");
  }

  @Test
  public void testRunReplaceModeWithMockWebServerSkipsSchemaUpdateWhenTempTableDeleteFails()
      throws Exception {
    // Ruby always updates the schema first and deletes the temp table afterward, so the temp
    // table is always cleaned up. Java deletes the temp table first and only then calls
    // updateTableIfNeed(), so when the delete fails, the run aborts before the schema update is
    // ever attempted, even though retain_column_descriptions is on here.
    // TODO: update the schema before deleting the temp table, like ruby does, so a delete
    // failure doesn't also block the schema update.
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
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            errorResponse(400, "boom", "invalid"));

    assertEquals(8, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");

    assertGetTable(requests.get(2), "table"); // storeCachedSrcFieldsIfNeed()

    assertPostJobs(requests.get(3));

    assertGetJobStatus(requests.get(4), "testjob");

    assertPostJobs(requests.get(5));

    assertGetJobStatus(requests.get(6), "testjob");

    assertDeleteTable(requests.get(7), tempTableId);
  }

  @Test
  public void testRunDeleteInAdvanceModeWithMockWebServer() throws Exception {
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
            createCopyJobResponse("testjob"),
            waitForCopyJobResponse("testjob"),
            deleteResponse(),
            tableResponse());

    assertEquals(9, requests.size());

    assertGetDataset(requests.get(0));

    assertDeleteTable(requests.get(1), "table");

    assertPostTables(requests.get(2));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(2)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostJobs(requests.get(3));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(3), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(4), "testjob");

    assertPostJobs(requests.get(5));
    JSONObject copyConfig = jobConfig(requests.get(5), "copy");
    assertEquals(tempTableId, firstSourceTableId(copyConfig));
    assertEquals("table", tableIdOf(copyConfig, "destinationTable"));
    assertEquals("WRITE_TRUNCATE", copyConfig.getString("writeDisposition"));

    assertGetJobStatus(requests.get(6), "testjob");

    assertDeleteTable(requests.get(7), tempTableId);

    assertGetTable(requests.get(8), "table");
  }

  @Test
  public void testRunMergeModeWithMockWebServer() throws Exception {
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
            createQueryJobResponse("testjob"),
            waitForQueryJobResponse("testjob"),
            deleteResponse(),
            tableResponse());

    assertEquals(9, requests.size());

    assertGetDataset(requests.get(0));

    assertPostTables(requests.get(1));
    String tempTableId = tableIdOf(requestBodyJson(requests.get(1)), "tableReference");
    assertMatches(tempTableId, "LOAD_TEMP_.*_table");

    assertPostTables(requests.get(2));
    assertEquals("table", tableIdOf(requestBodyJson(requests.get(2)), "tableReference"));

    assertPostJobs(requests.get(3));
    assertEquals(tempTableId, tableIdOf(jobConfig(requests.get(3), "load"), "destinationTable"));

    assertGetJobStatus(requests.get(4), "testjob");

    assertPostJobs(requests.get(5));
    String mergeQuery = jobConfig(requests.get(5), "query").getString("query");
    assertTrue(
        mergeQuery.matches(
            "(?s).*MERGE.*`table`.*" + java.util.regex.Pattern.quote(tempTableId) + ".*"));

    assertGetJobStatus(requests.get(6), "testjob");

    assertDeleteTable(requests.get(7), tempTableId);

    assertGetTable(requests.get(8), "table");
  }
}
