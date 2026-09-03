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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.PolicyTags;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
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
import org.mockito.Mockito;

public class TestBigqueryClient {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private static final String BASIC_RESOURCE_PATH =
      "/java/org/embulk/output/bigquery_java/bigquery_client/";

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

  private static StandardSQLTypeName toBQType(BigqueryColumnOption column) {
    Map<String, StandardSQLTypeName> bqTypeMap = new HashMap<>();
    bqTypeMap.put("INTEGER", StandardSQLTypeName.INT64);
    bqTypeMap.put("STRING", StandardSQLTypeName.STRING);
    return bqTypeMap.getOrDefault(column.getType().orElse("STRING"), StandardSQLTypeName.STRING);
  }

  @Test
  public void TestIsNeedUpdateTable() {
    ConfigSource baseConfig = loadYamlResource(embulk, "takeover.yml");

    // Helper function to create config and test isNeedUpdateTable
    Function<String, Function<Boolean, Function<Boolean, Boolean>>> testIsNeedUpdate =
        mode ->
            retainPolicyTags ->
                retainDescriptions ->
                    BigqueryClient.isNeedUpdateTable(
                        CONFIG_MAPPER.map(
                            baseConfig
                                .set("mode", mode)
                                .set("retain_column_policy_tags", retainPolicyTags)
                                .set("retain_column_descriptions", retainDescriptions),
                            PluginTask.class));

    // Test cases
    assertTrue(testIsNeedUpdate.apply("replace").apply(false).apply(true));
    assertTrue(testIsNeedUpdate.apply("replace").apply(true).apply(false));
    assertFalse(testIsNeedUpdate.apply("replace").apply(false).apply(false));
    assertFalse(testIsNeedUpdate.apply("insert").apply(true).apply(true));
  }

  private Schema invokeTakeoverBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      Function<Field.Builder, Field> setupField0,
      Function<Field.Builder, Field> setupField1) {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");
    PluginTask baseTask = CONFIG_MAPPER.map(config, PluginTask.class);
    PluginTask task = CONFIG_MAPPER.map(setupConfig.apply(config), PluginTask.class);

    List<Field> currentFields =
        baseTask.getColumnOptions().orElse(java.util.Collections.emptyList()).stream()
            .map(column -> Field.newBuilder(column.getName(), toBQType(column)).build())
            .collect(Collectors.toList());

    HashMap<String, Function<Field.Builder, Field>> setups = new HashMap<>();
    setups.put("c0", setupField0);
    setups.put("c1", setupField1);

    List<Field> fieldList =
        baseTask.getColumnOptions().orElse(java.util.Collections.emptyList()).stream()
            .map(c -> setups.get(c.getName()).apply(Field.newBuilder(c.getName(), toBQType(c))))
            .collect(Collectors.toList());

    return BigqueryClient.buildPatchSchema(
        task, FieldList.of(currentFields), FieldList.of(fieldList));
  }

  private Schema invokeRetainDescriptionBuildSchema(
      String mode, Boolean retainColumnDescriptions, String d0, String d1) {
    return invokeRetainDescriptionBuildSchema(
        configSource -> configSource, mode, retainColumnDescriptions, d0, d1);
  }

  private Schema invokeRetainDescriptionBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      String mode,
      Boolean retainColumnDescriptions,
      String d0,
      String d1) {
    return invokeTakeoverBuildSchema(
        configSource ->
            setupConfig.apply(
                configSource
                    .set("mode", mode)
                    .set("retain_column_policy_tags", true)
                    .set("retain_column_descriptions", retainColumnDescriptions)),
        builder -> builder.setDescription(d0).build(),
        builder -> builder.setDescription(d1).build());
  }

  @Test
  public void testRetainDescriptionTrue() {
    Schema schema = invokeRetainDescriptionBuildSchema("replace", true, "prev_c0", "prev_c1");
    assertEquals("d0", schema.getFields().get(0).getDescription());
    assertEquals("prev_c1", schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionFalse() {
    Schema schema = invokeRetainDescriptionBuildSchema("replace", false, "prev_c0", "prev_c1");
    assertEquals("d0", schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionTrueWithColumnOptionNull() {
    Schema schema =
        invokeRetainDescriptionBuildSchema(
            c -> c.set("column_options", null), "replace", true, "prev_c0", "prev_c1");
    assertEquals("prev_c0", schema.getFields().get(0).getDescription());
    assertEquals("prev_c1", schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionFalseWithColumnOptionNull() {
    Schema schema =
        invokeRetainDescriptionBuildSchema(
            c -> c.set("column_options", null), "replace", false, "prev_c0", "prev_c1");
    assertNull(schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionTrueButNotModeReplace() {
    // Ruby applies `column_options[].description` (here, c0's "d0" from takeover.yml) on every
    // run regardless of mode. Java only reflects it when isNeedUpdateTable() is true (mode:
    // replace with a retain flag on), so for any other mode buildPatchSchema() returns null and
    // the configured description is never applied, even though it's set here.
    // TODO: apply `column_options[].description` regardless of mode, like ruby does.
    Schema schema = invokeRetainDescriptionBuildSchema("insert", true, "prev_c0", "prev_c1");
    assertNull(schema);
  }

  private Schema invokeRetainPolicyTagsBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      String mode,
      Boolean retainPolicyTags,
      String[] tags0,
      String[] tags1) {
    List<String> n0 = Arrays.stream(tags0).collect(Collectors.toList());
    List<String> n1 = Arrays.stream(tags1).collect(Collectors.toList());
    PolicyTags p0 = PolicyTags.newBuilder().setNames(n0).build();
    PolicyTags p1 = PolicyTags.newBuilder().setNames(n1).build();
    return invokeTakeoverBuildSchema(
        configSource ->
            setupConfig.apply(
                configSource
                    .set("mode", mode)
                    .set("retain_column_policy_tags", retainPolicyTags)
                    .set("retain_column_descriptions", true)),
        builder -> builder.setPolicyTags(p0).build(),
        builder -> builder.setPolicyTags(p1).build());
  }

  private Schema invokeRetainPolicyTagsBuildSchema(
      String mode, Boolean retainPolicyTags, String[] tags0, String[] tags1) {
    return invokeRetainPolicyTagsBuildSchema(c -> c, mode, retainPolicyTags, tags0, tags1);
  }

  @Test
  public void testRetainColumnPolicyTagsTrue() {
    Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "replace", true, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertArrayEquals(
        new String[] {"c0"},
        schema.getFields().get(0).getPolicyTags().getNames().toArray(new String[0]));
    assertArrayEquals(
        new String[] {"c10", "c11"},
        schema.getFields().get(1).getPolicyTags().getNames().toArray(new String[0]));
  }

  @Test
  public void testRetainColumnPolicyTagsFalse() {
    Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "replace", false, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertNull(schema.getFields().get(0).getPolicyTags());
    assertNull(schema.getFields().get(1).getPolicyTags());
  }

  @Test
  public void testRetainColumnPolicyTagsTrueButNotModeReplace() {
    Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "insert", true, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertNull(schema);
  }

  @Test
  public void testStoreCachedSrcFieldsIfNeed() throws NoSuchFieldException, IllegalAccessException {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");

    // Test case 1: Mode is "replace" with retainColumnDescriptions = true
    PluginTask replaceTask =
        CONFIG_MAPPER.map(
            config
                .set("mode", "replace")
                .set("retain_column_descriptions", true)
                .set("retain_column_policy_tags", false),
            PluginTask.class);

    BigqueryClient client = Mockito.mock(BigqueryClient.class);
    Table mockTable = Mockito.mock(Table.class);
    TableDefinition mockTableDef = Mockito.mock(TableDefinition.class);

    Schema mockSchema = Schema.of(Field.newBuilder("field1", StandardSQLTypeName.STRING).build());

    Mockito.when(client.getTable(Mockito.anyString())).thenReturn(mockTable);
    Mockito.when(mockTable.getDefinition()).thenReturn(mockTableDef);
    Mockito.when(mockTableDef.getSchema()).thenReturn(mockSchema);
    Mockito.when(client.storeCachedSrcFieldsIfNeed()).thenCallRealMethod();

    // Set task field
    java.lang.reflect.Field taskField = BigqueryClient.class.getDeclaredField("task");
    taskField.setAccessible(true);
    taskField.set(client, replaceTask);

    assertEquals(client.storeCachedSrcFieldsIfNeed(), mockSchema.getFields());

    // Test case 2: Mode is "insert" - should return null
    PluginTask insertTask =
        CONFIG_MAPPER.map(
            config
                .set("mode", "insert")
                .set("retain_column_descriptions", true)
                .set("retain_column_policy_tags", true),
            PluginTask.class);

    taskField.set(client, insertTask);

    assertNull(client.storeCachedSrcFieldsIfNeed());

    // Test case 3: Table doesn't exist - should return null
    Mockito.when(client.getTable(Mockito.anyString())).thenReturn(null);

    taskField.set(client, replaceTask);

    assertNull(client.storeCachedSrcFieldsIfNeed());

    // Test case 4: Schema is null - should return null
    Table nullSchemaTable = Mockito.mock(Table.class);
    TableDefinition nullSchemaTableDef = Mockito.mock(TableDefinition.class);

    Mockito.when(client.getTable(Mockito.anyString())).thenReturn(nullSchemaTable);
    Mockito.when(nullSchemaTable.getDefinition()).thenReturn(nullSchemaTableDef);
    Mockito.when(nullSchemaTableDef.getSchema()).thenReturn(null);

    taskField.set(client, replaceTask);

    assertNull(client.storeCachedSrcFieldsIfNeed());
  }

  // --- MockWebServer-based tests below: exercise BigqueryClient's HTTP-facing behavior directly,
  // without going through the plugin's transaction/output flow. ---

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
    // This retry count currently comes from the client library's own default (maxAttempts=6 via
    // ServiceOptions.getDefaultRetrySettings()); it is not yet driven by our task's `retries`
    // config. If createTableIfNotExist() is ever changed to honor `retries`, this test should be
    // updated to reflect that config value instead of the library default.
    // TODO: reflect task's `retries` config in createTableIfNotExist()'s retry behavior.
    List<MockResponse> responses = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      responses.add(errorResponse(500, "boom", "internalError"));
    }
    responses.add(tableResponse());

    List<RecordedRequest> requests =
        runWithMockServer(
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            responses.toArray(new MockResponse[0]));

    assertEquals(6, requests.size());
    for (RecordedRequest request : requests) {
      assertPostTables(request);
    }
  }

  @Test
  public void testCreateTableIfNotExistGivesUpAfterMaxTransientErrors() throws Exception {
    // As above, this retry count currently comes from the client library's own default
    // (maxAttempts=6), not our task's `retries` config; update this test if that ever changes.
    // TODO: reflect task's `retries` config in createTableIfNotExist()'s retry behavior.
    List<MockResponse> responses = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      responses.add(errorResponse(500, "boom", "internalError"));
    }

    List<RecordedRequest> requests =
        runWithMockServerExpectingFailure(
            BigqueryException.class,
            "(?s).*failed to create table.*",
            server -> buildTestHostClient(server).createTableIfNotExist("table"),
            responses.toArray(new MockResponse[0]));

    assertEquals(6, requests.size());
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
