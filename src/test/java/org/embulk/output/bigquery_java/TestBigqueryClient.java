package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.PolicyTags;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryUploadException;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;

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

  // The upload path can't be exercised through MockWebServer: TableDataWriteChannel's resumable
  // upload hardcodes the real BigQuery host (see BigqueryTestHostSupport). Instead, BigqueryClient
  // is a Mockito mock whose load() runs for real, with its collaborators injected by reflection
  // and its network steps (writeToStream / waitForLoad) left to the mock.
  private static class LoadFixture {
    final BigQuery bigquery = Mockito.mock(BigQuery.class);
    final TableDataWriteChannel writer = Mockito.mock(TableDataWriteChannel.class);
    final Logger logger = Mockito.mock(Logger.class);
    final BigqueryClient client = Mockito.mock(BigqueryClient.class);

    LoadFixture(PluginTask task) throws ReflectiveOperationException {
      Mockito.when(
              bigquery.writer(
                  Mockito.any(JobId.class), Mockito.any(WriteChannelConfiguration.class)))
          .thenReturn(writer);
      Mockito.when(
              client.load(
                  Mockito.any(Path.class),
                  Mockito.anyString(),
                  Mockito.any(JobInfo.WriteDisposition.class)))
          .thenCallRealMethod();
      setField("task", task);
      setField("bigquery", bigquery);
      setField("schema", new org.embulk.spi.Schema(Collections.emptyList()));
      setField("columnOptions", Collections.emptyList());
      setField("destinationProject", "project");
      setField("destinationDataset", "dataset");
      setField("logger", logger);
    }

    private void setField(String name, Object value) throws ReflectiveOperationException {
      java.lang.reflect.Field field = BigqueryClient.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(client, value);
    }

    void verifyUploadAttempts(int times) throws IOException {
      Mockito.verify(bigquery, Mockito.times(times))
          .writer(Mockito.any(JobId.class), Mockito.any(WriteChannelConfiguration.class));
      Mockito.verify(client, Mockito.times(times))
          .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    }

    void verifyUploadErrorsLogged(int times) {
      Mockito.verify(logger, Mockito.times(times))
          .error(Mockito.contains("failed to upload"), Mockito.any(IOException.class));
    }
  }

  private LoadFixture loadFixture(int retries) throws ReflectiveOperationException {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml").set("retries", retries);
    return new LoadFixture(CONFIG_MAPPER.map(config, PluginTask.class));
  }

  @Test
  public void testLoadRetriesUploadIOExceptionThenGivesUp()
      throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doCallRealMethod()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path notAFile = testFolder.newFolder().toPath();

    BigqueryUploadException thrown =
        assertThrows(
            BigqueryUploadException.class,
            () -> f.client.load(notAFile, "table", JobInfo.WriteDisposition.WRITE_APPEND));

    assertTrue(thrown.getMessage().contains("failed to upload"));
    assertTrue(thrown.getCause() instanceof IOException);
    f.verifyUploadAttempts(2);
    f.verifyUploadErrorsLogged(2);
    Mockito.verify(f.logger).error("embulk-output-bigquery: Give up retrying for Load job");
    Mockito.verify(f.writer, Mockito.never()).getJob();
  }

  @Test
  public void testLoadRetriesUploadIOExceptionThenSucceeds()
      throws IOException, ReflectiveOperationException {
    LoadFixture f = loadFixture(1);
    Mockito.doThrow(new IOException("Connection reset"))
        .doNothing()
        .when(f.client)
        .writeToStream(Mockito.any(Path.class), Mockito.any(OutputStream.class));
    Path loadFile = testFolder.newFile().toPath();

    f.client.load(loadFile, "table", JobInfo.WriteDisposition.WRITE_APPEND);

    f.verifyUploadAttempts(2);
    f.verifyUploadErrorsLogged(1);
    Mockito.verify(f.logger, Mockito.never())
        .error("embulk-output-bigquery: Give up retrying for Load job");
    Mockito.verify(f.writer).getJob();
  }
}
