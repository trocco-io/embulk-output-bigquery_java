package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.PolicyTags;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.http.HttpTransportOptions;
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
import org.embulk.output.bigquery_java.util.PluginTaskUtil;
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
  public void testIsNeedUpdateTableReplaceModeAllCombinations() {
    Function<String, Function<Boolean, Function<Boolean, Boolean>>> isNeedUpdate =
        columnOptionDescription ->
            retainPolicyTags ->
                retainDescriptions ->
                    BigqueryClient.isNeedUpdateTable(
                        PluginTaskUtil.buildTaskWithIntegerColumnOptionForReplaceMode(
                            embulk, columnOptionDescription, retainPolicyTags, retainDescriptions));

    assertFalse(isNeedUpdate.apply(null).apply(false).apply(false));
    assertTrue(isNeedUpdate.apply(null).apply(false).apply(true));
    assertTrue(isNeedUpdate.apply(null).apply(true).apply(false));
    assertTrue(isNeedUpdate.apply(null).apply(true).apply(true));
    assertTrue(isNeedUpdate.apply("d0").apply(false).apply(false));
    assertTrue(isNeedUpdate.apply("d0").apply(false).apply(true));
    assertTrue(isNeedUpdate.apply("d0").apply(true).apply(false));
    assertTrue(isNeedUpdate.apply("d0").apply(true).apply(true));
  }

  @Test
  public void testIsNeedUpdateTableNonReplaceModesDescriptionOnly() {
    Function<String, Function<String, Boolean>> isNeedUpdate =
        mode ->
            columnOptionDescription ->
                BigqueryClient.isNeedUpdateTable(
                    PluginTaskUtil.buildTaskWithIntegerColumnOptionForMode(
                        embulk, mode, columnOptionDescription));

    assertFalse(isNeedUpdate.apply("delete_in_advance").apply(null));
    assertFalse(isNeedUpdate.apply("append").apply(null));
    assertFalse(isNeedUpdate.apply("merge").apply(null));
    assertFalse(isNeedUpdate.apply("replace_backup").apply(null));
    assertFalse(isNeedUpdate.apply("append_direct").apply(null));

    assertTrue(isNeedUpdate.apply("delete_in_advance").apply("d0"));
    assertTrue(isNeedUpdate.apply("append").apply("d0"));
    assertTrue(isNeedUpdate.apply("merge").apply("d0"));
    assertTrue(isNeedUpdate.apply("replace_backup").apply("d0"));
    assertTrue(isNeedUpdate.apply("append_direct").apply("d0"));
  }

  @Test
  public void testColumnOptionPresent() {
    Schema schema = invokeColumnOptionBuildSchema("d0");

    assertEquals("d0", schema.getFields().get(0).getDescription());
  }

  @Test
  public void testColumnOptionAbsent() {
    Schema schema = invokeColumnOptionBuildSchema(null);

    assertNull(schema);
  }

  private Schema invokeColumnOptionBuildSchema(String columnOptionDescription) {
    PluginTask task =
        PluginTaskUtil.buildTaskWithIntegerColumnOptionForReplaceMode(
            embulk, columnOptionDescription, false, false);
    Field field = Field.newBuilder("c0", StandardSQLTypeName.INT64).build();

    return BigqueryClient.buildPatchSchema(task, FieldList.of(field), FieldList.of(field));
  }

  @Test
  public void testNestedRecordColumnOptionPresent() {
    Map<String, String> nestedColumnOptionDescription = new HashMap<>();
    nestedColumnOptionDescription.put("c000", "d000");
    nestedColumnOptionDescription.put("c00", "d00");
    nestedColumnOptionDescription.put("c01", "d01");
    Schema schema = invokeNestedRecordBuildSchema(nestedColumnOptionDescription);

    Field c0 = schema.getFields().get(0);
    Field c00 = c0.getSubFields().get(0);
    Field c000 = c00.getSubFields().get(0);
    Field c01 = c0.getSubFields().get(1);
    Field c1 = schema.getFields().get(1);

    assertNull(c0.getDescription());
    assertEquals("d00", c00.getDescription());
    assertEquals("d000", c000.getDescription());
    assertEquals("d01", c01.getDescription());
    assertNull(c1.getDescription());
  }

  @Test
  public void testNestedRecordRetainPolicyTagsTrue() {
    PolicyTags c000Tags =
        PolicyTags.newBuilder().setNames(Collections.singletonList("p000")).build();
    PolicyTags c01Tags = PolicyTags.newBuilder().setNames(Collections.singletonList("p01")).build();

    Field c000Cur = Field.newBuilder("c000", StandardSQLTypeName.STRING).build();
    Field c00Cur = Field.newBuilder("c00", StandardSQLTypeName.STRUCT, c000Cur).build();
    Field c01Cur = Field.newBuilder("c01", StandardSQLTypeName.STRING).build();
    Field c0Cur = Field.newBuilder("c0", StandardSQLTypeName.STRUCT, c00Cur, c01Cur).build();
    Field c1Cur = Field.newBuilder("c1", StandardSQLTypeName.STRING).build();

    Field c000Dst =
        Field.newBuilder("c000", StandardSQLTypeName.STRING).setPolicyTags(c000Tags).build();
    Field c00Dst = Field.newBuilder("c00", StandardSQLTypeName.STRUCT, c000Dst).build();
    Field c01Dst =
        Field.newBuilder("c01", StandardSQLTypeName.STRING).setPolicyTags(c01Tags).build();
    Field c0Dst = Field.newBuilder("c0", StandardSQLTypeName.STRUCT, c00Dst, c01Dst).build();
    Field c1Dst = Field.newBuilder("c1", StandardSQLTypeName.STRING).build();

    PluginTask task =
        PluginTaskUtil.buildTaskRecordNestedColumnOption(
            embulk, "replace", Collections.emptyMap(), true, false);

    Schema schema =
        BigqueryClient.buildPatchSchema(
            task, FieldList.of(c0Cur, c1Cur), FieldList.of(c0Dst, c1Dst));

    Field c0 = schema.getFields().get(0);
    Field c00 = c0.getSubFields().get(0);
    Field c000 = c00.getSubFields().get(0);
    Field c01 = c0.getSubFields().get(1);
    Field c1 = schema.getFields().get(1);

    assertNull(c0.getPolicyTags());
    assertNull(c00.getPolicyTags());
    assertEquals(c000Tags, c000.getPolicyTags());
    assertEquals(c01Tags, c01.getPolicyTags());
    assertNull(c1.getPolicyTags());
  }

  @Test
  public void testNestedRecordRetainDescriptionTrue() {
    Field c000Cur = Field.newBuilder("c000", StandardSQLTypeName.STRING).build();
    Field c00Cur = Field.newBuilder("c00", StandardSQLTypeName.STRUCT, c000Cur).build();
    Field c01Cur = Field.newBuilder("c01", StandardSQLTypeName.STRING).build();
    Field c0Cur = Field.newBuilder("c0", StandardSQLTypeName.STRUCT, c00Cur, c01Cur).build();
    Field c1Cur = Field.newBuilder("c1", StandardSQLTypeName.STRING).build();

    Field c000Dst =
        Field.newBuilder("c000", StandardSQLTypeName.STRING).setDescription("prev_c000").build();
    Field c00Dst = Field.newBuilder("c00", StandardSQLTypeName.STRUCT, c000Dst).build();
    Field c01Dst =
        Field.newBuilder("c01", StandardSQLTypeName.STRING).setDescription("prev_c01").build();
    Field c0Dst = Field.newBuilder("c0", StandardSQLTypeName.STRUCT, c00Dst, c01Dst).build();
    Field c1Dst = Field.newBuilder("c1", StandardSQLTypeName.STRING).build();

    PluginTask task =
        PluginTaskUtil.buildTaskRecordNestedColumnOption(
            embulk, "replace", Collections.emptyMap(), false, true);

    Schema schema =
        BigqueryClient.buildPatchSchema(
            task, FieldList.of(c0Cur, c1Cur), FieldList.of(c0Dst, c1Dst));

    Field c0 = schema.getFields().get(0);
    Field c00 = c0.getSubFields().get(0);
    Field c000 = c00.getSubFields().get(0);
    Field c01 = c0.getSubFields().get(1);
    Field c1 = schema.getFields().get(1);

    assertNull(c0.getDescription());
    assertNull(c00.getDescription());
    assertEquals("prev_c000", c000.getDescription());
    assertEquals("prev_c01", c01.getDescription());
    assertNull(c1.getDescription());
  }

  @Test
  public void testNestedRecordNoUpdateNeeded() {
    Schema schema = invokeNestedRecordBuildSchema(Collections.emptyMap());

    assertNull(schema);
  }

  private Schema invokeNestedRecordBuildSchema(Map<String, String> nestedColumnOptionDescription) {
    PluginTask task =
        PluginTaskUtil.buildTaskRecordNestedColumnOption(
            embulk, "replace", nestedColumnOptionDescription, false, false);
    Field c000Field = Field.newBuilder("c000", StandardSQLTypeName.STRING).build();
    Field c00Field = Field.newBuilder("c00", StandardSQLTypeName.STRUCT, c000Field).build();
    Field c01Field = Field.newBuilder("c01", StandardSQLTypeName.STRING).build();
    Field c0Field = Field.newBuilder("c0", StandardSQLTypeName.STRUCT, c00Field, c01Field).build();
    Field c1Field = Field.newBuilder("c1", StandardSQLTypeName.STRING).build();

    FieldList fields = FieldList.of(c0Field, c1Field);
    return BigqueryClient.buildPatchSchema(task, fields, fields);
  }

  private Schema invokeTakeoverBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      Function<Field.Builder, Field> setupField0,
      Function<Field.Builder, Field> setupField1) {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");
    PluginTask baseTask = CONFIG_MAPPER.map(config, PluginTask.class);
    PluginTask task = CONFIG_MAPPER.map(setupConfig.apply(config), PluginTask.class);

    List<Field> currentFields =
        baseTask.getColumnOptions().orElse(Collections.emptyList()).stream()
            .map(column -> Field.newBuilder(column.getName(), toBQType(column)).build())
            .collect(Collectors.toList());

    HashMap<String, Function<Field.Builder, Field>> setups = new HashMap<>();
    setups.put("c0", setupField0);
    setups.put("c1", setupField1);

    List<Field> fieldList =
        baseTask.getColumnOptions().orElse(Collections.emptyList()).stream()
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
            c -> c.set("column_options", null), // clear takeover.yml's c0 description
            "replace",
            true,
            "prev_c0",
            "prev_c1");
    assertEquals("prev_c0", schema.getFields().get(0).getDescription());
    assertEquals("prev_c1", schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionFalseWithColumnOptionNull() {
    Schema schema =
        invokeRetainDescriptionBuildSchema(
            c -> c.set("column_options", null), // clear takeover.yml's c0 description
            "replace",
            false,
            "prev_c0",
            "prev_c1");
    assertNull(schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionTrueButNotModeReplace() {
    Schema schema = invokeRetainDescriptionBuildSchema("insert", true, "prev_c0", "prev_c1");
    assertEquals("d0", schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
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
    assertEquals("d0", schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(0).getPolicyTags());
    assertNull(schema.getFields().get(1).getPolicyTags());
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

    java.lang.reflect.Field taskField = BigqueryClient.class.getDeclaredField("task");
    taskField.setAccessible(true);
    taskField.set(client, replaceTask);

    assertEquals(client.storeCachedSrcFieldsIfNeed(), mockSchema.getFields());

    // Test case 2: Mode is "insert" with no column_options[].description - should return null
    PluginTask insertTask =
        CONFIG_MAPPER.map(
            config
                .set("mode", "insert")
                .set("column_options", null) // clear takeover.yml's c0 description
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

  @Test
  public void testBuildRetrySettingsReflectsConfiguredRetries() {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");
    PluginTask task = CONFIG_MAPPER.map(config.set("retries", 1), PluginTask.class);

    RetrySettings retrySettings = BigqueryClient.buildRetrySettings(task);

    // +1: task.getRetries() means "number of retries", so 1 retry means 2 total attempts.
    assertEquals(2, retrySettings.getMaxAttempts());
  }

  @Test
  public void testBuildTransportOptionsReflectsConfiguredTimeouts() {
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");
    PluginTask task =
        CONFIG_MAPPER.map(
            config
                .set("open_timeout_sec", 12)
                .set("read_timeout_sec", 34)
                .set("send_timeout_sec", 56),
            PluginTask.class);

    TransportOptions transportOptions = BigqueryClient.buildTransportOptions(task);

    assertTrue(transportOptions instanceof HttpTransportOptions);
    HttpTransportOptions httpTransportOptions = (HttpTransportOptions) transportOptions;
    assertEquals(12 * 1000, httpTransportOptions.getConnectTimeout());
    // See README.md for why send_timeout_sec folds into read_timeout_sec here.
    assertEquals((34 + 56) * 1000, httpTransportOptions.getReadTimeout());
  }
}
