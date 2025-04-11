package org.embulk.output.bigquery_java;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.PolicyTags;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

  private com.google.cloud.bigquery.Schema invokeTakeoverBuildSchema(
      Function<ConfigSource, ConfigSource> setupConfig,
      Function<com.google.cloud.bigquery.Field.Builder, com.google.cloud.bigquery.Field>
          setupField0,
      Function<com.google.cloud.bigquery.Field.Builder, com.google.cloud.bigquery.Field>
          setupField1)
      throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException,
          InvocationTargetException {
    ConfigSource testConfig = EmbulkTests.config("EMBULK_OUTPUT_BIGQUERY_TEST_CONFIG");
    TestBigqueryJavaOutputPlugin.TestTask testTask =
        CONFIG_MAPPER.map(testConfig, TestBigqueryJavaOutputPlugin.TestTask.class);
    ConfigSource config = loadYamlResource(embulk, "takeover.yml");
    config.set("json_keyfile", testTask.getJsonKeyfile());
    config.set("dataset", testTask.getDataset());
    config.set("table", testTask.getTable());
    PluginTask task = CONFIG_MAPPER.map(setupConfig.apply(config), PluginTask.class);
    Schema schema = Schema.builder().add("c0", Types.LONG).add("c1", Types.STRING).build();
    BigqueryClient bigqueryClient = new BigqueryClient(task, schema);
    Field field = BigqueryClient.class.getDeclaredField("cachedSrcFields");
    field.setAccessible(true);
    List<com.google.cloud.bigquery.Field> fieldList = new ArrayList<>();
    fieldList.add(
        setupField0.apply(
            com.google.cloud.bigquery.Field.newBuilder("c0", StandardSQLTypeName.INT64)));
    fieldList.add(
        setupField1.apply(
            com.google.cloud.bigquery.Field.newBuilder("c1", StandardSQLTypeName.STRING)));
    field.set(bigqueryClient, FieldList.of(fieldList));
    Method method = BigqueryClient.class.getDeclaredMethod("buildSchema", Schema.class, List.class);
    method.setAccessible(true);
    return (com.google.cloud.bigquery.Schema)
        method.invoke(bigqueryClient, schema, task.getColumnOptions().orElse(null));
  }

  private com.google.cloud.bigquery.Schema invokeRetainDescriptionBuildSchema(
      String mode, Boolean retainColumnDescriptions, String d0, String d1)
      throws IOException, NoSuchFieldException, InvocationTargetException, IllegalAccessException,
          NoSuchMethodException {
    return invokeTakeoverBuildSchema(
        configSource ->
            configSource
                .set("mode", mode)
                .set("retain_column_descriptions", retainColumnDescriptions),
        builder -> builder.setDescription(d0).build(),
        builder -> builder.setDescription(d1).build());
  }

  @Test
  public void testRetainDescriptionTrue()
      throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException,
          InvocationTargetException, IOException {
    com.google.cloud.bigquery.Schema schema =
        invokeRetainDescriptionBuildSchema("replace", true, "prev_c0", "prev_c1");
    assertEquals("c0", schema.getFields().get(0).getDescription());
    assertEquals("prev_c1", schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionFalse()
      throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException,
          InvocationTargetException, IOException {
    com.google.cloud.bigquery.Schema schema =
        invokeRetainDescriptionBuildSchema("replace", false, "prev_c0", "prev_c1");
    assertEquals("c0", schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  @Test
  public void testRetainDescriptionTrueButNotModeReplace()
      throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException,
          InvocationTargetException, IOException {
    com.google.cloud.bigquery.Schema schema =
        invokeRetainDescriptionBuildSchema("insert", true, "prev_c0", "prev_c1");
    assertEquals("c0", schema.getFields().get(0).getDescription());
    assertNull(schema.getFields().get(1).getDescription());
  }

  private com.google.cloud.bigquery.Schema invokeRetainPolicyTagsBuildSchema(
      String mode, Boolean retainPolicyTags, String[] tags0, String[] tags1)
      throws IOException, NoSuchFieldException, InvocationTargetException, IllegalAccessException,
          NoSuchMethodException {
    List<String> n0 = Arrays.stream(tags0).collect(Collectors.toList());
    List<String> n1 = Arrays.stream(tags1).collect(Collectors.toList());
    PolicyTags p0 = PolicyTags.newBuilder().setNames(n0).build();
    PolicyTags p1 = PolicyTags.newBuilder().setNames(n1).build();
    return invokeTakeoverBuildSchema(
        configSource ->
            configSource.set("mode", mode).set("retain_column_policy_tags", retainPolicyTags),
        builder -> builder.setPolicyTags(p0).build(),
        builder -> builder.setPolicyTags(p1).build());
  }

  @Test
  public void testRetainColumnPolicyTagsTrue()
      throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException,
          InvocationTargetException, IOException {
    com.google.cloud.bigquery.Schema schema =
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
  public void testRetainColumnPolicyTagsFalse()
      throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException,
          InvocationTargetException, IOException {
    com.google.cloud.bigquery.Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "replace", false, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertNull(schema.getFields().get(0).getPolicyTags());
    assertNull(schema.getFields().get(1).getPolicyTags());
  }

  @Test
  public void testRetainColumnPolicyTagsTrueButNotModeReplace()
      throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException,
          InvocationTargetException, IOException {
    com.google.cloud.bigquery.Schema schema =
        invokeRetainPolicyTagsBuildSchema(
            "insert", true, new String[] {"c0"}, new String[] {"c10", "c11"});
    assertNull(schema.getFields().get(0).getPolicyTags());
    assertNull(schema.getFields().get(1).getPolicyTags());
  }
}
