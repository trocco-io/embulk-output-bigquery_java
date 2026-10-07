package org.embulk.output.bigquery_java.config;

import static org.embulk.output.bigquery_java.util.AssertUtil.assertMatches;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;

public class TestBigqueryTaskBuilder {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private ConfigSource config;
  private static final String BASIC_RESOURCE_PATH = "/java/org/embulk/output/bigquery_java/";

  private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName) {
    return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .build();

  @Test
  public void expandStrftime_expandsDateTimeAndEpochSeconds() {
    Instant instant = Instant.parse("2026-09-17T14:30:15Z");
    assertEquals(
        "table_202609171430" + instant.getEpochSecond(),
        BigqueryTaskBuilder.expandStrftime("table_%Y%m%d%H%M%s", ZoneId.of("UTC"), instant));
  }

  @Test
  public void build_expandsTableAndOldTableWithSameTimestamp() {
    config =
        loadYamlResource(embulk, "base.yml")
            .set("table", "table_%Y%m%d%H%M%S%N")
            .set("old_table", "old_table_%Y%m%d%H%M%S%N");
    PluginTask task = BigqueryTaskBuilder.build(CONFIG_MAPPER.map(config, PluginTask.class));

    assertMatches(task.getTable(), "table_\\d{23}");
    assertMatches(task.getOldTable().get(), "old_table_\\d{23}");
    String tableSuffix = task.getTable().substring("table_".length());
    String oldTableSuffix = task.getOldTable().get().substring("old_table_".length());
    assertEquals(tableSuffix, oldTableSuffix);
  }

  @Test
  public void setAbortOnError_DefaultMaxBadRecord_True() {
    config =
        embulk
            .configLoader()
            .fromYamlString(
                String.join(
                    "\n",
                    "type: bigquery_java",
                    "mode: replace",
                    "auth_method: service_account",
                    "json_keyfile: { content: \"\" }",
                    "dataset: dataset",
                    "table: table",
                    "source_format: NEWLINE_DELIMITED_JSON",
                    "compression: GZIP",
                    "auto_create_dataset: false",
                    "auto_create_table: true",
                    "path_prefix: /tmp/bq_compress/bq_",
                    ""));
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryTaskBuilder.setAbortOnError(task);

    assertEquals(0, task.getMaxBadRecords());
    assertEquals(true, task.getAbortOnError().get());
  }

  // TODO jsonl without compression, csv with/out compression
  @Test
  public void setFileExt_JSONL_GZIP_JSONL_GZ() {
    config =
        embulk
            .configLoader()
            .fromYamlString(
                String.join(
                    "\n",
                    "type: bigquery_java",
                    "mode: replace",
                    "auth_method: service_account",
                    "json_keyfile: { content: \"\" }",
                    "dataset: dataset",
                    "table: table",
                    "source_format: NEWLINE_DELIMITED_JSON",
                    "compression: GZIP",
                    "auto_create_dataset: false",
                    "auto_create_table: true",
                    "path_prefix: /tmp/bq_compress/bq_",
                    ""));
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryTaskBuilder.setFileExt(task);
    assertEquals("NEWLINE_DELIMITED_JSON", task.getSourceFormat());
    assertEquals("GZIP", task.getCompression());
    assertEquals(".jsonl.gz", task.getFileExt().get());
  }

  @Test
  public void clustering() {
    config =
        embulk
            .configLoader()
            .fromYamlString(
                String.join(
                    "\n",
                    "type: bigquery_java",
                    "mode: replace",
                    "auth_method: service_account",
                    "json_keyfile: { content: \"\" }",
                    "dataset: dataset",
                    "table: table",
                    "source_format: NEWLINE_DELIMITED_JSON",
                    "compression: GZIP",
                    "auto_create_dataset: false",
                    "auto_create_table: true",
                    "path_prefix: /tmp/bq_compress/bq_",
                    "clustering:",
                    "  fields:",
                    "    - foo",
                    "    - bar",
                    "    - baz"));
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryTaskBuilder.setAbortOnError(task);
    System.out.println(task.getClustering().get().getFields());
    List<String> expectedOutput = Arrays.asList("foo", "bar", "baz");

    assertEquals(expectedOutput, task.getClustering().get().getFields().get());
  }
}
