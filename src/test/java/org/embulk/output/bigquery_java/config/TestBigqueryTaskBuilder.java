package org.embulk.output.bigquery_java.config;

import static org.embulk.output.bigquery_java.util.AssertUtil.assertMatches;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
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
  public void build_expandsTableInSystemDefaultTimeZone() {
    // README promises the embulk server's local time, so switch the JVM default away from UTC and
    // check the hour follows it. The expected value is sampled before and after build() so an
    // hour rolling over between the two can't fail the test.
    // CAUTION: TimeZone.setDefault is JVM-global. Other tests reading ZoneId.systemDefault() would
    // see Asia/Tokyo while this runs, so this test relies on the suite running sequentially within
    // a JVM (Gradle's default for JUnit 4). Revisit before enabling in-JVM parallel execution.
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
      DateTimeFormatter expected =
          DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.of("Asia/Tokyo"));
      config = loadYamlResource(embulk, "base.yml").set("table", "table_%Y%m%d%H");

      String before = "table_" + expected.format(Instant.now());
      PluginTask task = BigqueryTaskBuilder.build(CONFIG_MAPPER.map(config, PluginTask.class));
      String after = "table_" + expected.format(Instant.now());

      assertTrue(task.getTable(), task.getTable().equals(before) || task.getTable().equals(after));
    } finally {
      TimeZone.setDefault(original);
    }
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

  // The task for base.yml in `mode`, with temp_table configured to `tempTable` unless null, after
  // BigqueryTaskBuilder.setTempTable().
  private PluginTask setTempTable(String mode, String tempTable) {
    config = loadYamlResource(embulk, "base.yml").set("mode", mode);
    if (tempTable != null) {
      config = config.set("temp_table", tempTable);
    }
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryTaskBuilder.setTempTable(task);
    return task;
  }

  @Test
  public void setTempTable_assignsDefaultForReplace() {
    assertMatches(setTempTable("replace", null).getTempTable().get(), "LOAD_TEMP_.*_table");
  }

  @Test
  public void setTempTable_assignsDefaultForReplaceBackup() {
    assertMatches(setTempTable("replace_backup", null).getTempTable().get(), "LOAD_TEMP_.*_table");
  }

  @Test
  public void setTempTable_keepsConfiguredValueForReplace() {
    assertEquals("foo", setTempTable("replace", "foo").getTempTable().get());
  }

  @Test
  public void setTempTable_clearsConfiguredValueForDeleteInAdvance() {
    // Ruby sets temp_table to nil for every mode other than replace/append/merge/replace_backup.
    assertFalse(setTempTable("delete_in_advance", "foo").getTempTable().isPresent());
  }

  @Test
  public void setTempTable_clearsConfiguredValueForAppendDirect() {
    assertFalse(setTempTable("append_direct", "foo").getTempTable().isPresent());
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
