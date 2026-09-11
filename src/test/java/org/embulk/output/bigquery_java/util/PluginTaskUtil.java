package org.embulk.output.bigquery_java.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;

// Shared PluginTask-building helpers for isNeedUpdateTable()/buildPatchSchema()-related tests.
public final class PluginTaskUtil {
  private PluginTaskUtil() {}

  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private static ConfigSource loadTakeoverResource(TestingEmbulk embulk) {
    return embulk.loadYamlResource(
        "/java/org/embulk/output/bigquery_java/bigquery_client/takeover.yml");
  }

  public static Map<String, Object> columnOption(
      String name, String type, String mode, String description, Object fields) {
    Map<String, Object> option = new HashMap<>();
    option.put("name", name);
    option.put("type", type);
    option.put("mode", mode);
    if (description != null) {
      option.put("description", description);
    }
    if (fields != null) {
      option.put("fields", fields);
    }
    return option;
  }

  public static PluginTask buildTaskWithIntegerColumnOptionForReplaceMode(
      TestingEmbulk embulk,
      String columnOptionDescription,
      boolean retainPolicyTags,
      boolean retainDescriptions) {
    return buildTaskWithIntegerColumnOption(
        embulk, "replace", columnOptionDescription, retainPolicyTags, retainDescriptions);
  }

  public static PluginTask buildTaskWithIntegerColumnOptionForMode(
      TestingEmbulk embulk, String mode, String columnOptionDescription) {
    return buildTaskWithIntegerColumnOption(embulk, mode, columnOptionDescription, null, null);
  }

  private static PluginTask buildTaskWithIntegerColumnOption(
      TestingEmbulk embulk,
      String mode,
      String columnOptionDescription,
      Boolean retainPolicyTags,
      Boolean retainDescriptions) {
    ConfigSource baseConfig = loadTakeoverResource(embulk);
    ConfigSource configSource =
        baseConfig
            .set("mode", mode)
            .set(
                "column_options",
                Collections.singletonList(
                    columnOption("c0", "INTEGER", "NULLABLE", columnOptionDescription, null)));
    if (retainPolicyTags != null) {
      configSource = configSource.set("retain_column_policy_tags", retainPolicyTags);
    }
    if (retainDescriptions != null) {
      configSource = configSource.set("retain_column_descriptions", retainDescriptions);
    }
    return CONFIG_MAPPER.map(configSource, PluginTask.class);
  }

  public static PluginTask buildTaskRecordNestedColumnOption(
      TestingEmbulk embulk,
      String mode,
      Map<String, String> nestedColumnOptionDescription,
      boolean retainPolicyTags,
      boolean retainDescriptions) {
    ConfigSource baseConfig = loadTakeoverResource(embulk);
    return CONFIG_MAPPER.map(
        baseConfig
            .set("mode", mode)
            .set(
                "column_options",
                Arrays.asList(
                    columnOption(
                        "c0",
                        "RECORD",
                        "NULLABLE",
                        nestedColumnOptionDescription.get("c0"),
                        Arrays.asList(
                            columnOption(
                                "c00",
                                "RECORD",
                                "NULLABLE",
                                nestedColumnOptionDescription.get("c00"),
                                Collections.singletonList(
                                    columnOption(
                                        "c000",
                                        "STRING",
                                        "NULLABLE",
                                        nestedColumnOptionDescription.get("c000"),
                                        null))),
                            columnOption(
                                "c01",
                                "STRING",
                                "NULLABLE",
                                nestedColumnOptionDescription.get("c01"),
                                null))),
                    columnOption(
                        "c1", "STRING", "NULLABLE", nestedColumnOptionDescription.get("c1"), null)))
            .set("retain_column_policy_tags", retainPolicyTags)
            .set("retain_column_descriptions", retainDescriptions),
        PluginTask.class);
  }
}
