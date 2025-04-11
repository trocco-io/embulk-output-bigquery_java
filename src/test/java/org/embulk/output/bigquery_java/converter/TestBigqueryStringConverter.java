package org.embulk.output.bigquery_java.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.output.bigquery_java.exception.BigqueryTypeCastException;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;

public class TestBigqueryStringConverter {
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
  public void testConvertStringToString() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "STRING");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "data", BigqueryColumnOptionType.STRING, columnOption);

    assertEquals("data", node.get("key").asText());
  }

  @Test
  public void testConvertStringToInt() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "INTEGER");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "1", BigqueryColumnOptionType.INTEGER, columnOption);

    assertEquals(1, node.get("key").asInt());
  }

  @Test
  public void testConvertStringToFloat() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "FLOAT");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "1.0", BigqueryColumnOptionType.FLOAT, columnOption);

    assertEquals(1.0, node.get("key").asDouble(), 0);
  }

  @Test
  public void testConvertStringToDate() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "DATE");
    configSource.set("name", "key");
    configSource.set("timestamp_format", "%Y/%m/%d");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020/05/01", BigqueryColumnOptionType.DATE, columnOption);
    assertEquals("2020-05-01", node.get("key").asText());

    assertThrows(
        BigqueryTypeCastException.class,
        () -> {
          BigqueryStringConverter.convertAndSet(
              node, "key", "20200501", BigqueryColumnOptionType.DATE, columnOption);
        });
  }

  @Test
  public void testConvertStringToDate_withoutTimeFormat() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "DATE");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020-05-01", BigqueryColumnOptionType.DATE, columnOption);
    assertEquals("2020-05-01", node.get("key").asText());

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020/05/01", BigqueryColumnOptionType.DATE, columnOption);
    assertEquals("2020/05/01", node.get("key").asText());
  }

  @Test
  public void testConvertStringToDatetime() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "DATETIME");
    configSource.set("name", "key");
    configSource.set("timestamp_format", "%Y/%m/%d %H:%M:%S");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020/05/01 00:00:00", BigqueryColumnOptionType.DATETIME, columnOption);

    assertEquals("2020-05-01 00:00:00.000000", node.get("key").asText());
  }

  @Test
  public void testConvertStringToDatetime_withoutTimeFormat() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "DATETIME");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020-05-01 00:00:00.000000", BigqueryColumnOptionType.DATETIME, columnOption);
    assertEquals("2020-05-01 00:00:00.000000", node.get("key").asText());

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020/05/01 00:00:00.000000", BigqueryColumnOptionType.DATETIME, columnOption);
    assertEquals("2020/05/01 00:00:00.000000", node.get("key").asText());
  }

  @Test
  public void testConvertStringToTimestamp() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "TIMESTAMP");
    configSource.set("name", "key");
    configSource.set("timestamp_format", "%Y/%m/%d %H:%M:%S");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node, "key", "2020/05/01 00:00:00", BigqueryColumnOptionType.TIMESTAMP, columnOption);

    assertEquals("2020-05-01 00:00:00.000000 +00:00", node.get("key").asText());
  }

  @Test
  public void testConvertStringToTimestamp_withoutTimeFormat() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "TIMESTAMP");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryStringConverter.convertAndSet(
        node,
        "key",
        "2020-05-01 00:00:00.000000 +09:00",
        BigqueryColumnOptionType.TIMESTAMP,
        columnOption);
    assertEquals("2020-05-01 00:00:00.000000 +09:00", node.get("key").asText());

    BigqueryStringConverter.convertAndSet(
        node,
        "key",
        "2020/05/01 00:00:00.000000 +09:00",
        BigqueryColumnOptionType.TIMESTAMP,
        columnOption);
    assertEquals("2020/05/01 00:00:00.000000 +09:00", node.get("key").asText());
  }

  @Test
  public void testConvertStringToNumeric() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "NUMERIC");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryStringConverter.convertAndSet(
        node, "key", "123.456", BigqueryColumnOptionType.NUMERIC, columnOption);

    assertTrue(node.get("key").isBigDecimal());
    assertEquals(123.456, node.get("key").asDouble(), 0);
  }
}
