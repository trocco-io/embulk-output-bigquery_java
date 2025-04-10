package org.embulk.output.bigquery_java.converter;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;

public class TestBigqueryDoubleConverter {
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
  public void testConvertDoubleToString() {
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

    BigqueryDoubleConverter.convertAndSet(node, "key", 1.0, BigqueryColumnOptionType.STRING);

    assertEquals(node.get("key").asText(), "1.0");
  }

  @Test
  public void testConvertDoubleToInteger() {
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

    BigqueryDoubleConverter.convertAndSet(node, "key", 1.0, BigqueryColumnOptionType.FLOAT);

    assertEquals(node.get("key").asInt(), 1, 0);
  }

  @Test
  public void testConvertDoubleToDouble() {
    ObjectNode node = BigqueryUtil.getObjectMapper().createObjectNode();
    config = loadYamlResource(embulk, "base.yml");
    List<ConfigSource> configSources = new ArrayList<>();
    ConfigSource configSource = embulk.newConfig();
    configSource.set("type", "DOUBLE");
    configSource.set("name", "key");
    configSources.add(configSource);
    config.set("column_options", configSources);
    BigqueryColumnOption columnOption = CONFIG_MAPPER.map(configSource, BigqueryColumnOption.class);
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

    BigqueryDoubleConverter.convertAndSet(node, "key", 1.0, BigqueryColumnOptionType.FLOAT);

    assertEquals(node.get("key").asDouble(), 1.0, 0);
  }

  @Test
  public void testConvertDoubleToTimestamp() {
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

    BigqueryDoubleConverter.convertAndSet(node, "key", 1.0, BigqueryColumnOptionType.TIMESTAMP);

    assertEquals(node.get("key").asDouble(), 1.0, 0);
  }
}
