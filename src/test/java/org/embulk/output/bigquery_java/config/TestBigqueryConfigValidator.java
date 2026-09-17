package org.embulk.output.bigquery_java.config;

import static org.junit.Assert.*;

import java.util.Optional;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;

public class TestBigqueryConfigValidator {
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
  public void validateMode() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryConfigValidator.validateMode(task);

    assertEquals("replace", task.getMode());
  }

  @Test(expected = ConfigException.class)
  public void validateMode_invalid_configException() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setMode("foo");
    BigqueryConfigValidator.validateMode(task);
  }

  @Test
  public void validateModeAndAutoCreteTable() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryConfigValidator.validateModeAndAutoCreteTable(task);

    assertEquals("replace", task.getMode());
    assertTrue(task.getAutoCreateTable());
  }

  @Test(expected = ConfigException.class)
  public void validateModeAndAutoCreteTable_autoCreateTable_False_configException() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setAutoCreateTable(false);
    BigqueryConfigValidator.validateModeAndAutoCreteTable(task);
  }

  @Test
  public void validateSourceFormat() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setSourceFormat("csv");
    BigqueryConfigValidator.validateSourceFormat(task);

    assertEquals("CSV", task.getSourceFormat());
  }

  @Test
  public void validateSourceFormat_jsonlAlias_resolvesToNewlineDelimitedJson() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setSourceFormat("jsonl");
    BigqueryConfigValidator.validateSourceFormat(task);

    assertEquals("NEWLINE_DELIMITED_JSON", task.getSourceFormat());
  }

  @Test(expected = ConfigException.class)
  public void validateSourceFormat_invalid_configException() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setSourceFormat("foobar");
    BigqueryConfigValidator.validateSourceFormat(task);
  }

  @Test
  public void validateCompression() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setCompression("gzip");
    BigqueryConfigValidator.validateCompression(task);

    assertEquals("GZIP", task.getCompression());
  }

  @Test(expected = ConfigException.class)
  public void validateCompression_invalid_configException() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setCompression("foobar");
    BigqueryConfigValidator.validateCompression(task);
  }

  private BigqueryTimePartitioning buildTimePartitioning(String type) {
    ConfigSource timePartitioning = embulk.newConfig().set("type", type);
    return CONFIG_MAPPER.map(timePartitioning, BigqueryTimePartitioning.class);
  }

  @Test
  public void validateTimePartitioning() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setTimePartitioning(Optional.of(buildTimePartitioning("DAY")));
    BigqueryConfigValidator.validateTimePartitioning(task);
  }

  @Test(expected = ConfigException.class)
  public void validateTimePartitioning_invalidType_configException() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setTimePartitioning(Optional.of(buildTimePartitioning("WEEK")));
    BigqueryConfigValidator.validateTimePartitioning(task);
  }

  @Test(expected = ConfigException.class)
  public void validate_invalidTimePartitioning_configException() {
    config = loadYamlResource(embulk, "base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setTimePartitioning(Optional.of(buildTimePartitioning("WEEK")));
    BigqueryConfigValidator.validate(task);
  }
}
