package org.embulk.output.bigquery_java.config;

import java.util.function.Function;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;

// Shared fixture for the config-package tests: an embulk instance with the plugin registered, the
// ConfigMapper, and helpers that build a PluginTask from base.yml plus per-test overrides.
public abstract class BigqueryConfigTestBase {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  protected static final String BASIC_RESOURCE_PATH = "/java/org/embulk/output/bigquery_java/";

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .build();

  protected ConfigSource loadYamlResource(String fileName) {
    return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
  }

  protected PluginTask taskWith(Function<ConfigSource, ConfigSource> setup) {
    return CONFIG_MAPPER.map(setup.apply(loadYamlResource("base.yml")), PluginTask.class);
  }
}
