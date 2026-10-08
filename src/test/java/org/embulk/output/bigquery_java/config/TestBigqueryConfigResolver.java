package org.embulk.output.bigquery_java.config;

import static org.embulk.output.bigquery_java.config.BigqueryConfigResolver.effectiveReadTimeoutSec;
import static org.embulk.output.bigquery_java.config.BigqueryConfigResolver.resolveReadTimeoutSec;
import static org.junit.Assert.assertEquals;

import java.util.function.Function;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;

public class TestBigqueryConfigResolver {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  private static final String BASIC_RESOURCE_PATH = "/java/org/embulk/output/bigquery_java/";

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .build();

  private PluginTask taskWith(Function<ConfigSource, ConfigSource> setup) {
    ConfigSource config = embulk.loadYamlResource(BASIC_RESOURCE_PATH + "base.yml");
    return CONFIG_MAPPER.map(setup.apply(config), PluginTask.class);
  }

  @Test
  public void resolveReadTimeoutSec_prefersReadTimeoutSec() {
    assertEquals(
        34,
        resolveReadTimeoutSec(taskWith(c -> c.set("read_timeout_sec", 34).set("timeout_sec", 99))));
  }

  @Test
  public void resolveReadTimeoutSec_fallsBackToDeprecatedTimeoutSec() {
    assertEquals(99, resolveReadTimeoutSec(taskWith(c -> c.set("timeout_sec", 99))));
  }

  @Test
  public void resolveReadTimeoutSec_defaultsTo300WhenNeitherIsSet() {
    // 300 = BigqueryConfigResolver.DEFAULT_READ_TIMEOUT_SEC
    assertEquals(300, resolveReadTimeoutSec(taskWith(c -> c)));
  }

  @Test
  public void effectiveReadTimeoutSec_addsSendTimeoutSec() {
    assertEquals(
        34 + 56,
        effectiveReadTimeoutSec(
            taskWith(c -> c.set("read_timeout_sec", 34).set("send_timeout_sec", 56))));
  }

  @Test
  public void effectiveReadTimeoutSec_doesNotOverflowInt() {
    // Both values fit in int seconds on their own; the sum must be computed as long.
    assertEquals(
        (long) Integer.MAX_VALUE + 1,
        effectiveReadTimeoutSec(
            taskWith(
                c -> c.set("read_timeout_sec", Integer.MAX_VALUE).set("send_timeout_sec", 1))));
  }
}
