package org.embulk.output.bigquery_java;

import java.util.Collections;
import java.util.function.Function;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Rule;
import org.junit.Test;

public class AuthTest {
  protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .registerPlugin(OutputPlugin.class, "bigquery_java", BigqueryJavaOutputPlugin.class)
          .build();

  private PluginTask buildTask(Function<ConfigSource, ConfigSource> setupConfig) {
    ConfigSource config =
        embulk
            .newConfig()
            .set("type", "bigquery_java")
            .set("dataset", "dataset")
            .set("table", "table")
            .set("source_format", "NEWLINE_DELIMITED_JSON");
    return CONFIG_MAPPER.map(setupConfig.apply(config), PluginTask.class);
  }

  @Test(expected = ConfigException.class)
  public void testUnknownAuthMethod_throwsConfigException() throws Exception {
    PluginTask task = buildTask(c -> c.set("auth_method", "foobar"));
    new Auth(task).getCredentials();
  }

  @Test(expected = ConfigException.class)
  public void testServiceAccount_missingJsonKeyfile_throwsConfigException() throws Exception {
    PluginTask task = buildTask(c -> c.set("auth_method", "service_account"));
    new Auth(task).getCredentials();
  }

  @Test(expected = ConfigException.class)
  public void testWorkloadIdentityFederation_missingConfigBlock_throwsConfigException()
      throws Exception {
    PluginTask task = buildTask(c -> c.set("auth_method", "workload_identity_federation"));
    new Auth(task).getCredentials();
  }

  @Test(expected = ConfigException.class)
  public void testWorkloadIdentityFederation_missingAwsRoleArn_throwsConfigException() {
    buildTask(
        c ->
            c.set("auth_method", "workload_identity_federation")
                .set(
                    "workload_identity_federation",
                    Collections.singletonMap("config", Collections.singletonMap("content", "{}"))));
  }

  @Test(expected = ConfigException.class)
  public void testWorkloadIdentityFederation_missingConfig_throwsConfigException() {
    buildTask(
        c ->
            c.set("auth_method", "workload_identity_federation")
                .set(
                    "workload_identity_federation",
                    Collections.singletonMap(
                        "aws_role_arn", "arn:aws:iam::123456789012:role/test")));
  }
}
