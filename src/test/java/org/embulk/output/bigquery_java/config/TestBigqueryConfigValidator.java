package org.embulk.output.bigquery_java.config;

import static org.embulk.output.bigquery_java.config.BigqueryConfigValidator.validateRetrySettings;
import static org.embulk.output.bigquery_java.config.BigqueryConfigValidator.validateTimeouts;
import static org.junit.Assert.*;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.junit.Test;

public class TestBigqueryConfigValidator extends BigqueryConfigTestBase {
  @Test
  public void validateMode() {
    ConfigSource config = loadYamlResource("base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryConfigValidator.validateMode(task);

    assertEquals("replace", task.getMode());
  }

  @Test(expected = ConfigException.class)
  public void validateMode_invalid_configException() {
    ConfigSource config = loadYamlResource("base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setMode("foo");
    BigqueryConfigValidator.validateMode(task);
  }

  @Test(expected = ConfigException.class)
  public void validateMode_replaceBackup_configException() {
    // Known to Mode (so setTempTable and the auto_create_table check already follow ruby), but
    // not implemented yet.
    BigqueryConfigValidator.validateMode(taskWith(c -> c.set("mode", "replace_backup")));
  }

  @Test
  public void validate_lowerCasesMode() {
    // Ruby does task['mode'].downcase in configure, so `mode: Replace` is accepted.
    PluginTask task = taskWith(c -> c.set("mode", "Replace"));
    BigqueryConfigValidator.validate(task);
    assertEquals("replace", task.getMode());
  }

  @Test(expected = ConfigException.class)
  public void validateMode_isCaseSensitiveWithoutValidate() {
    // The lower-casing happens once in validate(); the individual checks expect canonical input.
    BigqueryConfigValidator.validateMode(taskWith(c -> c.set("mode", "Replace")));
  }

  @Test
  public void validateModeAndAutoCreteTable() {
    ConfigSource config = loadYamlResource("base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    BigqueryConfigValidator.validateModeAndAutoCreteTable(task);

    assertEquals("replace", task.getMode());
    assertTrue(task.getAutoCreateTable());
  }

  @Test
  public void validateModeAndAutoCreteTable_autoCreateTable_False_configException() {
    ConfigSource config = loadYamlResource("base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setAutoCreateTable(false);
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () -> BigqueryConfigValidator.validateModeAndAutoCreteTable(task));
    assertEquals("`mode: replace` requires `auto_create_table: true`", e.getMessage());
  }

  @Test
  public void validateModeAndAutoCreteTable_replaceBackup_autoCreateTable_False_configException() {
    ConfigSource config = loadYamlResource("base.yml");
    PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
    task.setMode("replace_backup");
    task.setAutoCreateTable(false);
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () -> BigqueryConfigValidator.validateModeAndAutoCreteTable(task));
    assertEquals("`mode: replace_backup` requires `auto_create_table: true`", e.getMessage());
  }

  @Test
  public void validateRetrySettings_defaultsAreValid() {
    validateRetrySettings(taskWith(c -> c));
  }

  @Test
  public void validateRetrySettings_negativeRetries_configException() {
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () -> validateRetrySettings(taskWith(c -> c.set("retries", -1))));
    assertTrue(e.getMessage().contains("`retries`"));
  }

  @Test(expected = ConfigException.class)
  public void validateRetrySettings_maxIntRetries_configException() {
    validateRetrySettings(taskWith(c -> c.set("retries", Integer.MAX_VALUE)));
  }

  @Test(expected = ConfigException.class)
  public void validateRetrySettings_multiplierBelowOne_configException() {
    validateRetrySettings(taskWith(c -> c.set("retry_delay_multiplier", 0.5)));
  }

  @Test(expected = ConfigException.class)
  public void validateRetrySettings_multiplierNaN_configException() {
    validateRetrySettings(taskWith(c -> c.set("retry_delay_multiplier", Double.NaN)));
  }

  @Test(expected = ConfigException.class)
  public void validateRetrySettings_maxDelayShorterThanInitial_configException() {
    validateRetrySettings(
        taskWith(c -> c.set("retry_initial_delay_sec", 10).set("retry_max_delay_sec", 5)));
  }

  @Test
  public void validateRetrySettings_jobRetryWaitOverflow_configException() {
    // 2147484 = BigqueryConfigValidator.MAX_TIMEOUT_SEC + 1 (Integer.MAX_VALUE / 1000 + 1)
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () -> validateRetrySettings(taskWith(c -> c.set("job_retry_max_wait_sec", 2147484))));
    assertTrue(e.getMessage().contains("`job_retry_max_wait_sec`"));
  }

  @Test
  public void validateTimeouts_defaultsAreValid() {
    validateTimeouts(taskWith(c -> c));
  }

  @Test
  public void validateTimeouts_negativeOpenTimeout_configException() {
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () -> validateTimeouts(taskWith(c -> c.set("open_timeout_sec", -1))));
    assertTrue(e.getMessage().contains("`open_timeout_sec`"));
  }

  @Test
  public void validateTimeouts_readPlusSendOverflow_configException() {
    // Each value fits on its own; only the folded sum exceeds Integer.MAX_VALUE milliseconds.
    // 2147483 = BigqueryConfigValidator.MAX_TIMEOUT_SEC (Integer.MAX_VALUE / 1000)
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () ->
                validateTimeouts(
                    taskWith(c -> c.set("read_timeout_sec", 2147483).set("send_timeout_sec", 1))));
    assertTrue(e.getMessage().contains("read_timeout_sec + send_timeout_sec"));
  }

  @Test
  public void validateTimeouts_readPlusSendOverflowViaDeprecatedTimeoutSec_configException() {
    // The folded sum is checked on the resolved read timeout, so the deprecated alias counts too.
    // 2147483 = BigqueryConfigValidator.MAX_TIMEOUT_SEC (Integer.MAX_VALUE / 1000)
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () ->
                validateTimeouts(
                    taskWith(c -> c.set("timeout_sec", 2147483).set("send_timeout_sec", 1))));
    assertTrue(e.getMessage().contains("read_timeout_sec + send_timeout_sec"));
  }

  @Test
  public void validateTimeouts_acceptsMaxValues() {
    // 2147483 = BigqueryConfigValidator.MAX_TIMEOUT_SEC (Integer.MAX_VALUE / 1000)
    validateTimeouts(
        taskWith(
            c ->
                c.set("open_timeout_sec", 2147483)
                    .set("read_timeout_sec", 2147483)
                    .set("send_timeout_sec", 0)));
  }

  @Test
  public void validateTimeouts_deprecatedTimeoutSecIsAccepted() {
    validateTimeouts(taskWith(c -> c.set("timeout_sec", 34)));
  }

  @Test(expected = ConfigException.class)
  public void validate_runsRetryAndTimeoutChecks() {
    BigqueryConfigValidator.validate(taskWith(c -> c.set("retries", -1)));
  }
}
