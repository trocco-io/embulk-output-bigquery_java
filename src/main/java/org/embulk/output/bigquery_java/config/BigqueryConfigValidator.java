package org.embulk.output.bigquery_java.config;

import java.util.Arrays;
import org.embulk.config.ConfigException;
import org.embulk.output.bigquery_java.BigqueryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigqueryConfigValidator {
  private static final Logger logger = LoggerFactory.getLogger(BigqueryConfigValidator.class);

  // Every *_sec value ends up as int milliseconds in the Google client or RetryExecutor.
  public static final long MAX_TIMEOUT_SEC = Integer.MAX_VALUE / 1000;

  public static void validate(PluginTask task) {
    validateMode(task);
    validateModeAndAutoCreteTable(task);
    validateClustering(task);
    validateRetrySettings(task);
    validateTimeouts(task);
  }

  public static void validateRetrySettings(PluginTask task) throws ConfigException {
    // gax takes maxAttempts = retries + 1, so Integer.MAX_VALUE would overflow, and a negative
    // value would mean "unlimited" to gax but "give up at once" to RetryExecutor.
    if (task.getRetries() < 0 || task.getRetries() == Integer.MAX_VALUE) {
      throw new ConfigException(
          String.format("`retries` must be between 0 and %d", Integer.MAX_VALUE - 1));
    }
    requireSecondsFitInIntMillis("retry_initial_delay_sec", task.getRetryInitialDelaySec());
    requireSecondsFitInIntMillis("retry_max_delay_sec", task.getRetryMaxDelaySec());
    requireSecondsFitInIntMillis("retry_total_timeout_sec", task.getRetryTotalTimeoutSec());
    if (task.getRetryMaxDelaySec() < task.getRetryInitialDelaySec()) {
      throw new ConfigException(
          "`retry_max_delay_sec` must not be shorter than `retry_initial_delay_sec`");
    }
    if (task.getRetryDelayMultiplier() < 1.0) {
      throw new ConfigException("`retry_delay_multiplier` must be at least 1.0");
    }
    requireSecondsFitInIntMillis("job_retry_initial_wait_sec", task.getJobRetryInitialWaitSec());
    requireSecondsFitInIntMillis("job_retry_max_wait_sec", task.getJobRetryMaxWaitSec());
  }

  public static void validateTimeouts(PluginTask task) throws ConfigException {
    requireSecondsFitInIntMillis("open_timeout_sec", task.getOpenTimeoutSec());
    requireSecondsFitInIntMillis("send_timeout_sec", task.getSendTimeoutSec());
    task.getReadTimeoutSec().ifPresent(v -> requireSecondsFitInIntMillis("read_timeout_sec", v));
    task.getTimeoutSec()
        .ifPresent(
            v -> {
              logger.warn(
                  "embulk-output-bigquery: timeout_sec is deprecated. Use read_timeout_sec instead");
              requireSecondsFitInIntMillis("timeout_sec", v);
            });
    // The folded value is what the transport actually receives, so it has to fit too.
    requireSecondsFitInIntMillis(
        "read_timeout_sec + send_timeout_sec", BigqueryClient.effectiveReadTimeoutSec(task));
  }

  // HttpTransportOptions silently treats a negative timeout as "unset" (library default 20s) and
  // RetryExecutor sleeps on its waits unchecked, so negative or int-overflowing values are
  // rejected here instead.
  private static void requireSecondsFitInIntMillis(String name, long seconds)
      throws ConfigException {
    if (seconds < 0 || seconds > MAX_TIMEOUT_SEC) {
      throw new ConfigException(
          String.format(
              "`%s` must be between 0 and %d seconds, got %d", name, MAX_TIMEOUT_SEC, seconds));
    }
  }

  public static void validateMode(PluginTask task) throws ConfigException {
    // TODO: append_direct delete_in_advance replace_backup
    String[] modes = {"replace", "append", "merge", "delete_in_advance", "append_direct"};
    if (!Arrays.asList(modes).contains(task.getMode().toLowerCase())) {
      throw new ConfigException(
          "replace, append, merge, delete_in_advance and append_direct are supported. Stay tuned!");
    }
  }

  public static void validateModeAndAutoCreteTable(PluginTask task) throws ConfigException {
    // TODO: modes are append replace delete_in_advance replace_backup and
    // !task['auto_create_table']
    String[] modes = {"replace", "append", "merge", "delete_in_advance"};
    if (Arrays.asList(modes).contains(task.getMode().toLowerCase()) && !task.getAutoCreateTable()) {
      throw new ConfigException(
          "replace, append, merge and delete_in_advance are supported. Stay tuned!");
    }
  }

  public static void validateTimePartitioning(PluginTask task) throws ConfigException {
    if (task.getTimePartitioning().isPresent()) {
      String[] types = {"HOUR", "DAY", "MONTH", "YEAR"};
      if (!Arrays.asList(types)
          .contains(task.getTimePartitioning().get().getType().toUpperCase())) {
        throw new ConfigException(
            "time_partitioning.type: HOUR, DAY, MONTH and YEAR are supported.");
      }
    }
  }

  public static void validateClustering(PluginTask task) throws ConfigException {
    if (task.getClustering().isPresent()) {
      if (!task.getClustering().get().getFields().isPresent()) {
        throw new ConfigException("`clustering` must have `fields` key");
      }
    }
  }
}
