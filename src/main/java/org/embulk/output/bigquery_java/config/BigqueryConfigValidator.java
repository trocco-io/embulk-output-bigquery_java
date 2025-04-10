package org.embulk.output.bigquery_java.config;

import java.util.Arrays;
import org.embulk.config.ConfigException;

public class BigqueryConfigValidator {
  public static void validate(PluginTask task) {
    validateMode(task);
    validateModeAndAutoCreteTable(task);
    validateClustering(task);
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
