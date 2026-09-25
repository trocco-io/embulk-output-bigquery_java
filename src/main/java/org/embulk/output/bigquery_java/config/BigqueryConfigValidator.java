package org.embulk.output.bigquery_java.config;

import java.util.Arrays;
import org.embulk.config.ConfigException;

public class BigqueryConfigValidator {
  public static void validate(PluginTask task) {
    validateMode(task);
    validateModeAndAutoCreteTable(task);
    validateSourceFormat(task);
    validateCompression(task);
    validateTimePartitioning(task);
    validateClustering(task);
  }

  public static void validateMode(PluginTask task) throws ConfigException {
    // TODO: replace_backup
    String[] modes = {"replace", "append", "merge", "delete_in_advance", "append_direct"};
    if (!Arrays.asList(modes).contains(task.getMode().toLowerCase())) {
      throw new ConfigException(
          "replace, append, merge, delete_in_advance and append_direct are supported. Stay tuned!");
    }
  }

  public static void validateModeAndAutoCreteTable(PluginTask task) throws ConfigException {
    // TODO: replace_backup and !task['auto_create_table']
    String[] modes = {"replace", "append", "merge", "delete_in_advance"};
    if (Arrays.asList(modes).contains(task.getMode().toLowerCase()) && !task.getAutoCreateTable()) {
      throw new ConfigException(
          "replace, append, merge and delete_in_advance require `auto_create_table: true`.");
    }
  }

  public static void validateSourceFormat(PluginTask task) throws ConfigException {
    String sourceFormat = task.getSourceFormat().toUpperCase();
    if (sourceFormat.equals("JSONL")) {
      sourceFormat = "NEWLINE_DELIMITED_JSON";
    }
    String[] formats = {"CSV", "NEWLINE_DELIMITED_JSON"};
    if (!Arrays.asList(formats).contains(sourceFormat)) {
      throw new ConfigException("`source_format` must be CSV or NEWLINE_DELIMITED_JSON (JSONL)");
    }
    task.setSourceFormat(sourceFormat);
  }

  public static void validateCompression(PluginTask task) throws ConfigException {
    String compression = task.getCompression().toUpperCase();
    String[] compressions = {"GZIP", "NONE"};
    if (!Arrays.asList(compressions).contains(compression)) {
      throw new ConfigException("`compression` must be GZIP or NONE");
    }
    task.setCompression(compression);
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
